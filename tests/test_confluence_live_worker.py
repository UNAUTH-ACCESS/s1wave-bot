"""
tests/test_confluence_live_worker.py
=======================================
Real-money executor for confluence_entry_v1 (2026-09-23).

This is the one worker in the codebase that can move real funds, so
coverage here leans hardest on the things that must NEVER fail silently:
  1. The CONFLUENCE_LIVE_ENABLED kill switch actually prevents any entry
     when False — nothing in this worker acts without it.
  2. Concurrency cap (CONFLUENCE_LIVE_MAX_CONCURRENT) is enforced — a
     second signal never opens a second position while one is open.
  3. The all-time-realized-loss backstop halts entries PERMANENTLY once
     realized losses reach the user's stated total capital ($10) — not
     just for the day.
  4. The daily-loss circuit breaker halts entries for the rest of the UTC
     day on a smaller drawdown, independent of #3.
  5. A failed buy never creates an 'open' position (records 'buy_failed'
     instead) and is never counted against the concurrency cap.
  6. A failed sell leaves the trade 'open' (for retry next cycle) rather
     than marking it 'closed' with fabricated exit fields.
  7. Exit priority matches engine/risk.py exactly (same assertions as
     the shadow worker's suite, since this worker duplicates that exact
     logic on purpose — see module docstring in confluence_live_worker.py).
  8. This worker never touches the real `trades` table or Token status.
  9. In-app notifications (2026-09-24) fire at the right moments with the
     right severity, AND — just as important — halt-state notifications
     are edge-triggered: _safe_to_enter() runs every ~1s cycle and must
     notify once per state CHANGE, never once per check, or a sustained
     halt would flood the feed with one row per second forever.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from config.settings import settings
from engine.execution import ExecutionResult
from models.orm import (
    CircuitBreakerState, ConfluenceLiveObservation, ConfluenceLiveTrade, ConfluenceNotification,
    MomentumSignalEvent, Token, TokenEvaluation, TokenStatus, Trade,
)
from workers.confluence_live_worker import ConfluenceLiveWorker


def make_token(**overrides) -> Token:
    now = datetime.now(timezone.utc)
    defaults = dict(
        mint_address=f"Mint{uuid.uuid4().hex[:36]}",
        symbol="TEST",
        status=TokenStatus.WATCHING,
        discovered_at=now - timedelta(minutes=10),
    )
    defaults.update(overrides)
    return Token(**defaults)


def make_signal(token: Token, n_rules_cofiring=2, **overrides) -> MomentumSignalEvent:
    defaults = dict(
        token_id=token.id,
        experiment_version="momentum_confluence_v1",
        triggered_at=datetime.now(timezone.utc),
        trigger_price=Decimal("0.001"),
        trailing_return_3min=Decimal("0.06"),
        n_rules_cofiring=n_rules_cofiring,
    )
    defaults.update(overrides)
    return MomentumSignalEvent(**defaults)


def make_worker(session, enabled=True) -> ConfluenceLiveWorker:
    with patch("workers.confluence_live_worker.ExecutionEngine"):
        worker = ConfluenceLiveWorker(asyncio.Event())
    # Default: no liquidity crisis detected, so tests that don't care about
    # the guard (added 2026-09-24, see TestLiquidityGuard) fall through to
    # the normal snapshot-based _check_exit() path exactly as before it
    # existed. Tests that DO care override this explicitly.
    worker._execution.get_sell_quote = AsyncMock(return_value=None)
    # Default: no real balance data, so the balance-zero reconciliation
    # check (2026-09-25, see the SEND incident) never fires for tests that
    # don't care about it — None means "unknown," never treated as zero.
    worker._execution.get_token_balance_raw = AsyncMock(return_value=None)
    return worker


def patched_session(session):
    """
    Patches get_session everywhere the worker's real request path touches
    it — including engine/halt_override.py (2026-09-28), which imports its
    own get_session reference and is NOT covered by patching the worker
    module's reference alone (confirmed the hard way: every halt/entry
    test in this file started making real postgres connection attempts
    the moment _safe_to_enter()/_refresh_halt_notifications() started
    calling get_halt_override()).
    """
    ctx = patch("workers.confluence_live_worker.get_session")
    mock_gs = ctx.start()
    mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
    mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)

    ctx2 = patch("engine.halt_override.get_session")
    mock_gs2 = ctx2.start()
    mock_gs2.return_value.__aenter__ = AsyncMock(return_value=session)
    mock_gs2.return_value.__aexit__ = AsyncMock(return_value=False)

    class _CombinedCtx:
        def stop(self):
            ctx.stop()
            ctx2.stop()

    return _CombinedCtx()


def mock_wallet_balance(worker, monkeypatch, equity_usd, sol_price=150.0) -> None:
    """Live wallet balance mock (2026-09-24, replacing the old
    CONFLUENCE_LIVE_MAX_TOTAL_CAPITAL_USD monkeypatch — equity is now the
    real on-chain balance, see engine/live_equity.py). worker._execution is
    already a MagicMock (make_worker patches the ExecutionEngine class), so
    get_wallet_balance_sol just needs an AsyncMock return value that, at
    sol_price, is worth equity_usd. Must be called AFTER make_worker()."""
    monkeypatch.setattr(settings, "SOL_PRICE_USD", sol_price)
    worker._execution.get_wallet_balance_sol = AsyncMock(
        return_value=Decimal(str(equity_usd)) / Decimal(str(sol_price))
    )


@pytest.mark.asyncio
async def test_disabled_kill_switch_prevents_any_entry(session, monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", False)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    session.add(make_signal(token))
    await session.flush()

    ctx = patched_session(session)
    try:
        # _maybe_enter itself has no ENABLED check inside it — the gate
        # lives in run()/_cycle(). Assert the cycle-level gate directly.
        assert settings.CONFLUENCE_LIVE_ENABLED is False
    finally:
        ctx.stop()


@pytest.mark.asyncio
async def test_qualifying_signal_opens_exactly_one_trade_on_buy_success(session, monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock(return_value=ExecutionResult(
        success=True, tx_signature="sig123", actual_price=Decimal("0.001"),
        actual_amount=Decimal("10000000"),
    ))
    session.add(make_signal(token, n_rules_cofiring=2))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    trades = result.scalars().all()
    assert len(trades) == 1
    assert trades[0].status == "open"
    assert trades[0].entry_tx_signature == "sig123"
    assert trades[0].entry_token_lamports == 10000000
    worker._execution.buy.assert_awaited_once()


@pytest.mark.asyncio
async def test_wash_trading_rejected_token_is_skipped_no_real_money_spent(session, monkeypatch):
    """Entry-quality filter (2026-09-24, workers/entry_filters.py) — a
    token whose most recent TIER1 evaluation before the signal was a
    WASH_TRADING rejection must never reach execution.buy(), and gets
    recorded as 'wash_skipped' so it's never reconsidered."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock()
    signal = make_signal(token, n_rules_cofiring=2)
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=False, reason_code="WASH_TRADING",
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={},
    ))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
        await worker._maybe_enter()  # must stay skipped, not retried
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    trades = result.scalars().all()
    assert len(trades) == 1
    assert trades[0].status == "wash_skipped"
    worker._execution.buy.assert_not_awaited()


@pytest.mark.asyncio
async def test_lp_not_burned_rejected_token_still_enters(session, monkeypatch):
    """LP_NOT_BURNED is a different rejection reason, confirmed safe by
    real data — must NOT be caught by the wash-trading filter."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock(return_value=ExecutionResult(
        success=True, tx_signature="sig456", actual_price=Decimal("0.001"),
        actual_amount=Decimal("10000000"),
    ))
    signal = make_signal(token, n_rules_cofiring=2)
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=False, reason_code="LP_NOT_BURNED",
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={},
    ))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    trades = result.scalars().all()
    assert len(trades) == 1
    assert trades[0].status == "open"
    worker._execution.buy.assert_awaited_once()


@pytest.mark.asyncio
async def test_high_liquidity_token_is_skipped_no_real_money_spent(session, monkeypatch):
    """Liquidity-ceiling filter (2026-09-24, workers/entry_filters.py) —
    a token whose most recent TIER1 evaluation reports liquidity >= $30k
    must never reach execution.buy(), and gets recorded as
    'high_liq_skip' so it's never reconsidered."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock()
    signal = make_signal(token, n_rules_cofiring=2)
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=True, reason_code=None,
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={"liquidity_usd": "75000"},
    ))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
        await worker._maybe_enter()  # must stay skipped, not retried
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    trades = result.scalars().all()
    assert len(trades) == 1
    assert trades[0].status == "high_liq_skip"
    worker._execution.buy.assert_not_awaited()


@pytest.mark.asyncio
async def test_low_liquidity_token_still_enters(session, monkeypatch):
    """A token below the liquidity ceiling must enter normally — the
    filter is a specific ceiling, not a blanket block on anything TIER1
    touched."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock(return_value=ExecutionResult(
        success=True, tx_signature="sig789", actual_price=Decimal("0.001"),
        actual_amount=Decimal("10000000"),
    ))
    signal = make_signal(token, n_rules_cofiring=2)
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=False, reason_code="LP_NOT_BURNED",
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={"liquidity_usd": "12000"},
    ))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    trades = result.scalars().all()
    assert len(trades) == 1
    assert trades[0].status == "open"
    worker._execution.buy.assert_awaited_once()


@pytest.mark.asyncio
async def test_low_buy_pressure_token_is_skipped_no_real_money_spent(session, monkeypatch):
    """Buy-pressure floor (2026-09-24, workers/entry_filters.py) — a
    signal with buy_pressure below 0.97 must never reach execution.buy(),
    even when it clears both other filters, and gets recorded as
    'low_bp_skip' so it's never reconsidered."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock()
    signal = make_signal(token, n_rules_cofiring=2, buy_pressure=Decimal("0.85"))
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=False, reason_code="LP_NOT_BURNED",
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={"liquidity_usd": "12000"},
    ))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
        await worker._maybe_enter()  # must stay skipped, not retried
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    trades = result.scalars().all()
    assert len(trades) == 1
    assert trades[0].status == "low_bp_skip"
    worker._execution.buy.assert_not_awaited()


@pytest.mark.asyncio
async def test_high_buy_pressure_token_still_enters(session, monkeypatch):
    """A signal at or above the buy-pressure floor, clearing both other
    filters, must enter normally."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock(return_value=ExecutionResult(
        success=True, tx_signature="sig999", actual_price=Decimal("0.001"),
        actual_amount=Decimal("10000000"),
    ))
    signal = make_signal(token, n_rules_cofiring=2, buy_pressure=Decimal("0.99"))
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=False, reason_code="LP_NOT_BURNED",
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={"liquidity_usd": "12000"},
    ))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    trades = result.scalars().all()
    assert len(trades) == 1
    assert trades[0].status == "open"
    worker._execution.buy.assert_awaited_once()


@pytest.mark.asyncio
async def test_non_qualifying_signal_never_enters(session, monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock()
    session.add(make_signal(token, n_rules_cofiring=1))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    assert result.scalars().all() == []
    worker._execution.buy.assert_not_awaited()


@pytest.mark.asyncio
async def test_historical_signals_before_worker_start_are_never_opened(session, monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    old_signal_time = datetime.now(timezone.utc) - timedelta(hours=3)
    session.add(make_signal(token, n_rules_cofiring=2, triggered_at=old_signal_time))
    await session.flush()

    worker = make_worker(session)  # _started_at = now, after old_signal_time
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    assert result.scalars().all() == []
    worker._execution.buy.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_buy_records_buy_failed_not_open(session, monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock(return_value=ExecutionResult(
        success=False, error_type="SlippageExceeded", error_detail="price moved too fast",
    ))
    session.add(make_signal(token, n_rules_cofiring=2))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
        # A buy_failed row must not count as "open" for the concurrency gate.
        open_count = await worker._open_trade_count()
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    trades = result.scalars().all()
    assert len(trades) == 1
    assert trades[0].status == "buy_failed"
    assert "SlippageExceeded" in trades[0].error_detail
    assert open_count == 0


@pytest.mark.asyncio
async def test_concurrency_cap_blocks_second_entry_while_one_open(session, monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_CONCURRENT", 1)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token_a = make_token()
    token_b = make_token()
    session.add_all([token_a, token_b])
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock(return_value=ExecutionResult(
        success=True, tx_signature="sig1", actual_price=Decimal("0.001"), actual_amount=Decimal("10000000"),
    ))
    session.add(make_signal(token_a, n_rules_cofiring=2))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()  # opens token_a
        session.add(make_signal(token_b, n_rules_cofiring=2))
        await session.flush()
        await worker._maybe_enter()  # must be blocked — one already open
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade))
    trades = result.scalars().all()
    assert len(trades) == 1
    assert trades[0].token_id == token_a.id
    assert worker._execution.buy.await_count == 1


@pytest.mark.asyncio
async def test_all_time_capital_exhausted_halts_entries_permanently(session, monkeypatch):
    """2026-09-24: 'all-time capital exhausted' is now a direct read of live
    wallet equity against CONFLUENCE_LIVE_MIN_TRADEABLE_USD (a dust floor),
    not a fixed-stake-minus-realized-pnl formula — see engine/live_equity.py.
    A prior closed trade is still recorded here for narrative realism (the
    wallet really did lose it all), but the halt itself is driven purely by
    the live balance mock below, exactly as it would be in production."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MIN_TRADEABLE_USD", 1.0)
    token = make_token()
    session.add(token)
    await session.flush()
    # A prior closed trade that lost the entire deposited budget.
    session.add(ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="closed",
        entry_time=datetime.now(timezone.utc) - timedelta(hours=1),
        entry_price=Decimal("1.0"), position_usd=Decimal("10.0"),
        exit_time=datetime.now(timezone.utc) - timedelta(minutes=30),
        exit_price=Decimal("0.0"), exit_reason="HARD_FLOOR",
        pnl_usd=Decimal("-10.0"),
    ))
    await session.flush()

    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=0.02)  # dust, below the $1 floor
    worker._execution.buy = AsyncMock()
    new_token = make_token()
    session.add(new_token)
    await session.flush()
    session.add(make_signal(new_token, n_rules_cofiring=2))
    await session.flush()

    ctx = patched_session(session)
    try:
        assert await worker._safe_to_enter() is False
        await worker._maybe_enter()
    finally:
        ctx.stop()

    worker._execution.buy.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_loss_limit_halts_entries_for_the_day(session, monkeypatch):
    """2026-09-24: the daily limit is now a fraction of TODAY'S STARTING live
    equity, back-derived as (current equity - today's realized pnl) — see
    engine/live_equity.py's is_daily_halted(). Starting the day at $10 and
    losing $6 today leaves $4 in the wallet; back-deriving gives the same
    $10 starting point the old fixed-stake formula used, so the 50% limit
    (-$5) is still crossed by today's -$6, same as before."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_DAILY_LOSS_LIMIT_PCT", 0.5)  # -$5 halts today
    token = make_token()
    session.add(token)
    await session.flush()
    session.add(ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="closed",
        entry_time=datetime.now(timezone.utc) - timedelta(hours=1),
        entry_price=Decimal("1.0"), position_usd=Decimal("10.0"),
        exit_time=datetime.now(timezone.utc) - timedelta(minutes=30),
        exit_price=Decimal("0.5"), exit_reason="STOP_LOSS",
        pnl_usd=Decimal("-6.0"),  # -60% of the $10 starting equity, past the 50% daily limit
    ))
    await session.flush()

    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=4.0)  # $10 starting - $6 lost today
    ctx = patched_session(session)
    try:
        assert await worker._safe_to_enter() is False
    finally:
        ctx.stop()


@pytest.mark.asyncio
async def test_idempotent_never_opens_same_token_twice(session, monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock(return_value=ExecutionResult(
        success=True, tx_signature="sig1", actual_price=Decimal("0.001"), actual_amount=Decimal("10000000"),
    ))
    session.add(make_signal(token, n_rules_cofiring=2))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
        # Close it so concurrency doesn't block the second attempt, isolating idempotency.
        result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
        trade = result.scalar_one()
        trade.status = "closed"
        trade.exit_time = datetime.now(timezone.utc)
        trade.pnl_usd = Decimal("1.0")
        await session.flush()
        await worker._maybe_enter()  # same signal still qualifies, same token already traded
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))
    assert len(result.scalars().all()) == 1
    worker._execution.buy.assert_awaited_once()


class TestLayeredExitPriority:
    """Velocity breaker -> HARD_FLOOR -> trailing-stop staircase -> TIME_EXIT
    (2026-09-23). See tests/test_trailing_stop.py for the staircase math
    itself and tests/test_confluence_shadow_worker.py's identical class for
    the paper-side twin of this coverage."""

    def setup_method(self):
        with patch("workers.confluence_live_worker.ExecutionEngine"):
            self.worker = ConfluenceLiveWorker(asyncio.Event())
        self.entry_price = Decimal("1.00")
        self.entry_time = datetime.now(timezone.utc) - timedelta(minutes=1)
        self.now = datetime.now(timezone.utc)
        self.initial_floor = self.entry_price * Decimal("0.88")  # -12%, widened 2026-09-24

    def test_hard_floor_triggers_at_threshold(self):
        floor = Decimal(str(settings.HARD_FLOOR_PCT))
        price = self.entry_price * (1 + floor)
        reason, _, _ = self.worker._check_exit(
            self.entry_price, self.entry_time, price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason == "HARD_FLOOR"

    def test_initial_stop_loss_triggers_as_stop_loss_before_any_new_high(self):
        price = self.entry_price * Decimal("0.87")  # below -12% initial floor, above -15% hard floor
        reason, new_floor, _ = self.worker._check_exit(
            self.entry_price, self.entry_time, price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason == "STOP_LOSS"

    def test_does_not_force_sell_at_old_take_profit_threshold(self):
        tp = Decimal(str(settings.TAKE_PROFIT_PCT))
        price = self.entry_price * (1 + tp)
        reason, new_floor, new_hwm = self.worker._check_exit(
            self.entry_price, self.entry_time, price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason is None
        assert new_hwm == price
        assert new_floor > self.initial_floor

    def test_trailing_stop_closes_after_a_higher_high_then_pullback(self):
        reason1, floor1, hwm1 = self.worker._check_exit(
            self.entry_price, self.entry_time, self.entry_price * Decimal("1.40"),
            self.now, self.initial_floor, self.entry_price,
        )
        assert reason1 is None
        assert floor1 == Decimal("1.300000000000")
        reason2, floor2, hwm2 = self.worker._check_exit(
            self.entry_price, self.entry_time, self.entry_price * Decimal("1.25"),
            self.now, floor1, hwm1,
        )
        assert reason2 == "TRAILING_STOP"

    def test_no_exit_when_flat(self):
        reason, _, _ = self.worker._check_exit(
            self.entry_price, self.entry_time, self.entry_price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason is None

    def test_time_exit_after_max_hold(self):
        old_entry = self.now - timedelta(seconds=settings.max_hold_seconds + 60)
        reason, _, _ = self.worker._check_exit(
            self.entry_price, old_entry, self.entry_price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason == "TIME_EXIT"

    def test_velocity_breaker_fires_as_hard_floor(self):
        price = self.entry_price * Decimal("0.70")
        reason, _, _ = self.worker._check_exit(
            self.entry_price, self.entry_time, price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason == "HARD_FLOOR"


@pytest.mark.asyncio
async def test_successful_exit_persists_pnl_and_closes(session, monkeypatch):
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    entry_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    trade = ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="open",
        entry_time=entry_time, entry_price=Decimal("1.00"),
        entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
    )
    session.add(trade)
    await session.flush()

    worker = make_worker(session)
    worker._execution.sell = AsyncMock(return_value=ExecutionResult(
        success=True, tx_signature="sellsig", actual_amount=Decimal("100000000"),  # 0.1 SOL back
    ))

    trade_dict = dict(
        id=trade.id, mint=token.mint_address, symbol=token.symbol,
        entry_price=trade.entry_price, entry_time=trade.entry_time,
        entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
    )

    ctx = patched_session(session)
    try:
        # Force a HARD_FLOOR-triggering price.
        floor = Decimal(str(settings.HARD_FLOOR_PCT))
        exit_price = trade.entry_price * (1 + floor)
        await worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))
    row = result.scalar_one()
    assert row.status == "closed"
    assert row.exit_reason == "HARD_FLOOR"
    assert row.exit_tx_signature == "sellsig"
    # proceeds = 0.1 SOL * $150 = $15; cost was $10 -> +$5 pnl
    assert row.pnl_usd == Decimal("5.0")


class TestLiquidityGuard:
    """
    Real-executable-liquidity backstop (2026-09-24) — real incident: BLK's
    DexScreener-sourced price showed +19.5% unrealized while a real Jupiter
    quote for the exact held size, routed through the pool actually
    trading now, showed 100% price impact and a real -76.5% loss.
    _check_exit() only ever sees the same stale snapshot price, so it
    structurally cannot catch this — these tests cover the independent
    real-quote-based guard added to close that gap.
    """

    def _make_trade_row(self, session, token):
        return ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=5), entry_price=Decimal("0.0001"),
            entry_token_lamports=500_000_000, position_usd=Decimal("0.04"),
        )

    async def _setup(self, session):
        """Real DB-backed trade row (2026-09-25: _check_liquidity_guard now
        persists real_price/real_pnl_pct onto this row on every real check,
        so these tests need one to exist, not just a plain dict)."""
        self.worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        row = self._make_trade_row(session, token)
        session.add(row)
        await session.flush()
        self.trade = dict(
            id=row.id, mint="SomeMint", symbol="TEST",
            entry_price=row.entry_price, entry_time=row.entry_time,
            entry_token_lamports=row.entry_token_lamports, position_usd=row.position_usd,
        )
        return row

    @pytest.mark.asyncio
    async def test_fires_on_high_price_impact(self, session, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        await self._setup(session)
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": 79842, "price_impact_pct": Decimal("1"),  # 100% impact, matches the real BLK quote
        })
        ctx = patched_session(session)
        try:
            reason, price = await self.worker._check_liquidity_guard(self.trade)
        finally:
            ctx.stop()
        assert reason == "LIQUIDITY_GUARD"
        assert price is not None

    @pytest.mark.asyncio
    async def test_fires_on_bad_real_pnl_even_with_low_impact(self, session, monkeypatch):
        """A pool can show low reported price impact while still paying out
        far less than the position cost — the real-P&L floor catches what
        the impact-percentage check alone might miss."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        await self._setup(session)
        # position_usd=0.04; proceeds worth ~$0.02 -> real pnl ~ -50%, impact reported low
        lamports_for_half = int((Decimal("0.02") / Decimal("116.0")) * Decimal("1e9"))
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": lamports_for_half, "price_impact_pct": Decimal("0.05"),
        })
        ctx = patched_session(session)
        try:
            reason, price = await self.worker._check_liquidity_guard(self.trade)
        finally:
            ctx.stop()
        assert reason == "LIQUIDITY_GUARD"

    @pytest.mark.asyncio
    async def test_does_not_fire_when_healthy(self, session, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        await self._setup(session)
        # proceeds roughly matching position cost, negligible impact
        lamports_at_cost = int((Decimal("0.04") / Decimal("116.0")) * Decimal("1e9"))
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": lamports_at_cost, "price_impact_pct": Decimal("0.01"),
        })
        ctx = patched_session(session)
        try:
            reason, price = await self.worker._check_liquidity_guard(self.trade)
        finally:
            ctx.stop()
        assert reason is None
        assert price is None

    @pytest.mark.asyncio
    async def test_returns_none_when_quote_fails(self, session, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        await self._setup(session)
        self.worker._execution.get_sell_quote = AsyncMock(return_value=None)
        reason, price = await self.worker._check_liquidity_guard(self.trade)
        assert reason is None
        assert price is None

    @pytest.mark.asyncio
    async def test_throttled_to_one_real_quote_per_interval(self, session, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        await self._setup(session)
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": 1, "price_impact_pct": Decimal("0.01"),
        })
        ctx = patched_session(session)
        try:
            await self.worker._check_liquidity_guard(self.trade)
            await self.worker._check_liquidity_guard(self.trade)  # immediately again
            assert self.worker._execution.get_sell_quote.await_count == 1  # second call skipped, not due yet

            # Simulate the interval having elapsed.
            import workers.confluence_live_worker as mod
            self.worker._last_liquidity_check[self.trade["id"]] -= mod._LIQUIDITY_CHECK_INTERVAL_S + 1
            await self.worker._check_liquidity_guard(self.trade)
            assert self.worker._execution.get_sell_quote.await_count == 2
        finally:
            ctx.stop()

    @pytest.mark.asyncio
    async def test_persists_real_price_on_every_real_check_not_just_crisis(self, session, monkeypatch):
        """2026-09-25: the whole point of the dashboard fix — a HEALTHY
        real check must still persist real_price/real_pnl_pct/
        real_price_checked_at, not only a crisis one."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        row = await self._setup(session)
        lamports_at_cost = int((Decimal("0.04") / Decimal("116.0")) * Decimal("1e9"))
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": lamports_at_cost, "price_impact_pct": Decimal("0.01"),
        })
        ctx = patched_session(session)
        try:
            reason, _ = await self.worker._check_liquidity_guard(self.trade)
        finally:
            ctx.stop()

        assert reason is None  # healthy, no crisis
        refreshed = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == row.id))).scalar_one()
        assert refreshed.real_price is not None
        assert refreshed.real_pnl_pct is not None

    @pytest.mark.asyncio
    async def test_checks_more_often_once_in_meaningful_profit(self, session, monkeypatch):
        """Real incident, 2026-09-25: Gavel's snapshot climbed to +40% while
        its real price had already collapsed to -33% underneath, and the
        60s-throttled guard only caught it at the very end of that window.
        A position showing a real unrealized gain must get checked on the
        faster _LIQUIDITY_CHECK_INTERVAL_FAST_S cadence, not the normal one."""
        import workers.confluence_live_worker as mod
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        await self._setup(session)
        lamports_at_cost = int((Decimal("0.04") / Decimal("116.0")) * Decimal("1e9"))
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": lamports_at_cost, "price_impact_pct": Decimal("0.01"),
        })
        profitable_price = self.trade["entry_price"] * Decimal("1.20")  # +20%, above the 15% fast threshold

        ctx = patched_session(session)
        try:
            await self.worker._check_liquidity_guard(self.trade, profitable_price)
            assert self.worker._execution.get_sell_quote.await_count == 1

            # Only the FAST interval has elapsed — the normal 60s one hasn't.
            self.worker._last_liquidity_check[self.trade["id"]] -= mod._LIQUIDITY_CHECK_INTERVAL_FAST_S + 1
            await self.worker._check_liquidity_guard(self.trade, profitable_price)
            assert self.worker._execution.get_sell_quote.await_count == 2
        finally:
            ctx.stop()

    @pytest.mark.asyncio
    async def test_does_not_check_faster_below_the_profit_threshold(self, session, monkeypatch):
        """A position that isn't meaningfully profitable yet keeps the
        normal, quota-conserving 60s cadence — the faster check is spent
        only where there's real profit worth protecting."""
        import workers.confluence_live_worker as mod
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        await self._setup(session)
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": 1, "price_impact_pct": Decimal("0.01"),
        })
        near_breakeven_price = self.trade["entry_price"] * Decimal("1.02")  # +2%, below the 15% fast threshold

        ctx = patched_session(session)
        try:
            await self.worker._check_liquidity_guard(self.trade, near_breakeven_price)
            assert self.worker._execution.get_sell_quote.await_count == 1

            # Only the FAST interval has elapsed — must NOT be due yet at this pnl.
            self.worker._last_liquidity_check[self.trade["id"]] -= mod._LIQUIDITY_CHECK_INTERVAL_FAST_S + 1
            await self.worker._check_liquidity_guard(self.trade, near_breakeven_price)
            assert self.worker._execution.get_sell_quote.await_count == 1  # still skipped
        finally:
            ctx.stop()

    @pytest.mark.asyncio
    async def test_maybe_exit_uses_guard_reason_and_price_over_snapshot(self, session, monkeypatch):
        """Integration: when the guard fires, _maybe_exit must use its
        reason and real price — never _check_exit()'s snapshot-based
        result — even though the snapshot price alone would say 'hold'."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        worker = make_worker(session)
        worker._check_liquidity_guard = AsyncMock(return_value=("LIQUIDITY_GUARD", Decimal("0.05")))
        worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="guardsig", actual_amount=Decimal("30000000"),  # 0.03 SOL back
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            # Snapshot price alone (near entry, no stop condition) would
            # normally mean "hold" — the guard must override that.
            snapshot_price = trade.entry_price * Decimal("1.10")
            await worker._maybe_exit(trade_dict, snapshot_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))
        row = result.scalar_one()
        assert row.status == "closed"
        assert row.exit_reason == "LIQUIDITY_GUARD"
        assert row.exit_price == Decimal("0.05")  # the guard's real price, not the 1.10 snapshot


@pytest.mark.asyncio
async def test_failed_sell_leaves_trade_open_for_retry(session, monkeypatch):
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    entry_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    trade = ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="open",
        entry_time=entry_time, entry_price=Decimal("1.00"),
        entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
    )
    session.add(trade)
    await session.flush()

    worker = make_worker(session)
    worker._execution.sell = AsyncMock(return_value=ExecutionResult(
        success=False, error_type="RpcTimeout", error_detail="no confirmation after 3 retries",
    ))

    trade_dict = dict(
        id=trade.id, mint=token.mint_address, symbol=token.symbol,
        entry_price=trade.entry_price, entry_time=trade.entry_time,
        entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
    )

    ctx = patched_session(session)
    try:
        floor = Decimal(str(settings.HARD_FLOOR_PCT))
        exit_price = trade.entry_price * (1 + floor)
        await worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
    finally:
        ctx.stop()

    result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))
    row = result.scalar_one()
    assert row.status == "open"  # NOT closed — must be retried next cycle
    assert row.exit_tx_signature is None
    assert row.pnl_usd is None


class TestUnsellablePositions:
    """
    Real incident (2026-09-24): SEND's exit failed every retry for 4+
    minutes straight, and manually retrying at 90% slippage tolerance hit
    the identical on-chain error — confirming it was never a slippage
    issue, and a plain "keep retrying forever" would occupy one of
    CONFLUENCE_LIVE_MAX_CONCURRENT's slots permanently. These tests cover
    the fix: downgrading to status='unsellable' after
    confluence_live_worker._UNSELLABLE_AFTER_S of continuous real sell
    failure, which frees the concurrency slot (_open_trade_count() only
    ever counts 'open') without ever fabricating a close — the only way
    out is a real successful sell.
    """

    @pytest.mark.asyncio
    async def test_marks_unsellable_after_sustained_failure(self, session, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=15), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        self.worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=False, error_type="SELL_FAILED_CRITICAL", error_detail="on-chain program error",
        ))
        # Simulate this trade having already been failing for longer than
        # the threshold, rather than sleeping 600s in a test.
        import workers.confluence_live_worker as mod
        self.worker._sell_failing_since[trade.id] = time.monotonic() - mod._UNSELLABLE_AFTER_S - 1

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.status == "unsellable"

    @pytest.mark.asyncio
    async def test_does_not_mark_unsellable_before_the_threshold(self, session, monkeypatch):
        """A sell failing for less time than the threshold stays plain
        'open' — this is just the existing retry behavior, unaffected."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        self.worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=False, error_type="SELL_FAILED_CRITICAL", error_detail="on-chain program error",
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.status == "open"

    @pytest.mark.asyncio
    async def test_unsellable_trades_excluded_from_concurrency_count(self, session):
        self.worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        session.add(ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="unsellable",
            entry_time=datetime.now(timezone.utc), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        ))
        await session.flush()

        with patch("workers.confluence_live_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            count = await self.worker._open_trade_count()

        assert count == 0  # 'unsellable' must NOT count against CONFLUENCE_LIVE_MAX_CONCURRENT

    @pytest.mark.asyncio
    async def test_load_open_trades_includes_unsellable(self, session):
        self.worker = make_worker(session)
        token = make_token(mint_address="MintUnsellable111111111111111111111")
        session.add(token)
        await session.flush()
        session.add(ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="unsellable",
            entry_time=datetime.now(timezone.utc), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        ))
        await session.flush()

        with patch("workers.confluence_live_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            open_trades = await self.worker._load_open_trades()

        assert token.mint_address in open_trades  # still priced/retried every cycle

    @pytest.mark.asyncio
    async def test_unsellable_trade_closes_for_real_on_a_later_successful_sell(self, session, monkeypatch):
        """The ONLY way out of 'unsellable' — a real successful sell,
        never a fabricated close."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="unsellable",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=20), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        self.worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="recoveredsig", actual_amount=Decimal("50000000"),  # 0.05 SOL back
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.status == "closed"
        assert row.exit_tx_signature == "recoveredsig"
        # proceeds = 0.05 SOL * $150 = $7.50; cost was $10 -> -$2.50
        assert row.pnl_usd == Decimal("7.50") - Decimal("10.0")


class TestSellCoordinationWithManualClose:
    """
    Real incident, 2026-09-24 (user: 'there are positions active but cant
    close there'): this worker's own automatic exit loop and the
    dashboard's manual close endpoint both submitted a real sell for the
    same trade within the same second — whichever landed second got
    rejected on-chain (real custom program errors, e.g. 0x1788/6024).
    See engine/sell_coordination.py.
    """

    @pytest.mark.asyncio
    async def test_skips_the_sell_when_a_manual_close_already_holds_the_lock(self, session, monkeypatch):
        from engine.sell_coordination import finish_sell, try_start_sell

        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        self.worker._execution.sell = AsyncMock(
            side_effect=AssertionError("must not submit a second, competing sell")
        )

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        assert try_start_sell(str(trade.id)) is True  # simulates a manual close in flight
        try:
            ctx = patched_session(session)
            try:
                floor = Decimal(str(settings.HARD_FLOOR_PCT))
                exit_price = trade.entry_price * (1 + floor)
                await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
            finally:
                ctx.stop()
        finally:
            finish_sell(str(trade.id))

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.status == "open"  # untouched — the manual close owns this trade right now

    @pytest.mark.asyncio
    async def test_releases_the_lock_after_its_own_sell_completes(self, session, monkeypatch):
        from engine.sell_coordination import try_start_sell

        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        self.worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="sig", actual_amount=Decimal("5000000"),
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        assert try_start_sell(str(trade.id)) is True  # lock released, not leaked
        from engine.sell_coordination import finish_sell
        finish_sell(str(trade.id))


class TestSellRetryBackoff:
    """
    Real incident, 2026-09-25 (user: 'why no trades'): SEND's exit reason
    (TIME_EXIT, permanently true forever once past max_hold_seconds)
    re-triggered a full real sell attempt on every ~1s poll cycle, all day,
    long after it was already known to be stuck. Confirmed live: a
    genuinely fillable new buy (Luckin) failed with swap_build_failed
    because Jupiter's shared /swap endpoint 429'd at the exact same
    instant SEND's own nonstop retry loop was hitting it — the bot's own
    stuck position was starving its real trade execution of API quota.
    These tests cover the fix: once a sell has been failing longer than
    _SELL_BACKOFF_AFTER_S, real attempts are spaced to at most one per
    _SELL_BACKOFF_INTERVAL_S.
    """

    def _make_trade(self, session):
        token = make_token()
        session.add(token)
        return token

    @pytest.mark.asyncio
    async def test_first_failure_is_never_backed_off(self, session, monkeypatch):
        """A fresh failure (never recorded before) must attempt immediately
        — backoff only applies to a sell already known to be failing."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = self._make_trade(session)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        self.worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=False, error_type="SELL_FAILED_CRITICAL", error_detail="on-chain program error",
        ))
        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        self.worker._execution.sell.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_backs_off_once_failing_past_the_threshold(self, session, monkeypatch):
        """Once a sell has been failing longer than _SELL_BACKOFF_AFTER_S
        and a real attempt happened recently, the next cycle must NOT
        submit another real sell."""
        import workers.confluence_live_worker as mod

        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = self._make_trade(session)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=15), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        # Already failing well past the backoff threshold, and a real
        # attempt was just made a moment ago (well within the backoff
        # interval) — the upcoming cycle must skip entirely.
        self.worker._sell_failing_since[trade.id] = time.monotonic() - mod._SELL_BACKOFF_AFTER_S - 5
        self.worker._last_sell_attempt[trade.id] = time.monotonic()
        self.worker._execution.sell = AsyncMock(
            side_effect=AssertionError("must not submit a real sell while backed off")
        )

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        self.worker._execution.sell.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_retries_again_once_the_backoff_interval_elapses(self, session, monkeypatch):
        """Backed off, but the interval has since elapsed — must attempt again."""
        import workers.confluence_live_worker as mod

        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = self._make_trade(session)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=15), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        self.worker._sell_failing_since[trade.id] = time.monotonic() - mod._SELL_BACKOFF_AFTER_S - 5
        self.worker._last_sell_attempt[trade.id] = time.monotonic() - mod._SELL_BACKOFF_INTERVAL_S - 1
        self.worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=False, error_type="SELL_FAILED_CRITICAL", error_detail="on-chain program error",
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        self.worker._execution.sell.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_successful_sell_clears_the_backoff_state(self, session, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = self._make_trade(session)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        # Failing a long time (past the backoff threshold), but the last
        # attempt was far enough back that this cycle is allowed to try
        # again — this time it succeeds, and cleanup must remove both.
        import workers.confluence_live_worker as mod
        self.worker._sell_failing_since[trade.id] = time.monotonic() - 100
        self.worker._last_sell_attempt[trade.id] = time.monotonic() - mod._SELL_BACKOFF_INTERVAL_S - 1
        self.worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="sig", actual_amount=Decimal("5000000"),
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        self.worker._execution.sell.assert_awaited_once()
        assert trade.id not in self.worker._last_sell_attempt
        assert trade.id not in self.worker._sell_failing_since


class TestBalanceAlreadyZeroReconciliation:
    """
    Real incident, 2026-09-25 (SEND): a real sell succeeded on-chain 5
    seconds after entry, but the service happened to restart mid-
    confirmation-poll, so the DB write never happened — the trade was left
    'open' with real tokens already gone. Every subsequent retry then
    failed on-chain (insufficient balance) with an error indistinguishable
    from a genuinely drained pool, for DAYS, until a manual forensic
    on-chain lookup found the real, already-successful sell. These tests
    cover the fix: once a sell has already failed at least once, a
    CONFIRMED-zero real balance check (never a lookup failure, which stays
    None) stops further pointless retries and flags the trade for manual
    reconciliation instead.
    """

    @pytest.mark.asyncio
    async def test_confirmed_zero_balance_marks_for_reconciliation(self, session, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        self.worker._sell_failing_since[trade.id] = time.monotonic() - 5  # already failed at least once
        self.worker._execution.get_token_balance_raw = AsyncMock(return_value=(0, 6))  # confirmed zero
        self.worker._execution.sell = AsyncMock(
            side_effect=AssertionError("must not attempt another sell once balance is confirmed zero")
        )

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.status == "balance_zero"
        assert row.pnl_usd is None  # never fabricated — real proceeds are unknown without forensics
        assert row.exit_time is None
        assert trade.id not in self.worker._sell_failing_since  # cleaned up, not retried forever

    @pytest.mark.asyncio
    async def test_a_lookup_failure_is_never_treated_as_zero(self, session, monkeypatch):
        """get_token_balance_raw returning None means 'unknown,' not 'zero'
        — a transient RPC hiccup must never short-circuit a real retry."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        self.worker._sell_failing_since[trade.id] = time.monotonic() - 5
        self.worker._execution.get_token_balance_raw = AsyncMock(return_value=None)  # lookup failed, unknown
        self.worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=False, error_type="SELL_FAILED_CRITICAL", error_detail="on-chain program error",
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        self.worker._execution.sell.assert_awaited_once()  # still retried normally
        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.status == "open"

    @pytest.mark.asyncio
    async def test_first_failure_never_checks_balance(self, session, monkeypatch):
        """The balance check only makes sense once a sell has already
        failed at least once — checking on a fresh, first-time exit
        attempt would add real RPC load for no benefit."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        self.worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()

        self.worker._execution.get_token_balance_raw = AsyncMock(
            side_effect=AssertionError("must not check balance on a first-time failure")
        )
        self.worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=False, error_type="SELL_FAILED_CRITICAL", error_detail="on-chain program error",
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await self.worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        self.worker._execution.sell.assert_awaited_once()


class TestRealOnChainAuditTrail:
    """
    Real audit, 2026-09-28 (triggered by a 3-day dry-spell investigation,
    then a full wallet reconciliation): entry_sol_lamports/exit_sol_lamports
    (both INTENDED/quoted amounts) understated real spend by 6.36x in
    aggregate across 71 real trades. Root cause, confirmed via a direct
    on-chain instruction trace: a mandatory Pump.fun protocol-fee token
    account (owned by Pump.fun's fee collector, never this wallet, never
    reclaimable) that some sells must create — on one confirmed trade this
    fee alone exceeded the entire quoted gain, turning a trade pnl_usd
    called profitable into a real net loss. These tests cover persisting
    the REAL numbers (from ExecutionResult.actual_sol_lamports/
    reclaim_tx_signature/reclaim_sol_lamports) onto the trade row.
    """

    @pytest.mark.asyncio
    async def test_entry_persists_the_real_wallet_delta(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
        token = make_token()
        session.add(token)
        await session.flush()
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
        worker._execution.buy = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="sig1", actual_price=Decimal("0.001"),
            actual_amount=Decimal("10000000"), actual_sol_lamports=-1603733,
        ))
        session.add(make_signal(token, n_rules_cofiring=2))
        await session.flush()

        ctx = patched_session(session)
        try:
            await worker._maybe_enter()
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.token_id == token.id))).scalar_one()
        assert row.entry_real_sol_lamports == -1603733

    @pytest.mark.asyncio
    async def test_exit_persists_real_deltas_and_computes_real_pnl_usd(self, session, monkeypatch):
        """Real MetaMask-incident numbers: entry real cost 1,603,733,
        exit real delta -1,429,987 (the sell itself net-LOST money after
        the non-reclaimable Pump.fun fee account), reclaim +0 (none this
        time) — real_pnl_usd must reflect the true, worse-than-pnl_usd
        outcome, not the quoted-amount-based one."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 119.41)
        worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("0.0001"),
            entry_token_lamports=10_000_000, position_usd=Decimal("0.02"),
            entry_real_sol_lamports=-1603733,
        )
        session.add(trade)
        await session.flush()

        worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="exitsig", actual_amount=Decimal("249417"),
            actual_sol_lamports=-1429987, reclaim_tx_signature=None, reclaim_sol_lamports=None,
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.exit_real_sol_lamports == -1429987
        assert row.reclaim_tx_signature is None
        # real net = -1603733 (entry) + -1429987 (exit) + 0 (no reclaim) = -3033720 lamports
        expected_real_pnl = (Decimal(-3033720) / Decimal("1e9")) * Decimal("119.41")
        assert row.real_pnl_usd == expected_real_pnl
        assert row.real_pnl_usd < 0  # a real loss, even if pnl_usd (quoted-amount-based) looked positive

    @pytest.mark.asyncio
    async def test_exit_persists_a_real_reclaim(self, session, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 120.0)
        worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("0.0001"),
            entry_token_lamports=10_000_000, position_usd=Decimal("0.02"),
            entry_real_sol_lamports=-1600000,
        )
        session.add(trade)
        await session.flush()

        worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="exitsig", actual_amount=Decimal("135000"),
            actual_sol_lamports=130000, reclaim_tx_signature="reclaimsig", reclaim_sol_lamports=1488440,
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.reclaim_tx_signature == "reclaimsig"
        assert row.reclaim_sol_lamports == 1488440
        expected_real_pnl = (Decimal(-1600000 + 130000 + 1488440) / Decimal("1e9")) * Decimal("120.0")
        assert row.real_pnl_usd == expected_real_pnl

    @pytest.mark.asyncio
    async def test_real_pnl_usd_stays_null_when_entry_real_delta_unknown(self, session, monkeypatch):
        """A historical trade from before this fix has no
        entry_real_sol_lamports — real_pnl_usd must stay NULL rather than
        compute a partial, misleading number from just the exit side."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 120.0)
        worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("0.0001"),
            entry_token_lamports=10_000_000, position_usd=Decimal("0.02"),
            entry_real_sol_lamports=None,
        )
        session.add(trade)
        await session.flush()

        worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="exitsig", actual_amount=Decimal("135000"), actual_sol_lamports=130000,
        ))

        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            await worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.exit_real_sol_lamports == 130000  # exit side still recorded
        assert row.real_pnl_usd is None  # but the combined real pnl is honestly unknown


class TestPeriodicRentSweep:
    """
    Real incident, 2026-09-25 (SEND): the only existing rent-reclaim path
    (execution.py's close_token_account(), called right after a sell this
    engine itself just executed) structurally cannot recover an account
    that went to zero any other way — SEND's went unreclaimed for a full
    day. _maybe_sweep_rent() is the general periodic safety net: calls
    ExecutionEngine.sweep_dead_token_accounts() on _RENT_SWEEP_INTERVAL_S,
    regardless of CONFLUENCE_LIVE_ENABLED, and never lets a failure there
    break the cycle that triggered it.
    """

    @pytest.mark.asyncio
    async def test_sweeps_on_the_first_cycle_without_waiting_a_full_interval(self, session):
        worker = make_worker(session)
        worker._execution.sweep_dead_token_accounts = AsyncMock(return_value=[])
        await worker._maybe_sweep_rent()
        worker._execution.sweep_dead_token_accounts.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_does_not_sweep_again_before_the_interval_elapses(self, session):
        worker = make_worker(session)
        worker._execution.sweep_dead_token_accounts = AsyncMock(return_value=[])
        await worker._maybe_sweep_rent()
        await worker._maybe_sweep_rent()
        worker._execution.sweep_dead_token_accounts.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sweeps_again_once_the_interval_elapses(self, session):
        import workers.confluence_live_worker as mod
        worker = make_worker(session)
        worker._execution.sweep_dead_token_accounts = AsyncMock(return_value=[])
        await worker._maybe_sweep_rent()
        worker._last_rent_sweep -= mod._RENT_SWEEP_INTERVAL_S + 1
        await worker._maybe_sweep_rent()
        assert worker._execution.sweep_dead_token_accounts.await_count == 2

    @pytest.mark.asyncio
    async def test_notifies_when_something_was_reclaimed(self, session):
        worker = make_worker(session)
        worker._execution.sweep_dead_token_accounts = AsyncMock(return_value=["SIG1"])

        ctx = patched_session(session)
        try:
            await worker._maybe_sweep_rent()
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        assert any(r.event == "rent_swept" for r in rows)

    @pytest.mark.asyncio
    async def test_no_notification_when_nothing_reclaimed(self, session):
        worker = make_worker(session)
        worker._execution.sweep_dead_token_accounts = AsyncMock(return_value=[])

        ctx = patched_session(session)
        try:
            await worker._maybe_sweep_rent()
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        assert not any(r.event == "rent_swept" for r in rows)

    @pytest.mark.asyncio
    async def test_a_sweep_failure_never_raises(self, session):
        worker = make_worker(session)
        worker._execution.sweep_dead_token_accounts = AsyncMock(side_effect=RuntimeError("RPC outage"))
        await worker._maybe_sweep_rent()  # must not raise


class TestAuditTrailBackfill:
    """
    Real problem, 2026-09-28 ("make the audit system more robust"): 71
    trades closed before entry_real_sol_lamports/exit_real_sol_lamports
    existed have real tx signatures stored but no real-delta data.
    _maybe_backfill_audit_trail() self-heals them a few at a time using
    the same real on-chain lookup every live trade already gets — no
    manual forensic script ever needed again.
    """

    def _make_backfillable_trade(self, session, token, **overrides):
        defaults = dict(
            token_id=token.id, n_rules_cofiring=2, status="closed",
            entry_time=datetime.now(timezone.utc) - timedelta(hours=1), entry_price=Decimal("0.0001"),
            entry_token_lamports=10_000_000, position_usd=Decimal("0.02"),
            entry_tx_signature="entrysig", exit_tx_signature="exitsig",
            exit_time=datetime.now(timezone.utc), exit_reason="TIME_EXIT", pnl_usd=Decimal("0.001"),
        )
        defaults.update(overrides)
        return ConfluenceLiveTrade(**defaults)

    @pytest.mark.asyncio
    async def test_backfills_missing_entry_and_exit_real_deltas(self, session):
        worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = self._make_backfillable_trade(session, token)
        session.add(trade)
        await session.flush()

        worker._execution.get_real_tx_delta = AsyncMock(side_effect=[
            (-1603733, 5000),   # entry lookup
            (-1429987, 38564),  # exit lookup
        ])

        ctx = patched_session(session)
        try:
            await worker._maybe_backfill_audit_trail()
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.entry_real_sol_lamports == -1603733
        assert row.entry_network_fee_lamports == 5000
        assert row.exit_real_sol_lamports == -1429987
        assert row.exit_network_fee_lamports == 38564
        # Never fabricated: no reclaim signature was ever stored for this
        # historical trade, so real_pnl_usd must stay NULL, not a partial
        # entry+exit-only number.
        assert row.real_pnl_usd is None

    @pytest.mark.asyncio
    async def test_skips_trades_that_already_have_both_real_deltas(self, session):
        worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = self._make_backfillable_trade(
            session, token, entry_real_sol_lamports=-100, exit_real_sol_lamports=50,
        )
        session.add(trade)
        await session.flush()

        worker._execution.get_real_tx_delta = AsyncMock(
            side_effect=AssertionError("must not look up a trade that's already fully backfilled")
        )

        ctx = patched_session(session)
        try:
            await worker._maybe_backfill_audit_trail()
        finally:
            ctx.stop()

    @pytest.mark.asyncio
    async def test_only_looks_up_the_missing_side(self, session):
        """A trade missing only exit_real_sol_lamports must not re-fetch
        the entry side that's already known."""
        worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = self._make_backfillable_trade(session, token, entry_real_sol_lamports=-1603733)
        session.add(trade)
        await session.flush()

        worker._execution.get_real_tx_delta = AsyncMock(return_value=(-1429987, 38564))

        ctx = patched_session(session)
        try:
            await worker._maybe_backfill_audit_trail()
        finally:
            ctx.stop()

        worker._execution.get_real_tx_delta.assert_awaited_once_with("exitsig")
        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.entry_real_sol_lamports == -1603733  # untouched
        assert row.exit_real_sol_lamports == -1429987

    @pytest.mark.asyncio
    async def test_a_failed_lookup_leaves_the_trade_for_the_next_sweep(self, session):
        worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        trade = self._make_backfillable_trade(session, token)
        session.add(trade)
        await session.flush()

        worker._execution.get_real_tx_delta = AsyncMock(return_value=None)  # both lookups fail

        ctx = patched_session(session)
        try:
            await worker._maybe_backfill_audit_trail()
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.entry_real_sol_lamports is None
        assert row.exit_real_sol_lamports is None

    @pytest.mark.asyncio
    async def test_respects_the_batch_size_limit(self, session, monkeypatch):
        import workers.confluence_live_worker as mod
        # Use a small batch size for this test regardless of the real
        # constant — the real one covers the entire historical backlog in
        # one sweep (200+), which would mean this test doing 400+ real
        # asyncio.sleep(_AUDIT_BACKFILL_LOOKUP_DELAY_S) pacing waits.
        monkeypatch.setattr(mod, "_AUDIT_BACKFILL_BATCH_SIZE", 5)
        worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        for _ in range(5 + 3):
            session.add(self._make_backfillable_trade(session, token))
        await session.flush()

        worker._execution.get_real_tx_delta = AsyncMock(return_value=(-100, 5000))

        ctx = patched_session(session)
        try:
            await worker._maybe_backfill_audit_trail()
        finally:
            ctx.stop()

        # 2 lookups (entry+exit) per trade, capped at the batch size.
        assert worker._execution.get_real_tx_delta.await_count == 5 * 2

    @pytest.mark.asyncio
    async def test_does_not_backfill_again_before_the_interval_elapses(self, session):
        worker = make_worker(session)
        token = make_token()
        session.add(token)
        await session.flush()
        session.add(self._make_backfillable_trade(session, token))
        await session.flush()

        worker._execution.get_real_tx_delta = AsyncMock(return_value=(-100, 5000))

        ctx = patched_session(session)
        try:
            await worker._maybe_backfill_audit_trail()
            call_count_after_first = worker._execution.get_real_tx_delta.await_count
            await worker._maybe_backfill_audit_trail()
        finally:
            ctx.stop()

        assert worker._execution.get_real_tx_delta.await_count == call_count_after_first  # no new calls

    @pytest.mark.asyncio
    async def test_a_query_failure_never_raises(self, session, monkeypatch):
        worker = make_worker(session)
        with patch("workers.confluence_live_worker.get_session", side_effect=RuntimeError("DB outage")):
            await worker._maybe_backfill_audit_trail()  # must not raise


class TestFilterCalibrationCheck:
    """
    Periodic self-audit (2026-09-28) — _maybe_check_filter_calibration()
    re-runs engine.filter_calibration.check_current_filter_population()
    on _CALIBRATION_CHECK_INTERVAL_S, and notifies (never adjusts a
    threshold) when it comes back needing review. See
    engine/filter_calibration.py's module docstring for why this is
    detect-and-flag only, never auto-adjusting.
    """

    def _fake_result(self, needs_review: bool, reasons=None, n=50):
        import workers.confluence_live_worker as mod
        from engine.filter_calibration import CalibrationResult
        return CalibrationResult(
            n=n, window_days=14, win_rate=0.5 if needs_review else 0.86,
            rug_rate=0.3 if needs_review else 0.08, capped_mean=-0.1 if needs_review else 0.30,
            needs_review=needs_review, reasons=reasons or (["win rate too low"] if needs_review else []),
        )

    @pytest.mark.asyncio
    async def test_checks_on_the_first_cycle_without_waiting_a_full_interval(self, session, monkeypatch):
        import workers.confluence_live_worker as mod
        worker = make_worker(session)
        mock_check = AsyncMock(return_value=self._fake_result(needs_review=False))
        monkeypatch.setattr(mod, "check_current_filter_population", mock_check)
        await worker._maybe_check_filter_calibration()
        mock_check.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_does_not_check_again_before_the_interval_elapses(self, session, monkeypatch):
        import workers.confluence_live_worker as mod
        worker = make_worker(session)
        mock_check = AsyncMock(return_value=self._fake_result(needs_review=False))
        monkeypatch.setattr(mod, "check_current_filter_population", mock_check)
        await worker._maybe_check_filter_calibration()
        await worker._maybe_check_filter_calibration()
        mock_check.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_checks_again_once_the_interval_elapses(self, session, monkeypatch):
        import workers.confluence_live_worker as mod
        worker = make_worker(session)
        mock_check = AsyncMock(return_value=self._fake_result(needs_review=False))
        monkeypatch.setattr(mod, "check_current_filter_population", mock_check)
        await worker._maybe_check_filter_calibration()
        worker._last_calibration_check -= mod._CALIBRATION_CHECK_INTERVAL_S + 1
        await worker._maybe_check_filter_calibration()
        assert mock_check.await_count == 2

    @pytest.mark.asyncio
    async def test_notifies_when_review_is_needed(self, session, monkeypatch):
        import workers.confluence_live_worker as mod
        worker = make_worker(session)
        mock_check = AsyncMock(return_value=self._fake_result(needs_review=True, reasons=["win rate 50.0% is below the 65% review floor"]))
        monkeypatch.setattr(mod, "check_current_filter_population", mock_check)

        ctx = patched_session(session)
        try:
            await worker._maybe_check_filter_calibration()
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        review_rows = [r for r in rows if r.event == "filter_calibration_needs_review"]
        assert len(review_rows) == 1
        assert "win rate" in review_rows[0].message

    @pytest.mark.asyncio
    async def test_no_notification_when_everything_still_looks_fine(self, session, monkeypatch):
        import workers.confluence_live_worker as mod
        worker = make_worker(session)
        mock_check = AsyncMock(return_value=self._fake_result(needs_review=False))
        monkeypatch.setattr(mod, "check_current_filter_population", mock_check)

        ctx = patched_session(session)
        try:
            await worker._maybe_check_filter_calibration()
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        assert not any(r.event == "filter_calibration_needs_review" for r in rows)

    @pytest.mark.asyncio
    async def test_a_check_failure_never_raises(self, session, monkeypatch):
        import workers.confluence_live_worker as mod
        worker = make_worker(session)
        monkeypatch.setattr(mod, "check_current_filter_population", AsyncMock(side_effect=RuntimeError("DB outage")))
        await worker._maybe_check_filter_calibration()  # must not raise


class TestExposurePercentageSizing:
    """config/settings.py's CONFLUENCE_LIVE_EXPOSURE_PCT formula
    (2026-09-23, replacing an earlier fixed-stake-plus-profit-share
    formula the same day): never more than this fraction of CURRENT
    total capital is at risk across every open position combined, split
    evenly across CONFLUENCE_LIVE_MAX_CONCURRENT slots. These tests pin
    CONFLUENCE_LIVE_MAX_CONCURRENT=1 so the arithmetic reduces to the
    simple single-slot case — see TestConcurrentSlotSizing below for the
    3-slot split that's actually deployed."""

    @pytest.mark.asyncio
    async def test_at_starting_equity_bets_the_exposure_fraction(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_POSITION_USD", 50.0)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_EXPOSURE_PCT", 0.10)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_CONCURRENT", 1)
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
        ctx = patched_session(session)
        try:
            position_usd = await worker._compute_position_usd()
        finally:
            ctx.stop()
        assert position_usd == Decimal("1.0")  # 10% of $10

    @pytest.mark.asyncio
    async def test_after_a_loss_exposure_shrinks_with_equity(self, session, monkeypatch):
        """2026-09-24: equity is the live wallet balance now, not a fixed
        stake plus a summed realized-pnl ledger — a $4 loss just means the
        wallet itself holds $6 less, mocked directly rather than inferred
        from a closed trade row."""
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_POSITION_USD", 50.0)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_EXPOSURE_PCT", 0.10)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_CONCURRENT", 1)
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=6.0)  # $10 - $4 loss
        ctx = patched_session(session)
        try:
            position_usd = await worker._compute_position_usd()
        finally:
            ctx.stop()
        assert position_usd == Decimal("0.6")  # 10% of $6

    @pytest.mark.asyncio
    async def test_after_growth_exposure_grows_with_equity(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_POSITION_USD", 50.0)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_EXPOSURE_PCT", 0.10)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_CONCURRENT", 1)
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=30.0)  # $10 + $20 gain
        ctx = patched_session(session)
        try:
            position_usd = await worker._compute_position_usd()
        finally:
            ctx.stop()
        assert position_usd == Decimal("3.0")  # 10% of $30

    @pytest.mark.asyncio
    async def test_position_size_never_exceeds_the_outer_ceiling(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_POSITION_USD", 15.0)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_EXPOSURE_PCT", 1.0)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_CONCURRENT", 1)
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=110.0)  # $10 + $100 gain
        ctx = patched_session(session)
        try:
            position_usd = await worker._compute_position_usd()
        finally:
            ctx.stop()
        assert position_usd == Decimal("15.0")  # would be $110 uncapped — clamped to the ceiling


class TestConcurrentSlotSizing:
    """CONFLUENCE_LIVE_MAX_CONCURRENT=3 (2026-09-23, raised from 1 per
    analysis/sl_tp_and_concurrency_sweep.py). Each concurrent slot gets an
    equal share of total exposure — see config/settings.py's comment."""

    @pytest.mark.asyncio
    async def test_at_starting_equity_each_slot_gets_a_third_of_total_exposure(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_POSITION_USD", 50.0)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_EXPOSURE_PCT", 0.10)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_CONCURRENT", 3)
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
        ctx = patched_session(session)
        try:
            position_usd = await worker._compute_position_usd()
        finally:
            ctx.stop()
        # total_exposure = 10% of $10 = $1; split 3 ways = $0.3333...
        assert abs(position_usd - Decimal("1.0") / 3) < Decimal("0.0001")

    @pytest.mark.asyncio
    async def test_three_full_slots_deploy_the_total_exposure_amount(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_POSITION_USD", 50.0)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_EXPOSURE_PCT", 0.10)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_CONCURRENT", 3)
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
        ctx = patched_session(session)
        try:
            per_slot = await worker._compute_position_usd()
        finally:
            ctx.stop()
        assert abs(per_slot * 3 - Decimal("1.0")) < Decimal("0.0001")  # 10% of $10 total

    @pytest.mark.asyncio
    async def test_after_growth_exposure_split_across_slots(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_POSITION_USD", 50.0)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_EXPOSURE_PCT", 0.10)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_CONCURRENT", 3)
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=40.0)  # $10 + $30 gain
        ctx = patched_session(session)
        try:
            position_usd = await worker._compute_position_usd()
        finally:
            ctx.stop()
        # equity=40, total_exposure=10% of 40=4, split 3 ways = 1.3333...
        assert abs(position_usd - Decimal("4.0") / 3) < Decimal("0.0001")

    @pytest.mark.asyncio
    async def test_three_concurrent_trades_allowed_a_fourth_blocked(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_CONCURRENT", 3)
        tokens = [make_token() for _ in range(4)]
        session.add_all(tokens)
        await session.flush()
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
        worker._execution.buy = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="sig", actual_price=Decimal("0.001"), actual_amount=Decimal("10000000"),
        ))

        ctx = patched_session(session)
        try:
            for t in tokens:
                session.add(make_signal(t, n_rules_cofiring=2))
                await session.flush()
                await worker._maybe_enter()
        finally:
            ctx.stop()

        result = await session.execute(select(ConfluenceLiveTrade))
        trades = result.scalars().all()
        assert len(trades) == 3  # the 4th signal was blocked by the concurrency gate
        assert worker._execution.buy.await_count == 3


@pytest.mark.asyncio
async def test_never_touches_real_trades_table(session, monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
    token = make_token()
    session.add(token)
    await session.flush()
    worker = make_worker(session)
    mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
    worker._execution.buy = AsyncMock(return_value=ExecutionResult(
        success=True, tx_signature="sig1", actual_price=Decimal("0.001"), actual_amount=Decimal("10000000"),
    ))
    session.add(make_signal(token, n_rules_cofiring=2))
    await session.flush()

    ctx = patched_session(session)
    try:
        await worker._maybe_enter()
    finally:
        ctx.stop()

    result = await session.execute(select(Trade))
    assert result.scalars().all() == []
    result = await session.execute(select(Token).where(Token.id == token.id))
    assert result.scalar_one().status == TokenStatus.WATCHING  # untouched


class TestEquityCache:
    """2026-09-24, real regression found live right after a restart: a
    failed equity read (RPC hiccup, or SOL_PRICE_USD not yet populated)
    used to get cached for the full TTL just like a successful one, turning
    one transient one-second hiccup into ~10s of the bot wrongly reporting
    equity as unknown and refusing to enter anything."""

    @pytest.mark.asyncio
    async def test_a_failed_read_is_not_cached(self, session, monkeypatch):
        worker = make_worker(session)
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 0.0)  # price not populated yet -> first read fails
        worker._execution.get_wallet_balance_sol = AsyncMock(return_value=Decimal("0.1"))
        assert await worker._get_equity_usd() is None
        assert worker._equity_cache is None  # the failure must NOT have been cached

        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)  # price now available
        equity = await worker._get_equity_usd()
        assert equity == Decimal("15.0")  # retried immediately, not stuck behind a 10s-old cached None

    @pytest.mark.asyncio
    async def test_a_successful_read_is_still_cached(self, session, monkeypatch):
        """The fix must not throw out caching altogether — only failures
        should skip it, or every cycle would hit the RPC needlessly."""
        worker = make_worker(session)
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)
        worker._execution.get_wallet_balance_sol = AsyncMock(return_value=Decimal("0.1"))
        first = await worker._get_equity_usd()
        assert first == Decimal("15.0")

        # Change the mock to prove the SECOND call reuses the cache rather than re-fetching.
        worker._execution.get_wallet_balance_sol = AsyncMock(return_value=Decimal("999"))
        second = await worker._get_equity_usd()
        assert second == Decimal("15.0")


class TestInAppNotifications:
    """2026-09-24: an in-app notification feed (models.orm.ConfluenceNotification),
    since Telegram alerting is still unconfigured. The one thing that MUST be
    right here isn't just "does a notification appear" — it's that halt-state
    notifications are edge-triggered, since _safe_to_enter() runs every ~1s
    and a naive implementation would write a row per second for as long as a
    halt lasts."""

    @pytest.mark.asyncio
    async def test_entry_filled_creates_info_notification(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
        token = make_token()
        session.add(token)
        await session.flush()
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
        worker._execution.buy = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="sig1", actual_price=Decimal("0.001"), actual_amount=Decimal("10000000"),
        ))
        session.add(make_signal(token, n_rules_cofiring=2))
        await session.flush()

        ctx = patched_session(session)
        try:
            await worker._maybe_enter()
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        entry_rows = [r for r in rows if r.event == "entry_filled"]
        assert len(entry_rows) == 1
        assert entry_rows[0].level == "info"
        assert token.symbol in entry_rows[0].message
        assert entry_rows[0].trade_id is not None

    @pytest.mark.asyncio
    async def test_entry_failed_creates_warning_notification(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
        token = make_token()
        session.add(token)
        await session.flush()
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
        worker._execution.buy = AsyncMock(return_value=ExecutionResult(
            success=False, error_type="SlippageExceeded", error_detail="price moved too fast",
        ))
        session.add(make_signal(token, n_rules_cofiring=2))
        await session.flush()

        ctx = patched_session(session)
        try:
            await worker._maybe_enter()
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        fail_rows = [r for r in rows if r.event == "entry_failed"]
        assert len(fail_rows) == 1
        assert fail_rows[0].level == "warning"
        assert "SlippageExceeded" in fail_rows[0].message

    @pytest.mark.asyncio
    async def test_exit_filled_win_creates_info_notification(self, session, monkeypatch):
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()
        worker = make_worker(session)
        worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="sellsig", actual_amount=Decimal("100000000"),
        ))
        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)  # loss-side price trigger, but SOL proceeds still net a gain
            monkeypatch.setattr(settings, "SOL_PRICE_USD", 150.0)  # 0.1 SOL * $150 = $15 > $10 cost
            await worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        exit_rows = [r for r in rows if r.event == "exit_filled"]
        assert len(exit_rows) == 1
        assert exit_rows[0].level == "info"  # +$5 pnl
        assert token.symbol in exit_rows[0].message

    @pytest.mark.asyncio
    async def test_exit_notification_shows_the_real_verified_pnl_not_the_stale_recorded_one(self, session, monkeypatch):
        """Real bug found live, 2026-09-28: the "Claude" trade recorded
        pnl_usd=-$0.0036 (rounds to "$-0.00") while its real, on-chain-
        verified P&L was -$0.037 — trade history correctly showed the real
        number (it was fixed for this back when real_pnl_usd was added),
        but the notification bar still built its message from the stale
        pnl_usd/pnl_pct fields, so the two disagreed about the same closed
        trade. Real numbers from that incident, reconstructed here."""
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(seconds=1), entry_price=Decimal("0.000069877099"),
            entry_token_lamports=10_000_000, position_usd=Decimal("0.705336"),
            entry_real_sol_lamports=-7_687_629, entry_network_fee_lamports=130_676,
        )
        session.add(trade)
        await session.flush()
        worker = make_worker(session)
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 117.0)
        # Real exit: 5,862,050 lamports back + a 1,513,840-lamport rent
        # reclaim -> real_pnl_usd ~ -$0.037, vs. the recorded pnl_usd
        # (computed from the intended swap amount only) rounding to $-0.00.
        worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="sellsig", actual_amount=Decimal("5862050"),
            actual_sol_lamports=5_862_050, network_fee_lamports=130_676,
            reclaim_tx_signature="reclaimsig", reclaim_sol_lamports=1_513_840,
        ))
        worker._check_liquidity_guard = AsyncMock(return_value=("LIQUIDITY_GUARD", trade.entry_price * Decimal("0.995")))
        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            await worker._maybe_exit(trade_dict, trade.entry_price * Decimal("0.995"), datetime.now(timezone.utc))
        finally:
            ctx.stop()

        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
        assert row.real_pnl_usd is not None
        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        exit_rows = [r for r in rows if r.event == "exit_filled"]
        assert len(exit_rows) == 1
        message = exit_rows[0].message
        # The message must reflect the REAL loss (~-$0.04), not the
        # stale recorded figure that rounds away to "$-0.00", and must be
        # marked as verified so it's visually distinguishable from a
        # recorded-only figure, matching trade history's own convention.
        assert "$-0.00" not in message
        assert "✓" in message
        assert str(round(float(row.real_pnl_usd), 2)) in message or f"{row.real_pnl_usd:+.2f}" in message

    @pytest.mark.asyncio
    async def test_permanently_halted_notifies_once_across_many_checks(self, session, monkeypatch):
        """The core regression this class exists to prevent: refreshing
        halt notifications repeatedly while permanently halted must write
        exactly ONE notification, not one per call."""
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MIN_TRADEABLE_USD", 1.0)
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=0.02)  # dust
        ctx = patched_session(session)
        try:
            for _ in range(5):
                await worker._refresh_halt_notifications(Decimal("0.02"), Decimal("0"))
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        halt_rows = [r for r in rows if r.event == "permanently_halted"]
        assert len(halt_rows) == 1
        assert halt_rows[0].level == "critical"

    @pytest.mark.asyncio
    async def test_max_loss_pct_halts_permanently_even_with_equity_remaining(self, session, monkeypatch):
        """2026-09-24, user's explicit instruction for the first live test:
        'set max loss to $3' (2026-09-28: converted to a 30%-of-baseline
        percentage — pinned here to a $10 baseline so the threshold is
        still exactly $3.00). This must trip even when the wallet still
        holds plenty of equity (e.g. topped up again) — it's independent of
        the dust-floor check, tracking cumulative REALIZED loss instead."""
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 0.30)
        import workers.confluence_live_worker as mod
        monkeypatch.setattr(mod, "DEPOSIT_USD", Decimal("10.0"))
        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            # Plenty of equity ($20) but $3.50 realized lost lifetime — must halt.
            await worker._refresh_halt_notifications(Decimal("20.0"), Decimal("0"), Decimal("-3.50"))
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        halt_rows = [r for r in rows if r.event == "permanently_halted"]
        assert len(halt_rows) == 1
        assert halt_rows[0].level == "critical"
        assert "3.00" in halt_rows[0].message  # names the configured limit, not just the number lost

    @pytest.mark.asyncio
    async def test_real_deposit_gap_halt_names_the_real_reason_not_dust_floor(self, session, monkeypatch):
        """Real bug found deploying the 2026-09-28 deposit-gap fix: a halt
        from THIS check was reported as 'wallet balance $4.35 is below the
        $1.00 minimum' — false on its face (4.35 > 1.00) since the message
        logic only knew about two of what are now three halt conditions.
        Real numbers from that incident."""
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 0.30)
        import workers.confluence_live_worker as mod
        monkeypatch.setattr(mod, "DEPOSIT_USD", Decimal("10.65"))
        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            # equity=$4.35, recorded all_time_pnl only -$1.21 (looks fine on
            # its own) — but real loss against the $10.65 deposit is $6.30,
            # well past the $3 cap.
            await worker._refresh_halt_notifications(Decimal("4.35"), Decimal("0"), Decimal("-1.21"))
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        halt_rows = [r for r in rows if r.event == "permanently_halted"]
        assert len(halt_rows) == 1
        message = halt_rows[0].message
        assert "below the $1.00 minimum" not in message  # the wrong, misleading reason
        assert "real loss" in message
        assert "10.65" in message  # names the real deposit, not just the limit

    @pytest.mark.asyncio
    async def test_max_loss_pct_does_not_halt_below_the_limit(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 0.30)
        import workers.confluence_live_worker as mod
        monkeypatch.setattr(mod, "DEPOSIT_USD", Decimal("10.0"))
        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            # $2.99 lost — under the $3 cap, must NOT halt.
            await worker._refresh_halt_notifications(Decimal("20.0"), Decimal("0"), Decimal("-2.99"))
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        assert not [r for r in rows if r.event == "permanently_halted"]

    @pytest.mark.asyncio
    async def test_safe_to_enter_blocked_by_max_loss_pct(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 0.30)
        token = make_token()
        session.add(token)
        await session.flush()
        session.add(ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="closed",
            entry_time=datetime.now(timezone.utc) - timedelta(hours=1), entry_price=Decimal("1.0"),
            position_usd=Decimal("10.0"), exit_time=datetime.now(timezone.utc) - timedelta(minutes=30),
            exit_price=Decimal("0.7"), exit_reason="STOP_LOSS", pnl_usd=Decimal("-3.50"),
        ))
        await session.flush()
        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=20.0)  # plenty of equity remaining
        ctx = patched_session(session)
        try:
            assert await worker._safe_to_enter() is False
        finally:
            ctx.stop()

    @pytest.mark.asyncio
    async def test_permanently_halted_recovery_notifies_info_once(self, session, monkeypatch):
        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            await worker._refresh_halt_notifications(Decimal("0.02"), Decimal("0"))  # dust — halted
            await worker._refresh_halt_notifications(Decimal("0.02"), Decimal("0"))  # still halted, no re-notify
            await worker._refresh_halt_notifications(Decimal("15.0"), Decimal("0"))  # funded — recovers
            await worker._refresh_halt_notifications(Decimal("15.0"), Decimal("0"))  # still funded, no re-notify
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        halted = [r for r in rows if r.event == "permanently_halted"]
        recovered = [r for r in rows if r.event == "wallet_funded"]
        assert len(halted) == 1
        assert len(recovered) == 1
        assert recovered[0].level == "info"

    @pytest.mark.asyncio
    async def test_first_check_after_restart_does_not_fire_spurious_recovery(self, session, monkeypatch):
        """Real bug found 2026-09-24 during a code audit: _last_permanently_halted
        and _last_daily_halted both start as None, and `False != None` is True,
        so the very first _refresh_halt_notifications() call after ANY worker
        restart looked like a state transition even when nothing had changed —
        firing a fake 'Wallet funded' / 'Daily loss limit cleared' notification
        on every deploy. A brand-new worker checking an already-healthy account
        for the first time must stay silent."""
        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            # Never halted, never checked before — this is the first read.
            await worker._refresh_halt_notifications(Decimal("15.0"), Decimal("0"))
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        assert not [r for r in rows if r.event == "wallet_funded"]
        assert not [r for r in rows if r.event == "daily_loss_limit_cleared"]

    @pytest.mark.asyncio
    async def test_first_check_still_fires_if_already_halted(self, session, monkeypatch):
        """The fix must not suppress a genuinely bad first read — only the
        'recovered' side is dangerous to fire spuriously."""
        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            await worker._refresh_halt_notifications(Decimal("0.02"), Decimal("0"))  # dust, first-ever check
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        assert len([r for r in rows if r.event == "permanently_halted"]) == 1

    @pytest.mark.asyncio
    async def test_recovery_still_fires_for_a_real_transition_after_first_check(self, session, monkeypatch):
        """A real recovery observed later in the SAME process's lifetime must
        still notify — the fix only suppresses the first read after restart."""
        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            await worker._refresh_halt_notifications(Decimal("15.0"), Decimal("0"))  # first check, healthy, silent
            await worker._refresh_halt_notifications(Decimal("0.02"), Decimal("0"))  # real halt
            await worker._refresh_halt_notifications(Decimal("15.0"), Decimal("0"))  # real recovery
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        funded = [r for r in rows if r.event == "wallet_funded"]
        halted = [r for r in rows if r.event == "permanently_halted"]
        assert len(halted) == 1
        assert len(funded) == 1

    @pytest.mark.asyncio
    async def test_halt_notifications_fire_even_while_trading_is_disabled(self, session, monkeypatch):
        """Real regression, found live 2026-09-24: a deposit landed while
        the user had manually paused trading (CONFLUENCE_LIVE_ENABLED=False)
        and produced zero notification, because the check used to live
        entirely inside _safe_to_enter(), which _cycle() only calls when
        ENABLED is True. _refresh_halt_notifications() must fire regardless
        — it's informational, not an entry decision."""
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", False)
        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            await worker._refresh_halt_notifications(Decimal("0.02"), Decimal("0"))  # dust — halted
            await worker._refresh_halt_notifications(Decimal("15.0"), Decimal("0"))  # funded — recovers
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        assert len([r for r in rows if r.event == "permanently_halted"]) == 1
        funded = [r for r in rows if r.event == "wallet_funded"]
        assert len(funded) == 1
        assert "PAUSED" in funded[0].message  # message reflects the real armed/paused state

    @pytest.mark.asyncio
    async def test_first_check_does_not_fire_spurious_daily_recovery(self, session, monkeypatch):
        """Same restart bug, daily-halt side: a fresh worker's first-ever
        check on a day that is NOT loss-limited must not announce
        'daily_loss_limit_cleared' — nothing actually cleared, it was never set."""
        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            await worker._refresh_halt_notifications(Decimal("15.0"), Decimal("0"))
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        assert not [r for r in rows if r.event == "daily_loss_limit_cleared"]

    @pytest.mark.asyncio
    async def test_daily_halted_notifies_once_across_many_checks(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_DAILY_LOSS_LIMIT_PCT", 0.5)
        # 2026-09-28: is_permanently_halted() now also checks equity against
        # the real DEPOSIT_USD constant — this test's equity=$4 dummy value
        # would otherwise collide with that real-world number and trip a
        # permanent halt before daily-halt logic (what this test actually
        # covers) ever runs. The max allowed pct (100%, the field's own
        # `le=1` ceiling) isolates the two checks the same way the old
        # $1000 ceiling did.
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 1.0)
        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            for _ in range(5):
                # equity=4 (started at $10, -$6 lost today), 50% limit = -$5, crossed
                await worker._refresh_halt_notifications(Decimal("4.0"), Decimal("-6.0"))
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        halt_rows = [r for r in rows if r.event == "daily_loss_limit_hit"]
        assert len(halt_rows) == 1
        assert halt_rows[0].level == "warning"

    @pytest.mark.asyncio
    async def test_stuck_exit_notifies_once_not_every_cycle(self, session, monkeypatch):
        """A sell that keeps failing gets re-attempted every ~1s cycle by
        design (module docstring) — the notification must not repeat for
        the same trade every time, or a token with dried-up liquidity would
        flood the feed for as long as it stays stuck."""
        token = make_token()
        session.add(token)
        await session.flush()
        trade = ConfluenceLiveTrade(
            token_id=token.id, n_rules_cofiring=2, status="open",
            entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), entry_price=Decimal("1.00"),
            entry_token_lamports=10_000_000, position_usd=Decimal("10.0"),
        )
        session.add(trade)
        await session.flush()
        worker = make_worker(session)
        worker._execution.sell = AsyncMock(return_value=ExecutionResult(
            success=False, error_type="SELL_FAILED_CRITICAL", error_detail="no route found",
        ))
        trade_dict = dict(
            id=trade.id, mint=token.mint_address, symbol=token.symbol,
            entry_price=trade.entry_price, entry_time=trade.entry_time,
            entry_token_lamports=trade.entry_token_lamports, position_usd=trade.position_usd,
        )

        ctx = patched_session(session)
        try:
            floor = Decimal(str(settings.HARD_FLOOR_PCT))
            exit_price = trade.entry_price * (1 + floor)
            # Simulate 3 consecutive ~1s cycles all failing to sell the same trade.
            for _ in range(3):
                await worker._maybe_exit(trade_dict, exit_price, datetime.now(timezone.utc))
        finally:
            ctx.stop()

        rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
        stuck_rows = [r for r in rows if r.event == "exit_failed_critical"]
        assert len(stuck_rows) == 1
        assert stuck_rows[0].level == "critical"


class TestCircuitBreaker:
    """
    2026-09-30 — real incident: 5 real trades fired within ~60 seconds, all
    LIQUIDITY_GUARD losses, with nothing in the pipeline able to stop it
    after the 3rd. CircuitBreakerState already existed in the schema (built
    for the pre-2026-09-23 pipeline) but confluence_live_worker.py never
    read or wrote it — re-wired here rather than building a new mechanism.
    """

    @pytest.mark.asyncio
    async def test_trips_after_configured_consecutive_losses(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CB_LOSS_COUNT", 3)
        monkeypatch.setattr(settings, "CB_PAUSE_MINUTES", 60)
        session.add(CircuitBreakerState(id=1))
        await session.flush()

        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            for _ in range(2):
                await worker._record_trade_outcome_for_circuit_breaker(Decimal("-0.30"))
            assert await worker._circuit_breaker_paused() is False  # only 2 so far

            await worker._record_trade_outcome_for_circuit_breaker(Decimal("-0.30"))  # 3rd
            assert await worker._circuit_breaker_paused() is True
        finally:
            ctx.stop()

        cb = (await session.execute(select(CircuitBreakerState).where(CircuitBreakerState.id == 1))).scalar_one()
        assert cb.consecutive_losses == 3
        assert cb.is_paused is True
        assert cb.resume_at is not None

    @pytest.mark.asyncio
    async def test_a_win_resets_the_streak(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CB_LOSS_COUNT", 3)
        session.add(CircuitBreakerState(id=1))
        await session.flush()

        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            await worker._record_trade_outcome_for_circuit_breaker(Decimal("-0.30"))
            await worker._record_trade_outcome_for_circuit_breaker(Decimal("-0.30"))
            await worker._record_trade_outcome_for_circuit_breaker(Decimal("+0.10"))  # win — resets
            await worker._record_trade_outcome_for_circuit_breaker(Decimal("-0.30"))
            assert await worker._circuit_breaker_paused() is False  # only 1 since the win
        finally:
            ctx.stop()

    @pytest.mark.asyncio
    async def test_unresolved_pnl_does_not_affect_the_streak(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CB_LOSS_COUNT", 3)
        session.add(CircuitBreakerState(id=1))
        await session.flush()

        worker = make_worker(session)
        ctx = patched_session(session)
        try:
            await worker._record_trade_outcome_for_circuit_breaker(Decimal("-0.30"))
            await worker._record_trade_outcome_for_circuit_breaker(None)  # unknown pnl, permissive
            await worker._record_trade_outcome_for_circuit_breaker(Decimal("-0.30"))
            assert await worker._circuit_breaker_paused() is False  # still only 2 real losses
        finally:
            ctx.stop()

    @pytest.mark.asyncio
    async def test_paused_worker_cannot_enter(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
        session.add(CircuitBreakerState(
            id=1, consecutive_losses=3, is_paused=True,
            pause_started_at=datetime.now(timezone.utc),
            resume_at=datetime.now(timezone.utc) + timedelta(minutes=60),
        ))
        await session.flush()

        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
        ctx = patched_session(session)
        try:
            assert await worker._safe_to_enter() is False
        finally:
            ctx.stop()

    @pytest.mark.asyncio
    async def test_auto_resumes_once_cooldown_has_elapsed(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_ENABLED", True)
        session.add(CircuitBreakerState(
            id=1, consecutive_losses=3, is_paused=True,
            pause_started_at=datetime.now(timezone.utc) - timedelta(minutes=61),
            resume_at=datetime.now(timezone.utc) - timedelta(minutes=1),  # already elapsed
        ))
        await session.flush()

        worker = make_worker(session)
        mock_wallet_balance(worker, monkeypatch, equity_usd=10.0)
        ctx = patched_session(session)
        try:
            assert await worker._circuit_breaker_paused() is False
            assert await worker._safe_to_enter() is True
        finally:
            ctx.stop()

        cb = (await session.execute(select(CircuitBreakerState).where(CircuitBreakerState.id == 1))).scalar_one()
        assert cb.is_paused is False
        assert cb.resume_at is None
        assert cb.consecutive_losses == 0  # clean slate, matching the pre-2026-09-23 semantics
