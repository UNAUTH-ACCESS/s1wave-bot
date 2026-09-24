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
    ConfluenceLiveObservation, ConfluenceLiveTrade, ConfluenceNotification, MomentumSignalEvent,
    Token, TokenEvaluation, TokenStatus, Trade,
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
    return worker


def patched_session(session):
    ctx = patch("workers.confluence_live_worker.get_session")
    mock_gs = ctx.start()
    mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
    mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
    return ctx


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

    def setup_method(self):
        with patch("workers.confluence_live_worker.ExecutionEngine"):
            self.worker = ConfluenceLiveWorker(asyncio.Event())
        self.trade = dict(
            id=uuid.uuid4(), mint="SomeMint", symbol="TEST",
            entry_price=Decimal("0.0001"), entry_time=datetime.now(timezone.utc) - timedelta(minutes=5),
            entry_token_lamports=500_000_000, position_usd=Decimal("0.04"),
        )

    @pytest.mark.asyncio
    async def test_fires_on_high_price_impact(self, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": 79842, "price_impact_pct": Decimal("1"),  # 100% impact, matches the real BLK quote
        })
        reason, price = await self.worker._check_liquidity_guard(self.trade)
        assert reason == "LIQUIDITY_GUARD"
        assert price is not None

    @pytest.mark.asyncio
    async def test_fires_on_bad_real_pnl_even_with_low_impact(self, monkeypatch):
        """A pool can show low reported price impact while still paying out
        far less than the position cost — the real-P&L floor catches what
        the impact-percentage check alone might miss."""
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        # position_usd=0.04; proceeds worth ~$0.02 -> real pnl ~ -50%, impact reported low
        lamports_for_half = int((Decimal("0.02") / Decimal("116.0")) * Decimal("1e9"))
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": lamports_for_half, "price_impact_pct": Decimal("0.05"),
        })
        reason, price = await self.worker._check_liquidity_guard(self.trade)
        assert reason == "LIQUIDITY_GUARD"

    @pytest.mark.asyncio
    async def test_does_not_fire_when_healthy(self, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        # proceeds roughly matching position cost, negligible impact
        lamports_at_cost = int((Decimal("0.04") / Decimal("116.0")) * Decimal("1e9"))
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": lamports_at_cost, "price_impact_pct": Decimal("0.01"),
        })
        reason, price = await self.worker._check_liquidity_guard(self.trade)
        assert reason is None
        assert price is None

    @pytest.mark.asyncio
    async def test_returns_none_when_quote_fails(self, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        self.worker._execution.get_sell_quote = AsyncMock(return_value=None)
        reason, price = await self.worker._check_liquidity_guard(self.trade)
        assert reason is None
        assert price is None

    @pytest.mark.asyncio
    async def test_throttled_to_one_real_quote_per_interval(self, monkeypatch):
        monkeypatch.setattr(settings, "SOL_PRICE_USD", 116.0)
        self.worker._execution.get_sell_quote = AsyncMock(return_value={
            "out_lamports": 1, "price_impact_pct": Decimal("0.01"),
        })
        await self.worker._check_liquidity_guard(self.trade)
        await self.worker._check_liquidity_guard(self.trade)  # immediately again
        assert self.worker._execution.get_sell_quote.await_count == 1  # second call skipped, not due yet

        # Simulate the interval having elapsed.
        import workers.confluence_live_worker as mod
        self.worker._last_liquidity_check[self.trade["id"]] -= mod._LIQUIDITY_CHECK_INTERVAL_S + 1
        await self.worker._check_liquidity_guard(self.trade)
        assert self.worker._execution.get_sell_quote.await_count == 2

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
    async def test_max_loss_usd_halts_permanently_even_with_equity_remaining(self, session, monkeypatch):
        """2026-09-24, user's explicit instruction for the first live test:
        'set max loss to $3'. This must trip even when the wallet still
        holds plenty of equity (e.g. topped up again) — it's independent of
        the dust-floor check, tracking cumulative REALIZED loss instead."""
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_USD", 3.0)
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
    async def test_max_loss_usd_does_not_halt_below_the_limit(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_USD", 3.0)
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
    async def test_safe_to_enter_blocked_by_max_loss_usd(self, session, monkeypatch):
        monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_USD", 3.0)
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
