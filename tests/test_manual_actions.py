"""
tests/test_manual_actions.py
===============================
engine/manual_actions.py's close_trade_manually() — the dashboard's real,
on-chain "Close Position" action (2026-09-24). Extracted after this
session's operator manually closed 5 real positions via one-off scripts;
this is the reusable, tested version those scripts became.

Coverage:
  1. Happy path: sells the full recorded amount, marks the trade closed
     with exit_reason='MANUAL_CLOSE', computes pnl_usd correctly, and
     records a notification.
  2. Trade not found.
  3. Trade already closed — never re-sells or re-records.
  4. No entry_token_lamports recorded — cannot size a sell.
  5. The sell itself fails — trade stays open, real error surfaced, no
     fabricated DB write.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from engine.execution import ExecutionResult
from engine.manual_actions import close_trade_manually
from engine.sell_coordination import finish_sell, try_start_sell
from models.orm import ConfluenceLiveTrade, ConfluenceNotification, Token, TokenStatus


class session_cm:
    """Wraps an already-open test session as an async context manager, so
    engine.manual_actions's two separate `async with get_session() as
    session:` blocks both reuse the SAME session/transaction the test set
    up — matching how tests/test_confluence_live_worker.py patches
    workers.confluence_live_worker.get_session."""
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


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


@pytest.mark.asyncio
async def test_closes_a_real_open_position_and_records_pnl(session):
    token = make_token(symbol="SI")
    session.add(token)
    await session.flush()
    trade = ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="open",
        entry_time=datetime.now(timezone.utc) - timedelta(hours=1), entry_price=Decimal("0.0001"),
        entry_token_lamports=500_000_000, position_usd=Decimal("0.05"),
    )
    session.add(trade)
    await session.flush()
    trade_id = str(trade.id)

    with patch("engine.manual_actions.get_session", return_value=session_cm(session)), \
         patch("engine.manual_actions.ExecutionEngine") as MockEngine:
        MockEngine.return_value.get_token_balance_raw = AsyncMock(return_value=(500_000_000, 6))
        MockEngine.return_value.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="closesig", actual_amount=Decimal("400000"),  # 0.0004 SOL back
        ))
        result = await close_trade_manually(trade_id, sol_price_usd=Decimal("116.0"))

    assert result.success is True
    assert result.symbol == "SI"
    assert result.tx_signature == "closesig"
    # proceeds = 0.0004 SOL * $116 = $0.0464; cost was $0.05 -> -$0.0036
    assert result.pnl_usd == Decimal("0.0004") * Decimal("116.0") - Decimal("0.05")

    row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
    # exit_price in real USD-per-whole-token terms: proceeds_usd / (raw_amount / 10**decimals)
    expected_exit_price = (Decimal("0.0004") * Decimal("116.0")) / (Decimal("500000000") / Decimal(10 ** 6))
    assert row.exit_price == expected_exit_price


@pytest.mark.asyncio
async def test_falls_back_to_documented_wrong_unit_price_when_balance_lookup_fails(session):
    """If the pre-sell balance lookup fails (rare), exit_price falls back to
    execution.py's documented sell-side actual_price rather than losing the
    field — pnl_usd is unaffected either way since it never depends on
    exit_price."""
    token = make_token(symbol="SI")
    session.add(token)
    await session.flush()
    trade = ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="open",
        entry_time=datetime.now(timezone.utc) - timedelta(hours=1), entry_price=Decimal("0.0001"),
        entry_token_lamports=500_000_000, position_usd=Decimal("0.05"),
    )
    session.add(trade)
    await session.flush()

    with patch("engine.manual_actions.get_session", return_value=session_cm(session)), \
         patch("engine.manual_actions.ExecutionEngine") as MockEngine:
        MockEngine.return_value.get_token_balance_raw = AsyncMock(return_value=None)
        MockEngine.return_value.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="closesig", actual_amount=Decimal("400000"), actual_price=Decimal("0.0008"),
        ))
        result = await close_trade_manually(str(trade.id), sol_price_usd=Decimal("116.0"))

    assert result.success is True
    row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
    assert row.exit_price == Decimal("0.0008")

    row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
    assert row.status == "closed"
    assert row.exit_reason == "MANUAL_CLOSE"
    assert row.exit_tx_signature == "closesig"
    assert row.pnl_usd == result.pnl_usd

    notif = (await session.execute(select(ConfluenceNotification).where(ConfluenceNotification.trade_id == trade.id))).scalar_one_or_none()
    assert notif is not None
    assert "SI" in notif.message


@pytest.mark.asyncio
async def test_trade_not_found_returns_clean_error(session):
    with patch("engine.manual_actions.get_session", return_value=session_cm(session)):
        result = await close_trade_manually(str(uuid.uuid4()), sol_price_usd=Decimal("116.0"))
    assert result.success is False
    assert "not found" in result.error.lower()


@pytest.mark.asyncio
async def test_already_closed_trade_is_never_resold(session):
    token = make_token()
    session.add(token)
    await session.flush()
    trade = ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="closed",
        entry_time=datetime.now(timezone.utc) - timedelta(hours=1), entry_price=Decimal("0.0001"),
        entry_token_lamports=500_000_000, position_usd=Decimal("0.05"),
        exit_time=datetime.now(timezone.utc), exit_reason="HARD_FLOOR", pnl_usd=Decimal("-0.01"),
    )
    session.add(trade)
    await session.flush()

    with patch("engine.manual_actions.get_session", return_value=session_cm(session)), \
         patch("engine.manual_actions.ExecutionEngine") as MockEngine:
        MockEngine.return_value.sell = AsyncMock(side_effect=AssertionError("must not sell an already-closed trade"))
        result = await close_trade_manually(str(trade.id), sol_price_usd=Decimal("116.0"))

    assert result.success is False
    assert "closed" in result.error.lower()


@pytest.mark.asyncio
async def test_no_token_amount_recorded_refuses_cleanly(session):
    token = make_token()
    session.add(token)
    await session.flush()
    trade = ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="open",
        entry_time=datetime.now(timezone.utc) - timedelta(hours=1), entry_price=Decimal("0.0001"),
        entry_token_lamports=None, position_usd=Decimal("0.05"),
    )
    session.add(trade)
    await session.flush()

    with patch("engine.manual_actions.get_session", return_value=session_cm(session)), \
         patch("engine.manual_actions.ExecutionEngine") as MockEngine:
        MockEngine.return_value.sell = AsyncMock(side_effect=AssertionError("must not attempt a sell with no sizing"))
        result = await close_trade_manually(str(trade.id), sol_price_usd=Decimal("116.0"))

    assert result.success is False
    assert "token amount" in result.error.lower()


@pytest.mark.asyncio
async def test_sell_failure_leaves_trade_open_with_real_error(session):
    token = make_token()
    session.add(token)
    await session.flush()
    trade = ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="open",
        entry_time=datetime.now(timezone.utc) - timedelta(hours=1), entry_price=Decimal("0.0001"),
        entry_token_lamports=500_000_000, position_usd=Decimal("0.05"),
    )
    session.add(trade)
    await session.flush()

    with patch("engine.manual_actions.get_session", return_value=session_cm(session)), \
         patch("engine.manual_actions.ExecutionEngine") as MockEngine:
        MockEngine.return_value.get_token_balance_raw = AsyncMock(return_value=(500_000_000, 6))
        MockEngine.return_value.sell = AsyncMock(return_value=ExecutionResult(
            success=False, error_type="SELL_FAILED_CRITICAL", error_detail="no route",
        ))
        result = await close_trade_manually(str(trade.id), sol_price_usd=Decimal("116.0"))

    assert result.success is False
    assert "SELL_FAILED_CRITICAL" in result.error

    row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
    assert row.status == "open"  # never fabricated a close


@pytest.mark.asyncio
async def test_refuses_to_double_sell_when_the_worker_is_already_closing_it(session):
    """Real incident, 2026-09-24 (user: 'there are positions active but
    cant close there'): confluence_live_worker.py's own automatic exit
    loop and this exact function both tried to sell the same trade within
    the same second, and whichever landed second was rejected on-chain.
    Simulates the worker already holding the lock for this trade — the
    manual close must refuse cleanly, never call ExecutionEngine.sell()
    (which would submit a second real, competing swap), and leave the
    trade untouched for the in-flight sell to finish on its own."""
    token = make_token()
    session.add(token)
    await session.flush()
    trade = ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="open",
        entry_time=datetime.now(timezone.utc) - timedelta(hours=1), entry_price=Decimal("0.0001"),
        entry_token_lamports=500_000_000, position_usd=Decimal("0.05"),
    )
    session.add(trade)
    await session.flush()
    trade_id = str(trade.id)

    assert try_start_sell(trade_id) is True  # the worker's loop "got there first"
    try:
        with patch("engine.manual_actions.get_session", return_value=session_cm(session)), \
             patch("engine.manual_actions.ExecutionEngine") as MockEngine:
            MockEngine.return_value.sell = AsyncMock(
                side_effect=AssertionError("must not submit a second, competing sell")
            )
            result = await close_trade_manually(trade_id, sol_price_usd=Decimal("116.0"))
    finally:
        finish_sell(trade_id)

    assert result.success is False
    assert "already closing" in result.error.lower()

    row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade.id))).scalar_one()
    assert row.status == "open"  # untouched — the in-flight sell owns this trade now


@pytest.mark.asyncio
async def test_releases_the_lock_after_a_successful_close(session):
    """The lock must not leak — a later close attempt (e.g. a retry after
    a transient failure) must be able to proceed once this one finishes."""
    token = make_token()
    session.add(token)
    await session.flush()
    trade = ConfluenceLiveTrade(
        token_id=token.id, n_rules_cofiring=2, status="open",
        entry_time=datetime.now(timezone.utc) - timedelta(hours=1), entry_price=Decimal("0.0001"),
        entry_token_lamports=500_000_000, position_usd=Decimal("0.05"),
    )
    session.add(trade)
    await session.flush()
    trade_id = str(trade.id)

    with patch("engine.manual_actions.get_session", return_value=session_cm(session)), \
         patch("engine.manual_actions.ExecutionEngine") as MockEngine:
        MockEngine.return_value.get_token_balance_raw = AsyncMock(return_value=(500_000_000, 6))
        MockEngine.return_value.sell = AsyncMock(return_value=ExecutionResult(
            success=True, tx_signature="closesig", actual_amount=Decimal("400000"),
        ))
        result = await close_trade_manually(trade_id, sol_price_usd=Decimal("116.0"))

    assert result.success is True
    assert try_start_sell(trade_id) is True  # lock was released, not leaked
    finish_sell(trade_id)
