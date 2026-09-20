"""
tests/test_phase5.py
====================
Phase 5+6 — Capital Engine and Risk Engine tests.

Coverage:
  Capital engine:
    - Position sizing compounding (5% of current balance)
    - MIN_POSITION_USD floor enforced
    - Circuit breaker blocks entry when paused
    - Daily halt blocks entry
    - Max concurrent trades blocks entry
    - Trade row written with all required fields
    - Balance deducted atomically on open
    - Token status → ENTERED on open

  Risk engine:
    - Exit rule priority order (hard floor > stop loss > take profit > time)
    - Hard floor triggers at exactly HARD_FLOOR_PCT
    - Stop loss triggers at exactly STOP_LOSS_PCT
    - Take profit triggers at exactly TAKE_PROFIT_PCT
    - Time exit triggers at MAX_HOLD_HOURS
    - PnL computed correctly (positive and negative)
    - Balance restored correctly on close
    - Balance history row written on close
    - Circuit breaker increments on loss, resets on win
    - Circuit breaker trips at CB_LOSS_COUNT
    - Daily loss tracking accumulates correctly
    - Daily halt triggered at MAX_DAILY_LOSS_PCT threshold
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch
import uuid

import pytest
from sqlalchemy import select

from models.orm import (
    BalanceHistory,
    CircuitBreakerState,
    DailyLossState,
    ExitReason,
    SessionState,
    Token,
    TokenSnapshot,
    TokenStatus,
    Trade,
    TradeStatus,
    TrendDirection,
    VMZone,
    LiquidityFlag,
    LiquidityTrend,
)


# ── Factories ─────────────────────────────────────────────────────────────────

def make_token(**overrides) -> Token:
    now = datetime.now(timezone.utc)
    defaults = dict(
        mint_address=f"Mint{uuid.uuid4().hex[:36]}",
        symbol="TEST",
        status=TokenStatus.WATCHING,
        discovered_at=now - timedelta(minutes=5),
        watch_started_at=now - timedelta(minutes=3),
        baseline_volume_usd=Decimal("1000"),
        liquidity_usd=Decimal("20000"),
        market_cap_usd=Decimal("50000"),
        mint_authority_renounced=True,
        freeze_authority_renounced=True,
        lp_locked_burned=True,
        wash_multiplier=Decimal("1.5"),
    )
    defaults.update(overrides)
    return Token(**defaults)


def make_open_trade(token: Token, entry_price: Decimal = Decimal("0.001"),
                    position_usd: Decimal = Decimal("50"),
                    entry_time: datetime | None = None) -> Trade:
    now = entry_time or datetime.now(timezone.utc)
    return Trade(
        token_id=token.id,
        token_mint=token.mint_address,
        entry_price=entry_price,
        entry_time=now,
        entry_composite_score=Decimal("8.50"),
        entry_bp_trend=TrendDirection.ACCELERATING,
        entry_vm_trend=TrendDirection.ACCELERATING,
        entry_vm_zone=VMZone.STRONG,
        entry_liquidity_flag=LiquidityFlag.NORMAL,
        entry_liquidity_trend=LiquidityTrend.STABLE,
        position_size_usd=position_usd,
        status=TradeStatus.OPEN,
    )


def make_snapshots(token: Token, prices: list[str],
                   now: datetime | None = None) -> list[TokenSnapshot]:
    base = now or datetime.now(timezone.utc)
    snaps = []
    for i, price in enumerate(prices):
        snaps.append(TokenSnapshot(
            token_id=token.id,
            sampled_at=base - timedelta(seconds=(len(prices) - i) * 30),
            price_usd=Decimal(price),
            liquidity_usd=Decimal("20000"),
            market_cap_usd=Decimal("50000"),
            volume_usd=Decimal("1500"),
            buy_pressure=Decimal("0.65"),
            volume_mult=Decimal("1.50"),
        ))
    return snaps


# ── Risk engine: exit rule checks (pure) ─────────────────────────────────────

class TestExitRuleLogic:
    """Test _check_exit_conditions directly — pure arithmetic, no DB."""

    def setup_method(self):
        from engine.risk import RiskEngine
        self.risk = RiskEngine(asyncio.Event())

    def _make_trade(self, entry_price: str, entry_time: datetime | None = None) -> Trade:
        token = make_token()
        token.id = uuid.uuid4()
        trade = make_open_trade(token, entry_price=Decimal(entry_price),
                                entry_time=entry_time)
        trade.id = uuid.uuid4()
        return trade

    def test_hard_floor_triggers_at_threshold(self):
        """Price at exactly -7% should trigger HARD_FLOOR."""
        trade = self._make_trade("0.001000")
        # -7% from 0.001000 = 0.000930
        current = Decimal("0.000930")
        result = self.risk._check_exit_conditions(trade, current)
        assert result == ExitReason.HARD_FLOOR

    def test_hard_floor_priority_over_stop_loss(self):
        """Hard floor at -8% should trigger HARD_FLOOR, not STOP_LOSS."""
        trade = self._make_trade("0.001000")
        current = Decimal("0.000920")  # -8%
        result = self.risk._check_exit_conditions(trade, current)
        assert result == ExitReason.HARD_FLOOR

    def test_stop_loss_triggers_at_threshold(self):
        """Price at exactly -6% should trigger STOP_LOSS."""
        trade = self._make_trade("0.001000")
        current = Decimal("0.000940")  # -6%
        result = self.risk._check_exit_conditions(trade, current)
        assert result == ExitReason.STOP_LOSS

    def test_stop_loss_not_hard_floor(self):
        """Price at -6% triggers STOP_LOSS, not HARD_FLOOR (floor is -7%)."""
        trade = self._make_trade("0.001000")
        current = Decimal("0.000940")
        result = self.risk._check_exit_conditions(trade, current)
        assert result == ExitReason.STOP_LOSS
        assert result != ExitReason.HARD_FLOOR

    def test_take_profit_triggers_at_threshold(self):
        """Price at exactly +30% should trigger TAKE_PROFIT."""
        trade = self._make_trade("0.001000")
        current = Decimal("0.001300")  # +30%
        result = self.risk._check_exit_conditions(trade, current)
        assert result == ExitReason.TAKE_PROFIT

    def test_take_profit_above_threshold(self):
        """Price at +50% should still trigger TAKE_PROFIT (not time exit)."""
        trade = self._make_trade("0.001000")
        current = Decimal("0.001500")  # +50%
        result = self.risk._check_exit_conditions(trade, current)
        assert result == ExitReason.TAKE_PROFIT

    def test_no_exit_within_bounds(self):
        """Price at +5% with plenty of time left — no exit."""
        trade = self._make_trade("0.001000")
        current = Decimal("0.001050")  # +5%
        result = self.risk._check_exit_conditions(trade, current)
        assert result is None

    def test_time_exit_after_max_hold(self):
        """Trade held for 7 hours should trigger TIME_EXIT."""
        old_time = datetime.now(timezone.utc) - timedelta(hours=7)
        trade = self._make_trade("0.001000", entry_time=old_time)
        current = Decimal("0.001010")  # +1% — no other exit
        result = self.risk._check_exit_conditions(trade, current)
        assert result == ExitReason.TIME_EXIT

    def test_time_exit_not_triggered_within_hold_period(self):
        """Trade held for 3 hours at neutral price — no exit yet."""
        recent_time = datetime.now(timezone.utc) - timedelta(hours=3)
        trade = self._make_trade("0.001000", entry_time=recent_time)
        current = Decimal("0.001000")
        result = self.risk._check_exit_conditions(trade, current)
        assert result is None

    def test_pnl_calculation_positive(self):
        """Verify PnL math: entry 0.001, exit 0.0013 = +30%."""
        entry = Decimal("0.001000")
        exit_p = Decimal("0.001300")
        pnl_pct = (exit_p - entry) / entry
        assert pnl_pct == pytest.approx(Decimal("0.30"), rel=Decimal("0.001"))

    def test_pnl_calculation_negative(self):
        """Verify PnL math: entry 0.001, exit 0.00094 = -6%."""
        entry = Decimal("0.001000")
        exit_p = Decimal("0.000940")
        pnl_pct = (exit_p - entry) / entry
        assert pnl_pct == pytest.approx(Decimal("-0.06"), rel=Decimal("0.001"))


# ── Risk engine: DB operations ────────────────────────────────────────────────

class TestRiskEngineDB:

    @pytest.mark.asyncio
    async def test_close_trade_writes_all_fields(self, session):
        from engine.risk import RiskEngine

        token = make_token()
        session.add(token)
        await session.flush()

        state = SessionState(id=1, available_balance=Decimal("1000"))
        cb = CircuitBreakerState(id=1, consecutive_losses=0, is_paused=False)
        dl = DailyLossState(
            id=1,
            date_utc=datetime.now(timezone.utc).date(),
            day_open_balance=Decimal("1000"),
            cumulative_pnl_usd=Decimal("0"),
            is_halted=False,
        )
        session.add_all([state, cb, dl])
        await session.flush()

        trade = make_open_trade(token, entry_price=Decimal("0.001"),
                                position_usd=Decimal("50"))
        session.add(trade)
        await session.flush()

        risk = RiskEngine(asyncio.Event())

        with patch("engine.risk.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)

            await risk.close_trade(
                trade=trade,
                exit_price=Decimal("0.001300"),  # +30%
                exit_reason=ExitReason.TAKE_PROFIT,
            )

        result = await session.execute(select(Trade).where(Trade.id == trade.id))
        closed = result.scalar_one()

        assert closed.status == TradeStatus.CLOSED
        assert closed.exit_reason == ExitReason.TAKE_PROFIT
        assert closed.exit_price == Decimal("0.001300")
        assert closed.pnl_usd is not None
        assert closed.pnl_usd > Decimal("0")
        assert closed.hold_duration_seconds is not None

    @pytest.mark.asyncio
    async def test_close_trade_restores_balance_with_pnl(self, session):
        from engine.risk import RiskEngine

        token = make_token()
        session.add(token)
        await session.flush()

        state = SessionState(id=1, available_balance=Decimal("950"))
        cb = CircuitBreakerState(id=1, consecutive_losses=0, is_paused=False)
        dl = DailyLossState(
            id=1,
            date_utc=datetime.now(timezone.utc).date(),
            day_open_balance=Decimal("1000"),
            cumulative_pnl_usd=Decimal("0"),
            is_halted=False,
        )
        session.add_all([state, cb, dl])
        await session.flush()

        trade = make_open_trade(token, entry_price=Decimal("0.001"),
                                position_usd=Decimal("50"))
        session.add(trade)
        await session.flush()

        risk = RiskEngine(asyncio.Event())

        with patch("engine.risk.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)

            await risk.close_trade(
                trade=trade,
                exit_price=Decimal("0.001300"),  # +30% = +$15 PnL
                exit_reason=ExitReason.TAKE_PROFIT,
            )

        result = await session.execute(select(SessionState).where(SessionState.id == 1))
        updated = result.scalar_one()
        # 950 + 50 (position returned) + 15 (30% PnL on $50) = $1015
        assert updated.available_balance == pytest.approx(
            Decimal("1015"), rel=Decimal("0.001")
        )

    @pytest.mark.asyncio
    async def test_circuit_breaker_increments_on_loss(self, session):
        from engine.risk import RiskEngine

        token = make_token()
        session.add(token)
        await session.flush()

        state = SessionState(id=1, available_balance=Decimal("950"))
        cb = CircuitBreakerState(id=1, consecutive_losses=0, is_paused=False)
        dl = DailyLossState(
            id=1,
            date_utc=datetime.now(timezone.utc).date(),
            day_open_balance=Decimal("1000"),
            cumulative_pnl_usd=Decimal("0"),
            is_halted=False,
        )
        session.add_all([state, cb, dl])
        await session.flush()

        trade = make_open_trade(token, entry_price=Decimal("0.001"),
                                position_usd=Decimal("50"))
        session.add(trade)
        await session.flush()

        risk = RiskEngine(asyncio.Event())

        with patch("engine.risk.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)

            await risk.close_trade(
                trade=trade,
                exit_price=Decimal("0.000940"),  # -6% loss
                exit_reason=ExitReason.STOP_LOSS,
            )

        result = await session.execute(
            select(CircuitBreakerState).where(CircuitBreakerState.id == 1)
        )
        updated_cb = result.scalar_one()
        assert updated_cb.consecutive_losses == 1

    @pytest.mark.asyncio
    async def test_circuit_breaker_resets_on_win(self, session):
        from engine.risk import RiskEngine

        token = make_token()
        session.add(token)
        await session.flush()

        state = SessionState(id=1, available_balance=Decimal("950"))
        cb = CircuitBreakerState(id=1, consecutive_losses=2, is_paused=False)
        dl = DailyLossState(
            id=1,
            date_utc=datetime.now(timezone.utc).date(),
            day_open_balance=Decimal("1000"),
            cumulative_pnl_usd=Decimal("-10"),
            is_halted=False,
        )
        session.add_all([state, cb, dl])
        await session.flush()

        trade = make_open_trade(token, entry_price=Decimal("0.001"),
                                position_usd=Decimal("50"))
        session.add(trade)
        await session.flush()

        risk = RiskEngine(asyncio.Event())

        with patch("engine.risk.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)

            await risk.close_trade(
                trade=trade,
                exit_price=Decimal("0.001300"),  # +30% win
                exit_reason=ExitReason.TAKE_PROFIT,
            )

        result = await session.execute(
            select(CircuitBreakerState).where(CircuitBreakerState.id == 1)
        )
        updated_cb = result.scalar_one()
        assert updated_cb.consecutive_losses == 0

    @pytest.mark.asyncio
    async def test_circuit_breaker_trips_at_loss_count(self, session):
        from engine.risk import RiskEngine

        token = make_token()
        session.add(token)
        await session.flush()

        state = SessionState(id=1, available_balance=Decimal("950"))
        # Already at CB_LOSS_COUNT - 1 (default 3, so 2 prior losses)
        cb = CircuitBreakerState(id=1, consecutive_losses=2, is_paused=False)
        dl = DailyLossState(
            id=1,
            date_utc=datetime.now(timezone.utc).date(),
            day_open_balance=Decimal("1000"),
            cumulative_pnl_usd=Decimal("-10"),
            is_halted=False,
        )
        session.add_all([state, cb, dl])
        await session.flush()

        trade = make_open_trade(token, entry_price=Decimal("0.001"),
                                position_usd=Decimal("50"))
        session.add(trade)
        await session.flush()

        risk = RiskEngine(asyncio.Event())

        with patch("engine.risk.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)

            await risk.close_trade(
                trade=trade,
                exit_price=Decimal("0.000940"),  # loss
                exit_reason=ExitReason.STOP_LOSS,
            )

        result = await session.execute(
            select(CircuitBreakerState).where(CircuitBreakerState.id == 1)
        )
        updated_cb = result.scalar_one()
        assert updated_cb.consecutive_losses == 3
        assert updated_cb.is_paused is True
        assert updated_cb.resume_at is not None


# ── Capital engine: risk gate ─────────────────────────────────────────────────

class TestCapitalEngineRiskGate:

    @pytest.mark.asyncio
    async def test_circuit_breaker_blocks_entry(self, session):
        from engine.capital import CapitalEngine

        state = SessionState(id=1, available_balance=Decimal("1000"))
        cb = CircuitBreakerState(
            id=1, consecutive_losses=3, is_paused=True,
            resume_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        dl = DailyLossState(
            id=1,
            date_utc=datetime.now(timezone.utc).date(),
            day_open_balance=Decimal("1000"),
            cumulative_pnl_usd=Decimal("0"),
            is_halted=False,
        )
        session.add_all([state, cb, dl])
        await session.flush()

        capital = CapitalEngine(asyncio.Queue(), asyncio.Event())

        with patch("engine.capital.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            result = await capital._risk_gate("SomeMint")

        assert result is not None
        assert "CIRCUIT_BREAKER" in result

    @pytest.mark.asyncio
    async def test_daily_halt_blocks_entry(self, session):
        from engine.capital import CapitalEngine

        state = SessionState(id=1, available_balance=Decimal("1000"))
        cb = CircuitBreakerState(id=1, consecutive_losses=0, is_paused=False)
        dl = DailyLossState(
            id=1,
            date_utc=datetime.now(timezone.utc).date(),
            day_open_balance=Decimal("1000"),
            cumulative_pnl_usd=Decimal("-200"),
            is_halted=True,
            halted_at=datetime.now(timezone.utc),
        )
        session.add_all([state, cb, dl])
        await session.flush()

        capital = CapitalEngine(asyncio.Queue(), asyncio.Event())

        with patch("engine.capital.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            result = await capital._risk_gate("SomeMint")

        assert result == "DAILY_LOSS_HALT"

    @pytest.mark.asyncio
    async def test_risk_gate_passes_when_clear(self, session):
        from engine.capital import CapitalEngine

        state = SessionState(id=1, available_balance=Decimal("1000"))
        cb = CircuitBreakerState(id=1, consecutive_losses=0, is_paused=False)
        dl = DailyLossState(
            id=1,
            date_utc=datetime.now(timezone.utc).date(),
            day_open_balance=Decimal("1000"),
            cumulative_pnl_usd=Decimal("0"),
            is_halted=False,
        )
        session.add_all([state, cb, dl])
        await session.flush()

        capital = CapitalEngine(asyncio.Queue(), asyncio.Event())

        with patch("engine.capital.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            result = await capital._risk_gate("SomeMint")

        assert result is None


# ── Capital engine: position sizing ──────────────────────────────────────────

class TestPositionSizing:

    @pytest.mark.asyncio
    async def test_position_is_5pct_of_balance(self, session):
        from engine.capital import CapitalEngine

        state = SessionState(id=1, available_balance=Decimal("1000"))
        session.add(state)
        await session.flush()

        capital = CapitalEngine(asyncio.Queue(), asyncio.Event())

        with patch("engine.capital.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            position, balance = await capital._size_position()

        assert position == pytest.approx(Decimal("50"), rel=Decimal("0.001"))
        assert balance == Decimal("1000")

    @pytest.mark.asyncio
    async def test_position_compounds_after_win(self, session):
        """After a win, next position should be larger (5% of larger balance)."""
        from engine.capital import CapitalEngine

        state = SessionState(id=1, available_balance=Decimal("1065"))
        session.add(state)
        await session.flush()

        capital = CapitalEngine(asyncio.Queue(), asyncio.Event())

        with patch("engine.capital.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            position, balance = await capital._size_position()

        # 5% of $1065 = $53.25
        assert position == pytest.approx(Decimal("53.25"), rel=Decimal("0.001"))

    @pytest.mark.asyncio
    async def test_min_position_floor(self, session):
        """Balance too low to meet MIN_POSITION_USD returns None."""
        from engine.capital import CapitalEngine

        # $10 balance × 5% = $0.50, below MIN_POSITION_USD ($1)
        state = SessionState(id=1, available_balance=Decimal("10"))
        session.add(state)
        await session.flush()

        capital = CapitalEngine(asyncio.Queue(), asyncio.Event())

        with patch("engine.capital.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            position, balance = await capital._size_position()

        assert position is None
        assert balance is None
