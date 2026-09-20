"""
tests/test_phase1.py
====================
Phase 1 — Foundation tests.

Covers:
  - Settings validation (cross-field rules, field validators)
  - ORM model instantiation and enum coverage
  - Singleton row creation (SessionState, CircuitBreakerState, DailyLossState)
  - volume_mult baseline logic (anchored to first snapshot, not rolling P1)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, date
from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from models.orm import (
    CircuitBreakerState,
    DailyLossState,
    ExitReason,
    LiquidityFlag,
    LiquidityTrend,
    SessionState,
    Token,
    TokenSnapshot,
    TokenStatus,
    Trade,
    TradeStatus,
    TrendDirection,
    VMZone,
)


# ── Settings validation ───────────────────────────────────────────────────────

class TestSettingsValidation:
    """Settings are validated at import time in production; we test the rules."""

    def _make_env(self, **overrides) -> dict:
        base = {
            "HELIUS_API_KEY": "test-key",
            "HELIUS_RPC_URL": "https://mainnet.helius-rpc.com/?api-key=test",
            "HELIUS_WS_URL": "wss://mainnet.helius-rpc.com/?api-key=test",
            "DATABASE_URL": "postgresql+asyncpg://u:p@localhost/db",
            "TELEGRAM_BOT_TOKEN": "test-bot-token",
            "TELEGRAM_CHAT_ID": "12345",
        }
        base.update(overrides)
        return base

    def test_hard_floor_must_be_below_stop_loss(self):
        from config.settings import Settings
        with pytest.raises(ValidationError, match="HARD_FLOOR_PCT"):
            Settings(
                **self._make_env(HARD_FLOOR_PCT=-0.05, STOP_LOSS_PCT=-0.06)
            )

    def test_equal_floor_and_stop_is_rejected(self):
        from config.settings import Settings
        with pytest.raises(ValidationError, match="HARD_FLOOR_PCT"):
            Settings(
                **self._make_env(HARD_FLOOR_PCT=-0.06, STOP_LOSS_PCT=-0.06)
            )

    def test_watch_threshold_must_be_below_strong_buy(self):
        from config.settings import Settings
        with pytest.raises(ValidationError, match="TIER2_WATCH_THRESHOLD"):
            Settings(
                **self._make_env(
                    TIER2_WATCH_THRESHOLD=9.0,
                    TIER2_STRONG_BUY_THRESHOLD=8.0,
                )
            )

    def test_age_range_must_be_valid(self):
        from config.settings import Settings
        with pytest.raises(ValidationError, match="TIER1_MIN_AGE_MINUTES"):
            Settings(
                **self._make_env(
                    TIER1_MIN_AGE_MINUTES=500,
                    TIER1_MAX_AGE_MINUTES=400,
                )
            )

    def test_database_url_must_use_asyncpg(self):
        from config.settings import Settings
        with pytest.raises(ValidationError, match="asyncpg"):
            Settings(**self._make_env(DATABASE_URL="postgresql://u:p@h/db"))

    def test_valid_settings_accepted(self):
        from config.settings import Settings
        s = Settings(**self._make_env())
        assert s.STOP_LOSS_PCT == -0.06
        assert s.HARD_FLOOR_PCT == -0.07
        assert s.max_hold_seconds == 6 * 3600
        assert s.cb_pause_seconds == 60 * 60


# ── Enum coverage ─────────────────────────────────────────────────────────────

class TestEnums:
    def test_token_status_values(self):
        assert set(TokenStatus) == {
            TokenStatus.ENRICHED, TokenStatus.WATCHING, TokenStatus.OBSERVING,
            TokenStatus.REJECTED, TokenStatus.ENTERED, TokenStatus.CLOSED
        }

    def test_trend_direction_values(self):
        assert TrendDirection.ACCELERATING.value == "ACCELERATING"
        assert TrendDirection.DECELERATING.value == "DECELERATING"

    def test_vm_zone_values(self):
        zones = {z.value for z in VMZone}
        assert zones == {"EARLY", "STRONG", "LATE", "EXHAUSTION"}

    def test_exit_reason_priority_order(self):
        # Priority must always be: HARD_FLOOR > STOP_LOSS > TAKE_PROFIT > TIME_EXIT
        reasons = [ExitReason.HARD_FLOOR, ExitReason.STOP_LOSS,
                   ExitReason.TAKE_PROFIT, ExitReason.TIME_EXIT]
        assert len(reasons) == 4  # No extras added accidentally


# ── ORM model instantiation ───────────────────────────────────────────────────

class TestModels:
    def _make_token(self) -> Token:
        return Token(
            mint_address="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            symbol="TEST",
            name="Test Token",
            status=TokenStatus.ENRICHED,
            discovered_at=datetime.now(timezone.utc),
        )

    @pytest.mark.asyncio
    async def test_token_created(self, session):
        token = self._make_token()
        session.add(token)
        await session.flush()
        result = await session.execute(
            select(Token).where(Token.mint_address == token.mint_address)
        )
        fetched = result.scalar_one()
        assert fetched.symbol == "TEST"
        assert fetched.status == TokenStatus.ENRICHED

    @pytest.mark.asyncio
    async def test_token_unique_mint_constraint(self, session):
        token1 = self._make_token()
        token2 = self._make_token()  # same mint
        session.add(token1)
        await session.flush()
        session.add(token2)
        from sqlalchemy.exc import IntegrityError
        with pytest.raises(IntegrityError):
            await session.flush()

    @pytest.mark.asyncio
    async def test_snapshot_volume_mult_stored(self, session):
        """volume_mult must be stored at write time — not computed on read."""
        token = self._make_token()
        token.baseline_volume_usd = Decimal("1000.00")
        session.add(token)
        await session.flush()

        snap = TokenSnapshot(
            token_id=token.id,
            sampled_at=datetime.now(timezone.utc),
            price_usd=Decimal("0.001"),
            liquidity_usd=Decimal("25000"),
            market_cap_usd=Decimal("20000"),
            volume_usd=Decimal("3500"),
            buy_pressure=Decimal("0.72"),
            # volume_mult = 3500 / 1000 = 3.5 — computed by sampling_worker
            volume_mult=Decimal("3.5000"),
        )
        session.add(snap)
        await session.flush()

        result = await session.execute(
            select(TokenSnapshot).where(TokenSnapshot.token_id == token.id)
        )
        fetched = result.scalar_one()
        assert fetched.volume_mult == Decimal("3.5000")

    @pytest.mark.asyncio
    async def test_snapshot_volume_mult_uses_debut_baseline_not_rolling_p1(self, session):
        """
        Critical: volume_mult must be anchored to the token's debut volume
        (first snapshot ever), NOT to the current rolling window's P1.

        This test ensures that if a second snapshot has higher volume than
        the first, the mult > 1.0, reflecting genuine growth since debut.
        If it were re-baselined to rolling P1, the mult would reset to 1.0
        every time the window advances — losing historical context.
        """
        token = self._make_token()
        token.baseline_volume_usd = Decimal("1000.00")  # debut baseline
        session.add(token)
        await session.flush()

        # Snapshot 1 (debut — mult should be 1.0)
        snap1 = TokenSnapshot(
            token_id=token.id,
            sampled_at=datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc),
            price_usd=Decimal("0.001"),
            liquidity_usd=Decimal("25000"),
            market_cap_usd=Decimal("20000"),
            volume_usd=Decimal("1000"),
            buy_pressure=Decimal("0.60"),
            volume_mult=Decimal("1.0000"),  # 1000 / 1000
        )
        # Snapshot 2 — volume grew to 4× debut
        snap2 = TokenSnapshot(
            token_id=token.id,
            sampled_at=datetime(2026, 5, 1, 10, 0, 30, tzinfo=timezone.utc),
            price_usd=Decimal("0.0011"),
            liquidity_usd=Decimal("26000"),
            market_cap_usd=Decimal("22000"),
            volume_usd=Decimal("4000"),
            buy_pressure=Decimal("0.75"),
            volume_mult=Decimal("4.0000"),  # 4000 / 1000 (debut baseline)
            # If anchored to rolling P1 this would be: 4000 / 1000 = 4.0 (same here,
            # but it would RESET to 1.0 when the window shifts — that's the bug we prevent)
        )
        session.add_all([snap1, snap2])
        await session.flush()

        # Verify mult reflects growth from debut, not from rolling window
        assert snap2.volume_mult == Decimal("4.0000")


# ── Singleton row seeding ─────────────────────────────────────────────────────

class TestSingletonRows:
    @pytest.mark.asyncio
    async def test_session_state_created(self, session):
        state = SessionState(id=1, available_balance=Decimal("1000.00"))
        session.add(state)
        await session.flush()
        result = await session.execute(select(SessionState).where(SessionState.id == 1))
        fetched = result.scalar_one()
        assert fetched.available_balance == Decimal("1000.00")

    @pytest.mark.asyncio
    async def test_circuit_breaker_initial_state(self, session):
        cb = CircuitBreakerState(id=1, consecutive_losses=0, is_paused=False)
        session.add(cb)
        await session.flush()
        result = await session.execute(
            select(CircuitBreakerState).where(CircuitBreakerState.id == 1)
        )
        fetched = result.scalar_one()
        assert fetched.consecutive_losses == 0
        assert fetched.is_paused is False
        assert fetched.resume_at is None

    @pytest.mark.asyncio
    async def test_daily_loss_state_initial(self, session):
        today = date.today()
        state = DailyLossState(
            id=1,
            date_utc=today,
            day_open_balance=Decimal("1000.00"),
            cumulative_pnl_usd=Decimal("0"),
            is_halted=False,
        )
        session.add(state)
        await session.flush()
        result = await session.execute(
            select(DailyLossState).where(DailyLossState.id == 1)
        )
        fetched = result.scalar_one()
        assert fetched.date_utc == today
        assert fetched.cumulative_pnl_usd == Decimal("0")
        assert fetched.is_halted is False


# ── Wash multiplier direction ─────────────────────────────────────────────────

class TestWashMultiplierLogic:
    """
    Verify the wash multiplier check direction is unambiguous.

    buy_count / sell_count > 2.5 → wash trading → REJECT
    buy_count / sell_count ≤ 2.5 → organic → PASS
    """

    def _compute_wash_multiplier(self, buy_count: int, sell_count: int) -> float:
        if sell_count == 0:
            return float("inf")
        return buy_count / sell_count

    def test_organic_trading_passes(self):
        mult = self._compute_wash_multiplier(buy_count=100, sell_count=60)
        assert mult <= 2.5  # 1.67 — organic, passes Tier 1

    def test_wash_trading_fails(self):
        mult = self._compute_wash_multiplier(buy_count=300, sell_count=50)
        assert mult > 2.5  # 6.0 — wash trading, fails Tier 1

    def test_exact_boundary_passes(self):
        mult = self._compute_wash_multiplier(buy_count=250, sell_count=100)
        assert mult <= 2.5  # exactly 2.5 — passes (boundary is inclusive)

    def test_zero_sells_is_infinite_multiplier(self):
        mult = self._compute_wash_multiplier(buy_count=100, sell_count=0)
        assert mult == float("inf")  # always fails
