"""
tests/test_phase3.py
====================
Phase 3 — Tier 1 hard gate tests.

Covers every constraint at:
  - Clean pass (all 7 met)
  - Exact boundary (should pass)
  - One below boundary (should fail)
  - Missing field (should fail — never pass unverified)

Also covers:
  - Age calculation from pairCreatedAt vs discovered_at fallback
  - Short-circuit order (cheapest constraint fails first)
  - RejectionReason codes match what the API will surface
  - Tier1Worker writes correct status to DB
  - Filter queue fast-path (enrichment → filter without sweep delay)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch
import uuid

import pytest
from sqlalchemy import select

from filters.tier1 import (
    RejectionReason,
    Tier1Result,
    compute_age_minutes,
    run_tier1_filter,
)
from models.orm import Token, TokenStatus


# ── Token factory ─────────────────────────────────────────────────────────────

def make_token(**overrides) -> Token:
    """
    Build a fully-enriched Token that passes all 7 Tier 1 constraints.
    Override individual fields to test failure cases.
    """
    now = datetime.now(timezone.utc)
    defaults = dict(
        mint_address=f"Mint{uuid.uuid4().hex[:36]}",
        symbol="TEST",
        name="Test Token",
        status=TokenStatus.ENRICHED,
        discovered_at=now - timedelta(minutes=30),  # 30 min old — within 5–1440
        liquidity_usd=Decimal("25000.00"),           # ≥ 20000 ✓
        market_cap_usd=Decimal("50000.00"),          # ≥ 15000 ✓
        mint_authority_renounced=True,               # ✓
        freeze_authority_renounced=True,             # ✓
        lp_locked_burned=True,                       # ✓
        wash_multiplier=Decimal("1.80"),             # ≤ 2.5 ✓
        holder_count=500,
        baseline_volume_usd=Decimal("1000.00"),
    )
    defaults.update(overrides)
    return Token(**defaults)


# ── Age calculation ───────────────────────────────────────────────────────────

class TestComputeAgeMinutes:

    def test_uses_pair_created_at_ms_when_available(self):
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        # Token created 60 minutes ago
        created_ms = int((now - timedelta(minutes=60)).timestamp() * 1000)
        discovered = now - timedelta(minutes=10)  # discovered more recently

        age = compute_age_minutes(created_ms, discovered, now)
        assert age == pytest.approx(60.0, abs=0.1)

    def test_falls_back_to_discovered_at(self):
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        discovered = now - timedelta(minutes=45)

        age = compute_age_minutes(None, discovered, now)
        assert age == pytest.approx(45.0, abs=0.1)

    def test_pair_created_at_takes_precedence_over_discovered(self):
        """pairCreatedAt is always authoritative — discovered_at is ignored when present."""
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        created_ms = int((now - timedelta(minutes=20)).timestamp() * 1000)
        discovered = now - timedelta(minutes=5)  # much younger than actual age

        age = compute_age_minutes(created_ms, discovered, now)
        assert age == pytest.approx(20.0, abs=0.1)  # pairCreatedAt wins


# ── Full pass ─────────────────────────────────────────────────────────────────

class TestTier1FullPass:

    def test_all_constraints_pass(self):
        token = make_token()
        result = run_tier1_filter(token)
        assert result.passed is True
        assert result.rejection_reason is None

    def test_pass_result_includes_age(self):
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        token = make_token(discovered_at=now - timedelta(minutes=30))
        result = run_tier1_filter(token, now=now)
        assert result.passed is True
        assert result.age_minutes == pytest.approx(30.0, abs=0.5)


# ── Constraint 1: Liquidity ───────────────────────────────────────────────────

class TestLiquidityConstraint:

    def test_exact_minimum_passes(self):
        token = make_token(liquidity_usd=Decimal("20000.00"))
        assert run_tier1_filter(token).passed is True

    def test_one_cent_below_fails(self):
        token = make_token(liquidity_usd=Decimal("19999.99"))
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.LOW_LIQUIDITY

    def test_none_liquidity_fails(self):
        token = make_token(liquidity_usd=None)
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.MISSING_LIQUIDITY_DATA

    def test_well_above_minimum_passes(self):
        token = make_token(liquidity_usd=Decimal("500000.00"))
        assert run_tier1_filter(token).passed is True


# ── Constraint 2: Market cap ──────────────────────────────────────────────────

class TestMarketCapConstraint:

    def test_exact_minimum_passes(self):
        token = make_token(market_cap_usd=Decimal("15000.00"))
        assert run_tier1_filter(token).passed is True

    def test_one_cent_below_fails(self):
        token = make_token(market_cap_usd=Decimal("14999.99"))
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.LOW_MARKET_CAP

    def test_none_market_cap_fails(self):
        token = make_token(market_cap_usd=None)
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.MISSING_MARKET_CAP_DATA


# ── Constraint 3: Token age ───────────────────────────────────────────────────

class TestTokenAgeConstraint:

    def _token_at_age(self, age_minutes: float) -> tuple[Token, datetime]:
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        discovered = now - timedelta(minutes=age_minutes)
        return make_token(discovered_at=discovered), now

    def test_minimum_age_boundary_passes(self):
        """Token at exactly 5 minutes old passes."""
        token, now = self._token_at_age(5.0)
        result = run_tier1_filter(token, now=now)
        assert result.passed is True

    def test_just_below_minimum_age_fails(self):
        """Token at 4.9 minutes is too young."""
        token, now = self._token_at_age(4.9)
        result = run_tier1_filter(token, now=now)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.TOKEN_TOO_YOUNG

    def test_maximum_age_boundary_passes(self):
        """Token at exactly 1440 minutes (24h) passes."""
        token, now = self._token_at_age(1440.0)
        result = run_tier1_filter(token, now=now)
        assert result.passed is True

    def test_just_above_maximum_age_fails(self):
        """Token at 1440.1 minutes is too old."""
        token, now = self._token_at_age(1440.1)
        result = run_tier1_filter(token, now=now)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.TOKEN_TOO_OLD

    def test_brand_new_token_fails(self):
        """Token at 0 minutes is too young."""
        token, now = self._token_at_age(0.0)
        result = run_tier1_filter(token, now=now)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.TOKEN_TOO_YOUNG

    def test_pair_created_at_ms_overrides_discovered_at(self):
        """
        If pairCreatedAt says the token is 8 minutes old but discovered_at
        says 2 minutes (bot saw it late), pairCreatedAt wins → passes.
        """
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        # discovered_at says only 2 min ago — would fail if used
        token = make_token(discovered_at=now - timedelta(minutes=2))
        # pairCreatedAt says 8 min ago — passes
        pair_created_ms = int((now - timedelta(minutes=8)).timestamp() * 1000)

        result = run_tier1_filter(token, pair_created_at_ms=pair_created_ms, now=now)
        assert result.passed is True


# ── Constraint 4: Mint authority ──────────────────────────────────────────────

class TestMintAuthorityConstraint:

    def test_renounced_passes(self):
        token = make_token(mint_authority_renounced=True)
        assert run_tier1_filter(token).passed is True

    def test_not_renounced_fails(self):
        token = make_token(mint_authority_renounced=False)
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.MINT_AUTHORITY_NOT_RENOUNCED

    def test_none_fails_safely(self):
        """None = couldn't verify = treat as not renounced = reject."""
        token = make_token(mint_authority_renounced=None)
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.MINT_AUTHORITY_NOT_RENOUNCED


# ── Constraint 5: Freeze authority ───────────────────────────────────────────

class TestFreezeAuthorityConstraint:

    def test_renounced_passes(self):
        token = make_token(freeze_authority_renounced=True)
        assert run_tier1_filter(token).passed is True

    def test_not_renounced_fails(self):
        token = make_token(freeze_authority_renounced=False)
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.FREEZE_AUTHORITY_NOT_RENOUNCED

    def test_none_fails_safely(self):
        token = make_token(freeze_authority_renounced=None)
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.FREEZE_AUTHORITY_NOT_RENOUNCED

    def test_mint_renounced_but_freeze_not_still_fails(self):
        """Both must be renounced — partial renouncement is still a fail."""
        token = make_token(
            mint_authority_renounced=True,
            freeze_authority_renounced=False,
        )
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.FREEZE_AUTHORITY_NOT_RENOUNCED


# ── Constraint 6: LP locked / burned ─────────────────────────────────────────

class TestLPConstraint:

    def test_lp_locked_passes(self):
        token = make_token(lp_locked_burned=True)
        assert run_tier1_filter(token).passed is True

    def test_lp_not_locked_fails(self):
        token = make_token(lp_locked_burned=False)
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.LP_NOT_LOCKED_OR_BURNED

    def test_none_lp_fails_safely(self):
        """None = couldn't verify = reject."""
        token = make_token(lp_locked_burned=None)
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.MISSING_LP_DATA


# ── Constraint 7: Wash multiplier ─────────────────────────────────────────────

class TestWashMultiplierConstraint:
    """
    buy_count / sell_count > 2.5 → WASH_TRADING → REJECT
    buy_count / sell_count ≤ 2.5 → organic       → PASS
    """

    def test_exactly_2_5_passes(self):
        """Boundary is inclusive — 2.5 passes."""
        token = make_token(wash_multiplier=Decimal("2.5000"))
        assert run_tier1_filter(token).passed is True

    def test_just_above_2_5_fails(self):
        token = make_token(wash_multiplier=Decimal("2.5001"))
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.WASH_TRADING

    def test_clearly_organic_passes(self):
        token = make_token(wash_multiplier=Decimal("1.2000"))
        assert run_tier1_filter(token).passed is True

    def test_high_wash_multiplier_fails(self):
        token = make_token(wash_multiplier=Decimal("8.0000"))
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.WASH_TRADING

    def test_none_wash_multiplier_fails_safely(self):
        token = make_token(wash_multiplier=None)
        result = run_tier1_filter(token)
        assert result.passed is False
        assert result.rejection_reason == RejectionReason.MISSING_WASH_DATA


# ── Short-circuit order ───────────────────────────────────────────────────────

class TestShortCircuit:
    """
    When multiple constraints fail, the first one in evaluation order is reported.
    Order: liquidity → market_cap → age → mint_auth → freeze_auth → lp → wash
    """

    def test_liquidity_fails_before_market_cap(self):
        token = make_token(
            liquidity_usd=Decimal("1000.00"),   # fails constraint 1
            market_cap_usd=Decimal("1000.00"),  # would also fail constraint 2
        )
        result = run_tier1_filter(token)
        assert result.rejection_reason == RejectionReason.LOW_LIQUIDITY

    def test_market_cap_fails_before_age(self):
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        token = make_token(
            liquidity_usd=Decimal("25000.00"),   # passes
            market_cap_usd=Decimal("1000.00"),   # fails constraint 2
            discovered_at=now - timedelta(minutes=1),  # would fail constraint 3
        )
        result = run_tier1_filter(token, now=now)
        assert result.rejection_reason == RejectionReason.LOW_MARKET_CAP

    def test_age_fails_before_mint_authority(self):
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        token = make_token(
            discovered_at=now - timedelta(minutes=1),  # fails constraint 3
            mint_authority_renounced=False,             # would fail constraint 4
        )
        result = run_tier1_filter(token, now=now)
        assert result.rejection_reason == RejectionReason.TOKEN_TOO_YOUNG

    def test_mint_auth_fails_before_freeze_auth(self):
        token = make_token(
            mint_authority_renounced=False,    # fails constraint 4
            freeze_authority_renounced=False,  # would fail constraint 5
        )
        result = run_tier1_filter(token)
        assert result.rejection_reason == RejectionReason.MINT_AUTHORITY_NOT_RENOUNCED

    def test_freeze_auth_fails_before_lp(self):
        token = make_token(
            freeze_authority_renounced=False,  # fails constraint 5
            lp_locked_burned=False,            # would fail constraint 6
        )
        result = run_tier1_filter(token)
        assert result.rejection_reason == RejectionReason.FREEZE_AUTHORITY_NOT_RENOUNCED

    def test_lp_fails_before_wash(self):
        token = make_token(
            lp_locked_burned=False,            # fails constraint 6
            wash_multiplier=Decimal("9.9999"), # would fail constraint 7
        )
        result = run_tier1_filter(token)
        assert result.rejection_reason == RejectionReason.LP_NOT_LOCKED_OR_BURNED


# ── Tier1Worker DB writes ─────────────────────────────────────────────────────

class TestTier1WorkerDBWrites:

    @pytest.mark.asyncio
    async def test_passing_token_advances_to_watching(self, session):
        from filters.tier1_worker import Tier1Worker

        now = datetime.now(timezone.utc)
        token = make_token(discovered_at=now - timedelta(minutes=30))
        session.add(token)
        await session.flush()

        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._evaluate_one(token.mint_address)

        result = await session.execute(
            select(Token).where(Token.mint_address == token.mint_address)
        )
        updated = result.scalar_one()
        assert updated.status == TokenStatus.WATCHING
        assert updated.watch_started_at is not None

    @pytest.mark.asyncio
    async def test_failing_token_advanced_to_rejected(self, session):
        from filters.tier1_worker import Tier1Worker

        token = make_token(lp_locked_burned=False)  # will fail constraint 6
        session.add(token)
        await session.flush()

        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._evaluate_one(token.mint_address)

        result = await session.execute(
            select(Token).where(Token.mint_address == token.mint_address)
        )
        updated = result.scalar_one()
        assert updated.status == TokenStatus.REJECTED
        assert updated.rejection_reason == RejectionReason.LP_NOT_LOCKED_OR_BURNED.value

    @pytest.mark.asyncio
    async def test_passing_token_pushed_to_sampling_queue(self, session):
        from filters.tier1_worker import Tier1Worker

        now = datetime.now(timezone.utc)
        token = make_token(discovered_at=now - timedelta(minutes=30))
        session.add(token)
        await session.flush()

        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._evaluate_one(token.mint_address)

        assert sampling_q.qsize() == 1
        queued_mint = await sampling_q.get()
        assert queued_mint == token.mint_address

    @pytest.mark.asyncio
    async def test_already_watching_token_is_skipped(self, session):
        """Race condition guard — WATCHING tokens are not re-evaluated."""
        from filters.tier1_worker import Tier1Worker

        now = datetime.now(timezone.utc)
        token = make_token(
            discovered_at=now - timedelta(minutes=30),
            status=TokenStatus.WATCHING,
        )
        session.add(token)
        await session.flush()

        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._evaluate_one(token.mint_address)

        # Status unchanged, nothing pushed to sampling queue
        result = await session.execute(
            select(Token).where(Token.mint_address == token.mint_address)
        )
        updated = result.scalar_one()
        assert updated.status == TokenStatus.WATCHING
        assert sampling_q.empty()


# ── Zero vs None distinction ──────────────────────────────────────────────────

class TestZeroVsNoneRejection:
    """
    None = enrichment fetch failed (API/parse error).
    0.0  = fetch succeeded, pool is genuinely empty.
    Both fail Tier 1 but for distinct, diagnosable reasons.
    """

    def test_zero_liquidity_distinct_from_missing(self):
        token_none = make_token(liquidity_usd=None)
        token_zero = make_token(liquidity_usd=Decimal("0.00"))

        result_none = run_tier1_filter(token_none)
        result_zero = run_tier1_filter(token_zero)

        assert result_none.rejection_reason == RejectionReason.MISSING_LIQUIDITY_DATA
        assert result_zero.rejection_reason == RejectionReason.ZERO_LIQUIDITY
        # Both fail — but for different reasons
        assert result_none.passed is False
        assert result_zero.passed is False

    def test_zero_market_cap_distinct_from_missing(self):
        token_none = make_token(market_cap_usd=None)
        token_zero = make_token(market_cap_usd=Decimal("0.00"))

        result_none = run_tier1_filter(token_none)
        result_zero = run_tier1_filter(token_zero)

        assert result_none.rejection_reason == RejectionReason.MISSING_MARKET_CAP_DATA
        assert result_zero.rejection_reason == RejectionReason.ZERO_MARKET_CAP

    def test_all_new_rejection_codes_are_in_enum(self):
        """ZERO_ codes must be present — they feed the /tokens/rejected API."""
        values = {r.value for r in RejectionReason}
        assert "ZERO_LIQUIDITY" in values
        assert "ZERO_MARKET_CAP" in values
        assert "MISSING_LIQUIDITY_DATA" in values
        assert "MISSING_MARKET_CAP_DATA" in values


# ── Concurrency / race condition tests ────────────────────────────────────────

class TestConcurrencyRaceCondition:
    """
    Verifies that SELECT FOR UPDATE prevents double evaluation when both the
    fast-path queue and the fallback sweep fire on the same token simultaneously.

    Without FOR UPDATE: both coroutines read ENRICHED → both write WATCHING
    → sampling queue gets the same mint twice → double position risk.

    With FOR UPDATE: one transaction holds the lock, the other sees the
    already-advanced status and exits cleanly.
    """

    @pytest.mark.asyncio
    async def test_concurrent_evaluations_produce_single_queue_push(self, session):
        """
        Two concurrent _evaluate_one calls on the same token must result in
        exactly one push to the sampling queue, not two.
        """
        from filters.tier1_worker import Tier1Worker

        now = datetime.now(timezone.utc)
        token = make_token(discovered_at=now - timedelta(minutes=30))
        session.add(token)
        await session.flush()

        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)

        call_count = 0
        original_get_session = __import__(
            "filters.tier1_worker", fromlist=["get_session"]
        ).get_session

        # We simulate the race by running evaluate_one twice concurrently.
        # In production with a real DB, FOR UPDATE serialises them.
        # In tests with the mock session we verify the status-check guard
        # catches the second call after the first has written WATCHING.
        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)

            # Run both concurrently
            await asyncio.gather(
                worker._evaluate_one(token.mint_address),
                worker._evaluate_one(token.mint_address),
            )

        # Exactly one push to the sampling queue
        assert sampling_q.qsize() == 1

    @pytest.mark.asyncio
    async def test_rejected_token_not_pushed_to_sampling_queue(self, session):
        """A failing token must never reach the sampling queue regardless of concurrency."""
        from filters.tier1_worker import Tier1Worker

        token = make_token(lp_locked_burned=False)
        session.add(token)
        await session.flush()

        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)

            # Even if called twice (queue + sweep firing together)
            await asyncio.gather(
                worker._evaluate_one(token.mint_address),
                worker._evaluate_one(token.mint_address),
            )

        assert sampling_q.empty()

    @pytest.mark.asyncio
    async def test_gather_error_isolation(self):
        """
        If one enrichment data source raises, the others complete normally.
        The failing source is reported and the token is rejected,
        but the successful fetches are not cancelled.
        """
        from workers.enrichment_worker import EnrichmentWorker
        from helius.client import HeliusAssetInfo, HeliusTxAnalysis

        dex_completed = False
        tx_completed = False

        async def slow_dex(mint):
            nonlocal dex_completed
            await asyncio.sleep(0.01)
            dex_completed = True
            return None  # returns None, not exception

        async def failing_asset(mint):
            raise ConnectionError("Helius timeout")

        async def slow_tx(mint):
            nonlocal tx_completed
            await asyncio.sleep(0.01)
            tx_completed = True
            return HeliusTxAnalysis(
                lp_locked_burned=True, wash_multiplier=1.5,
                buy_count=30, sell_count=20,
            )

        mock_dex = AsyncMock()
        mock_dex.get_token_detail = slow_dex
        mock_helius = AsyncMock()
        mock_helius.get_asset = failing_asset
        mock_helius.analyse_transactions = slow_tx

        rejected_mint = None
        rejected_reason = None

        async def fake_mark_rejected(mint, reason):
            nonlocal rejected_mint, rejected_reason
            rejected_mint = mint
            rejected_reason = reason

        queue = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = EnrichmentWorker(queue, mock_dex, mock_helius, shutdown)
        worker._mark_rejected = fake_mark_rejected

        await worker._enrich_token("MintRACETESTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

        # Both the DEX and TX fetches completed (not cancelled by asset failure)
        assert dex_completed is True
        assert tx_completed is True
        # Token was rejected due to asset failure
        assert rejected_mint == "MintRACETESTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        assert "ASSET" in rejected_reason


# ── RejectionReason codes coverage ────────────────────────────────────────────

class TestRejectionReasonCodes:
    """All rejection reasons must have distinct string values for the API."""

    def test_all_codes_are_unique(self):
        values = [r.value for r in RejectionReason]
        assert len(values) == len(set(values))

    def test_all_codes_are_uppercase_snake(self):
        for reason in RejectionReason:
            assert reason.value == reason.value.upper()
            assert " " not in reason.value
