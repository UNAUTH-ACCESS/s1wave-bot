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

from config.settings import settings
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
    """Pins TIER1_MIN_LIQUIDITY_USD to a known value for the duration of
    this class so the boundary logic itself is verified regardless of
    whatever production's current threshold happens to be (it drifted
    from this file's original assumption of 20000 to the current
    12000 default at some point without these tests being updated)."""

    @pytest.fixture(autouse=True)
    def _pin_threshold(self, monkeypatch):
        monkeypatch.setattr(settings, "TIER1_MIN_LIQUIDITY_USD", 20000.0)

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
    """Pins TIER1_MIN_AGE_MINUTES to a known value (5) for this class — the
    same drift as TestLiquidityConstraint's threshold: production's actual
    default is now 0 (no minimum at all, per the S1 Wave "enter before
    burst confirmation" design), so these boundary tests must supply their
    own known threshold rather than assume production's current value."""

    @pytest.fixture(autouse=True)
    def _pin_threshold(self, monkeypatch):
        monkeypatch.setattr(settings, "TIER1_MIN_AGE_MINUTES", 5)

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


# LP locked/burned is no longer a run_tier1_filter() constraint — removed
# when enrichment_worker.py started hardcoding lp_locked_burned=True for
# every token (pump.fun's graduation protocol guarantees LP burn, so it's
# no longer independently checked here). RejectionReason.LP_NOT_LOCKED_
# OR_BURNED / MISSING_LP_DATA remain in the enum only for historical rows
# already written under the old constraint. TestLPConstraint (3 tests
# asserting lp_locked_burned=False/None reject) removed 2026-09-23 — it
# tested a check that no longer exists.


# ── Constraint 6 (was 7): Wash multiplier ─────────────────────────────────────

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
    Order: liquidity → market_cap → age → mint_auth → freeze_auth → wash
    (LP is no longer a separate constraint — see the note above
    TestWashMultiplierConstraint.)
    """

    @pytest.fixture(autouse=True)
    def _pin_age_threshold(self, monkeypatch):
        monkeypatch.setattr(settings, "TIER1_MIN_AGE_MINUTES", 5)

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

    def test_freeze_auth_fails_before_wash(self):
        token = make_token(
            freeze_authority_renounced=False,  # fails constraint 5
            wash_multiplier=Decimal("9.9999"), # would fail constraint 6
        )
        result = run_tier1_filter(token)
        assert result.rejection_reason == RejectionReason.FREEZE_AUTHORITY_NOT_RENOUNCED


# ── Tier1Worker DB writes ─────────────────────────────────────────────────────

class TestTier1WorkerDBWrites:
    """
    Rewritten 2026-09-23: Tier1Worker was fully rewritten at some point from
    a Token-row-based design (fetch by mint, evaluate via run_tier1_filter(),
    write REJECTED/WATCHING) to a dict-based one (filters.tier1_worker.
    Tier1Worker._evaluate(t) — a raw dict pushed by DiscoveryWorker, no DB
    read first, its own inline threshold checks, no dependency on
    filters/tier1.py or RejectionReason at all). The old tests called a
    method (_evaluate_one) that no longer exists and asserted a REJECTED
    DB write that no longer happens (a hard-gate failure is now log-only —
    see _reject() — the token row, if any, is simply left untouched).
    These replacements exercise the real current behavior.
    """

    def _passing_dict(self, mint: str, **overrides) -> dict:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        defaults = dict(
            mint=mint, symbol="TEST", name="Test Token",
            pool_created_at_ms=now_ms - 30 * 60_000,  # 30 min old
            liquidity_usd=Decimal("25000.00"), market_cap_usd=Decimal("50000.00"),
            mint_authority=None, freeze_authority=None,
            lp_burn=100, buys=40, sells=20,
        )
        defaults.update(overrides)
        return defaults

    @pytest.mark.asyncio
    async def test_passing_token_advances_to_watching(self, session):
        from filters.tier1_worker import Tier1Worker

        mint = "MintPASSAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._evaluate(self._passing_dict(mint))

        result = await session.execute(select(Token).where(Token.mint_address == mint))
        updated = result.scalar_one()
        assert updated.status == TokenStatus.WATCHING
        assert updated.watch_started_at is not None

    @pytest.mark.asyncio
    async def test_failing_token_is_written_as_observing_control_group(self, session):
        """A hard-gate failure is no longer dropped outright — it becomes
        the control group: written as OBSERVING (not REJECTED — there is
        no REJECTED write at this layer anymore) and still pushed to
        sampling_queue so sampling_worker can sample it for a short,
        bounded window (settings.OBSERVE_WINDOW_SECONDS) before it's
        eventually aged out. See Tier1Worker._reject()."""
        from filters.tier1_worker import Tier1Worker

        mint = "MintFAILAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._evaluate(self._passing_dict(mint, liquidity_usd=Decimal("100.00")))

        result = await session.execute(select(Token).where(Token.mint_address == mint))
        updated = result.scalar_one()
        assert updated.status == TokenStatus.OBSERVING
        assert updated.watch_started_at is None  # never eligible to trade
        assert sampling_q.qsize() == 1

    @pytest.mark.asyncio
    async def test_passing_token_pushed_to_sampling_queue(self, session):
        from filters.tier1_worker import Tier1Worker

        mint = "MintQUEUEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)
        t = self._passing_dict(mint)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._evaluate(t)

        assert sampling_q.qsize() == 1
        queued = await sampling_q.get()
        assert queued["mint"] == mint

    @pytest.mark.asyncio
    async def test_evaluate_needs_no_preexisting_token_row(self, session):
        """Unlike the old Token-row-based design, _evaluate() never reads
        a Token row before deciding pass/fail — it works entirely off the
        dict DiscoveryWorker handed it. A mint never before seen in the DB
        evaluates and writes correctly on its own."""
        from filters.tier1_worker import Tier1Worker

        mint = "MintNEVERSEENAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        result = await session.execute(select(Token).where(Token.mint_address == mint))
        assert result.scalar_one_or_none() is None  # confirm no pre-existing row

        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._evaluate(self._passing_dict(mint))

        result = await session.execute(select(Token).where(Token.mint_address == mint))
        assert result.scalar_one().status == TokenStatus.WATCHING


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
    Rewritten 2026-09-23. The old premise (SELECT FOR UPDATE guarding a
    Token-row-based Tier1Worker against a fast-path queue and a fallback
    sweep both firing on the same token) no longer applies: the current
    dict-based Tier1Worker._evaluate() never reads a Token row or checks
    its status before evaluating, and DiscoveryWorker's own in-memory
    `_promoted` set already prevents the same mint from ever being queued
    to filter_queue twice — there is no "fallback sweep" in the current
    architecture at all. Calling _evaluate() twice for the same dict WILL
    push to sampling_queue twice (there is no de-dup at this layer); that
    is documented here as real, current behavior rather than papered over
    with a test asserting a guard that doesn't exist.
    """

    def _passing_dict(self, mint: str, **overrides) -> dict:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        defaults = dict(
            mint=mint, symbol="TEST", name="Test Token",
            pool_created_at_ms=now_ms - 30 * 60_000,
            liquidity_usd=Decimal("25000.00"), market_cap_usd=Decimal("50000.00"),
            mint_authority=None, freeze_authority=None,
            lp_burn=100, buys=40, sells=20,
        )
        defaults.update(overrides)
        return defaults

    @pytest.mark.asyncio
    async def test_repeated_evaluation_of_the_same_mint_upserts_one_row(self, session):
        """The upsert (on_conflict_do_update by mint_address) means calling
        _evaluate() twice for the same mint never creates two Token rows —
        this part of the old safety property does still hold."""
        from filters.tier1_worker import Tier1Worker

        mint = "MintDUPEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)
        t = self._passing_dict(mint)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._evaluate(t)
            await worker._evaluate(t)

        result = await session.execute(select(Token).where(Token.mint_address == mint))
        assert len(result.scalars().all()) == 1

    @pytest.mark.asyncio
    async def test_a_passing_and_a_failing_token_evaluated_together_dont_cross_contaminate(self, session):
        """A passing token and a failing one, evaluated back to back, must
        each land in their own correct state — WATCHING+eligible-to-trade
        for the pass, OBSERVING+control-group for the fail — with no
        state leaking between the two calls (e.g. via a mutated shared
        `inputs` dict)."""
        from filters.tier1_worker import Tier1Worker

        pass_mint = "MintPASS2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        fail_mint = "MintFAIL2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        filter_q = asyncio.Queue()
        sampling_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = Tier1Worker(filter_q, sampling_q, shutdown)

        with patch("filters.tier1_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._evaluate(self._passing_dict(pass_mint))
            await worker._evaluate(self._passing_dict(fail_mint, liquidity_usd=Decimal("100.00")))

        result = await session.execute(select(Token).where(Token.mint_address == pass_mint))
        assert result.scalar_one().status == TokenStatus.WATCHING
        result = await session.execute(select(Token).where(Token.mint_address == fail_mint))
        assert result.scalar_one().status == TokenStatus.OBSERVING
        assert sampling_q.qsize() == 2

    @pytest.mark.asyncio
    async def test_helius_asset_failure_does_not_block_enrichment(self, session):
        """Real current error-isolation property: a Helius get_asset()
        failure is caught and logged, token_decimals defaults to 6, and
        enrichment still completes and writes. Replaces the old 3-way
        asyncio.gather test — TX analysis was removed entirely (see
        TestHeliusClient's analyse_transactions stub tests), and DEX/Helius
        are now fetched sequentially, not concurrently. That old test's
        dex mock returned None to simulate "one source failed", but None
        now means "not indexed yet" and triggers a real multi-minute retry
        loop (5 attempts, 10/20/40/60s delays) — reusing that shape here
        would make this test extremely slow for no reason, so this uses a
        real pair instead and fails only the Helius call."""
        from workers.enrichment_worker import EnrichmentWorker
        from dexscreener.client import DexTokenDetail, DexPairSnapshot

        mint = "MintHELIUSFAILAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        token = Token(mint_address=mint, symbol="HF", status=TokenStatus.ENRICHED,
                      discovered_at=datetime.now(timezone.utc))
        session.add(token)
        await session.flush()

        mock_dex = AsyncMock()
        mock_dex.get_token_detail.return_value = DexTokenDetail(
            token_address=mint, symbol="HF", name="Helius Fail",
            best_pair=DexPairSnapshot(
                pair_address="PairHF", base_token_address=mint, base_token_symbol="HF",
                base_token_name="Helius Fail", price_usd=Decimal("0.001"),
                liquidity_usd=Decimal("35000"), market_cap_usd=Decimal("70000"),
                volume_m5_usd=Decimal("5000"), volume_h1_usd=Decimal("20000"),
                buy_pressure_m5=Decimal("0.65"), buy_pressure_h1=Decimal("0.60"),
                pair_created_at_ms=1746057600000,
                txns_m5_buys=65, txns_m5_sells=35, txns_h1_buys=250, txns_h1_sells=150,
            ),
        )
        mock_helius = AsyncMock()
        mock_helius.get_asset.side_effect = ConnectionError("Helius timeout")

        queue = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = EnrichmentWorker(queue, mock_dex, mock_helius, shutdown)

        with patch("workers.enrichment_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._enrich_token(mint)

        result = await session.execute(select(Token).where(Token.mint_address == mint))
        updated = result.scalar_one()
        assert updated.liquidity_usd == Decimal("35000")  # enrichment still completed
        assert updated.token_decimals == 6  # defaulted rather than fetched


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
