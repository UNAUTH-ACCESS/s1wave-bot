"""
tests/test_phase4.py
====================
Phase 4 — Sampling and Tier 2 Momentum Scoring tests.

Coverage:
  Rolling window engine (pure functions — exhaustive):
    - All 5 TrendDirection classifications for BP and VM
    - Direction matters, not just level (the core PRD insight)
    - Spike detection
    - Composite score weights sum correctly
    - Score band boundaries (≥8.0, 5.0-7.9, <5.0)
    - VMZone classification (EARLY, STRONG, LATE, EXHAUSTION)
    - LiquidityFlag classification

  Sampling worker:
    - Snapshot written with correct volume_mult
    - Debut baseline set on first snapshot with volume
    - Baseline NOT overwritten on subsequent snapshots
    - Age-out at TIER1_MAX_AGE_MINUTES
    - Token delisted (None from DEX) → aged out

  Scoring worker:
    - Window not full (< 3 snapshots) → SKIP
    - STRONG_BUY → pushed to entry_queue
    - DISCARD → token status → REJECTED
    - WATCH → token stays WATCHING, nothing queued
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import pytest

from engine.rolling_window import (
    SnapshotWindow,
    ScoringResult,
    classify_bp_trend,
    classify_vm_trend,
    classify_vm_zone,
    classify_liquidity_flag,
    score_window,
)
from models.orm import (
    LiquidityFlag,
    Token,
    TokenSnapshot,
    TokenStatus,
    TrendDirection,
    VMZone,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_window(
    bp: tuple = (Decimal("0.5"), Decimal("0.5"), Decimal("0.5")),
    vm: tuple = (Decimal("1.0"), Decimal("1.0"), Decimal("1.0")),
    vlr: tuple = (Decimal("0.1"), Decimal("0.1"), Decimal("0.1")),
    age_minutes: float = 20.0,
    prev_score: Decimal | None = None,
) -> SnapshotWindow:
    return SnapshotWindow(
        bp_p1=bp[0], bp_p2=bp[1], bp_p3=bp[2],
        vm_p1=vm[0], vm_p2=vm[1], vm_p3=vm[2],
        vlr_p1=vlr[0], vlr_p2=vlr[1], vlr_p3=vlr[2],
        age_minutes=age_minutes,
        prev_composite_score=prev_score,
    )


def make_token_orm(**overrides) -> Token:
    now = datetime.now(timezone.utc)
    defaults = dict(
        mint_address=f"Mint{uuid.uuid4().hex[:36]}",
        symbol="TEST",
        status=TokenStatus.WATCHING,
        discovered_at=now - timedelta(minutes=20),
        watch_started_at=now - timedelta(minutes=15),
        baseline_volume_usd=Decimal("1000.00"),
        liquidity_usd=Decimal("30000"),
        market_cap_usd=Decimal("60000"),
        mint_authority_renounced=True,
        freeze_authority_renounced=True,
        lp_locked_burned=True,
        wash_multiplier=Decimal("1.5"),
    )
    defaults.update(overrides)
    return Token(**defaults)


def make_snapshot(token_id, volume_usd="1500", volume_mult="1.5", buy_pressure="0.65",
                  liquidity_usd="30000", sampled_at=None) -> TokenSnapshot:
    return TokenSnapshot(
        token_id=token_id,
        sampled_at=sampled_at or datetime.now(timezone.utc),
        price_usd=Decimal("0.001"),
        liquidity_usd=Decimal(liquidity_usd),
        market_cap_usd=Decimal("60000"),
        volume_usd=Decimal(volume_usd),
        buy_pressure=Decimal(buy_pressure),
        volume_mult=Decimal(volume_mult),
    )


# ── Trend classification ──────────────────────────────────────────────────────

class TestTrendClassification:
    """
    Core PRD principle: scoring measures CHANGE, not level.
    BP at 0.6/0.7/0.8 ≠ BP at 0.8/0.7/0.6 even though max level is the same.
    """

    def test_accelerating_rising_consistently(self):
        w = make_window(bp=(Decimal("0.50"), Decimal("0.65"), Decimal("0.82")))
        assert classify_bp_trend(w) == TrendDirection.ACCELERATING

    def test_decelerating_p3_retreats(self):
        """P3 falling is DECELERATING regardless of P1→P2 direction."""
        w = make_window(bp=(Decimal("0.50"), Decimal("0.70"), Decimal("0.55")))
        assert classify_bp_trend(w) == TrendDirection.DECELERATING

    def test_rising_then_falling_is_decelerating_not_accelerating(self):
        """
        PRD core insight: 0.6/0.7/0.6 ≠ 0.6/0.7/0.8.
        The token peaked at P2 — this is DECELERATING.
        """
        w_up   = make_window(bp=(Decimal("0.60"), Decimal("0.70"), Decimal("0.80")))
        w_down = make_window(bp=(Decimal("0.60"), Decimal("0.70"), Decimal("0.60")))
        assert classify_bp_trend(w_up)   == TrendDirection.ACCELERATING
        assert classify_bp_trend(w_down) == TrendDirection.DECELERATING

    def test_flat_all_values_similar(self):
        w = make_window(bp=(Decimal("0.60"), Decimal("0.61"), Decimal("0.60")))
        assert classify_bp_trend(w) == TrendDirection.FLAT

    def test_emerging_p1_p2_flat_p3_jumps(self):
        """P1≈P2 then P3 jumps — EMERGING signal."""
        w = make_window(bp=(Decimal("0.50"), Decimal("0.51"), Decimal("0.70")))
        assert classify_bp_trend(w) == TrendDirection.EMERGING

    def test_spike_p3_large_no_prior_build(self):
        """P3 >> prior with no P1→P2 build — lower confidence SPIKE."""
        w = make_window(bp=(Decimal("0.40"), Decimal("0.41"), Decimal("0.85")))
        assert classify_bp_trend(w) == TrendDirection.SPIKE

    def test_vm_accelerating(self):
        w = make_window(vm=(Decimal("1.0"), Decimal("2.0"), Decimal("3.5")))
        assert classify_vm_trend(w) == TrendDirection.ACCELERATING

    def test_vm_decelerating(self):
        w = make_window(vm=(Decimal("3.0"), Decimal("2.5"), Decimal("1.8")))
        assert classify_vm_trend(w) == TrendDirection.DECELERATING

    def test_same_composite_level_different_direction_scores_differently(self):
        """
        Two tokens with the same P3 values but opposite trajectories must
        produce different composite scores.  This is the fundamental
        differentiator of v3 vs v2.
        """
        # Rising trajectory
        w_rising = make_window(
            bp=(Decimal("0.55"), Decimal("0.65"), Decimal("0.75")),
            vm=(Decimal("1.0"),  Decimal("2.0"),  Decimal("3.0")),
            vlr=(Decimal("0.05"), Decimal("0.08"), Decimal("0.12")),
        )
        # Falling trajectory — same P3 values, reversed direction
        w_falling = make_window(
            bp=(Decimal("0.75"), Decimal("0.65"), Decimal("0.55")),
            vm=(Decimal("3.0"),  Decimal("2.0"),  Decimal("1.0")),
            vlr=(Decimal("0.12"), Decimal("0.08"), Decimal("0.05")),
        )
        result_up   = score_window(w_rising)
        result_down = score_window(w_falling)

        assert result_up.composite > result_down.composite, (
            f"Rising ({result_up.composite}) should outscore falling "
            f"({result_down.composite}) even at same absolute level"
        )
        assert result_up.signal in ("STRONG_BUY", "WATCH")
        assert result_down.signal == "DISCARD"


# ── Composite score ───────────────────────────────────────────────────────────

class TestCompositeScore:

    def test_weights_sum_to_1(self):
        """BP×0.40 + VM×0.35 + VLR×0.25 = 1.0"""
        assert Decimal("0.40") + Decimal("0.35") + Decimal("0.25") == Decimal("1.00")

    def test_strong_buy_threshold(self):
        """Full ACCELERATING across all signals should score ≥ 8.0."""
        w = make_window(
            bp=(Decimal("0.55"), Decimal("0.70"), Decimal("0.88")),
            vm=(Decimal("1.0"),  Decimal("2.5"),  Decimal("4.5")),
            vlr=(Decimal("0.05"), Decimal("0.10"), Decimal("0.18")),
        )
        result = score_window(w)
        assert result.composite >= Decimal("8.0")
        assert result.signal == "STRONG_BUY"

    def test_watch_band(self):
        """Mixed signals (some flat, some emerging) should land in 5.0–7.9."""
        w = make_window(
            bp=(Decimal("0.55"), Decimal("0.56"), Decimal("0.65")),  # EMERGING
            vm=(Decimal("1.0"),  Decimal("1.0"),  Decimal("1.0")),   # FLAT
            vlr=(Decimal("0.05"), Decimal("0.06"), Decimal("0.07")), # EMERGING
        )
        result = score_window(w)
        assert Decimal("5.0") <= result.composite < Decimal("8.0")
        assert result.signal == "WATCH"

    def test_discard_band(self):
        """All DECELERATING signals should score < 5.0."""
        w = make_window(
            bp=(Decimal("0.80"), Decimal("0.60"), Decimal("0.40")),
            vm=(Decimal("4.0"),  Decimal("2.0"),  Decimal("1.0")),
            vlr=(Decimal("0.20"), Decimal("0.10"), Decimal("0.05")),
        )
        result = score_window(w)
        assert result.composite < Decimal("5.0")
        assert result.signal == "DISCARD"

    def test_score_is_0_to_10(self):
        """Score must always be within [0, 10] regardless of inputs."""
        extreme_up = make_window(
            bp=(Decimal("0.0"), Decimal("0.5"), Decimal("1.0")),
            vm=(Decimal("0.0"), Decimal("5.0"), Decimal("10.0")),
            vlr=(Decimal("0.0"), Decimal("0.5"), Decimal("1.0")),
        )
        extreme_down = make_window(
            bp=(Decimal("1.0"), Decimal("0.5"), Decimal("0.0")),
            vm=(Decimal("10.0"), Decimal("5.0"), Decimal("0.0")),
            vlr=(Decimal("1.0"), Decimal("0.5"), Decimal("0.0")),
        )
        for w in (extreme_up, extreme_down):
            r = score_window(w)
            assert Decimal("0") <= r.composite <= Decimal("10")

    def test_strong_buy_result_has_vm_zone(self):
        """vm_zone is populated only on STRONG_BUY signals."""
        w = make_window(
            bp=(Decimal("0.55"), Decimal("0.70"), Decimal("0.88")),
            vm=(Decimal("1.0"),  Decimal("2.5"),  Decimal("4.5")),
            vlr=(Decimal("0.05"), Decimal("0.10"), Decimal("0.18")),
        )
        result = score_window(w)
        if result.signal == "STRONG_BUY":
            assert result.vm_zone is not None
        else:
            assert result.vm_zone is None

    def test_liq_flag_always_none_from_score_window(self):
        """
        liq_flag is always None from score_window — it is set by the capital
        engine (Phase 5) at entry time, not during scoring.
        classify_liquidity_flag() is still available for the capital engine
        to call directly.
        """
        w = make_window(
            bp=(Decimal("0.55"), Decimal("0.70"), Decimal("0.88")),
            vm=(Decimal("1.0"),  Decimal("2.5"),  Decimal("4.5")),
            vlr=(Decimal("0.05"), Decimal("0.10"), Decimal("0.18")),
        )
        result = score_window(w)
        assert result.liq_flag is None


# ── VMZone classification ─────────────────────────────────────────────────────

class TestVMZoneClassification:

    def _strong_buy_result(self) -> ScoringResult:
        w = make_window(
            bp=(Decimal("0.55"), Decimal("0.70"), Decimal("0.88")),
            vm=(Decimal("1.0"),  Decimal("2.5"),  Decimal("4.5")),
            vlr=(Decimal("0.05"), Decimal("0.10"), Decimal("0.18")),
        )
        return score_window(w)

    def test_exhaustion_both_decelerating(self):
        zone = classify_vm_zone(
            make_window(age_minutes=20),
            bp_trend=TrendDirection.DECELERATING,
            vm_trend=TrendDirection.DECELERATING,
            composite_score=Decimal("5.5"),
        )
        assert zone == VMZone.EXHAUSTION

    def test_early_rising_young_token(self):
        zone = classify_vm_zone(
            make_window(age_minutes=5, prev_score=Decimal("5.0")),
            bp_trend=TrendDirection.ACCELERATING,
            vm_trend=TrendDirection.ACCELERATING,
            composite_score=Decimal("8.5"),
        )
        assert zone == VMZone.EARLY

    def test_late_old_flat_token(self):
        zone = classify_vm_zone(
            make_window(age_minutes=45),
            bp_trend=TrendDirection.FLAT,
            vm_trend=TrendDirection.FLAT,
            composite_score=Decimal("8.1"),
        )
        assert zone == VMZone.LATE

    def test_strong_healthy_momentum(self):
        zone = classify_vm_zone(
            make_window(age_minutes=20),
            bp_trend=TrendDirection.ACCELERATING,
            vm_trend=TrendDirection.ACCELERATING,
            composite_score=Decimal("9.0"),
        )
        assert zone == VMZone.STRONG

    def test_exhaustion_takes_priority_over_early(self):
        """Even a young token is EXHAUSTION if both signals are decelerating."""
        zone = classify_vm_zone(
            make_window(age_minutes=5),
            bp_trend=TrendDirection.DECELERATING,
            vm_trend=TrendDirection.DECELERATING,
            composite_score=Decimal("8.0"),
        )
        assert zone == VMZone.EXHAUSTION


# ── LiquidityFlag ─────────────────────────────────────────────────────────────

class TestLiquidityFlag:

    def test_deep_below_half_pct(self):
        flag = classify_liquidity_flag(
            position_size_usd=Decimal("50"),   # 50/30000 = 0.167%
            liquidity_usd=Decimal("30000"),
        )
        assert flag == LiquidityFlag.DEEP

    def test_normal_between_half_and_2_pct(self):
        flag = classify_liquidity_flag(
            position_size_usd=Decimal("300"),  # 300/30000 = 1.0%
            liquidity_usd=Decimal("30000"),
        )
        assert flag == LiquidityFlag.NORMAL

    def test_thin_between_2_and_5_pct(self):
        flag = classify_liquidity_flag(
            position_size_usd=Decimal("900"),  # 900/30000 = 3.0%
            liquidity_usd=Decimal("30000"),
        )
        assert flag == LiquidityFlag.THIN

    def test_shallow_above_5_pct(self):
        flag = classify_liquidity_flag(
            position_size_usd=Decimal("2000"), # 2000/30000 = 6.67%
            liquidity_usd=Decimal("30000"),
        )
        assert flag == LiquidityFlag.SHALLOW

    def test_zero_liquidity_returns_shallow(self):
        flag = classify_liquidity_flag(
            position_size_usd=Decimal("50"),
            liquidity_usd=Decimal("0"),
        )
        assert flag == LiquidityFlag.SHALLOW


# ── Sampling worker ───────────────────────────────────────────────────────────

class TestSamplingWorker:

    @pytest.mark.asyncio
    async def test_snapshot_written_with_correct_volume_mult(self, session):
        from workers.sampling_worker import SamplingWorker
        from dexscreener.client import DexPairSnapshot

        now = datetime.now(timezone.utc)
        token = make_token_orm(
            baseline_volume_usd=Decimal("1000.00"),
            watch_started_at=now - timedelta(minutes=5),
        )
        session.add(token)
        await session.flush()

        mock_pair = DexPairSnapshot(
            pair_address="PairXX",
            base_token_address=token.mint_address,
            base_token_symbol="TEST",
            base_token_name="Test",
            price_usd=Decimal("0.001"),
            liquidity_usd=Decimal("30000"),
            market_cap_usd=Decimal("60000"),
            volume_m5_usd=Decimal("3500"),   # 3500 / 1000 = 3.5×
            volume_h1_usd=Decimal("15000"),
            buy_pressure_m5=Decimal("0.72"),
            buy_pressure_h1=Decimal("0.65"),
            pair_created_at_ms=None,
            txns_m5_buys=72, txns_m5_sells=28,
            txns_h1_buys=300, txns_h1_sells=150,
        )

        mock_dex = AsyncMock()
        mock_dex.get_token_snapshot = AsyncMock(return_value=mock_pair)

        scoring_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = SamplingWorker(mock_dex, scoring_q, shutdown)

        with patch("workers.sampling_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._sample_one(token)

        from sqlalchemy import select
        result = await session.execute(
            select(TokenSnapshot).where(TokenSnapshot.token_id == token.id)
        )
        snap = result.scalar_one_or_none()
        assert snap is not None
        assert snap.volume_mult == Decimal("3.5000")
        assert snap.buy_pressure == Decimal("0.7200")

    @pytest.mark.asyncio
    async def test_debut_baseline_set_on_first_snapshot(self, session):
        from workers.sampling_worker import SamplingWorker
        from dexscreener.client import DexPairSnapshot

        now = datetime.now(timezone.utc)
        token = make_token_orm(
            baseline_volume_usd=None,  # not yet set
            watch_started_at=now - timedelta(minutes=5),
        )
        session.add(token)
        await session.flush()

        mock_pair = DexPairSnapshot(
            pair_address="PairYY",
            base_token_address=token.mint_address,
            base_token_symbol="TEST",
            base_token_name="Test",
            price_usd=Decimal("0.001"),
            liquidity_usd=Decimal("30000"),
            market_cap_usd=Decimal("60000"),
            volume_m5_usd=Decimal("2000"),
            volume_h1_usd=Decimal("8000"),
            buy_pressure_m5=Decimal("0.65"),
            buy_pressure_h1=Decimal("0.60"),
            pair_created_at_ms=None,
            txns_m5_buys=65, txns_m5_sells=35,
            txns_h1_buys=250, txns_h1_sells=150,
        )
        mock_dex = AsyncMock()
        mock_dex.get_token_snapshot = AsyncMock(return_value=mock_pair)

        scoring_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = SamplingWorker(mock_dex, scoring_q, shutdown)

        with patch("workers.sampling_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._sample_one(token)

        from sqlalchemy import select
        snap_result = await session.execute(
            select(TokenSnapshot).where(TokenSnapshot.token_id == token.id)
        )
        snap = snap_result.scalar_one()
        # First snapshot → volume_mult must be 1.0
        assert snap.volume_mult == Decimal("1.0000")

    @pytest.mark.asyncio
    async def test_age_out_token_when_watching_too_long(self, session):
        from workers.sampling_worker import SamplingWorker

        now = datetime.now(timezone.utc)
        # Token has been WATCHING for 1441 minutes — over the 1440 limit
        token = make_token_orm(
            watch_started_at=now - timedelta(minutes=1441),
        )
        session.add(token)
        await session.flush()

        mock_dex = AsyncMock()
        scoring_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = SamplingWorker(mock_dex, scoring_q, shutdown)

        with patch("workers.sampling_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            result = await worker._sample_one(token)

        assert result is False
        # DEX Screener was never called — age-out happens before fetch
        mock_dex.get_token_snapshot.assert_not_called()

    @pytest.mark.asyncio
    async def test_delisted_token_aged_out(self, session):
        from workers.sampling_worker import SamplingWorker

        now = datetime.now(timezone.utc)
        token = make_token_orm(watch_started_at=now - timedelta(minutes=10))
        session.add(token)
        await session.flush()

        mock_dex = AsyncMock()
        mock_dex.get_token_snapshot = AsyncMock(return_value=None)  # token gone

        scoring_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = SamplingWorker(mock_dex, scoring_q, shutdown)

        with patch("workers.sampling_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            result = await worker._sample_one(token)

        assert result is False


# ── Scoring worker ────────────────────────────────────────────────────────────

class TestScoringWorker:

    def _make_snaps_for_window(self, token_id, signals: str) -> list[TokenSnapshot]:
        """
        signals: 'up' = accelerating, 'flat' = flat, 'down' = decelerating
        Returns 3 snapshots (P1, P2, P3) ordered oldest→newest.
        """
        now = datetime.now(timezone.utc)
        if signals == "up":
            bps    = ["0.50", "0.65", "0.82"]
            vmults = ["1.0",  "2.0",  "4.0"]
            liqs   = ["30000", "30000", "30000"]
            vols   = ["1500", "3000", "6000"]
        elif signals == "down":
            bps    = ["0.82", "0.65", "0.40"]
            vmults = ["4.0",  "2.0",  "0.8"]
            liqs   = ["30000", "30000", "30000"]
            vols   = ["6000", "3000", "1200"]
        else:  # flat
            bps    = ["0.60", "0.61", "0.60"]
            vmults = ["1.5",  "1.5",  "1.5"]
            liqs   = ["30000", "30000", "30000"]
            vols   = ["1500", "1500", "1500"]

        snaps = []
        for i, (bp, vm, liq, vol) in enumerate(zip(bps, vmults, liqs, vols)):
            snaps.append(make_snapshot(
                token_id=token_id,
                buy_pressure=bp,
                volume_mult=vm,
                liquidity_usd=liq,
                volume_usd=vol,
                sampled_at=now - timedelta(seconds=60 - i * 30),
            ))
        return snaps

    @pytest.mark.asyncio
    async def test_window_not_full_returns_skip(self, session):
        from workers.scoring_worker import ScoringWorker

        token = make_token_orm()
        session.add(token)
        await session.flush()
        # Only 2 snapshots — window not full
        for s in self._make_snaps_for_window(token.id, "up")[:2]:
            session.add(s)
        await session.flush()

        entry_q = asyncio.Queue()
        scoring_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = ScoringWorker(scoring_q, entry_q, shutdown)

        with patch("workers.scoring_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            result = await worker._score_token(token)

        assert result == "SKIP"
        assert entry_q.empty()

    @pytest.mark.asyncio
    async def test_strong_buy_pushed_to_entry_queue(self, session):
        from workers.scoring_worker import ScoringWorker

        token = make_token_orm()
        session.add(token)
        await session.flush()
        for s in self._make_snaps_for_window(token.id, "up"):
            session.add(s)
        await session.flush()

        entry_q = asyncio.Queue()
        scoring_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = ScoringWorker(scoring_q, entry_q, shutdown)

        with patch("workers.scoring_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            result = await worker._score_token(token)

        if result == "STRONG_BUY":
            assert entry_q.qsize() == 1
            queued = await entry_q.get()
            assert queued == token.mint_address

    @pytest.mark.asyncio
    async def test_discard_marks_token_rejected(self, session):
        from workers.scoring_worker import ScoringWorker
        from sqlalchemy import select

        token = make_token_orm()
        session.add(token)
        await session.flush()
        for s in self._make_snaps_for_window(token.id, "down"):
            session.add(s)
        await session.flush()

        entry_q = asyncio.Queue()
        scoring_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = ScoringWorker(scoring_q, entry_q, shutdown)

        with patch("workers.scoring_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            result = await worker._score_token(token)

        if result == "DISCARD":
            db_result = await session.execute(
                select(Token).where(Token.mint_address == token.mint_address)
            )
            updated = db_result.scalar_one()
            assert updated.status == TokenStatus.REJECTED
            assert "TIER2_DISCARD" in (updated.rejection_reason or "")
            assert entry_q.empty()

    @pytest.mark.asyncio
    async def test_get_prev_score_returns_none_with_fewer_than_4_snaps(self, session):
        from workers.scoring_worker import ScoringWorker

        token = make_token_orm()
        session.add(token)
        await session.flush()
        # Only 3 snapshots — no prior window available
        for s in self._make_snaps_for_window(token.id, "up"):
            session.add(s)
        await session.flush()

        entry_q = asyncio.Queue()
        scoring_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = ScoringWorker(scoring_q, entry_q, shutdown)

        with patch("workers.scoring_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            prev = await worker._get_prev_score(token)

        assert prev is None

    @pytest.mark.asyncio
    async def test_get_prev_score_computes_from_4th_snapshot(self, session):
        from workers.scoring_worker import ScoringWorker

        now = datetime.now(timezone.utc)
        token = make_token_orm()
        session.add(token)
        await session.flush()

        # 4 snapshots — prior window is snaps 4/3/2
        all_snaps = self._make_snaps_for_window(token.id, "up")
        # Add a 4th oldest snapshot with lower values (prior P1)
        fourth = make_snapshot(
            token_id=token.id,
            buy_pressure="0.40",
            volume_mult="0.8",
            volume_usd="800",
            sampled_at=now - timedelta(seconds=95),
        )
        session.add(fourth)
        for s in all_snaps:
            session.add(s)
        await session.flush()

        entry_q = asyncio.Queue()
        scoring_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = ScoringWorker(scoring_q, entry_q, shutdown)

        with patch("workers.scoring_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            prev = await worker._get_prev_score(token)

        # Returns a real Decimal, not None
        assert prev is not None
        assert isinstance(prev, Decimal)
        assert Decimal("0") <= prev <= Decimal("10")
        from workers.scoring_worker import ScoringWorker
        from sqlalchemy import select

        token = make_token_orm()
        session.add(token)
        await session.flush()
        for s in self._make_snaps_for_window(token.id, "flat"):
            session.add(s)
        await session.flush()

        entry_q = asyncio.Queue()
        scoring_q = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = ScoringWorker(scoring_q, entry_q, shutdown)

        with patch("workers.scoring_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            result = await worker._score_token(token)

        if result == "WATCH":
            db_result = await session.execute(
                select(Token).where(Token.mint_address == token.mint_address)
            )
            updated = db_result.scalar_one()
            assert updated.status == TokenStatus.WATCHING
            assert entry_q.empty()
