"""
engine/rolling_window.py
========================
Rolling window engine — PRD §6

Pure functions.  No I/O, no database, no async.  Takes three snapshots
(P1=oldest, P2=middle, P3=most recent) and produces classified trend
signals for BP and VM.

Trend classification is based on DIRECTION AND RATE OF CHANGE across
P1→P2→P3, not absolute level.  A token at BP 0.6/0.7/0.8 scores
differently from one at 0.8/0.7/0.6 even at identical composite levels.

Terminology
-----------
P1  oldest snapshot in the rolling window (sampled ~60s ago)
P2  middle snapshot                        (sampled ~30s ago)
P3  most recent snapshot                   (sampled now)

Signal components (PRD §6.2)
-----------------------------
BP  — buy_pressure:  buy_volume / total_volume per snapshot
VM  — volume_mult:   snapshot.volume_usd / token.baseline_volume_usd
VLR — vol/liq ratio: snapshot.volume_usd / snapshot.liquidity_usd

Trend classification (PRD §6.3)
---------------------------------
ACCELERATING  P1<P2<P3 AND rate of change (Δ2 > Δ1 or both positive)
EMERGING      P1≈P2 AND P3 meaningfully above both
SPIKE         P3 significantly above P1 and P2 with no prior build-up
FLAT          P1≈P2≈P3 (all deltas within tolerance)
DECELERATING  P3 < P2 (regardless of P1→P2 direction)

Composite score (PRD §6.4)
--------------------------
score = (BP_score × 0.40) + (VM_score × 0.35) + (VLR_score × 0.25)
Range 0–10.

Score bands (PRD §6.5)
-----------------------
≥ 8.0  STRONG_BUY → proceed to entry gate
5.0–7.9  WATCH → re-score next cycle
< 5.0  DISCARD

VMZone classification (PRD resolution from Phase 1)
----------------------------------------------------
EARLY      score rising, token age < 10 minutes
STRONG     score ≥ TIER2_STRONG_BUY_THRESHOLD, sustained momentum
LATE       score plateauing or declining, age > 30 minutes
EXHAUSTION VM DECELERATING and BP DECELERATING simultaneously
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from decimal import Decimal
from typing import NamedTuple

from config.settings import settings
from models.orm import TrendDirection, VMZone


# ── Tuning constants ──────────────────────────────────────────────────────────

# Minimum absolute delta to call a direction meaningful (not noise).
# Below this threshold, P1≈P2 or P2≈P3 is treated as FLAT.
_BP_DELTA_THRESHOLD  = Decimal("0.02")   # 2 percentage points
_VM_DELTA_THRESHOLD  = Decimal("0.10")   # 10% multiplier change
_VLR_DELTA_THRESHOLD = Decimal("0.005")  # 0.5% vol/liq change

# Spike detection: P3 must be this many times larger than max(P1, P2)
_SPIKE_MULTIPLIER = Decimal("1.5")

# EMERGING: P3 must exceed both P1 and P2 by at least this delta
_EMERGING_THRESHOLD = Decimal("0.05")   # for BP
_EMERGING_VM_THRESHOLD = Decimal("0.25")


# ── Snapshot triple ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SnapshotWindow:
    """
    Three consecutive snapshots for a single token, oldest→newest.

    All values are pre-computed and stored (volume_mult was written by
    the sampling_worker at snapshot time).
    """
    # P1 = oldest, P2 = middle, P3 = most recent
    bp_p1: Decimal   # buy pressure
    bp_p2: Decimal
    bp_p3: Decimal

    vm_p1: Decimal   # volume multiplier (vs debut baseline)
    vm_p2: Decimal
    vm_p3: Decimal

    vlr_p1: Decimal  # volume / liquidity ratio
    vlr_p2: Decimal
    vlr_p3: Decimal

    # Token age at P3 sample time (minutes since pair creation)
    age_minutes: float

    # Previous composite score (None on first scoring cycle)
    prev_composite_score: Decimal | None = None


# ── Trend classification ──────────────────────────────────────────────────────

def _classify_trend(
    p1: Decimal,
    p2: Decimal,
    p3: Decimal,
    delta_threshold: Decimal,
    emerging_threshold: Decimal,
) -> TrendDirection:
    """
    Classify the direction of change across P1→P2→P3.

    Evaluation order matters — DECELERATING is checked first because a
    falling P3 is always a bearish signal regardless of the P1→P2 move.
    """
    d1 = p2 - p1  # delta from P1→P2
    d2 = p3 - p2  # delta from P2→P3

    # ── DECELERATING: P3 retreating from P2 ──────────────────────────────
    if d2 < -delta_threshold:
        return TrendDirection.DECELERATING

    # ── FLAT: all values within noise threshold ───────────────────────────
    total_range = p3 - p1
    if abs(total_range) <= delta_threshold and abs(d1) <= delta_threshold:
        return TrendDirection.FLAT

    # ── ACCELERATING: consistently rising, second leg ≥ first leg ────────
    if d1 > delta_threshold and d2 > delta_threshold:
        return TrendDirection.ACCELERATING

    # ── SPIKE: P3 large jump with no meaningful prior build ───────────────
    prior_max = max(p1, p2)
    if p3 > prior_max * _SPIKE_MULTIPLIER and abs(d1) <= delta_threshold:
        return TrendDirection.SPIKE

    # ── EMERGING: P3 clearly above both P1 and P2 ────────────────────────
    if (p3 - p1) >= emerging_threshold and (p3 - p2) >= delta_threshold:
        return TrendDirection.EMERGING

    # ── Default: treat as FLAT if nothing else matched ────────────────────
    return TrendDirection.FLAT


def classify_bp_trend(w: SnapshotWindow) -> TrendDirection:
    return _classify_trend(
        w.bp_p1, w.bp_p2, w.bp_p3,
        _BP_DELTA_THRESHOLD, _EMERGING_THRESHOLD,
    )


def classify_vm_trend(w: SnapshotWindow) -> TrendDirection:
    return _classify_trend(
        w.vm_p1, w.vm_p2, w.vm_p3,
        _VM_DELTA_THRESHOLD, _EMERGING_VM_THRESHOLD,
    )


def classify_vlr_trend(w: SnapshotWindow) -> TrendDirection:
    return _classify_trend(
        w.vlr_p1, w.vlr_p2, w.vlr_p3,
        _VLR_DELTA_THRESHOLD, _VLR_DELTA_THRESHOLD * 5,
    )


# ── Per-signal scoring ────────────────────────────────────────────────────────

_TREND_SCORES: dict[TrendDirection, Decimal] = {
    TrendDirection.ACCELERATING: Decimal("9.5"),
    TrendDirection.EMERGING:     Decimal("7.5"),
    TrendDirection.SPIKE:        Decimal("6.0"),  # lower confidence
    TrendDirection.FLAT:         Decimal("4.0"),
    TrendDirection.DECELERATING: Decimal("1.5"),  # penalised
}


def _signal_score(trend: TrendDirection, absolute_level: Decimal) -> Decimal:
    """
    Convert a trend classification into a 0–10 score.

    Absolute level provides a small modifier (±0.5) on top of the trend
    base score, so a strongly ACCELERATING signal at a high absolute level
    scores slightly better than the same trend at a low level.
    The modifier is capped so it never inverts the trend ordering.
    """
    base = _TREND_SCORES[trend]
    # Level modifier: 0.0→0.5 mapped to -0.5→+0.5
    # Clamp absolute_level to [0, 1] before scaling
    clamped = max(Decimal("0"), min(Decimal("1"), absolute_level))
    modifier = (clamped - Decimal("0.5")) * Decimal("1.0")
    raw = base + modifier
    return max(Decimal("0"), min(Decimal("10"), raw))


# ── VMZone classification ──────────────────────────────────────────────────────

def classify_vm_zone(
    w: SnapshotWindow,
    bp_trend: TrendDirection,
    vm_trend: TrendDirection,
    composite_score: Decimal,
) -> VMZone:
    """
    Classify the volume momentum zone at the point of entry scoring.

    Rules (from PRD resolution):
        EXHAUSTION  — VM DECELERATING and BP DECELERATING simultaneously
        EARLY       — composite score rising (prev < current), age < 10 min
        LATE        — score plateauing or declining, age > 30 min
        STRONG      — score ≥ TIER2_STRONG_BUY_THRESHOLD, sustained momentum
    """
    # EXHAUSTION takes priority — both signals fading simultaneously
    if vm_trend == TrendDirection.DECELERATING and bp_trend == TrendDirection.DECELERATING:
        return VMZone.EXHAUSTION

    # EARLY — catching momentum at inception
    if w.age_minutes < 10:
        prev = w.prev_composite_score
        if prev is None or composite_score > prev:
            return VMZone.EARLY

    # LATE — momentum fading with age
    if w.age_minutes > 30 and (
        vm_trend == TrendDirection.DECELERATING
        or vm_trend == TrendDirection.FLAT
    ):
        return VMZone.LATE

    # STRONG — default for a healthy STRONG_BUY signal
    return VMZone.STRONG


# ── Liquidity awareness ───────────────────────────────────────────────────────

from models.orm import LiquidityFlag, LiquidityTrend  # noqa: E402


def classify_liquidity_flag(
    position_size_usd: Decimal,
    liquidity_usd: Decimal,
) -> LiquidityFlag:
    """
    PRD §7: position size as fraction of pool liquidity.
    Logged at entry — never gates a trade.
    """
    if liquidity_usd <= Decimal("0"):
        return LiquidityFlag.SHALLOW
    pct = position_size_usd / liquidity_usd
    if pct < Decimal("0.005"):
        return LiquidityFlag.DEEP
    if pct < Decimal("0.02"):
        return LiquidityFlag.NORMAL
    if pct < Decimal("0.05"):
        return LiquidityFlag.THIN
    return LiquidityFlag.SHALLOW


def classify_liquidity_trend(w: SnapshotWindow) -> LiquidityTrend:
    """
    Liquidity direction across the rolling window.
    VLR trend is a proxy — rising VLR with stable volume suggests draining liq.
    We use direct liquidity comparison instead for clarity.
    """
    # Passed via SnapshotWindow.vlr fields which embed liquidity implicitly;
    # for a cleaner signal we'd need raw liquidity_usd per snapshot.
    # Phase 4 passes vlr = volume / liquidity, so we can infer direction
    # from VLR trend when volume is approximately stable.
    vlr_trend = classify_vlr_trend(w)
    if vlr_trend == TrendDirection.ACCELERATING or vlr_trend == TrendDirection.EMERGING:
        # Rising VLR = liq draining relative to volume, or volume growing
        # We conservatively call this STABLE unless we see strong drain signals
        return LiquidityTrend.STABLE
    if vlr_trend == TrendDirection.DECELERATING:
        return LiquidityTrend.GROWING  # VLR falling = liquidity growing faster
    return LiquidityTrend.STABLE


# ── Composite scorer ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ScoringResult:
    """
    Full result of a Tier 2 scoring pass.

    signal        — STRONG_BUY / WATCH / DISCARD
    composite     — 0.0–10.0
    bp_trend      — TrendDirection
    vm_trend      — TrendDirection
    vlr_trend     — TrendDirection (informational)
    vm_zone       — VMZone (populated for STRONG_BUY only; None otherwise)
    liq_flag      — LiquidityFlag. None when called from the scoring_worker
                    (no position size yet). Populated by the capital engine
                    (Phase 5) at entry time using classify_liquidity_flag()
                    directly — it owns the position_size_usd and
                    liquidity_usd values at that point.
    liq_trend     — LiquidityTrend
    """
    signal: str          # "STRONG_BUY" | "WATCH" | "DISCARD"
    composite: Decimal
    bp_trend: TrendDirection
    vm_trend: TrendDirection
    vlr_trend: TrendDirection
    vm_zone: VMZone | None
    liq_flag: LiquidityFlag | None  # set by capital engine at entry, not here
    liq_trend: LiquidityTrend

    @property
    def is_strong_buy(self) -> bool:
        return self.signal == "STRONG_BUY"

    @property
    def is_watch(self) -> bool:
        return self.signal == "WATCH"

    @property
    def is_discard(self) -> bool:
        return self.signal == "DISCARD"


def score_window(window: SnapshotWindow) -> ScoringResult:
    """
    Compute the full Tier 2 composite score for a rolling window.

    liq_flag is always None here — it requires position_size_usd and
    liquidity_usd which are only available at entry time.  The capital
    engine (Phase 5) calls classify_liquidity_flag() directly when
    opening a position and writes the result to the Trade record.

    Parameters
    ----------
    window : SnapshotWindow with P1/P2/P3 values.

    Returns
    -------
    ScoringResult — caller checks .signal to decide next action.
    """
    bp_trend  = classify_bp_trend(window)
    vm_trend  = classify_vm_trend(window)
    vlr_trend = classify_vlr_trend(window)

    bp_score  = _signal_score(bp_trend,  window.bp_p3)
    vm_score  = _signal_score(vm_trend,  window.vm_p3)
    vlr_score = _signal_score(vlr_trend, min(window.vlr_p3, Decimal("1")))

    # Weighted composite: BP×0.40 + VM×0.35 + VLR×0.25
    composite = (
        bp_score  * Decimal("0.40") +
        vm_score  * Decimal("0.35") +
        vlr_score * Decimal("0.25")
    ).quantize(Decimal("0.01"))

    strong_buy_threshold = Decimal(str(settings.TIER2_STRONG_BUY_THRESHOLD))
    watch_threshold      = Decimal(str(settings.TIER2_WATCH_THRESHOLD))

    if composite >= strong_buy_threshold:
        signal = "STRONG_BUY"
    elif composite >= watch_threshold:
        signal = "WATCH"
    else:
        signal = "DISCARD"

    # VMZone only meaningful at entry-gate (STRONG_BUY); None otherwise
    vm_zone: VMZone | None = None
    if signal == "STRONG_BUY":
        vm_zone = classify_vm_zone(window, bp_trend, vm_trend, composite)

    liq_trend = classify_liquidity_trend(window)

    return ScoringResult(
        signal=signal,
        composite=composite,
        bp_trend=bp_trend,
        vm_trend=vm_trend,
        vlr_trend=vlr_trend,
        vm_zone=vm_zone,
        liq_flag=None,  # set by capital engine (Phase 5) at entry time
        liq_trend=liq_trend,
    )
