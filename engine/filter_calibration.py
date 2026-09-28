"""
engine/filter_calibration.py
==============================
Periodic self-audit (2026-09-28) for the 3 production entry filters
(workers/entry_filters.py) — re-runs the exact bucket analysis this
session used to derive and validate them (win rate / rug rate / capped
mean for the population passing buy_pressure>=0.97, liquidity<$30k, not
wash-trading-rejected), on a trailing window, and compares against the
baseline established the day they were last fully re-validated.

Deliberately does NOT adjust any threshold itself — only flags a real
divergence for a human (or a future Claude session) to look at. This is
not caution for its own sake: the SAME DAY this module was built, this
codebase spent hours raising and reverting `_MAX_ENTRY_RENT_OVERHEAD_
LAMPORTS` twice, each raise backed by real, reasoned evidence, and each
one still produced a real-money surprise only live trading revealed. An
algorithm that can quietly retune its own risk parameters based on its
own metrics has no such check — it would compound a bad read instead of
surfacing it. Detect-and-flag, human decides, every time.

Runs from workers/confluence_live_worker.py's periodic-check pattern
(same shape as _maybe_sweep_rent/_maybe_backfill_audit_trail) even though
it only reads shadow-side data — shadow's much larger sample is what
actually validated these filters in the first place, and checking it
here keeps "is the SIGNAL still good" cleanly separate from "is EXECUTION
still good" (today's whole entry-cost-ceiling saga was entirely the
latter question, which this check does not attempt to answer).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from database.engine import get_session
from models.orm import ConfluenceShadowPosition, MomentumSignalEvent, TokenEvaluation

_CAP = 1.0
_MIN_SAMPLE = 15
_TRAILING_WINDOW_DAYS = 14

# Baseline established 2026-09-28 from a 404-shadow-trade sample — the
# population passing all 3 current filters (buy_pressure>=0.97,
# liquidity<$30k, not wash-trading-rejected): n=196, win 86.2%, rug 7.7%,
# capped mean +29.6%. Update these three numbers (with a new dated
# comment recording the sample this came from) only after a deliberate,
# reviewed recalibration — never automatically, and never just because
# this check fired once.
BASELINE_WIN_RATE = 0.862
BASELINE_RUG_RATE = 0.077
BASELINE_CAPPED_MEAN = 0.296
BASELINE_ESTABLISHED_AT = "2026-09-28"
BASELINE_SAMPLE_N = 196

# "Needs review" triggers — deliberately generous. This session's own
# smallest reliable buckets (n=15-30) showed real double-digit-point win-
# rate swings from ordinary noise alone; these thresholds are meant to
# catch an actual regime shift, not a routine short-term dip.
_REVIEW_WIN_RATE_FLOOR = 0.65
_REVIEW_RUG_RATE_CEILING = 0.20
_REVIEW_CAPPED_MEAN_FLOOR = 0.0


@dataclass
class CalibrationResult:
    n: int
    window_days: int
    win_rate: float | None
    rug_rate: float | None
    capped_mean: float | None
    needs_review: bool
    reasons: list[str] = field(default_factory=list)


def _capped(p: float) -> float:
    return min(p, _CAP)


def _bucket_stats(pnls: list[float]) -> tuple[float, float, float]:
    n = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    rugs = sum(1 for p in pnls if p <= -0.40)
    mean_capped = sum(_capped(p) for p in pnls) / n
    return wins / n, rugs / n, mean_capped


def evaluate(pnls: list[float], window_days: int = _TRAILING_WINDOW_DAYS) -> CalibrationResult:
    """Pure function (separated from the DB query below so it's trivially
    testable): given the trailing window's pnl_pct list for the
    current-filter population, returns whether it still looks like the
    2026-09-28 baseline."""
    n = len(pnls)
    if n < _MIN_SAMPLE:
        return CalibrationResult(
            n=n, window_days=window_days, win_rate=None, rug_rate=None, capped_mean=None, needs_review=False,
            reasons=[f"only {n} trades in the last {window_days}d — below the {_MIN_SAMPLE}-trade minimum to judge"],
        )

    win_rate, rug_rate, capped_mean = _bucket_stats(pnls)
    reasons = []
    if win_rate < _REVIEW_WIN_RATE_FLOOR:
        reasons.append(
            f"win rate {win_rate:.1%} is below the {_REVIEW_WIN_RATE_FLOOR:.0%} review floor "
            f"(baseline {BASELINE_WIN_RATE:.1%} from {BASELINE_ESTABLISHED_AT})"
        )
    if rug_rate > _REVIEW_RUG_RATE_CEILING:
        reasons.append(
            f"rug rate {rug_rate:.1%} is above the {_REVIEW_RUG_RATE_CEILING:.0%} review ceiling "
            f"(baseline {BASELINE_RUG_RATE:.1%} from {BASELINE_ESTABLISHED_AT})"
        )
    if capped_mean < _REVIEW_CAPPED_MEAN_FLOOR:
        reasons.append(
            f"capped mean {capped_mean:+.1%} has gone negative "
            f"(baseline {BASELINE_CAPPED_MEAN:+.1%} from {BASELINE_ESTABLISHED_AT})"
        )

    return CalibrationResult(n=n, window_days=window_days, win_rate=win_rate, rug_rate=rug_rate,
                              capped_mean=capped_mean, needs_review=bool(reasons), reasons=reasons)


async def check_current_filter_population(window_days: int = _TRAILING_WINDOW_DAYS) -> CalibrationResult:
    """
    Re-derives win/rug/capped-mean for the population passing ALL 3
    current entry filters over the last `window_days` of shadow data, and
    compares against the baseline above. See workers/entry_filters.py for
    what each filter is and why; this only checks whether their combined
    real-world performance still looks the same, not whether the
    thresholds themselves are still the best possible choice — that's a
    bigger, periodic research pass (see CLAUDE.md's "How past
    improvements got made"), not this quick weekly check.
    """
    since = datetime.now(timezone.utc) - timedelta(days=window_days)
    async with get_session() as session:
        rows = (await session.execute(
            select(ConfluenceShadowPosition)
            .where(ConfluenceShadowPosition.status == "closed", ConfluenceShadowPosition.entry_time >= since)
        )).scalars().all()
        sigs = (await session.execute(select(MomentumSignalEvent))).scalars().all()
        tes = (await session.execute(
            select(TokenEvaluation).where(TokenEvaluation.gate == "TIER1")
        )).scalars().all()

    sig_by_token = {s.token_id: s for s in sigs}
    te_by_token: dict = defaultdict(list)
    for te in tes:
        te_by_token[te.token_id].append(te)

    pnls: list[float] = []
    for r in rows:
        if r.pnl_pct is None:
            continue
        sig = sig_by_token.get(r.token_id)
        if sig is None or sig.buy_pressure is None or float(sig.buy_pressure) < 0.97:
            continue
        cands = [te for te in te_by_token.get(r.token_id, []) if te.evaluated_at <= r.entry_time]
        te = max(cands, key=lambda te: te.evaluated_at) if cands else None
        if te is not None and te.reason_code == "WASH_TRADING" and not te.passed:
            continue
        liq = te.inputs_json.get("liquidity_usd") if te and te.inputs_json else None
        if liq is not None and float(liq) >= 30000:
            continue
        pnls.append(float(r.pnl_pct))

    return evaluate(pnls, window_days=window_days)
