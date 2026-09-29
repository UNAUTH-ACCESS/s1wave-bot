"""
analysis/token_lifecycle_analysis.py
=====================================
ANALYSIS-ONLY, READ-ONLY. Full token-lifecycle study across the whole
discovered-token population and every resolved confluence_entry_v1 paper
position — built 2026-09-29 per the user's explicit request: momentum/pump
structure, conditional outcomes, the momentum "sweet spot", and survival
curves, using the snapshot data properly.

Primary dataset for outcome/sweet-spot/survival analysis: confluence_shadow_
positions + confluence_shadow_observations — real-time-monitored (1-second
cadence) paper trades using the EXACT same entry/exit rules real money uses
(workers/confluence_shadow_worker.py), at ~5x the resolved sample size of
confluence_live_trades. Real live trades are reported separately and never
blended into the shadow stats — different capital/execution reality, much
smaller n.

Regime discipline (same reasoning as workers/entry_filters.py's
CURRENT_FILTER_REGIME_SINCE, which exists because mixing pre/post-filter
trades once produced a misleading number): all outcome/sweet-spot/survival
numbers are reported BOTH all-time and "current regime only" side by side.
The exit side has its own regime split too — the fixed STOP_LOSS/TAKE_PROFIT
pair was replaced by engine/trailing_stop.py's staircase on 2026-09-23;
EXIT_REGIME_SINCE marks that.

Pump-structure section uses token_snapshots (150k+ rows, ~30-60s cadence)
across the FULL discovered population, not just entered tokens — this
answers "what does a typical pump actually look like" independent of
whether the bot ever traded it.

Usage: PYTHONPATH=. python analysis/token_lifecycle_analysis.py
Output: analysis/output/token_lifecycle_<timestamp>.md
"""

from __future__ import annotations

import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import settings  # noqa: E402
from workers.entry_filters import CURRENT_FILTER_REGIME_SINCE  # noqa: E402

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
EXIT_REGIME_SINCE = datetime(2026, 9, 23, 0, 0, 0, tzinfo=timezone.utc)  # trailing-stop staircase went live


def _n(v):
    return float(v) if v is not None else None


def pct(x, digits=1):
    return f"{x:+.{digits}%}" if x is not None else "n/a"


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def dist_line(label: str, values: list[float], fmt=lambda x: f"{x:.1f}") -> str:
    if not values:
        return f"- {label}: n=0"
    return (f"- {label}: n={len(values)}, "
            f"p10={fmt(percentile(values, 0.10))}, "
            f"median={fmt(percentile(values, 0.50))}, "
            f"p90={fmt(percentile(values, 0.90))}, "
            f"mean={fmt(statistics.mean(values))}")


# ── SQL ──────────────────────────────────────────────────────────────────

PUMP_STRUCTURE_SQL = """
WITH ranked AS (
    SELECT token_id, sampled_at, price_usd,
           FIRST_VALUE(price_usd) OVER w AS first_price,
           FIRST_VALUE(sampled_at) OVER w AS first_time,
           ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY price_usd DESC, sampled_at ASC) AS peak_rank,
           COUNT(*) OVER (PARTITION BY token_id) AS n_snapshots
    FROM token_snapshots
    WINDOW w AS (PARTITION BY token_id ORDER BY sampled_at)
)
SELECT token_id, first_price, first_time, sampled_at AS peak_time, price_usd AS peak_price, n_snapshots
FROM ranked
WHERE peak_rank = 1 AND n_snapshots >= 5;
"""

SHADOW_SIGNALS_SQL = """
-- confluence_shadow_positions.experiment_version ('confluence_entry_v1', the
-- TRADING strategy) and momentum_signal_events.experiment_version
-- ('momentum_confluence_v1', the underlying SIGNAL) are deliberately
-- different labels for related-but-distinct things — join on token_id only.
-- momentum_signal_events has a UNIQUE(token_id, experiment_version), and we
-- only ever care about the one 'momentum_confluence_v1' row per token, so
-- this is still a clean 1:1 join, not a fan-out.
SELECT sp.id AS position_id, sp.token_id, sp.entry_time, sp.exit_time, sp.exit_reason,
       sp.pnl_pct, sp.status,
       me.n_rules_cofiring, me.buy_pressure, me.trailing_return_3min, me.trailing_return_5min,
       me.liquidity_usd
FROM confluence_shadow_positions sp
JOIN momentum_signal_events me
  ON me.token_id = sp.token_id AND me.experiment_version = 'momentum_confluence_v1'
WHERE sp.status = 'closed'
ORDER BY sp.entry_time;
"""

SHADOW_OBS_PEAK_SQL = """
SELECT position_id, MAX(price_usd) AS max_price, MIN(price_usd) AS min_price
FROM confluence_shadow_observations
GROUP BY position_id;
"""

SHADOW_OBS_TIME_TO_PEAK_SQL = """
SELECT DISTINCT ON (position_id) position_id, observed_at AS peak_time
FROM confluence_shadow_observations
ORDER BY position_id, price_usd DESC, observed_at ASC;
"""

LIVE_TRADES_SQL = """
SELECT status, exit_reason, real_pnl_usd, pnl_usd, entry_time, exit_time
FROM confluence_live_trades
WHERE status = 'closed'
ORDER BY entry_time;
"""


# ── Section A: pump structure ───────────────────────────────────────────

def section_pump_structure(rows) -> list[str]:
    out = ["## 1. Pump structure — the whole discovered population", ""]
    out.append(f"Every token with >=5 snapshots (n={len(rows)}), price relative to its FIRST "
               "snapshot, wherever the peak actually landed in its snapshot history.")
    out.append("")

    time_to_peak_min, peak_multiple, snapshot_span_min = [], [], []
    for r in rows:
        first_p, peak_p = _n(r["first_price"]), _n(r["peak_price"])
        if not first_p or first_p <= 0:
            continue
        ttp = (r["peak_time"] - r["first_time"]).total_seconds() / 60
        time_to_peak_min.append(ttp)
        peak_multiple.append(peak_p / first_p)

    out.append(dist_line("Time from first snapshot to peak (minutes)", time_to_peak_min))
    out.append(dist_line("Peak price / first-snapshot price (multiple)", peak_multiple,
                          fmt=lambda x: f"{x:.2f}x"))
    zero_ttp = sum(1 for t in time_to_peak_min if t < 1.0)
    out.append(f"- Peaked within the FIRST minute of being seen: {zero_ttp}/{len(time_to_peak_min)} "
               f"({zero_ttp/len(time_to_peak_min):.0%}) — these are tokens already past their high the "
               "moment discovery/sampling caught them; nothing downstream can act on a peak that already happened.")
    out.append("")
    return out


# ── Section B/C: conditional outcomes + momentum sweet spot ────────────

def _outcome_stats(rows) -> dict:
    n = len(rows)
    if n == 0:
        return dict(n=0)
    pnls = [_n(r["pnl_pct"]) for r in rows]
    wins = [p for p in pnls if p > 0]
    rugs = [p for p in pnls if p <= -0.50]  # near-total wipeout, matches entry_filters.py's own language
    return dict(
        n=n,
        win_rate=len(wins) / n,
        rug_rate=len(rugs) / n,
        mean_pnl=statistics.mean(pnls),
        median_pnl=statistics.median(pnls),
    )


def _fmt_bucket(label, s) -> str:
    if s["n"] == 0:
        return f"  - {label}: n=0"
    return (f"  - {label}: n={s['n']}, win_rate={s['win_rate']:.0%}, rug_rate={s['rug_rate']:.0%}, "
            f"mean_pnl={pct(s['mean_pnl'])}, median_pnl={pct(s['median_pnl'])}")


def section_conditional_outcomes(rows, regime_label: str) -> list[str]:
    out = [f"## 2. Conditional outcomes ({regime_label}, n={len(rows)})", ""]
    if not rows:
        out.append("- n=0, nothing to report in this regime.")
        out.append("")
        return out

    out.append(f"Overall: {_fmt_bucket('all resolved shadow positions', _outcome_stats(rows))}")
    out.append("")

    out.append("**By n_rules_cofiring:**")
    for k in sorted(set(r["n_rules_cofiring"] for r in rows)):
        bucket = [r for r in rows if r["n_rules_cofiring"] == k]
        out.append(_fmt_bucket(f"n_rules_cofiring={k}", _outcome_stats(bucket)))
    out.append("")

    out.append("**By buy_pressure at signal time (validates the 0.97 floor):**")
    bp_bins = [(0.0, 0.90), (0.90, 0.97), (0.97, 1.01)]
    for lo, hi in bp_bins:
        bucket = [r for r in rows if r["buy_pressure"] is not None and lo <= _n(r["buy_pressure"]) < hi]
        out.append(_fmt_bucket(f"buy_pressure [{lo:.2f}, {hi:.2f})", _outcome_stats(bucket)))
    out.append("")

    out.append("**By liquidity at signal time (validates the $30k ceiling):**")
    liq_bins = [(0, 10_000), (10_000, 30_000), (30_000, 100_000), (100_000, float("inf"))]
    for lo, hi in liq_bins:
        bucket = [r for r in rows if r["liquidity_usd"] is not None and lo <= _n(r["liquidity_usd"]) < hi]
        label = f"${lo:,.0f}-{hi:,.0f}" if hi != float("inf") else f">=${lo:,.0f}"
        out.append(_fmt_bucket(f"liquidity {label}", _outcome_stats(bucket)))
    out.append("")
    return out


def section_momentum_sweet_spot(rows, regime_label: str) -> list[str]:
    out = [f"## 3. Momentum \"sweet spot\" — trailing_return_3min vs outcome ({regime_label})", ""]
    if not rows:
        out.append("- n=0, nothing to report in this regime.")
        out.append("")
        return out

    out.append("Is a BIGGER initial pump always better, or does it fade past some point? "
               "Fine-grained bins on the trailing-3-minute return that triggered the signal:")
    out.append("")
    bins = [(0.053, 0.08), (0.08, 0.12), (0.12, 0.18), (0.18, 0.28), (0.28, 0.45), (0.45, float("inf"))]
    for lo, hi in bins:
        bucket = [r for r in rows if r["trailing_return_3min"] is not None
                  and lo <= _n(r["trailing_return_3min"]) < hi]
        label = f"{lo:.0%}-{hi:.0%}" if hi != float("inf") else f">={lo:.0%}"
        out.append(_fmt_bucket(f"trailing_return_3min {label}", _outcome_stats(bucket)))
    out.append("")
    return out


# ── Section D: survival curves ──────────────────────────────────────────

def section_survival(rows, regime_label: str) -> list[str]:
    out = [f"## 4. Survival curves — how long positions actually last ({regime_label})", ""]
    closed = [r for r in rows if r["exit_time"] is not None]
    if not closed:
        out.append("- n=0, nothing to report in this regime.")
        out.append("")
        return out

    durations_all = [(r["exit_time"] - r["entry_time"]).total_seconds() for r in closed]
    checkpoints_s = [10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200, 21600]

    def survival_row(durations, label):
        n = len(durations)
        cells = []
        for cp in checkpoints_s:
            still_alive = sum(1 for d in durations if d >= cp)
            cells.append(f"{still_alive/n:.0%}")
        return f"  - {label} (n={n}): " + " | ".join(f"{cp}s={c}" for cp, c in zip(checkpoints_s, cells))

    out.append("Fraction of positions STILL OPEN at each elapsed time (i.e. hasn't hit "
               "HARD_FLOOR/STOP_LOSS/TRAILING_STOP/TIME_EXIT yet):")
    out.append("")
    out.append(survival_row(durations_all, "all closed positions"))
    out.append("")

    out.append("**Hold-duration by exit reason** (how fast losers die vs winners run):")
    for reason in sorted(set(r["exit_reason"] for r in closed if r["exit_reason"])):
        durs_min = [(r["exit_time"] - r["entry_time"]).total_seconds() / 60
                    for r in closed if r["exit_reason"] == reason]
        out.append(dist_line(f"{reason} hold time (min)", durs_min))
    out.append("")

    out.append("**Segmented by buy_pressure (does the entry filter also predict how a trade DIES, "
               "not just whether it wins?):**")
    high_bp = [r for r in closed if r["buy_pressure"] is not None and _n(r["buy_pressure"]) >= 0.97]
    low_bp = [r for r in closed if r["buy_pressure"] is not None and _n(r["buy_pressure"]) < 0.97]
    for label, group in (("buy_pressure >= 0.97", high_bp), ("buy_pressure < 0.97", low_bp)):
        if group:
            out.append(survival_row([(r["exit_time"] - r["entry_time"]).total_seconds() for r in group], label))
    out.append("")
    return out


# ── Section E: MFE/MAE from 1-second observations ──────────────────────

def section_mfe_mae(rows, peaks_by_pos, entry_price_by_pos) -> list[str]:
    out = ["## 5. Using the 1-second observation data: MFE/MAE per trade", ""]
    out.append(f"confluence_shadow_observations gives a full 1-second price path per position "
               f"(n={len(peaks_by_pos)} positions with observations). Max Favorable / Adverse Excursion "
               "relative to entry — how far a trade moved in EITHER direction before its actual exit, "
               "independent of when the exit rule actually fired:")
    out.append("")

    mfe_all, mae_all = [], []
    mfe_by_reason: dict[str, list[float]] = {}
    row_by_pos = {r["position_id"]: r for r in rows}
    for pos_id, peak in peaks_by_pos.items():
        entry_price = entry_price_by_pos.get(pos_id)
        if not entry_price or entry_price <= 0:
            continue
        mfe = _n(peak["max_price"]) / entry_price - 1
        mae = _n(peak["min_price"]) / entry_price - 1
        mfe_all.append(mfe)
        mae_all.append(mae)
        r = row_by_pos.get(pos_id)
        if r and r["exit_reason"]:
            mfe_by_reason.setdefault(r["exit_reason"], []).append(mfe)

    out.append(dist_line("MFE (best unrealized gain during the trade)", mfe_all, fmt=lambda x: f"{x:+.1%}"))
    out.append(dist_line("MAE (worst unrealized drawdown during the trade)", mae_all, fmt=lambda x: f"{x:+.1%}"))
    out.append("")
    out.append("**MFE by how the trade actually ended** (did losers ever have a real winning moment?):")
    for reason, vals in sorted(mfe_by_reason.items()):
        out.append(dist_line(f"MFE | exited via {reason}", vals, fmt=lambda x: f"{x:+.1%}"))
    out.append("")
    return out


# ── Live trades, reported separately ────────────────────────────────────

def section_live(rows) -> list[str]:
    out = [f"## 6. Real live trades, for context ONLY — never blended with shadow stats above (n={len(rows)})", ""]
    if not rows:
        out.append("- n=0")
        out.append("")
        return out
    pnls = [_n(r["real_pnl_usd"]) if r["real_pnl_usd"] is not None else _n(r["pnl_usd"]) for r in rows]
    pnls = [p for p in pnls if p is not None]
    if not pnls:
        out.append("- n=0 usable pnl values")
        out.append("")
        return out
    wins = [p for p in pnls if p > 0]
    out.append(f"- n={len(pnls)}, win_rate={len(wins)/len(pnls):.0%}, "
               f"total_pnl_usd={sum(pnls):+.2f}, mean_pnl_usd={statistics.mean(pnls):+.3f}")
    out.append("- Sample is far too small for its own sweet-spot/survival breakdown — the shadow "
               "dataset above is the statistically meaningful one; this is a sanity check that live "
               "isn't wildly diverging from what shadow predicts, not an independent study.")
    out.append("")
    return out


async def main():
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        pump_rows = await conn.fetch(PUMP_STRUCTURE_SQL)
        shadow_rows = await conn.fetch(SHADOW_SIGNALS_SQL)
        peak_rows = await conn.fetch(SHADOW_OBS_PEAK_SQL)
        live_rows = await conn.fetch(LIVE_TRADES_SQL)
    finally:
        await conn.close()

    peaks_by_pos = {r["position_id"]: r for r in peak_rows}
    entry_price_by_pos = {}
    entry_price_rows_dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn2 = await asyncpg.connect(entry_price_rows_dsn)
    try:
        ep_rows = await conn2.fetch("SELECT id, entry_price FROM confluence_shadow_positions WHERE status='closed'")
    finally:
        await conn2.close()
    for r in ep_rows:
        entry_price_by_pos[r["id"]] = _n(r["entry_price"])

    now = datetime.now(timezone.utc)
    print(f"Loaded: {len(pump_rows)} tokens w/ pump structure, {len(shadow_rows)} resolved shadow signals, "
          f"{len(peak_rows)} positions w/ observations, {len(live_rows)} closed live trades.")

    all_time = shadow_rows
    current_regime = [r for r in shadow_rows if r["entry_time"] >= max(CURRENT_FILTER_REGIME_SINCE, EXIT_REGIME_SINCE)]

    ts = now.strftime("%Y%m%d_%H%M%S")
    lines = [f"# Token Lifecycle Analysis — {ts}", "",
             "ANALYSIS-ONLY, read-only. No trading logic touched.", "",
             f"Entry filter regime since: {CURRENT_FILTER_REGIME_SINCE.isoformat()}",
             f"Exit rule (trailing-stop staircase) since: {EXIT_REGIME_SINCE.isoformat()}",
             f"\"Current regime\" below = entries at/after {max(CURRENT_FILTER_REGIME_SINCE, EXIT_REGIME_SINCE).isoformat()}",
             ""]

    lines += section_pump_structure(pump_rows)
    lines += section_conditional_outcomes(all_time, "ALL-TIME")
    lines += section_conditional_outcomes(current_regime, "CURRENT REGIME ONLY")
    lines += section_momentum_sweet_spot(all_time, "ALL-TIME")
    lines += section_momentum_sweet_spot(current_regime, "CURRENT REGIME ONLY")
    lines += section_survival(all_time, "ALL-TIME")
    lines += section_mfe_mae(all_time, peaks_by_pos, entry_price_by_pos)
    lines += section_live(live_rows)

    OUTPUT_DIR.mkdir(exist_ok=True)
    out_path = OUTPUT_DIR / f"token_lifecycle_{ts}.md"
    out_path.write_text("\n".join(lines))
    print(f"Written: {out_path}")
    print()
    print("\n".join(lines))


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
