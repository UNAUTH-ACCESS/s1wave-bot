"""
analysis/confluence_entry_v1_evaluation.py
=============================================
ANALYSIS-ONLY, READ-ONLY. The "go/no-go" evaluation for confluence_entry_v1,
run once the experiment reaches n>=MIN_TRADES_FOR_EVALUATION closed
positions (or SolanaTracker's account is exhausted again, whichever comes
first, per explicit user instruction 2026-09-22 — "Freeze the strategy.
Change nothing."). No trading logic, thresholds, or scorer weights are
touched by this script or by running it.

Every number here comes directly from confluence_shadow_positions rows
closed live by workers/confluence_shadow_worker.py — real 1-second
DexScreener monitoring, real engine/risk.py exit priority. No simulation.

Metrics computed (per the exact evaluation spec requested)
-------------------------------------------------------------
  - Net cumulative P&L (additive/R-multiple AND compounding, both reported
    — see "Cumulative P&L methodology" below for why both)
  - Expectancy (mean pnl_pct)
  - Win rate
  - Profit factor (gross profit / gross loss)
  - Median and mean winner, median and mean loser
  - Maximum drawdown (on the additive equity curve, chronological by
    exit_time)
  - Rug frequency and loss magnitude (HARD_FLOOR exits beyond a severe
    threshold vs a "normal" floor breach)
  - Performance by confluence count (2 vs 3 vs 4 rules co-firing)
  - Performance over chronological quarters (does the edge persist across
    the sample's own history, or is it concentrated/decaying?)
  - Chronological train/test split: does +16.5% (or whatever the current
    pooled expectancy is) survive on a genuinely held-out later half?

Cumulative P&L methodology
------------------------------
No real position sizing has been decided yet (that's exactly what the
accompanying go-live plan proposes). Two honest ways to read "cumulative
P&L" from a series of independent %-return trades:
  - ADDITIVE (R-multiples): treat each trade as risking the same fixed
    unit, sum the pnl_pct values directly. This is the standard way
    traders read "how many R did this system make" and is what "maximum
    drawdown" is computed against below.
  - COMPOUNDING: multiply (1+pnl_pct) sequentially, assuming full
    reinvestment of the entire balance into a single position each trade
    (unrealistic — real position sizing would risk a fraction of
    balance, not the whole thing) — reported only as a secondary,
    clearly-labeled reference, not the primary read.

Rug definition used here
----------------------------
A HARD_FLOOR exit with pnl_pct <= RUG_THRESHOLD (-40%) is counted as a
"rug" (a real flash-crash/liquidity-pull event, like NASA's confirmed
-97.6% case from the 2026-09-22 checkpoint) as distinct from a HARD_FLOOR
exit closer to the nominal -7% (the floor working close to as intended,
just past the -6% STOP_LOSS line). This distinction matters because it
separates "the floor worked, roughly" from "a real, currently-
uncappable tail event happened."

Usage
-----
    PYTHONPATH=. python analysis/confluence_entry_v1_evaluation.py

Outputs
-------
    analysis/output/confluence_entry_v1_evaluation_<timestamp>.md
    analysis/output/confluence_entry_v1_evaluation_dataset_<timestamp>.csv
"""

from __future__ import annotations

import asyncio
import csv
import statistics
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
from scipy import stats as scipy_stats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
EXPERIMENT_VERSION = "confluence_entry_v1"
MIN_TRADES_FOR_EVALUATION = 100
RUG_THRESHOLD = -0.40

POSITIONS_SQL = """
SELECT csp.id, t.symbol, csp.entry_time, csp.entry_price, csp.n_rules_cofiring,
       csp.status, csp.exit_time, csp.exit_price, csp.exit_reason, csp.pnl_pct
FROM confluence_shadow_positions csp
JOIN tokens t ON t.id = csp.token_id
WHERE csp.experiment_version = $1
ORDER BY csp.exit_time ASC NULLS LAST;
"""


def _num(v):
    return float(v) if v is not None else None


def profit_factor(pnls):
    gross_profit = sum(p for p in pnls if p > 0)
    gross_loss = abs(sum(p for p in pnls if p <= 0))
    if gross_loss == 0:
        return float("inf") if gross_profit > 0 else None
    return gross_profit / gross_loss


def max_drawdown_additive(pnls_in_order):
    """Chronological additive equity curve (R-multiples). Returns
    (max_drawdown_R, peak_R, trough_R, index_of_trough)."""
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    dd_peak, dd_trough = 0.0, 0.0
    for p in pnls_in_order:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
            dd_peak, dd_trough = peak, equity
    return max_dd, dd_peak, dd_trough


def summarize(pnls, label):
    n = len(pnls)
    if n == 0:
        return f"- {label}: n=0"
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    win_rate = len(wins) / n
    expectancy = statistics.mean(pnls)
    pf = profit_factor(pnls)
    pf_txt = f"{pf:.2f}" if pf is not None and pf != float("inf") else ("inf" if pf == float("inf") else "n/a")
    return (f"- {label}: n={n}, win_rate={win_rate:.0%}, expectancy={expectancy:+.1%}, "
            f"profit_factor={pf_txt}, median={statistics.median(pnls):+.1%}")


async def main():
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(POSITIONS_SQL, EXPERIMENT_VERSION)
    finally:
        await conn.close()

    now = datetime.now(timezone.utc)
    records = []
    for r in rows:
        records.append(dict(
            symbol=r["symbol"], position_id=str(r["id"]), entry_time=r["entry_time"],
            entry_price=_num(r["entry_price"]), n_rules_cofiring=r["n_rules_cofiring"],
            status=r["status"], exit_time=r["exit_time"], exit_price=_num(r["exit_price"]),
            exit_reason=r["exit_reason"], pnl_pct=_num(r["pnl_pct"]),
        ))

    closed = [r for r in records if r["status"] == "closed" and r["pnl_pct"] is not None]
    closed.sort(key=lambda r: r["exit_time"])
    n = len(closed)
    pnls = [r["pnl_pct"] for r in closed]

    ts = now.strftime("%Y%m%d_%H%M%S")
    csv_path = OUTPUT_DIR / f"confluence_entry_v1_evaluation_dataset_{ts}.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(records[0].keys()) if records else [])
        w.writeheader()
        for rec in records:
            w.writerow(rec)

    lines = []
    p = lines.append
    p(f"# confluence_entry_v1 — Go/No-Go Evaluation — {ts}")
    p("")
    p("ANALYSIS-ONLY. No simulation. No trading logic touched. Strategy frozen per explicit "
      "instruction — this evaluation observes, it does not tune.")
    p("")
    p(f"- Closed positions: **{n}** (target: {MIN_TRADES_FOR_EVALUATION}+, or run early if "
      "SolanaTracker's account exhausted again — see caveats).")
    if n < MIN_TRADES_FOR_EVALUATION:
        p(f"- **NOTE: n={n} is below the {MIN_TRADES_FOR_EVALUATION} target.** Every number below is "
          "real, but treat this as an interim read, not the final evaluation, unless this run was "
          "explicitly triggered early by API exhaustion.")
    p("")

    if n == 0:
        p("No closed positions yet — nothing to evaluate.")
        report_path = OUTPUT_DIR / f"confluence_entry_v1_evaluation_{ts}.md"
        report_path.write_text("\n".join(lines))
        print(f"Wrote {report_path}")
        return

    # ── Core metrics ────────────────────────────────────────────────────
    wins = [pl for pl in pnls if pl > 0]
    losses = [pl for pl in pnls if pl <= 0]
    win_rate = len(wins) / n
    expectancy = statistics.mean(pnls)
    pf = profit_factor(pnls)
    additive_cum = sum(pnls)
    compounding_cum = 1.0
    for pl in pnls:
        compounding_cum *= (1 + pl)
    compounding_cum -= 1.0
    max_dd, dd_peak, dd_trough = max_drawdown_additive(pnls)

    p("## Core metrics")
    p("")
    p(f"- **Net cumulative P&L (additive, R-multiples): {additive_cum:+.2f}R** "
      f"(sum of pnl_pct across all {n} trades, equal unit risk assumed per trade)")
    p(f"- Net cumulative P&L (compounding, full-reinvestment reference only): {compounding_cum:+.1%}")
    p(f"- **Expectancy: {expectancy:+.2%}** per trade")
    p(f"- **Win rate: {win_rate:.1%}** ({len(wins)}/{n})")
    p(f"- **Profit factor: {pf:.2f}**" if pf and pf != float('inf') else f"- Profit factor: {pf}")
    p(f"- Median winner: {statistics.median(wins):+.1%}" if wins else "- Median winner: n/a")
    p(f"- Mean winner: {statistics.mean(wins):+.1%}" if wins else "- Mean winner: n/a")
    p(f"- Median loser: {statistics.median(losses):+.1%}" if losses else "- Median loser: n/a")
    p(f"- Mean loser: {statistics.mean(losses):+.1%}" if losses else "- Mean loser: n/a")
    p(f"- **Maximum drawdown: {max_dd:.2f}R** (peak {dd_peak:+.2f}R -> trough {dd_trough:+.2f}R, "
      "on the chronological additive equity curve)")
    p("")

    # ── Rug frequency ───────────────────────────────────────────────────
    hard_floors = [pl for pl in pnls if pl <= float(settings.HARD_FLOOR_PCT) + 1e-9]  # any HARD_FLOOR-magnitude loss
    rugs = [pl for pl in pnls if pl <= RUG_THRESHOLD]
    normal_floors = [pl for pl in hard_floors if pl > RUG_THRESHOLD]
    p("## Rug frequency and loss magnitude")
    p("")
    p(f"- Total HARD_FLOOR-magnitude exits: {len(hard_floors)}/{n} ({len(hard_floors)/n:.0%})")
    p(f"- Of those, \"normal\" floor breaches (worse than -6% but not below {RUG_THRESHOLD:.0%}): "
      f"{len(normal_floors)}, mean {statistics.mean(normal_floors):+.1%}" if normal_floors else
      f"- Of those, \"normal\" floor breaches: 0")
    p(f"- **Confirmed rugs (<= {RUG_THRESHOLD:.0%}): {len(rugs)}/{n} ({len(rugs)/n:.1%})**, "
      f"mean {statistics.mean(rugs):+.1%}, worst {min(rugs):+.1%}" if rugs else
      f"- Confirmed rugs (<= {RUG_THRESHOLD:.0%}): 0")
    p("")

    # ── By confluence count ─────────────────────────────────────────────
    p("## Performance by confluence count")
    p("")
    by_count = {}
    for r in closed:
        by_count.setdefault(r["n_rules_cofiring"], []).append(r["pnl_pct"])
    for count in sorted(by_count):
        p(summarize(by_count[count], f"{count} rules co-firing"))
    p("")

    # ── Chronological quarters ──────────────────────────────────────────
    p("## Performance over chronological quarters (does the edge persist?)")
    p("")
    q_size = max(1, n // 4)
    quarters = [closed[i:i + q_size] for i in range(0, n, q_size)]
    if len(quarters) > 4:
        quarters[3].extend(sum((q for q in quarters[4:]), []))
        quarters = quarters[:4]
    for i, q in enumerate(quarters, 1):
        q_pnls = [r["pnl_pct"] for r in q]
        span = f"{q[0]['exit_time'].isoformat()} to {q[-1]['exit_time'].isoformat()}" if q else "n/a"
        p(f"### Quarter {i} ({span})")
        p(summarize(q_pnls, "quarter"))
        p("")

    # ── Out-of-sample: chronological train/test ─────────────────────────
    p("## Out-of-sample check: does the pooled expectancy survive a chronological split?")
    p("")
    split_idx = n // 2
    train = pnls[:split_idx]
    test = pnls[split_idx:]
    p(summarize(train, "TRAIN (earlier half)"))
    p(summarize(test, "TEST (later half)"))
    if train and test:
        try:
            u, pval = scipy_stats.mannwhitneyu(train, test, alternative="two-sided")
            p(f"- Mann-Whitney U test (train vs test distributions): p={pval:.3f} "
              f"({'no significant difference — consistent with a stable edge' if pval > 0.05 else 'statistically different — investigate whether the edge is drifting'})")
        except Exception:
            pass
        both_positive = statistics.mean(train) > 0 and statistics.mean(test) > 0
        p(f"- **{'Expectancy sign survives out-of-sample (positive in both halves).' if both_positive else 'Expectancy sign does NOT survive in both halves — treat pooled expectancy with caution.'}**")
    p("")

    # ── Exit reason breakdown ───────────────────────────────────────────
    p("## Exit reason breakdown")
    p("")
    reasons = Counter(r["exit_reason"] for r in closed)
    for reason, cnt in reasons.most_common():
        subset = [r["pnl_pct"] for r in closed if r["exit_reason"] == reason]
        p(f"- {reason}: n={cnt} ({cnt/n:.0%}), mean={statistics.mean(subset):+.1%}")
    p("")

    p("## Caveats")
    p("")
    p("- All figures are real, live 1-second-monitored outcomes — no simulation.")
    p("- Position sizing is not yet decided; additive (R-multiple) cumulative P&L and drawdown "
      "assume equal risk per trade, not a specific dollar amount.")
    p("- No trading logic, thresholds, or scorer weights were touched to produce this evaluation.")

    report_path = OUTPUT_DIR / f"confluence_entry_v1_evaluation_{ts}.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")
    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
