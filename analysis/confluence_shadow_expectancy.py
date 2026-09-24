"""
analysis/confluence_shadow_expectancy.py
===========================================
ANALYSIS-ONLY, READ-ONLY. The direct, non-proxy answer to the question
analysis/momentum_signal_expectancy.py could only approximate: real
entry-to-exit expectancy for the confluence_entry_v1 strategy, using
ACTUAL 1-second-cadence DexScreener monitoring and ACTUAL exits already
computed live by workers/confluence_shadow_worker.py — not a backtest
replaying WATCHING-tier 30-60s token_snapshots against tokens that were
never really entered.

No simulation happens in this script. confluence_shadow_positions rows
already carry the real pnl_pct, exit_reason, entry/exit price and time —
this script only aggregates and reports them.

Note on scope: unlike the earlier momentum_signal_prospective/expectancy
checkpoints, there is no "0-1 rules vs 2+ rules" split possible here —
workers/confluence_shadow_worker.py ONLY opens a position when
n_rules_cofiring >= 2 (that's the entry rule itself), so every row in this
table already IS the "2+ rules" population. The comparison this script
makes instead is against the earlier proxy-backtest's own "2+ rules"
numbers (analysis/momentum_signal_expectancy.py, WATCHING-tier data):
train expectancy -10.9%, test expectancy +12.3% — to see whether real
1-second monitoring actually closes that gap, as hypothesized.

Data-quality note: two early positions (X7, CENTS) closed on a single
DexScreener bad tick before the 2026-09-22 _best_pair() volume-preference
fix (see workers/trade_monitor_worker.py) and the tick-confirmation guard
(workers/confluence_shadow_worker.py) landed. Both were manually reset to
'open' and their corrupted observation rows removed once the root cause
was found — this script's dataset reflects the corrected, post-fix state
of the table, not a script-side filter.

Usage
-----
    PYTHONPATH=. python analysis/confluence_shadow_expectancy.py

Outputs
-------
    analysis/output/confluence_shadow_expectancy_<timestamp>.md
    analysis/output/confluence_shadow_expectancy_dataset_<timestamp>.csv
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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
EXPERIMENT_VERSION = "confluence_entry_v1"

# From analysis/momentum_signal_expectancy.py's last run (2026-09-22,
# n=198 signals, WATCHING-tier proxy backtest) — the comparison point.
PROXY_TRAIN_EXPECTANCY = -0.109
PROXY_TEST_EXPECTANCY = 0.123

POSITIONS_SQL = """
SELECT csp.id, t.symbol, csp.entry_time, csp.entry_price, csp.n_rules_cofiring,
       csp.status, csp.exit_time, csp.exit_price, csp.exit_reason, csp.pnl_pct
FROM confluence_shadow_positions csp
JOIN tokens t ON t.id = csp.token_id
WHERE csp.experiment_version = $1
ORDER BY csp.entry_time ASC;
"""


def _num(v):
    return float(v) if v is not None else None


def expectancy_stats(pnl_list):
    n = len(pnl_list)
    if n == 0:
        return dict(n=0, win_rate=None, expectancy=None, avg_win=None, avg_loss=None, median=None)
    wins = [p for p in pnl_list if p > 0]
    losses = [p for p in pnl_list if p <= 0]
    return dict(
        n=n, win_rate=len(wins) / n, expectancy=statistics.mean(pnl_list),
        avg_win=statistics.mean(wins) if wins else None,
        avg_loss=statistics.mean(losses) if losses else None,
        median=statistics.median(pnl_list),
    )


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
            hold_min=round((r["exit_time"] - r["entry_time"]).total_seconds() / 60, 1) if r["exit_time"] else None,
        ))

    ts = now.strftime("%Y%m%d_%H%M%S")
    csv_path = OUTPUT_DIR / f"confluence_shadow_expectancy_dataset_{ts}.csv"
    if records:
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(records[0].keys()))
            w.writeheader()
            for rec in records:
                w.writerow(rec)

    closed = [r for r in records if r["status"] == "closed"]
    open_ = [r for r in records if r["status"] == "open"]
    pnls = [r["pnl_pct"] for r in closed]
    stats = expectancy_stats(pnls)

    lines = []
    p = lines.append
    p(f"# Confluence Entry v1 — Real Entry-to-Exit Expectancy — {ts}")
    p("")
    p("ANALYSIS-ONLY. No simulation — every number below comes directly from "
      "confluence_shadow_positions rows already closed live by workers/confluence_shadow_worker.py "
      "using real 1-second DexScreener monitoring and engine/risk.py's real exit priority.")
    p("")
    p(f"- Total positions: {len(records)}. Closed: {len(closed)}. Still open: {len(open_)}.")
    p(f"- Every row here has n_rules_cofiring >= 2 by construction (the entry rule itself) — "
      f"there is no 0-1-vs-2+ split possible in this table, unlike the earlier proxy checkpoints.")
    p("")
    p("## Real expectancy (closed positions only)")
    p("")
    if stats["n"] == 0:
        p("No closed positions yet.")
    else:
        p(f"- n = {stats['n']}")
        p(f"- win rate = {stats['win_rate']:.0%}")
        p(f"- **expectancy (mean pnl_pct) = {stats['expectancy']:+.1%}**")
        p(f"- median pnl_pct = {stats['median']:+.1%}")
        avg_win_txt = f"{stats['avg_win']:+.1%}" if stats["avg_win"] is not None else "n/a"
        avg_loss_txt = f"{stats['avg_loss']:+.1%}" if stats["avg_loss"] is not None else "n/a"
        p(f"- avg win = {avg_win_txt}")
        p(f"- avg loss = {avg_loss_txt}")
        if stats["n"] < 30:
            p(f"- **LOW CONFIDENCE (n={stats['n']} < 30)** — early real-money-equivalent read, not yet a stable estimate.")
    p("")
    p("## Comparison: real (this table) vs the earlier WATCHING-tier proxy backtest")
    p("")
    p(f"- Proxy backtest (analysis/momentum_signal_expectancy.py, n=198 signals, 30-60s cadence): "
      f"train expectancy {PROXY_TRAIN_EXPECTANCY:+.1%}, test expectancy {PROXY_TEST_EXPECTANCY:+.1%}")
    if stats["n"] > 0:
        p(f"- Real (this table, 1-second cadence): expectancy {stats['expectancy']:+.1%} (n={stats['n']})")
        verdict = "closes the gap toward positive" if stats["expectancy"] > PROXY_TRAIN_EXPECTANCY else "does NOT yet show improvement over the pessimistic proxy"
        p(f"- **{verdict}.**")
    p("")
    p("## Exit reason breakdown")
    p("")
    reasons = Counter(r["exit_reason"] for r in closed)
    for reason, n in reasons.most_common():
        subset = [r["pnl_pct"] for r in closed if r["exit_reason"] == reason]
        p(f"- {reason}: n={n} ({n/len(closed):.0%}), mean pnl_pct={statistics.mean(subset):+.1%}")
    p("")
    p("## Hold time (closed positions)")
    p("")
    holds = [r["hold_min"] for r in closed if r["hold_min"] is not None]
    if holds:
        p(f"- median hold: {statistics.median(holds):.1f} min, min={min(holds):.1f}, max={max(holds):.1f}")
    p("")
    p("## All closed positions")
    p("")
    p("| symbol | n_cofiring | exit_reason | pnl_pct | hold_min |")
    p("|---|---|---|---|---|")
    for r in sorted(closed, key=lambda r: r["exit_time"]):
        p(f"| {r['symbol']} | {r['n_rules_cofiring']} | {r['exit_reason']} | {r['pnl_pct']:+.1%} | {r['hold_min']:.1f} |")
    p("")
    p("## Caveats")
    p("")
    p("- Small, young sample — treat direction as a lead, not a confirmed result, until n is much larger.")
    p("- Two early positions (X7, CENTS) that closed on a DexScreener bad tick before the "
      "_best_pair volume-preference fix and tick-confirmation guard landed were manually reset to "
      "'open' with corrupted observations removed — this reflects the corrected table, not a "
      "script-side filter. If they appear above as closed, it is on their real, post-fix outcome.")
    p("- No trading logic, thresholds, or scorer weights touched. Nothing implemented from this checkpoint.")

    report_path = OUTPUT_DIR / f"confluence_shadow_expectancy_{ts}.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")
    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
