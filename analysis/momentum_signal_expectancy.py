"""
analysis/momentum_signal_expectancy.py
========================================
ANALYSIS-ONLY, READ-ONLY. Answers a precise question the prior checkpoint
(analysis/momentum_signal_prospective.py) could NOT answer: does >=2 rules
co-firing at the momentum_confluence_v1 signal produce better REAL
entry-to-exit expectancy, not merely a higher win rate?

Why win rate alone is the wrong measure
------------------------------------------
A 70% win rate is worthless if the 30% of losses are catastrophic (-90%)
and the wins are small (+5%) — win rate says nothing about magnitude.
Expectancy (mean realized P&L per simulated trade) is the number that
actually matters for "is this worth trading", and it requires simulating
an actual entry-to-exit round trip, not just reading off a fixed-horizon
forward return.

Reuses analysis/shadow_backtest.py's simulate_exit() UNCHANGED
------------------------------------------------------------------
simulate_exit() already replays engine/risk.py's exact, unmodified exit
priority (HARD_FLOOR -7% > STOP_LOSS -6% > TAKE_PROFIT +30% > TIME_EXIT
6h) against observed prices — built and bug-fixed earlier this session
(the max_gain/max_drawdown-must-stop-at-exit fix). Importing it directly
here rather than re-implementing it: this is the same simulation logic
already trusted for the scorer_v2_threshold_4 backtest, now pointed at
momentum_confluence_v1's live signals instead. Never touches RiskEngine,
CapitalEngine, or any real Trade row — read-only simulation.

What "resolved" means here
-----------------------------
A signal only has a real, countable P&L once simulate_exit() actually
fires (hit a floor/stop/profit level, or reached the 6-hour time limit).
A signal from 20 minutes ago that hasn't hit any threshold yet is neither
a win nor a loss — it's UNRESOLVED, and is excluded from expectancy math
entirely (not counted as a loss, not counted as a win) rather than
silently coerced into one. Reported explicitly per bucket.

Usage
-----
    PYTHONPATH=. python analysis/momentum_signal_expectancy.py

Outputs
-------
    analysis/output/momentum_signal_expectancy_<timestamp>.md
    analysis/output/momentum_signal_expectancy_dataset_<timestamp>.csv
"""

from __future__ import annotations

import asyncio
import csv
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from analysis.shadow_backtest import (
    simulate_exit, HARD_FLOOR_PCT, STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_HOLD_SECONDS,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
EXPERIMENT_VERSION = "momentum_confluence_v1"
MIN_FOR_SPLIT = 193  # matches the sample size the last prospective checkpoint used

SIGNALS_SQL = """
SELECT me.token_id, tok.symbol, me.triggered_at, me.trigger_price, me.n_rules_cofiring
FROM momentum_signal_events me
JOIN tokens tok ON tok.id = me.token_id
WHERE me.experiment_version = $1
ORDER BY me.triggered_at ASC;
"""
SNAPSHOTS_SQL = "SELECT sampled_at, price_usd FROM token_snapshots WHERE token_id = $1 ORDER BY sampled_at ASC;"


def _num(v):
    return float(v) if v is not None else None


def expectancy_stats(rows):
    """rows: list of resolved pnl_pct floats. Returns win_rate, expectancy
    (mean pnl_pct), avg_win, avg_loss, n."""
    n = len(rows)
    if n == 0:
        return dict(n=0, win_rate=None, expectancy=None, avg_win=None, avg_loss=None)
    wins = [r for r in rows if r > 0]
    losses = [r for r in rows if r <= 0]
    return dict(
        n=n,
        win_rate=len(wins) / n,
        expectancy=statistics.mean(rows),
        avg_win=statistics.mean(wins) if wins else None,
        avg_loss=statistics.mean(losses) if losses else None,
    )


def fmt_stats(label, s):
    if s["n"] == 0:
        return f"- {label}: n=0 (nothing resolved yet)"
    wr = f"{s['win_rate']:.0%}" if s["win_rate"] is not None else "n/a"
    exp = f"{s['expectancy']:+.1%}" if s["expectancy"] is not None else "n/a"
    aw = f"{s['avg_win']:+.1%}" if s["avg_win"] is not None else "n/a"
    al = f"{s['avg_loss']:+.1%}" if s["avg_loss"] is not None else "n/a"
    return f"- {label}: n={s['n']}, win_rate={wr}, **expectancy={exp}**, avg_win={aw}, avg_loss={al}"


async def main():
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(SIGNALS_SQL, EXPERIMENT_VERSION)
        snaps_by_token = {}
        for r in rows:
            if r["token_id"] not in snaps_by_token:
                srows = await conn.fetch(SNAPSHOTS_SQL, r["token_id"])
                snaps_by_token[r["token_id"]] = [(sr["sampled_at"], _num(sr["price_usd"])) for sr in srows]
    finally:
        await conn.close()

    now = datetime.now(timezone.utc)
    print(f"Loaded {len(rows)} signals. now={now.isoformat()}")
    print(f"Exit rule (from engine/risk.py, unmodified): HARD_FLOOR={float(HARD_FLOOR_PCT):.0%}, "
          f"STOP_LOSS={float(STOP_LOSS_PCT):.0%}, TAKE_PROFIT={float(TAKE_PROFIT_PCT):.0%}, "
          f"TIME_EXIT={MAX_HOLD_SECONDS/3600:.0f}h")

    records = []
    for r in rows:
        entry_price = _num(r["trigger_price"])
        snaps = snaps_by_token[r["token_id"]]
        sim = simulate_exit(snaps, r["triggered_at"], entry_price, now)
        records.append(dict(
            symbol=r["symbol"], token_id=str(r["token_id"]), triggered_at=r["triggered_at"],
            n_rules_cofiring=r["n_rules_cofiring"],
            resolved=sim["triggered"], exit_reason=sim["reason"], pnl_pct=sim["pnl_pct"],
            max_gain=sim["max_gain"], max_drawdown=sim["max_drawdown"],
            age_min=round((now - r["triggered_at"]).total_seconds() / 60, 1),
        ))

    ts = now.strftime("%Y%m%d_%H%M%S")
    csv_path = OUTPUT_DIR / f"momentum_signal_expectancy_dataset_{ts}.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        w.writeheader()
        for rec in records:
            w.writerow(rec)

    n_total = len(records)
    resolved = [r for r in records if r["resolved"]]
    unresolved = [r for r in records if not r["resolved"]]
    can_split = n_total >= MIN_FOR_SPLIT

    lines = []
    p = lines.append
    p(f"# Momentum Confluence — Entry-to-Exit Expectancy — {ts}")
    p("")
    p("ANALYSIS-ONLY. Read-only, no trading logic touched. Simulates a REAL entry-to-exit round trip "
      "for every momentum_confluence_v1 signal using engine/risk.py's exact, unmodified exit priority "
      "(via analysis/shadow_backtest.py's simulate_exit(), imported not reimplemented) — answers "
      "\"is expectancy actually better\", not just \"is the win rate higher\".")
    p("")
    p(f"- Exit rule replayed: HARD_FLOOR {float(HARD_FLOOR_PCT):.0%} > STOP_LOSS {float(STOP_LOSS_PCT):.0%} > "
      f"TAKE_PROFIT {float(TAKE_PROFIT_PCT):+.0%} > TIME_EXIT {MAX_HOLD_SECONDS/3600:.0f}h")
    p(f"- Total signals: {n_total}. Resolved (hit an exit condition): {len(resolved)}. "
      f"Unresolved (still open, too young or data ran out before any exit fired): {len(unresolved)}.")
    p("")
    p("## Expectancy by confluence bucket (0-1 rules vs 2+ rules) — resolved trades only")
    p("")
    if can_split:
        sorted_recs = sorted(records, key=lambda r: r["triggered_at"])
        split_idx = len(sorted_recs) // 2
        split_time = sorted_recs[split_idx]["triggered_at"]
        train = [r for r in sorted_recs[:split_idx] if r["resolved"]]
        test = [r for r in sorted_recs[split_idx:] if r["resolved"]]
        p(f"n={n_total} >= MIN_FOR_SPLIT={MIN_FOR_SPLIT} — chronological time-split active. "
          f"Split at {split_time.isoformat()}.")
        p("")
        for label, pool in [("TRAIN", train), ("TEST", test)]:
            low = [r["pnl_pct"] for r in pool if r["n_rules_cofiring"] <= 1]
            high = [r["pnl_pct"] for r in pool if r["n_rules_cofiring"] >= 2]
            p(f"### {label}")
            p(fmt_stats("0-1 rules", expectancy_stats(low)))
            p(fmt_stats("2+ rules  ", expectancy_stats(high)))
            p("")
        s_train_low = expectancy_stats([r["pnl_pct"] for r in train if r["n_rules_cofiring"] <= 1])
        s_train_high = expectancy_stats([r["pnl_pct"] for r in train if r["n_rules_cofiring"] >= 2])
        s_test_low = expectancy_stats([r["pnl_pct"] for r in test if r["n_rules_cofiring"] <= 1])
        s_test_high = expectancy_stats([r["pnl_pct"] for r in test if r["n_rules_cofiring"] >= 2])
        if all(x["n"] > 0 for x in (s_train_low, s_train_high, s_test_low, s_test_high)):
            holds = (s_train_high["expectancy"] > s_train_low["expectancy"]) == (s_test_high["expectancy"] > s_test_low["expectancy"])
            p(f"**{'Expectancy advantage for 2+ rules holds in test.' if holds else 'Expectancy advantage did NOT hold in test — treat as unconfirmed.'}**")
        min_n = min((x["n"] for x in (s_train_low, s_train_high, s_test_low, s_test_high)), default=0)
        if min_n < 15:
            p(f"**LOW CONFIDENCE (smallest bucket n={min_n}).**")
    else:
        low = [r["pnl_pct"] for r in resolved if r["n_rules_cofiring"] <= 1]
        high = [r["pnl_pct"] for r in resolved if r["n_rules_cofiring"] >= 2]
        p(f"n={n_total} < MIN_FOR_SPLIT={MIN_FOR_SPLIT} — pooled only, too early to split.")
        p("")
        p(fmt_stats("0-1 rules", expectancy_stats(low)))
        p(fmt_stats("2+ rules  ", expectancy_stats(high)))
    p("")
    p("## Exit-reason breakdown by confluence bucket (resolved trades only)")
    p("")
    from collections import Counter
    for label, cond in [("0-1 rules", lambda r: r["n_rules_cofiring"] <= 1), ("2+ rules", lambda r: r["n_rules_cofiring"] >= 2)]:
        pool = [r for r in resolved if cond(r)]
        c = Counter(r["exit_reason"] for r in pool)
        total = len(pool)
        p(f"- {label} (n={total}): " + ", ".join(f"{k}={v} ({v/total:.0%})" for k, v in c.most_common()) if total else f"- {label}: n=0")
    p("")
    p("## Unresolved signals by confluence bucket (excluded from expectancy — not a win, not a loss)")
    p("")
    for label, cond in [("0-1 rules", lambda r: r["n_rules_cofiring"] <= 1), ("2+ rules", lambda r: r["n_rules_cofiring"] >= 2)]:
        pool = [r for r in unresolved if cond(r)]
        p(f"- {label}: n={len(pool)} still open (median age {statistics.median([r['age_min'] for r in pool]):.0f}min)" if pool else f"- {label}: n=0")
    p("")
    p("## Caveats")
    p("")
    p("- `pnl_pct` here is the exact simulated round-trip result of engine/risk.py's real exit rules — "
      "not a fixed-horizon forward return like the earlier prospective checkpoint used. This is the "
      "correct number for \"would this have been a good trade\", the earlier checkpoint's win-rate/median "
      "was not.")
    p("- Unresolved signals are excluded, not treated as 0% or as losses — a young signal that hasn't hit "
      "any exit condition yet is genuinely undetermined, and folding it into either bucket would bias the result.")
    p("- No trading logic, thresholds, or scorer weights touched. Nothing implemented from this checkpoint.")

    report_path = OUTPUT_DIR / f"momentum_signal_expectancy_{ts}.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")
    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
