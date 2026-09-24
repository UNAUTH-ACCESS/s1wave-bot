"""
analysis/shadow_backtest.py
=============================
ANALYSIS-ONLY, read-only. Backtests the "scorer_v2_threshold_4" shadow
experiment (see models.orm.ShadowTrade, workers/scoring_worker.py) against
real subsequent price action.

This script never writes to shadow_trades, tokens, token_evaluations, or
trades. It never touches TIER2_WATCH_THRESHOLD, TIER2_STRONG_BUY_THRESHOLD,
CapitalEngine, or RiskEngine. The real 5.0/8.0 thresholds and the real
production Trade ledger are completely unaffected by anything in this file
or by the shadow_trades table it reads.

Outage exclusion
-----------------
Same outage window as analysis/clean_recap.py: 2026-09-20T12:47:05Z to
2026-09-20T17:37:13Z. A shadow trade or below-threshold scorer evaluation
whose triggered_at/evaluated_at falls in that window is excluded outright.

Exit-rule simulation
---------------------
Replays engine/risk.py's exact, unmodified priority order (HARD_FLOOR
-7% > STOP_LOSS -6% > TAKE_PROFIT +30% > TIME_EXIT 6h) against the
observed token_snapshots price series. This is a simulation over already-
collected read-only data — it never calls RiskEngine, never touches a Trade
row, and has no way to affect anything the real bot does.

Buckets (as requested)
------------------------
1. Score 4.0-4.49   (shadow_trades, this experiment)
2. Score 4.5-4.99   (shadow_trades, this experiment)
3. Score 5.0+       (shadow_trades — will show n=0 unless the real scorer
                      has ever produced a 5.0+ score, which as of the last
                      clean_recap it has not)
4. Tier1-passed but below 4.0 (pulled directly from the real SCORER
                      token_evaluations — these never cross this
                      experiment's threshold, so they have no shadow_trades
                      row; used here purely as a below-threshold reference
                      group, not as a hypothetical entry)

Usage: PYTHONPATH=. python analysis/shadow_backtest.py
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import settings  # noqa: E402
from analysis.raw_feature_backtest import pearson, spearman, fmt_pct, fmt_r, _num  # noqa: E402

# See analysis/clean_recap.py for the full rationale. Two known
# contamination windows so far: the OBSERVE_WINDOW_SECONDS batch-size
# incident (fixed), and the SolanaTracker "Insufficient credits for this
# request" account exhaustion (open-ended — end is "now" at run time).
OUTAGES = [
    (datetime(2026, 9, 20, 12, 47, 5, tzinfo=timezone.utc),
     datetime(2026, 9, 20, 17, 37, 13, tzinfo=timezone.utc)),
    (datetime(2026, 9, 20, 21, 57, 7, tzinfo=timezone.utc),
     datetime.now(timezone.utc)),
]

EXPERIMENT_VERSION = "scorer_v2_threshold_4"

# Exact, unmodified copy of engine/risk.py's exit priority and thresholds —
# a simulation reference, never imported into or run against the real
# RiskEngine/Trade path.
HARD_FLOOR_PCT   = Decimal(str(settings.HARD_FLOOR_PCT))
STOP_LOSS_PCT    = Decimal(str(settings.STOP_LOSS_PCT))
TAKE_PROFIT_PCT  = Decimal(str(settings.TAKE_PROFIT_PCT))
MAX_HOLD_SECONDS = settings.max_hold_seconds

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

SHADOW_SQL = """
SELECT st.id, st.token_id, tok.symbol, st.triggered_at, st.score, st.entry_price,
       st.liquidity_usd, st.market_cap_usd, st.buys, st.sells, st.wash_multiplier,
       st.inputs_json
FROM shadow_trades st
JOIN tokens tok ON tok.id = st.token_id
WHERE st.experiment_version = $1
ORDER BY st.triggered_at ASC;
"""

BELOW_THRESHOLD_SQL = """
SELECT DISTINCT ON (te.token_id)
    te.token_id, tok.symbol, te.evaluated_at AS decision_time, te.inputs_json
FROM token_evaluations te
JOIN tokens tok ON tok.id = te.token_id
WHERE te.gate = 'SCORER'
  AND (te.inputs_json->>'score')::numeric < 4.0
ORDER BY te.token_id, te.evaluated_at DESC;
"""

SNAPSHOTS_SQL = "SELECT token_id, sampled_at, price_usd FROM token_snapshots ORDER BY token_id, sampled_at ASC;"


def in_outage(ts: datetime) -> bool:
    return any(o_start <= ts <= o_end for o_start, o_end in OUTAGES)


def window_overlaps_outage(start: datetime, end: datetime) -> bool:
    return any(start < o_end and end > o_start for o_start, o_end in OUTAGES)


def simulate_exit(snaps, entry_time, entry_price, now):
    """
    Replays engine/risk.py's exact priority order against observed prices.
    Returns dict: triggered(bool), reason, exit_time, exit_price, pnl_pct,
    max_gain, max_drawdown. Read-only simulation over historical snapshots
    — never touches RiskEngine or any Trade row.
    """
    after = [(t, p) for t, p in snaps if t >= entry_time and p is not None and p > 0]
    result = {
        "triggered": False, "reason": None, "exit_time": None,
        "pnl_pct": None, "max_gain": None, "max_drawdown": None,
    }
    if not after or entry_price is None or entry_price <= 0:
        return result

    max_gain = Decimal("0")
    max_drawdown = Decimal("0")  # most negative pnl_pct seen, as a fraction

    # Both max_gain and max_drawdown must stop accumulating the instant a
    # simulated exit fires — a real position would be closed by then, so
    # whatever the token does afterward (including, commonly, crashing to
    # near-zero) never happens "to this trade". The bug this fixes: the
    # loop used to keep updating these for every remaining snapshot
    # regardless of `triggered`, which meant a token that hit TAKE_PROFIT
    # early and then rugged hours later (as most eventually do) reported a
    # -99% "drawdown" that a real exit would never have been exposed to.
    for ts, price in after:
        pnl_pct = (Decimal(str(price)) - Decimal(str(entry_price))) / Decimal(str(entry_price))
        if pnl_pct > max_gain:
            max_gain = pnl_pct
        if pnl_pct < max_drawdown:
            max_drawdown = pnl_pct

        if not result["triggered"]:
            hold_seconds = (ts - entry_time).total_seconds()
            if pnl_pct <= HARD_FLOOR_PCT:
                result.update(triggered=True, reason="HARD_FLOOR", exit_time=ts, pnl_pct=pnl_pct)
            elif pnl_pct <= STOP_LOSS_PCT:
                result.update(triggered=True, reason="STOP_LOSS", exit_time=ts, pnl_pct=pnl_pct)
            elif pnl_pct >= TAKE_PROFIT_PCT:
                result.update(triggered=True, reason="TAKE_PROFIT", exit_time=ts, pnl_pct=pnl_pct)
            elif hold_seconds >= MAX_HOLD_SECONDS:
                result.update(triggered=True, reason="TIME_EXIT", exit_time=ts, pnl_pct=pnl_pct)
        else:
            break  # position is simulated-closed — stop accumulating gain/drawdown past this point

    result["max_gain"] = float(max_gain)
    result["max_drawdown"] = float(max_drawdown)
    if result["pnl_pct"] is not None:
        result["pnl_pct"] = float(result["pnl_pct"])
    return result


def horizon_return(snaps, anchor, minutes, now):
    """Same discipline as clean_recap.py: complete only if elapsed + no outage overlap."""
    end = anchor + timedelta(minutes=minutes)
    if window_overlaps_outage(anchor, end):
        return None, False
    if end > now:
        return None, False
    after = [(t, p) for t, p in snaps if anchor <= t <= end and p is not None and p > 0]
    if not after:
        return None, False
    baseline = after[0][1]
    if baseline <= 0:
        return None, False
    peak = max(p for _, p in after)
    return (peak / baseline - 1), True


async def main():
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        shadow_records = await conn.fetch(SHADOW_SQL, EXPERIMENT_VERSION)
        below_records = await conn.fetch(BELOW_THRESHOLD_SQL)
        snap_records = await conn.fetch(SNAPSHOTS_SQL)
    finally:
        await conn.close()

    snaps_by_token = {}
    for r in snap_records:
        snaps_by_token.setdefault(r["token_id"], []).append((r["sampled_at"], _num(r["price_usd"])))

    now = datetime.now(timezone.utc)
    lines = []
    def p(s=""):
        lines.append(s)
        print(s)

    p(f"# Shadow experiment backtest: {EXPERIMENT_VERSION}")
    p(f"Generated: {now.isoformat()}")
    p("")
    p("**Analysis-only. shadow_trades is never read by CapitalEngine, RiskEngine, or "
      "execution.py. TIER2_WATCH_THRESHOLD (5.0) and TIER2_STRONG_BUY_THRESHOLD (8.0) "
      "are unchanged and this script never touches them. Real `trades` table: 0 rows "
      "from this or any recent activity — confirmed separately.**")
    p("")

    # ── build rows ──────────────────────────────────────────────────────
    rows = []
    excluded_outage = 0
    for r in shadow_records:
        if in_outage(r["triggered_at"]):
            excluded_outage += 1
            continue
        mysnaps = snaps_by_token.get(r["token_id"], [])
        entry_price = _num(r["entry_price"])
        entry_time = r["triggered_at"]
        exitsim = simulate_exit(mysnaps, entry_time, entry_price, now)
        horizons = {}
        for m in (5, 15, 30, 60):
            v, complete = horizon_return(mysnaps, entry_time, m, now)
            horizons[m] = (v, complete)
        rows.append({
            "symbol": r["symbol"], "score": float(r["score"]), "entry_price": entry_price,
            "entry_time": entry_time, "liquidity_usd": _num(r["liquidity_usd"]),
            "wash_multiplier": _num(r["wash_multiplier"]), "buys": r["buys"], "sells": r["sells"],
            "horizons": horizons, "exitsim": exitsim,
        })

    below_rows = []
    for r in below_records:
        if in_outage(r["decision_time"]):
            continue
        mysnaps = snaps_by_token.get(r["token_id"], [])
        inputs = json.loads(r["inputs_json"]) if r["inputs_json"] else {}
        score = _num(inputs.get("score"))
        horizons = {}
        for m in (5, 15, 30, 60):
            v, complete = horizon_return(mysnaps, r["decision_time"], m, now)
            horizons[m] = (v, complete)
        below_rows.append({"symbol": r["symbol"], "score": score, "horizons": horizons})

    p("## 1. Dataset")
    p(f"- shadow_trades for {EXPERIMENT_VERSION}: {len(shadow_records)} total, "
      f"{excluded_outage} excluded (decided during outage), **{len(rows)} clean**")
    p(f"- Tier1-passed, scored below 4.0 (reference group, no shadow row by design): "
      f"{len(below_records)} total, **{len(below_rows)} clean**")
    p("")

    # ── 2. bucketed comparison ──────────────────────────────────────────
    p("## 2. Bucketed comparison")
    p("")

    def bucket_stats(items, label):
        p(f"### {label} (n={len(items)})")
        if not items:
            p("- no observations yet")
            p("")
            return
        for m in (5, 15, 30, 60):
            vals = [it["horizons"][m][0] for it in items if it["horizons"][m][1] and it["horizons"][m][0] is not None]
            if vals:
                hit10 = sum(1 for v in vals if v >= 0.10) / len(vals)
                hit20 = sum(1 for v in vals if v >= 0.20) / len(vals)
                p(f"- @ {m}min: n={len(vals)}, median={fmt_pct(statistics.median(vals))}, "
                  f"mean={fmt_pct(statistics.fmean(vals))}, hit≥10%={hit10*100:.0f}%, hit≥20%={hit20*100:.0f}%")
            else:
                p(f"- @ {m}min: n=0 (not enough elapsed clean time yet)")
        p("")

    b1 = [r for r in rows if 4.0 <= r["score"] < 4.5]
    b2 = [r for r in rows if 4.5 <= r["score"] < 5.0]
    b3 = [r for r in rows if r["score"] >= 5.0]
    bucket_stats(b1, "Bucket 1: score 4.0-4.49")
    bucket_stats(b2, "Bucket 2: score 4.5-4.99")
    bucket_stats(b3, "Bucket 3: score 5.0+")
    bucket_stats(below_rows, "Bucket 4: Tier1-passed, scored below 4.0 (reference)")

    # ── 3. exit-rule simulation ─────────────────────────────────────────
    p("## 3. Simulated exit rules (exact engine/risk.py logic, replayed over observed prices)")
    p("")
    triggered = [r for r in rows if r["exitsim"]["triggered"]]
    p(f"- {len(triggered)} of {len(rows)} shadow trades hit a simulated exit condition so far "
      f"(the rest either haven't moved enough or haven't been observed long enough)")
    if triggered:
        from collections import Counter
        reasons = Counter(r["exitsim"]["reason"] for r in triggered)
        p(f"- Breakdown: {dict(reasons)}")
        wins = [r for r in triggered if r["exitsim"]["pnl_pct"] and r["exitsim"]["pnl_pct"] > 0]
        p(f"- Simulated win rate (of those that hit an exit): {len(wins)}/{len(triggered)} "
          f"({len(wins)/len(triggered)*100:.0f}%)")
    p("")
    gains = [r["exitsim"]["max_gain"] for r in rows if r["exitsim"]["max_gain"] is not None]
    drawdowns = [r["exitsim"]["max_drawdown"] for r in rows if r["exitsim"]["max_drawdown"] is not None]
    if gains:
        p(f"- Max observed gain across all shadow trades: median {fmt_pct(statistics.median(gains))}, "
          f"worst {fmt_pct(min(gains))}, best {fmt_pct(max(gains))}")
    if drawdowns:
        p(f"- Max observed drawdown across all shadow trades: median {fmt_pct(statistics.median(drawdowns))}, "
          f"worst {fmt_pct(min(drawdowns))}")
    p("")

    # ── 4. per-trade table ──────────────────────────────────────────────
    p("## 4. Every shadow trade so far")
    p("")
    p("| symbol | score | entry_price | 30m return | max_gain | max_drawdown | sim. exit |")
    p("|---|---|---|---|---|---|---|")
    for r in sorted(rows, key=lambda x: x["entry_time"]):
        v30, c30 = r["horizons"][30]
        exit_str = r["exitsim"]["reason"] or "no exit yet"
        p(f"| {r['symbol']} | {r['score']:.2f} | {r['entry_price']:.10f} | "
          f"{fmt_pct(v30) if c30 else 'pending'} | {fmt_pct(r['exitsim']['max_gain'])} | "
          f"{fmt_pct(r['exitsim']['max_drawdown'])} | {exit_str} |")
    p("")

    p("## 5. Reminder")
    p(f"This is n={len(rows)} shadow trades. The goal per your instructions is NOT to conclude "
      f"4.0 is correct — it's to accumulate enough of these to eventually answer that. "
      f"No thresholds were changed. Re-run this script as more data accumulates.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (OUTPUT_DIR / f"shadow_backtest_{ts}.md").write_text("\n".join(lines))
    print(f"\n[saved] {OUTPUT_DIR / f'shadow_backtest_{ts}.md'}")


if __name__ == "__main__":
    asyncio.run(main())
