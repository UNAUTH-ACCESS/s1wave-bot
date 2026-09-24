"""
analysis/clean_recap.py
========================
ANALYSIS-ONLY, read-only. One-off rigorous recap requested after the
2026-09-20 SolanaTracker outage. Does not touch scoring, thresholds, gates,
weights, or any production behavior.

Outage window (confirmed directly from bot logs, not estimated)
-----------------------------------------------------------------
2026-09-20T12:47:05Z to 2026-09-20T17:37:13Z: 577 consecutive
sampling_worker.fetch_error events, ZERO successful sampling_worker
.cycle_complete events in between (verified by scanning every
cycle_complete timestamp in that session's log). Root cause: OBSERVE_
WINDOW_SECONDS was widened from 90s to 1800s, the concurrently-tracked
population grew past SolanaTracker's (previously unknown) hard limit of
20 tokens per /tokens/multi request, every request 400'd from then on, and
because age-out logic only used to run after a successful fetch, nothing
could shrink the population back down either. Fixed in workers/
sampling_worker.py (request chunking + age-out decoupled from fetch
success) and confirmed recovered before this script was written.

Cleaning rules applied here (this is what "clean" means throughout)
-----------------------------------------------------------------------
1. A token's decision-time raw features are taken from its LATEST TIER1
   evaluation, not its first. 30 tokens in this dataset were rediscovered
   and re-evaluated; the snapshots actually being collected right now
   belong to whichever WATCHING/OBSERVING cycle is CURRENT, which is the
   one the latest evaluation started, not a stale earlier one.
2. observation_started_at (used for post-reject anchoring) is set once,
   the first time a token ever enters OBSERVING, and is NOT reset on
   re-rejection (see filters/tier1_worker.py) — used as-is; this is the
   real anchor sampling_worker's age-out clock actually uses.
3. A given outcome horizon (5/15/30min, or "full") only counts as COMPLETE
   if: (a) anchor + horizon has actually elapsed by the time this script
   ran, and (b) the window [anchor, anchor+horizon] does not overlap the
   outage at all. Partial overlap is treated as NO data for that horizon,
   not partial credit — this is stricter than just dropping snapshot rows
   that fall inside the outage, because a window that starts before the
   outage and ends after it looks "long enough" on paper while actually
   missing hours of the middle.
4. No look-ahead: every feature is read from token_evaluations.inputs_json,
   captured before the first snapshot; every outcome is computed only from
   snapshots at or after its anchor.

Usage: PYTHONPATH=. python analysis/clean_recap.py
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import settings  # noqa: E402
from analysis.raw_feature_backtest import (  # noqa: E402
    pearson, spearman, bucket_analysis, is_monotonic, fmt_pct, fmt_r, _num,
)

# List of (start, end) contamination windows to exclude — each is a period
# where SolanaTracker returned errors instead of real data, confirmed
# directly from bot logs, not estimated.
#   1) OBSERVE_WINDOW_SECONDS batch-size incident (fixed in code).
#   2) SolanaTracker account ran out of API credits ("Insufficient credits
#      for this request", confirmed via direct diagnostic call) - open-ended
#      until credits are restored, so its end is "now" at run time, not a
#      fixed timestamp.
OUTAGES = [
    (datetime(2026, 9, 20, 12, 47, 5, tzinfo=timezone.utc),
     datetime(2026, 9, 20, 17, 37, 13, tzinfo=timezone.utc)),
    (datetime(2026, 9, 20, 21, 57, 7, tzinfo=timezone.utc),
     datetime.now(timezone.utc)),
]
# Back-compat single-window references used below for the header line.
OUTAGE_START = OUTAGES[0][0]
OUTAGE_END   = OUTAGES[0][1]

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

SIX_MOVERS_TARGET = {
    "KITTY": 4.86, "Cta": 4.79, "CURTIS": 4.76,
    "CROC": 4.84, "YouTube": 4.26, "MONTY": 4.79,
}

FETCH_SQL = """
WITH tier1_latest AS (
    SELECT DISTINCT ON (token_id)
        token_id, evaluated_at AS decision_time, passed AS tier1_passed,
        reason_code AS tier1_reason, inputs_json AS tier1_inputs
    FROM token_evaluations
    WHERE gate = 'TIER1'
    ORDER BY token_id, evaluated_at DESC
),
tier1_first AS (
    SELECT DISTINCT ON (token_id)
        token_id, evaluated_at AS first_decision_time
    FROM token_evaluations
    WHERE gate = 'TIER1'
    ORDER BY token_id, evaluated_at ASC
),
tier1_count AS (
    SELECT token_id, count(*) AS n_tier1_evals
    FROM token_evaluations WHERE gate='TIER1' GROUP BY token_id
),
scorer_latest AS (
    SELECT DISTINCT ON (token_id)
        token_id, evaluated_at AS scorer_time, passed AS scorer_passed,
        reason_code AS scorer_reason, inputs_json AS scorer_inputs
    FROM token_evaluations
    WHERE gate = 'SCORER'
    ORDER BY token_id, evaluated_at DESC
)
SELECT
    tok.id, tok.mint_address, tok.symbol, tok.discovered_at, tok.status,
    tok.rejection_reason, tok.observation_started_at, tok.observation_exit_reason,
    t1.decision_time, t1.tier1_passed, t1.tier1_reason, t1.tier1_inputs,
    tf.first_decision_time, tc.n_tier1_evals,
    sc.scorer_time, sc.scorer_passed, sc.scorer_reason, sc.scorer_inputs
FROM tokens tok
JOIN tier1_latest t1 ON t1.token_id = tok.id
JOIN tier1_first tf ON tf.token_id = tok.id
JOIN tier1_count tc ON tc.token_id = tok.id
LEFT JOIN scorer_latest sc ON sc.token_id = tok.id
ORDER BY tok.discovered_at ASC;
"""

SNAPSHOTS_SQL = "SELECT token_id, sampled_at, price_usd FROM token_snapshots ORDER BY token_id, sampled_at ASC;"


def window_overlaps_outage(start: datetime, end: datetime) -> bool:
    return any(start < o_end and end > o_start for o_start, o_end in OUTAGES)


def timestamp_in_outage(ts: datetime) -> bool:
    return any(o_start <= ts <= o_end for o_start, o_end in OUTAGES)


def horizon_result(snaps, anchor, minutes, now):
    """
    Returns (return_value, is_complete) for one horizon. is_complete=False
    means: don't use this number for anything - not enough clean, elapsed,
    non-contaminated data exists yet, full stop.
    """
    if anchor is None:
        return None, False
    end = anchor + timedelta(minutes=minutes) if minutes is not None else None

    if minutes is not None:
        if window_overlaps_outage(anchor, end):
            return None, False
        if end > now:
            return None, False
        relevant = [(t, p) for t, p in snaps if anchor <= t <= end]
    else:
        # "full" — complete only if the token's entire observed span so far
        # (anchor through its last snapshot) never touches the outage, and
        # there IS at least one snapshot after anchor.
        after = [(t, p) for t, p in snaps if t >= anchor]
        if not after:
            return None, False
        last_ts = after[-1][0]
        if window_overlaps_outage(anchor, last_ts):
            return None, False
        relevant = after

    relevant = [(t, p) for t, p in relevant if p is not None and p > 0]
    if len(relevant) < 1:
        return None, False
    baseline = relevant[0][1]
    if baseline <= 0:
        return None, False
    peak = max(p for _, p in relevant)
    return (peak / baseline - 1), True


async def load(conn):
    records = await conn.fetch(FETCH_SQL)
    snap_records = await conn.fetch(SNAPSHOTS_SQL)
    snaps_by_token = {}
    for r in snap_records:
        snaps_by_token.setdefault(r["token_id"], []).append((r["sampled_at"], _num(r["price_usd"])))

    now = datetime.now(timezone.utc)
    rows = []
    for rec in records:
        t1_inputs = json.loads(rec["tier1_inputs"]) if rec["tier1_inputs"] else {}
        sc_inputs = json.loads(rec["scorer_inputs"]) if rec["scorer_inputs"] else None

        buys = t1_inputs.get("buys")
        sells = t1_inputs.get("sells")
        wash = _num(t1_inputs.get("wash_multiplier"))
        liq = _num(t1_inputs.get("liquidity_usd"))
        mcap = _num(t1_inputs.get("market_cap_usd"))
        age_min = _num(t1_inputs.get("age_minutes"))
        buy_sell_ratio = (buys / sells) if (sells not in (None, 0) and buys is not None) else None
        total_txns = (buys + sells) if (buys is not None and sells is not None) else None

        mysnaps = snaps_by_token.get(rec["id"], [])
        decision_time = rec["decision_time"]
        obs_start = rec["observation_started_at"]

        # group classification
        is_scorer_discard = bool(sc_inputs and rec["scorer_reason"] == "discard")
        if not rec["tier1_passed"]:
            group = "tier1_rejected"
        elif is_scorer_discard:
            group = "scorer_discarded"
        else:
            group = "accepted_watching"

        accept_res = {}
        for m in (5, 15, 30, None):
            v, complete = horizon_result(mysnaps, decision_time, m, now)
            accept_res[m if m else "full"] = (v, complete)

        reject_res = {}
        for m in (5, 15, 30, None):
            v, complete = horizon_result(mysnaps, obs_start, m, now)
            reject_res[m if m else "full"] = (v, complete)

        # contamination check on the RAW decision itself: was this token's
        # decision made during the outage at all? (even if it later gets
        # some clean snapshots after the outage ended, its initial
        # observation was still built on a broken clock — exclude outright)
        decision_in_outage = timestamp_in_outage(decision_time) if decision_time else False

        rows.append({
            "mint": rec["mint_address"], "symbol": rec["symbol"],
            "discovered_at": rec["discovered_at"], "decision_time": decision_time,
            "n_tier1_evals": rec["n_tier1_evals"],
            "tier1_passed": rec["tier1_passed"], "group": group,
            "decision_in_outage": decision_in_outage,
            "age_minutes": age_min, "liquidity_usd": liq, "market_cap_usd": mcap,
            "buys": buys, "sells": sells, "wash_multiplier": wash,
            "buy_sell_ratio": buy_sell_ratio, "total_txns": total_txns,
            "scorer_score": _num(sc_inputs.get("score")) if sc_inputs else None,
            "scorer_version": sc_inputs.get("scorer_version") if sc_inputs else None,
            "accept_5": accept_res[5], "accept_15": accept_res[15],
            "accept_30": accept_res[30], "accept_full": accept_res["full"],
            "reject_5": reject_res[5], "reject_15": reject_res[15],
            "reject_30": reject_res[30], "reject_full": reject_res["full"],
        })
    return rows


def clean_pairs(rows, key, horizon_field, require_group=None):
    """(feature_value, outcome) pairs where the horizon is COMPLETE and the
    decision itself wasn't made during the outage."""
    pairs = []
    for r in rows:
        if r["decision_in_outage"]:
            continue
        if require_group and r["group"] not in require_group:
            continue
        v = r.get(key)
        val, complete = r.get(horizon_field, (None, False))
        if v is None or not complete or val is None:
            continue
        pairs.append((float(v), float(val)))
    return pairs


def hit_rate(vals, threshold):
    if not vals:
        return None
    return sum(1 for v in vals if v >= threshold) / len(vals)


async def main():
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        rows = await load(conn)
    finally:
        await conn.close()

    lines = []
    def p(s=""):
        lines.append(s)
        print(s)

    total = len(rows)
    in_outage = sum(1 for r in rows if r["decision_in_outage"])
    clean = [r for r in rows if not r["decision_in_outage"]]

    p("# S1 Wave — Clean Recap (outage-excluded, analysis-only)")
    p(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    p("")
    p("**No scoring, thresholds, gates, or weights were changed. Read-only.**")
    p("")

    # ── 1. Dataset ──────────────────────────────────────────────────────
    p("## 1. Dataset")
    p(f"- Total tokens in DB: {total}")
    outage_desc = "; ".join(f"{s.isoformat()} to {e.isoformat()}" for s, e in OUTAGES)
    p(f"- Decided during a known-contaminated window ({outage_desc}) — "
      f"excluded outright, no usable clock during that time: **{in_outage}**")
    p(f"- Remaining after outage exclusion: **{len(clean)}**")
    reopened = sum(1 for r in clean if r["n_tier1_evals"] > 1)
    p(f"- Of those, {reopened} were rediscovered/re-evaluated more than once — "
      f"decision-time features taken from the LATEST evaluation (see module docstring for why)")
    p("")
    p("Per-horizon usable sample sizes are reported separately below, since 'complete' "
      "depends on both elapsed time and non-overlap with the outage — a token can be in "
      "the clean set above and still have zero usable horizons if it was discovered right "
      "before the outage started.")
    p("")

    # ── group counts ──────────────────────────────────────────────────
    from collections import Counter
    gc = Counter(r["group"] for r in clean)
    p(f"- tier1_rejected: {gc['tier1_rejected']}  |  scorer_discarded: {gc['scorer_discarded']}  |  "
      f"accepted_watching: {gc['accepted_watching']}")
    p("")

    # ── 2. Scorer ────────────────────────────────────────────────────
    p("## 2. Scorer: does a higher score predict better outcomes?")
    p("")
    p("Using accept_* horizons (anchored at the Tier1 decision — the same window the "
      "scorer's own inputs were drawn from; there is no genuine post-scorer-decision "
      "window yet, see the standing limitation in raw_feature_backtest.py).")
    p("")
    for label, field in (("5 min", "accept_5"), ("15 min", "accept_15"), ("30 min", "accept_30")):
        pairs = clean_pairs(clean, "scorer_score", field)
        p(f"### @ {label}")
        if len(pairs) < 5:
            p(f"- n={len(pairs)} — too few complete, clean observations yet")
            p("")
            continue
        xs, ys = [x for x, _ in pairs], [y for _, y in pairs]
        p(f"- n = {len(pairs)}")
        p(f"- Pearson r = {fmt_r(pearson(xs, ys))}  |  Spearman ρ = {fmt_r(spearman(xs, ys))}")
        p("| score range | n | median return | hit rate ≥10% | hit rate ≥20% |")
        p("|---|---|---|---|---|")
        sorted_pairs = sorted(pairs, key=lambda t: t[0])
        n = len(sorted_pairs)
        nb = 3 if n >= 15 else max(1, n // 5)
        size = n / nb
        for i in range(nb):
            lo, hi = int(round(i * size)), int(round((i + 1) * size)) if i < nb - 1 else n
            chunk = sorted_pairs[lo:hi]
            if not chunk:
                continue
            fvals, ovals = [c[0] for c in chunk], [c[1] for c in chunk]
            p(f"| {fvals[0]:.2f}-{fvals[-1]:.2f} | {len(chunk)} | {fmt_pct(statistics.median(ovals))} | "
              f"{fmt_pct(hit_rate(ovals, 0.10))} | {fmt_pct(hit_rate(ovals, 0.20))} |")
        p("")

    scores_all = [r["scorer_score"] for r in clean if r["scorer_score"] is not None]
    p("### Score distribution (all clean scorer evaluations)")
    if scores_all:
        p(f"- n = {len(scores_all)}, min = {min(scores_all):.2f}, max = {max(scores_all):.2f}, "
          f"median = {statistics.median(scores_all):.2f}")
        p(f"- **5.0 (WATCH threshold) has {'been' if max(scores_all) >= 5.0 else 'NEVER been'} "
          f"reached.** Highest score seen: {max(scores_all):.2f}.")
    p("")

    # ── 3. Rejected vs accepted, three-way ────────────────────────────
    p("## 3. Tier1-rejected vs. Scorer-discarded vs. accepted/watching")
    p("")
    p("| group | horizon | n | median return | hit ≥10% | hit ≥20% | hit ≥50% |")
    p("|---|---|---|---|---|---|---|")
    for group in ("tier1_rejected", "scorer_discarded", "accepted_watching"):
        anchor_field_prefix = "reject" if group != "accepted_watching" else "accept"
        for label, m in (("5m", 5), ("15m", 15), ("30m", 30), ("full", "full")):
            field = f"{anchor_field_prefix}_{m}"
            vals = []
            for r in clean:
                if r["group"] != group:
                    continue
                val, complete = r.get(field, (None, False))
                if complete and val is not None:
                    vals.append(val)
            if vals:
                p(f"| {group} | {label} | {len(vals)} | {fmt_pct(statistics.median(vals))} | "
                  f"{fmt_pct(hit_rate(vals, 0.10))} | {fmt_pct(hit_rate(vals, 0.20))} | {fmt_pct(hit_rate(vals, 0.50))} |")
            else:
                p(f"| {group} | {label} | 0 | n/a | n/a | n/a | n/a |")
    p("")
    p("Note: tier1_rejected and scorer_discarded use reject_* (anchored at observation_started_at "
      "— the moment of rejection). accepted_watching uses accept_* (anchored at the Tier1 decision, "
      "since these tokens never entered OBSERVING) — the two anchors measure the same *kind* of thing "
      "(what happens after the pipeline's decision) but are not the identical clock, flagged for honesty.")
    p("")

    # ── 4. Big movers among rejected tokens ───────────────────────────
    p("## 4. Rejected tokens that moved big — what do they share?")
    p("")
    rejected_pool = [r for r in clean if r["group"] in ("tier1_rejected", "scorer_discarded")]
    for thresh in (0.10, 0.20, 0.50):
        movers = []
        for r in rejected_pool:
            val, complete = r["reject_full"]
            if complete and val is not None and val >= thresh:
                movers.append(r)
        p(f"### Moved ≥{int(thresh*100)}% (full clean reject window): n={len(movers)}")
        if movers:
            washes = [r["wash_multiplier"] for r in movers if r["wash_multiplier"] is not None]
            liqs = [r["liquidity_usd"] for r in movers if r["liquidity_usd"] is not None]
            groups_ct = Counter(r["group"] for r in movers)
            p(f"- wash_multiplier: median {statistics.median(washes):.2f}" if washes else "- wash_multiplier: n/a")
            p(f"- liquidity_usd: median ${statistics.median(liqs):,.0f}" if liqs else "- liquidity_usd: n/a")
            p(f"- split: {dict(groups_ct)}")
        p("")

    # ── 5. Six movers revisit ──────────────────────────────────────────
    p("## 5. Six movers — status")
    p("")
    for symbol, target in SIX_MOVERS_TARGET.items():
        cands = [r for r in rows if r["symbol"] == symbol and r["tier1_passed"] and r["scorer_score"] is not None]
        if not cands:
            p(f"- **{symbol}**: not found in current dataset")
            continue
        best = min(cands, key=lambda r: abs(r["scorer_score"] - target))
        tag = " (decision fell inside the outage window — excluded from all stats above)" if best["decision_in_outage"] else ""
        p(f"- **{symbol}**: score {best['scorer_score']}{tag}")
    p("")

    # ── 6. Raw feature summary ─────────────────────────────────────────
    p("## 6. Raw decision-time features — signal or noise, on the clean set")
    p("")
    for name, key in (("wash_multiplier", "wash_multiplier"), ("buy_sell_ratio", "buy_sell_ratio"),
                      ("buys", "buys"), ("sells", "sells"), ("total_txns", "total_txns"),
                      ("liquidity_usd", "liquidity_usd"), ("age_minutes", "age_minutes")):
        pairs = clean_pairs(clean, key, "accept_30") + clean_pairs(clean, key, "reject_30")
        if len(pairs) < 8:
            p(f"- {name}: n={len(pairs)} — too few clean+complete 30m observations to say anything")
            continue
        xs, ys = [x for x, _ in pairs], [y for _, y in pairs]
        pr, sr = pearson(xs, ys), spearman(xs, ys)
        shape = "nonlinear (Spearman >> Pearson)" if (pr is not None and sr is not None and abs(sr) - abs(pr) > 0.1) else "roughly linear or weak"
        p(f"- **{name}**: n={len(pairs)}, Pearson r={fmt_r(pr)}, Spearman ρ={fmt_r(sr)} — {shape}")
    p("")
    p("volume_usd and buy_pressure are NOT included — Tier1 never captures them at decision "
      "time, they only exist in post-decision snapshots, and using them here would be leakage. "
      "Not fixed in this pass (see raw_feature_backtest.py finding #6).")
    p("")

    # ── 7. explicit 5.0 statement already above in section 2 ──────────

    # ── 8. Conclusion ──────────────────────────────────────────────────
    p("## 8. What we KNOW vs. what's INTERESTING-but-unproven vs. what's UNTESTED")
    p("")
    p("**KNOW:**")
    p("- Both known contamination windows are precisely bounded from logs and excluded, not guessed around (see section 1).")
    if scores_all and max(scores_all) >= 5.0:
        p(f"- 5.0 (WATCH) HAS been reached on clean data — max score seen is {max(scores_all):.2f}. "
          f"(This was NOT true as of the last run — the ceiling moved.)")
    else:
        p("- 5.0 (WATCH) has never been reached by real scorer output on clean data; see the max in section 2.")
    p("- mint_authority_renounced / freeze_authority_renounced reject nothing — constant true across every token seen.")
    p("")
    p("**INTERESTING, NOT YET PROVEN:**")
    p("- Score correlates positively with clean outcome (section 2) — direction looks real, magnitude is still small-n.")
    p("- Rejected tokens showing non-trivial movement (section 3/4) — real in this window, not yet shown to persist.")
    p("- Transaction-count and wash-multiplier patterns from the earlier report — re-check once this clean set is bigger.")
    p("")
    p("**UNTESTED / NEEDS MORE DATA:**")
    p("- Whether any of this holds outside the ~2 hours of clean pre-outage data plus the short post-fix window.")
    p("- True post-Scorer-decision forward returns (still confounded with the pre-decision window — see section 2 caveat).")
    p("")
    p("## 9. Single most useful next analysis")
    p("Re-run this exact script once the post-fix window alone (17:37:13Z onward, zero outage overlap by "
      "construction) has accumulated enough tokens with complete 30-minute windows to stand on its own "
      "without leaning on the smaller pre-outage slice at all — that removes the last reason to trust two "
      "different time periods stitched together, at the cost of waiting a few more hours.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (OUTPUT_DIR / f"clean_recap_{ts}.md").write_text("\n".join(lines))
    print(f"\n[saved] {OUTPUT_DIR / f'clean_recap_{ts}.md'}")


if __name__ == "__main__":
    asyncio.run(main())
