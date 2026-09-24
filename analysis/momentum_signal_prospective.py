"""
analysis/momentum_signal_prospective.py
=========================================
ANALYSIS-ONLY, READ-ONLY. First prospective check of the momentum_confluence_v1
forward-tracking experiment (workers/momentum_signal.py, deployed 2026-09-22).

Why this is different from every earlier script this session
----------------------------------------------------------------
analysis/pump_timing_research.py and analysis/pump_signal_quality.py found
and validated (via chronological time-split, not random) that a >5.3%
trailing-3-minute price move reliably precedes a token's peak, and that
requiring >=2 of 4 related rules to co-fire at that same moment roughly
doubles the odds the move holds rather than dumping. Both of those results
came from RECONSTRUCTING signals retrospectively out of historical
token_snapshots. This script is the first look at signals the bot detected
LIVE, in real time, on tokens it had never seen when the rule was written —
the actual prospective test the whole exercise was built toward. Still an
early, small sample (see population/coverage section) — this is a first
checkpoint, not a verdict.

Baseline price
--------------
Unlike every earlier script's snapshot-derived baseline, here the baseline
IS the exact price at the moment the rule fired (momentum_signal_events.
trigger_price) — no need to look up a nearest snapshot, it's already exact.

Outcome computation, and what "too young" means
-------------------------------------------------
For each signal and each horizon (5/15/30/60 minutes forward), a value is
only computed if enough wall-clock time has actually elapsed since
triggered_at (triggered_at + horizon <= now()) — a signal from 10 minutes
ago cannot yet have a real 60-minute outcome, and this script says so
explicitly per horizon rather than silently treating "no data yet" the same
as "no data because the token went dark". Coverage (how many signals are
mature enough to evaluate at each horizon, and of those, how many actually
have a snapshot to use) is reported for every horizon.

Time-split validation
----------------------
Self-adapting: once the sample reaches MIN_FOR_SPLIT (200 — chosen as
roughly half the 420-token retrospective sample, enough for two real halves
rather than a token gesture), the script automatically sorts all signals
chronologically by triggered_at and splits at the midpoint into train/test,
then reports the confluence-bucket comparison for EACH half separately —
same discipline as analysis/pump_signal_quality.py. Below that threshold it
still runs, but only reports the pooled comparison with an explicit "too
early to split" note, so this same script can be run at any time from day
one through full maturity without ever needing a rewrite.

Usage
-----
    PYTHONPATH=. python analysis/momentum_signal_prospective.py

Outputs
-------
    analysis/output/momentum_signal_prospective_<timestamp>.md
    analysis/output/momentum_signal_prospective_dataset_<timestamp>.csv
"""

from __future__ import annotations

import asyncio
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
HORIZONS = [5, 15, 30, 60]
EXPERIMENT_VERSION = "momentum_confluence_v1"
MIN_FOR_SPLIT = 200

SIGNALS_SQL = """
SELECT me.token_id, tok.symbol, tok.status, me.triggered_at, me.trigger_price,
       me.trailing_return_3min, me.trailing_return_5min, me.buy_pressure,
       me.volume_mult, me.n_rules_cofiring, me.liquidity_usd, me.market_cap_usd
FROM momentum_signal_events me
JOIN tokens tok ON tok.id = me.token_id
WHERE me.experiment_version = $1
ORDER BY me.triggered_at ASC;
"""

SNAPSHOTS_SQL = "SELECT sampled_at, price_usd FROM token_snapshots WHERE token_id = $1 ORDER BY sampled_at ASC;"


def _num(v):
    return float(v) if v is not None else None


def compute_outcome(snaps, baseline_price, trigger_time, horizon_min, now):
    cutoff = trigger_time + timedelta(minutes=horizon_min)
    mature = now >= cutoff
    if not mature:
        return dict(mature=False, forward_return=None, peak_return=None, max_drawdown=None, n_obs=0)
    window = [(t, p) for t, p in snaps if trigger_time <= t <= cutoff and p and p > 0]
    if not window:
        return dict(mature=True, forward_return=None, peak_return=None, max_drawdown=None, n_obs=0)
    prices = [p for _, p in window]
    return dict(
        mature=True,
        forward_return=prices[-1] / baseline_price - 1,
        peak_return=max(prices) / baseline_price - 1,
        max_drawdown=min(prices) / baseline_price - 1,
        n_obs=len(window),
    )


def median_iqr(vals):
    v = sorted(x for x in vals if x is not None)
    if not v:
        return (None, None, None, 0)
    n = len(v)
    med = statistics.median(v)
    q1 = v[max(0, n // 4)]
    q3 = v[min(n - 1, (3 * n) // 4)]
    return (q1, med, q3, n)


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

    records = []
    for r in rows:
        baseline = _num(r["trigger_price"])
        snaps = snaps_by_token[r["token_id"]]
        rec = dict(
            symbol=r["symbol"], token_id=str(r["token_id"]), status=r["status"],
            triggered_at=r["triggered_at"], n_rules_cofiring=r["n_rules_cofiring"],
            trailing_return_3min=_num(r["trailing_return_3min"]),
            age_min=round((now - r["triggered_at"]).total_seconds() / 60, 1),
        )
        for h in HORIZONS:
            o = compute_outcome(snaps, baseline, r["triggered_at"], h, now)
            rec[f"mature_{h}"] = o["mature"]
            rec[f"forward_return_{h}"] = o["forward_return"]
            rec[f"peak_return_{h}"] = o["peak_return"]
            rec[f"max_drawdown_{h}"] = o["max_drawdown"]
            rec[f"n_obs_{h}"] = o["n_obs"]
        records.append(rec)

    ts = now.strftime("%Y%m%d_%H%M%S")
    import csv
    csv_path = OUTPUT_DIR / f"momentum_signal_prospective_dataset_{ts}.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(records[0].keys()) if records else [])
        w.writeheader()
        for rec in records:
            w.writerow(rec)

    lines = []
    p = lines.append
    p(f"# Momentum Confluence — Prospective Check — {ts}")
    p("")
    p("ANALYSIS-ONLY. Read-only, no trading logic touched. First live-data checkpoint "
      f"of the `{EXPERIMENT_VERSION}` forward-tracking experiment deployed 2026-09-22.")
    p("")
    p(f"- Total signals recorded so far: **{len(records)}**")
    p(f"- Time span: {rows[0]['triggered_at'].isoformat()} to {rows[-1]['triggered_at'].isoformat()} "
      f"(~{round((now - rows[0]['triggered_at']).total_seconds()/3600, 1)}h)")
    p(f"- This is an EARLY checkpoint, not a conclusion — the retrospective validation used 420 real-pump "
      f"tokens; this prospective sample is a fraction of that. Read every number here as \"first look\", "
      f"not \"confirmed\".")
    p("")
    p("## Coverage: how many signals are mature enough to judge at each horizon")
    p("")
    p("| horizon | mature (enough time passed) | has data | no snapshot despite maturity |")
    p("|---|---|---|---|")
    for h in HORIZONS:
        mature_n = sum(1 for r in records if r[f"mature_{h}"])
        has_data = sum(1 for r in records if r[f"forward_return_{h}"] is not None)
        p(f"| {h}min | {mature_n}/{len(records)} | {has_data} | {mature_n - has_data} |")
    p("")
    def bucket_stats(recs, h):
        low = [r[f"forward_return_{h}"] for r in recs if r["n_rules_cofiring"] <= 1 and r[f"forward_return_{h}"] is not None]
        high = [r[f"forward_return_{h}"] for r in recs if r["n_rules_cofiring"] >= 2 and r[f"forward_return_{h}"] is not None]
        _, medl, _, nl = median_iqr(low)
        _, medh, _, nh = median_iqr(high)
        pos_low = sum(1 for x in low if x > 0)
        pos_high = sum(1 for x in high if x > 0)
        return dict(nl=nl, nh=nh, medl=medl, medh=medh, pos_low=pos_low, pos_high=pos_high)

    def fmt_side(n, med, pos):
        if not n:
            return "n=0, n/a"
        med_txt = f"{med:.1%}" if med is not None else "n/a"
        return f"n={n}, median={med_txt}, win-rate={pos}/{n} ({pos/n:.0%})"

    def fmt_bucket_line(label, s):
        low_txt = fmt_side(s["nl"], s["medl"], s["pos_low"])
        high_txt = fmt_side(s["nh"], s["medh"], s["pos_high"])
        return f"- {label} — 0-1 rules: {low_txt} | 2+ rules: {high_txt}"

    n_total = len(records)
    can_split = n_total >= MIN_FOR_SPLIT
    p("## Outcome by confluence bucket (0-1 rules vs 2+ rules) — the exact split the retrospective work validated")
    p("")
    if can_split:
        sorted_recs = sorted(records, key=lambda r: r["triggered_at"])
        split_idx = len(sorted_recs) // 2
        split_time = sorted_recs[split_idx]["triggered_at"]
        train = sorted_recs[:split_idx]
        test = sorted_recs[split_idx:]
        p(f"n={n_total} >= MIN_FOR_SPLIT={MIN_FOR_SPLIT} — chronological time-split now active. "
          f"Split at {split_time.isoformat()}: train n={len(train)}, test n={len(test)}.")
        p("")
        for h in HORIZONS:
            p(f"### {h}-minute forward return")
            p("")
            p(fmt_bucket_line("TRAIN", bucket_stats(train, h)))
            p(fmt_bucket_line("TEST ", bucket_stats(test, h)))
            s_train, s_test = bucket_stats(train, h), bucket_stats(test, h)
            holds = (
                s_train["nl"] and s_train["nh"] and s_test["nl"] and s_test["nh"]
                and s_train["medh"] is not None and s_train["medl"] is not None
                and s_test["medh"] is not None and s_test["medl"] is not None
                and (s_train["medh"] > s_train["medl"]) == (s_test["medh"] > s_test["medl"])
            )
            if s_train["nl"] and s_train["nh"] and s_test["nl"] and s_test["nh"]:
                p(f"- **{'Direction holds in test.' if holds else 'Direction did NOT hold in test — treat as unconfirmed at this horizon.'}**")
            min_n = min(s_train["nl"] or 0, s_train["nh"] or 0, s_test["nl"] or 0, s_test["nh"] or 0)
            if min_n < 15:
                p(f"- **LOW CONFIDENCE (smallest bucket n={min_n}) even with a split active.**")
            p("")
    else:
        p(f"n={n_total} < MIN_FOR_SPLIT={MIN_FOR_SPLIT} — too early for a chronological time-split. "
          f"Pooled comparison only (same caveat as the very first checkpoint): every number below could "
          f"still be an early-sample artifact, not yet a validated effect.")
        p("")
        for h in HORIZONS:
            p(f"### {h}-minute forward return")
            p("")
            s = bucket_stats(records, h)
            p(fmt_bucket_line("POOLED", s))
            if (s["nl"] or 0) < 15 or (s["nh"] or 0) < 15:
                p(f"- **LOW CONFIDENCE (n<15 on at least one side) — too early to draw a conclusion at this horizon.**")
            p("")
    p("## Raw distribution: n_rules_cofiring counts in this sample")
    p("")
    from collections import Counter
    c = Counter(r["n_rules_cofiring"] for r in records)
    for k in sorted(c):
        p(f"- {k} rules co-firing: n={c[k]}")
    p("")
    p("## Still-open signals (status WATCHING/OBSERVING) vs concluded (REJECTED/other)")
    p("")
    from collections import Counter as C2
    sc = C2(r["status"].name if hasattr(r["status"], "name") else str(r["status"]) for r in records)
    for k, v in sc.items():
        p(f"- {k}: {v}")
    p("")
    p("## Caveats")
    p("")
    age_h = round((now - rows[0]['triggered_at']).total_seconds() / 3600, 1)
    if can_split:
        p(f"- Sample (n={n_total}, ~{age_h}h old) is now large enough for a real chronological time-split — "
          f"see the train/test comparison above. Still worth growing further before treating this as final; "
          f"the retrospective study used n=420.")
    else:
        p(f"- Sample is small (n={n_total}) and young (oldest signal ~{age_h}h) — this is a checkpoint to "
          f"establish the tracking is working correctly and watch the trend, not a validated result. "
          f"Time-split activates automatically once n>={MIN_FOR_SPLIT} — just re-run this same script.")
    p("- No trading logic, thresholds, or scorer weights touched. Nothing implemented from this checkpoint.")

    report_path = OUTPUT_DIR / f"momentum_signal_prospective_{ts}.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")
    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
