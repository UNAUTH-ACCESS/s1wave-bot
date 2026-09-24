"""
analysis/shadow_lifetime_followup.py
======================================
ANALYSIS-ONLY. Read-only against the live database (SELECT only). Never
imports or touches scorer_worker.py, settings.py, risk.py,
sampling_worker.py, discovery_worker.py, or any other production path.
Safe to run at any time, including while main.py is live.

Purpose
-------
analysis/edge_research.py found that the scorer_v2_threshold_4 shadow
population had a median decision-anchored forward_return_full of about
-91.5% (return measured from the moment a token's score crossed 4.0 to
the last available snapshot), versus about +7.1% for the Tier1-rejected
OBSERVING control group over the same kind of window. That report flagged
a likely confound: a shadow trade can only fire once the Scorer has
several snapshots of rolling-window history, so its decision_time is by
construction LATER in a token's life than a Tier1 reject's decision_time
(which fires in the token's first ~30-60 seconds). If a token had already
pumped before crossing 4.0, decision-anchored return would look
artificially bad - you would be measuring "what happens after you buy
the top", not "was this a bad token".

This script re-anchors everything to DISCOVERY (tokens.discovered_at)
instead of decision time, for both the shadow population and the same
OBSERVING control group, and asks directly: does the -91.5%-style result
survive when measured from first discovery?

Reused from analysis/edge_research.py, unchanged
--------------------------------------------------
- OUTAGES (the corrected, closed-interval version - see that file's own
  docstring for why the second interval must be closed at
  2026-09-21T07:17:51Z, not open-ended like the stale copies in
  shadow_backtest.py / clean_recap.py).
- window_overlaps_outage(start, end)
- compute_outcomes(snaps, decision_time) - anchor-agnostic by design:
  called once per candidate with decision_time=discovered_at (the
  "lifetime" view) and again with decision_time=triggered_at /
  observation_started_at (the "decision" view, reproducing
  edge_research.py's own numbers as a sanity check). NOT reimplemented
  here - imported directly so both scripts can never silently diverge in
  how an outcome is computed.
- HORIZONS, EXPERIMENT_VERSION, _num, SNAPSHOTS_SQL, OBSERVING_TOKENS_SQL.

max_drawdown convention (inherited, stated explicitly because it matters
for this report): compute_outcomes defines max_drawdown as
min(price in window) / baseline - 1, i.e. BASELINE-to-trough, not
peak-to-trough. For the lifetime view, baseline = first available
snapshot at/after discovered_at, so "max drawdown" here means "how far
below the first-seen price did it ever get", not "how far did it fall
from its own peak". This is the same convention used everywhere else in
this session's analysis output, kept for comparability.

New in this script
--------------------
- Lifetime-anchored SQL: shadow_trades joined to tokens.discovered_at
  (edge_research.py's SHADOW_SQL does not select discovered_at).
- time_to_peak_minutes(snaps, anchor): among snapshots at/after anchor,
  minutes from anchor to the timestamp of the maximum price.
- pre_decision_peak_return(snaps, decision_time): among snapshots
  STRICTLY BEFORE decision_time, (max_price/first_price - 1). None if no
  such snapshots exist (has_pre_decision_data=False) - by construction
  this will be near-universal for OBSERVING tokens (decision fires
  seconds after discovery) and variable for shadow tokens (decision
  fires after enough history exists for the Scorer to compute a score).
- Substantial-upside split: >=20% pre-decision peak return is the primary,
  stated threshold (a token that had already more than tripled its price
  range before ever crossing the score threshold is unambiguously "not
  discovered fresh" by the time the bot flagged it). A median-split
  version is reported alongside as a robustness check, since 20% is a
  judgment call and the data-driven median split cannot be gamed by that
  choice.

Usage
-----
    PYTHONPATH=. /tmp/s1wave-venv/bin/python analysis/shadow_lifetime_followup.py

Outputs
-------
    analysis/output/shadow_lifetime_followup_<timestamp>.md
    analysis/output/shadow_lifetime_followup_dataset_<timestamp>.csv
"""

from __future__ import annotations

import sys
import asyncio
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg  # noqa: E402

from config.settings import settings  # noqa: E402
from analysis.edge_research import (  # noqa: E402
    OUTAGES, HORIZONS, EXPERIMENT_VERSION, _num,
    compute_outcomes, window_overlaps_outage,
    OBSERVING_TOKENS_SQL, SNAPSHOTS_SQL,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

SHADOW_WITH_DISCOVERY_SQL = """
SELECT st.token_id, tok.symbol, tok.discovered_at, st.triggered_at,
       st.score, st.entry_price
FROM shadow_trades st
JOIN tokens tok ON tok.id = st.token_id
WHERE st.experiment_version = $1
ORDER BY st.triggered_at ASC;
"""

SUBSTANTIAL_UPSIDE_THRESHOLD = 0.20  # +20% pre-decision peak return


def time_to_peak_minutes(snaps, anchor):
    after = sorted([(t, p) for t, p in snaps if t is not None and p is not None
                    and p > 0 and t >= anchor], key=lambda x: x[0])
    if not after:
        return None
    peak_t, _ = max(after, key=lambda x: x[1])
    return (peak_t - anchor).total_seconds() / 60.0


def pre_decision_peak_return(snaps, decision_time):
    before = sorted([(t, p) for t, p in snaps if t is not None and p is not None
                      and p > 0 and t < decision_time], key=lambda x: x[0])
    if len(before) < 2:
        return None, False
    first_price = before[0][1]
    max_price = max(p for _, p in before)
    if first_price <= 0:
        return None, False
    return (max_price / first_price - 1), True


async def load(conn):
    shadow_rows = await conn.fetch(SHADOW_WITH_DISCOVERY_SQL, EXPERIMENT_VERSION)
    obs_tokens = await conn.fetch(OBSERVING_TOKENS_SQL)
    snap_rows = await conn.fetch(SNAPSHOTS_SQL)
    snaps_by_token = {}
    for r in snap_rows:
        snaps_by_token.setdefault(r["token_id"], []).append((r["sampled_at"], _num(r["price_usd"])))
    return shadow_rows, obs_tokens, snaps_by_token


def build_records(shadow_rows, obs_tokens, snaps_by_token):
    recs = []

    for r in shadow_rows:
        token_id = r["token_id"]
        discovered_at = r["discovered_at"]
        decision_time = r["triggered_at"]
        entry_price = _num(r["entry_price"])
        raw_snaps = list(snaps_by_token.get(token_id, []))
        # entry_price is the authoritative decision-time price (captured
        # at trigger time) - splice it in exactly like edge_research.py
        # does, so the decision-anchored view here matches that report.
        snaps_for_decision = [(decision_time, entry_price)] + [
            (t, p) for t, p in raw_snaps if t > decision_time
        ]
        # Lifetime view must NOT get the spliced synthetic point - it
        # would corrupt "earliest available price" with a later value if
        # a real earlier snapshot doesn't exist. Use raw_snaps as-is.
        lifetime = compute_outcomes(raw_snaps, discovered_at)["full"]
        decision = compute_outcomes(snaps_for_decision, decision_time)["full"]
        pre_peak, has_pre = pre_decision_peak_return(raw_snaps, decision_time)

        n_total_snaps = len(raw_snaps)
        first_snap_ts = min((t for t, p in raw_snaps if p and p > 0), default=None)
        latency_min = ((first_snap_ts - discovered_at).total_seconds() / 60.0
                        if first_snap_ts else None)

        rec = dict(
            group="shadow", token_id=str(token_id), symbol=r["symbol"],
            discovered_at=discovered_at, decision_time=decision_time,
            score=_num(r["score"]),
            lifetime_return_full=lifetime["forward_return"],
            lifetime_peak_return_full=lifetime["peak_return"],
            lifetime_max_drawdown_full=lifetime["max_drawdown"],
            lifetime_n_obs=lifetime["n_observations_used"],
            lifetime_outage_excluded=lifetime["outage_excluded"],
            decision_return_full=decision["forward_return"],
            decision_peak_return_full=decision["peak_return"],
            decision_max_drawdown_full=decision["max_drawdown"],
            decision_n_obs=decision["n_observations_used"],
            decision_outage_excluded=decision["outage_excluded"],
            time_discovery_to_decision_min=(decision_time - discovered_at).total_seconds() / 60.0,
            time_discovery_to_peak_min=time_to_peak_minutes(raw_snaps, discovered_at),
            time_decision_to_peak_min=time_to_peak_minutes(snaps_for_decision, decision_time),
            pre_decision_peak_return=pre_peak,
            has_pre_decision_data=has_pre,
            n_total_snaps=n_total_snaps,
            discovery_to_first_snap_min=latency_min,
        )
        recs.append(rec)

    for t in obs_tokens:
        token_id = t["id"]
        discovered_at = t["discovered_at"]
        decision_time = t["observation_started_at"]
        raw_snaps = list(snaps_by_token.get(token_id, []))
        if not raw_snaps:
            continue  # zero snapshots at all - can't compute anything, excluded (counted separately below)
        decision_snaps = [(ts, p) for ts, p in raw_snaps if ts >= decision_time]
        if not decision_snaps:
            continue  # no post-decision data - same exclusion rule edge_research.py uses

        lifetime = compute_outcomes(raw_snaps, discovered_at)["full"]
        decision = compute_outcomes(raw_snaps, decision_time)["full"]
        pre_peak, has_pre = pre_decision_peak_return(raw_snaps, decision_time)

        n_total_snaps = len(raw_snaps)
        first_snap_ts = min((tt for tt, p in raw_snaps if p and p > 0), default=None)
        latency_min = ((first_snap_ts - discovered_at).total_seconds() / 60.0
                        if first_snap_ts else None)

        rec = dict(
            group="observing", token_id=str(token_id), symbol=t["symbol"],
            discovered_at=discovered_at, decision_time=decision_time,
            score=None,
            lifetime_return_full=lifetime["forward_return"],
            lifetime_peak_return_full=lifetime["peak_return"],
            lifetime_max_drawdown_full=lifetime["max_drawdown"],
            lifetime_n_obs=lifetime["n_observations_used"],
            lifetime_outage_excluded=lifetime["outage_excluded"],
            decision_return_full=decision["forward_return"],
            decision_peak_return_full=decision["peak_return"],
            decision_max_drawdown_full=decision["max_drawdown"],
            decision_n_obs=decision["n_observations_used"],
            decision_outage_excluded=decision["outage_excluded"],
            time_discovery_to_decision_min=(decision_time - discovered_at).total_seconds() / 60.0,
            time_discovery_to_peak_min=time_to_peak_minutes(raw_snaps, discovered_at),
            time_decision_to_peak_min=time_to_peak_minutes(raw_snaps, decision_time),
            pre_decision_peak_return=pre_peak,
            has_pre_decision_data=has_pre,
            n_total_snaps=n_total_snaps,
            discovery_to_first_snap_min=latency_min,
        )
        recs.append(rec)

    return recs


def score_bucket(score):
    if score is None or pd.isna(score):
        return None
    if score < 4.5:
        return "4.0-4.49"
    if score < 5.0:
        return "4.5-4.99"
    return "5.0+"


def median_iqr(s):
    s = s.dropna()
    if len(s) == 0:
        return "n/a"
    return f"{s.median():.3g} (IQR {s.quantile(0.25):.3g} to {s.quantile(0.75):.3g})"


def mw(a, b):
    a = a.dropna()
    b = b.dropna()
    if len(a) < 3 or len(b) < 3:
        return len(a), len(b), None
    _, p = stats.mannwhitneyu(a, b, alternative="two-sided")
    return len(a), len(b), p


def fmt_p(p):
    return "n/a" if p is None else f"{p:.4f}"


def flag_low_n(n1, n2):
    return " **LOW CONFIDENCE (n<20)**" if min(n1, n2) < 20 else ""


async def main():
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        shadow_rows, obs_tokens, snaps_by_token = await load(conn)
    finally:
        await conn.close()

    print(f"Loaded {len(shadow_rows)} shadow_trades rows ({EXPERIMENT_VERSION}), "
          f"{len(obs_tokens)} OBSERVING-anchored tokens, "
          f"{sum(len(v) for v in snaps_by_token.values())} total snapshots.")

    # Missing-data accounting BEFORE build_records drops anything, per
    # the user's explicit "report missing-data coverage for every
    # comparison" requirement.
    shadow_total = len(shadow_rows)
    shadow_zero_snaps = sum(1 for r in shadow_rows if not snaps_by_token.get(r["token_id"]))
    obs_total = len(obs_tokens)
    obs_zero_snaps = sum(1 for t in obs_tokens if not snaps_by_token.get(t["id"]))

    recs = build_records(shadow_rows, obs_tokens, snaps_by_token)
    df = pd.DataFrame(recs)
    df["score_bucket"] = df["score"].apply(score_bucket)
    df["substantial_upside_flag"] = np.where(
        df["has_pre_decision_data"],
        df["pre_decision_peak_return"] >= SUBSTANTIAL_UPSIDE_THRESHOLD,
        None,
    )
    median_pre_peak_shadow = df.loc[(df["group"] == "shadow") & df["has_pre_decision_data"],
                                     "pre_decision_peak_return"].median()
    df["substantial_upside_flag_medsplit"] = np.where(
        df["has_pre_decision_data"],
        df["pre_decision_peak_return"] >= median_pre_peak_shadow,
        None,
    )

    shadow_df = df[df["group"] == "shadow"]
    obs_df = df[df["group"] == "observing"]

    obs_dropped_no_eval_or_postdata = obs_total - obs_zero_snaps - len(obs_df)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = OUTPUT_DIR / f"shadow_lifetime_followup_dataset_{ts}.csv"
    df.to_csv(csv_path, index=False)

    lines = []
    p = lines.append
    p(f"# Shadow Lifetime Follow-up — {ts}")
    p("")
    p("ANALYSIS-ONLY. Read-only, no trading logic touched. Re-anchors "
      "analysis/edge_research.py's shadow-vs-OBSERVING comparison to "
      "discovery time instead of decision time, using that script's own "
      "`compute_outcomes` and `OUTAGES` unchanged (imported directly, not "
      "reimplemented).")
    p("")
    p("## Methodology notes")
    p("")
    p(f"- Lifetime anchor = `tokens.discovered_at`. Decision anchor = "
      f"`shadow_trades.triggered_at` (shadow) / `tokens.observation_started_at` (OBSERVING).")
    p("- `max_drawdown` = min(price)/baseline - 1, i.e. baseline-to-trough, NOT peak-to-trough "
      "(inherited convention from edge_research.py's compute_outcomes - kept identical here).")
    p(f"- Substantial-upside threshold: pre-decision peak return >= {SUBSTANTIAL_UPSIDE_THRESHOLD:.0%} "
      "(a token that already moved meaningfully before the bot ever scored it). Also reported: a "
      f"median-split version at the shadow population's own median pre-decision peak return "
      f"({median_pre_peak_shadow:.1%}) as a robustness check against the 20% figure being an "
      "arbitrary pick.")
    p(f"- Outage-excluded (lifetime, full horizon): shadow n={shadow_df['lifetime_outage_excluded'].sum()}, "
      f"OBSERVING n={obs_df['lifetime_outage_excluded'].sum()}. "
      f"Outage-excluded (decision, full horizon): shadow n={shadow_df['decision_outage_excluded'].sum()}, "
      f"OBSERVING n={obs_df['decision_outage_excluded'].sum()}.")
    p("")
    p("## Missing-data coverage")
    p("")
    p("| group | n total | n zero snapshots | n dropped (no post-decision data) | n analyzed | "
      "n with pre-decision data | median discovery-to-first-snapshot latency |")
    p("|---|---|---|---|---|---|---|")
    p(f"| shadow | {shadow_total} | {shadow_zero_snaps} | "
      f"{shadow_total - shadow_zero_snaps - len(shadow_df)} | {len(shadow_df)} | "
      f"{int(shadow_df['has_pre_decision_data'].sum())} | "
      f"{median_iqr(shadow_df['discovery_to_first_snap_min'])} min |")
    p(f"| observing | {obs_total} | {obs_zero_snaps} | {obs_dropped_no_eval_or_postdata} | "
      f"{len(obs_df)} | {int(obs_df['has_pre_decision_data'].sum())} | "
      f"{median_iqr(obs_df['discovery_to_first_snap_min'])} min |")
    p("")
    p("Note: OBSERVING's \"n dropped\" here only reflects tokens with zero snapshots or zero "
      "post-decision snapshots (this script's own filters) - edge_research.py additionally drops "
      "OBSERVING tokens with no matching token_evaluations row before decision_time, which this "
      "script does not need (it doesn't use decision-time features here, only price).")
    p("")

    # ── THE key verdict ──────────────────────────────────────────────
    n_life, n_dec = len(shadow_df["lifetime_return_full"].dropna()), len(shadow_df["decision_return_full"].dropna())
    life_med = shadow_df["lifetime_return_full"].median()
    dec_med = shadow_df["decision_return_full"].median()
    p("## Does the -91.5%-style result survive when measured from discovery?")
    p("")
    p(f"**Shadow population, decision-anchored median return: {dec_med:.1%} (n={n_dec}).**")
    p(f"**Shadow population, lifetime-anchored median return: {life_med:.1%} (n={n_life}).**")
    if life_med >= -0.20:
        verdict = ("**Does NOT survive.** The severe negative median is mostly a decision-timing "
                   "artifact, not evidence the flagged tokens were bad from the start.")
    elif life_med <= dec_med - 0.10:
        verdict = "**Survives and is not fully explained by timing** - lifetime view is still deeply negative."
    else:
        verdict = ("**Survives, but weakened** - the lifetime view is less severe than the "
                   "decision-anchored view, consistent with part (not all) of the effect being a "
                   "timing artifact.")
    p(f"**Verdict: {verdict}**")
    p("")

    p("## Headline comparison: shadow vs OBSERVING control, both anchors")
    p("")
    p("| metric | shadow median (n) | observing median (n) | Mann-Whitney p |")
    p("|---|---|---|---|")
    for label, col in [
        ("lifetime_return_full", "lifetime_return_full"),
        ("decision_return_full", "decision_return_full"),
        ("lifetime max upside (peak_return)", "lifetime_peak_return_full"),
        ("lifetime max drawdown", "lifetime_max_drawdown_full"),
        ("time_discovery_to_decision_min", "time_discovery_to_decision_min"),
        ("time_discovery_to_peak_min", "time_discovery_to_peak_min"),
        ("time_decision_to_peak_min", "time_decision_to_peak_min"),
    ]:
        a, b = shadow_df[col], obs_df[col]
        na, nb, pval = mw(a, b)
        flag = flag_low_n(na, nb)
        a_med = a.median() if a.notna().any() else float("nan")
        b_med = b.median() if b.notna().any() else float("nan")
        fmt = (lambda x: f"{x:.1%}") if "return" in col or "drawdown" in col or "upside" in col else (lambda x: f"{x:.1f}")
        p(f"| {label} | {fmt(a_med)} (n={na}) | {fmt(b_med)} (n={nb}) | {fmt_p(pval)}{flag} |")
    p("")

    p("## Score-bucket stratification (shadow only)")
    p("")
    p("Note: `TIER2_WATCH_THRESHOLD=5.0` in production settings, so the 5.0+ bucket below overlaps "
      "with tokens that also crossed the live WATCH signal - context, not a caveat.")
    p("")
    p("| bucket | n | median lifetime_return_full | median decision_return_full | "
      "median lifetime_max_drawdown | median time_discovery_to_decision_min |")
    p("|---|---|---|---|---|---|")
    for bucket in ["4.0-4.49", "4.5-4.99", "5.0+"]:
        sub = shadow_df[shadow_df["score_bucket"] == bucket]
        if len(sub) == 0:
            p(f"| {bucket} | 0 | n/a | n/a | n/a | n/a |")
            continue
        flag = " **LOW CONFIDENCE (n<20)**" if len(sub) < 20 else ""
        p(f"| {bucket} | {len(sub)}{flag} | {sub['lifetime_return_full'].median():.1%} | "
          f"{sub['decision_return_full'].median():.1%} | "
          f"{sub['lifetime_max_drawdown_full'].median():.1%} | "
          f"{sub['time_discovery_to_decision_min'].median():.1f} |")
    p("")
    concentrated = shadow_df.groupby("score_bucket")["decision_return_full"].median()
    worst_bucket = concentrated.idxmin() if len(concentrated.dropna()) else "n/a"
    p(f"The most negative decision-anchored median sits in the **{worst_bucket}** bucket "
      f"({concentrated.min():.1%} if applicable) - "
      f"{'the effect is concentrated there, not spread evenly' if concentrated.max() - concentrated.min() > 0.30 else 'the effect looks broadly spread across buckets, not concentrated in one'}.")
    p("")

    p("## Substantial-upside split (shadow, tokens with pre-decision data only)")
    p("")
    for label, col in [
        (f"20% threshold", "substantial_upside_flag"),
        (f"median split (>= {median_pre_peak_shadow:.1%})", "substantial_upside_flag_medsplit"),
    ]:
        sub = shadow_df[shadow_df["has_pre_decision_data"]]
        already = sub[sub[col] == True]
        not_yet = sub[sub[col] == False]
        p(f"### {label}")
        p("")
        p("| segment | n | median lifetime_return_full | median decision_return_full |")
        p("|---|---|---|---|")
        for name, seg in [("already pumped pre-decision", already), ("had not yet pumped", not_yet)]:
            flag = " **LOW CONFIDENCE (n<20)**" if len(seg) < 20 else ""
            lr = seg["lifetime_return_full"].median() if len(seg) else float("nan")
            dr = seg["decision_return_full"].median() if len(seg) else float("nan")
            p(f"| {name} | {len(seg)}{flag} | {lr:.1%} | {dr:.1%} |")
        p("")
    already20 = shadow_df[(shadow_df["has_pre_decision_data"]) & (shadow_df["substantial_upside_flag"] == True)]
    notyet20 = shadow_df[(shadow_df["has_pre_decision_data"]) & (shadow_df["substantial_upside_flag"] == False)]
    if len(already20) >= 3 and len(notyet20) >= 3:
        life_gap = already20["lifetime_return_full"].median() - notyet20["lifetime_return_full"].median()
        dec_gap = already20["decision_return_full"].median() - notyet20["decision_return_full"].median()
        if already20["decision_return_full"].median() < -0.3 and already20["lifetime_return_full"].median() > -0.1:
            diag = ("This CONFIRMS the \"bought the top\" explanation for the already-pumped subset: "
                    "their lifetime return is fine but their decision-anchored return is bad, meaning "
                    "the bad decision-anchored number reflects buying after the move, not a bad token.")
        elif already20["lifetime_return_full"].median() < -0.3 and notyet20["lifetime_return_full"].median() < -0.3:
            diag = ("Both subsets show poor LIFETIME returns too - this points to a more fundamental "
                    "issue with what the scorer flags, not purely a timing/entry-point artifact.")
        else:
            diag = "Mixed pattern - see the numbers above directly rather than a one-line characterization."
        p(f"**Diagnostic: {diag}**")
        p("")

    p("## Caveats")
    p("")
    p(f"- Shadow n analyzed = {len(shadow_df)}, OBSERVING n analyzed = {len(obs_df)}. Several "
      "sub-comparisons above have one or both sides under n=20 - flagged inline.")
    p("- This is descriptive, not out-of-sample validated (unlike edge_research.py's shortlist). "
      "Treat this as a direct diagnostic of the -91.5%-style finding, not a new predictive claim.")
    p("- No trading logic, thresholds, or scorer weights were touched. Nothing implemented.")

    report_path = OUTPUT_DIR / f"shadow_lifetime_followup_{ts}.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")
    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
