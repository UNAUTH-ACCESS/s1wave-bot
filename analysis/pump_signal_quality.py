"""
analysis/pump_signal_quality.py
================================
ANALYSIS-ONLY. Read-only against the live database (SELECT only). Never
imports or touches scorer_worker.py, settings.py, risk.py,
sampling_worker.py, discovery_worker.py, or any other production path.

Purpose
-------
analysis/pump_timing_research.py found a causal, no-lookahead signal
(trailing_return_3min > 0.053, plus 3 co-validated features) that reliably
precedes a token's peak by a median 8 minutes, firing for ~96% of "real
pump" tokens. That answers WHEN a pump is starting. This script asks a
harder, different question: once that signal fires, can anything known AT
THAT MOMENT (or shortly after, as an explicit delayed-confirmation check)
tell you whether THIS SPECIFIC pump is going to be a winner (holds the
gain) versus a failed pump (classic pump-and-dump, gives it all back)?

Reuse discipline
-----------------
This script imports directly from analysis.pump_timing_research rather
than reimplementing: load(), reconstruct_token(), nearest_at_or_before(),
nearest_at_or_after(), minutes_between(), window_overlaps_outage(),
median_iqr(), OUTAGES, _num(). It also reuses that script's exact
walk-forward per-snapshot feature computation for
trailing_return_3min/5min/buy_pressure/volume_mult (copied verbatim from
the block in its main() that computes `fired_at` for the best rule - same
formulas, same causal-only construction) so signal_time is identical to
what that script would find, not a redefinition.

Taxonomy (winner/failed_pump/rug/flat_no_event) is loaded directly from
the already-produced analysis/output/pump_timing_research_dataset_*.csv
(most recent one is auto-discovered) and joined by token_id - NOT
recomputed here, per the parent's explicit instruction.

No-lookahead discipline
-------------------------
Step 2 ("at-signal" features) uses only data at or before primary_signal_time.
Step 3 ("delayed-confirmation") uses data strictly AFTER primary_signal_time
by construction, and is labeled as such everywhere it appears in the
report - it is a "wait N minutes before committing" strategy check, not a
zero-delay signal, and must never be confused with the Step 2 features.
The is_winner LABEL depends only on the token's eventual outcome (already
computed, non-leaking, in the source taxonomy) - never fed back into any
feature. Train/test split is chronological by primary_signal_time.

Usage
-----
    PYTHONPATH=. /tmp/s1wave-venv/bin/python analysis/pump_signal_quality.py

Outputs
-------
    analysis/output/pump_signal_quality_<timestamp>.md
    analysis/output/pump_signal_quality_dataset_<timestamp>.csv
"""

from __future__ import annotations

import sys
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg
from config.settings import settings
from analysis.pump_timing_research import (
    load, reconstruct_token, nearest_at_or_before, nearest_at_or_after,
    minutes_between, window_overlaps_outage, median_iqr, OUTAGES, _num,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

VALIDATED_RULES = [
    ("trailing_return_3min", 0.053),
    ("trailing_return_5min", 0.116),
    ("buy_pressure", 0.667),
    ("volume_mult", 1.004),
]
PRIMARY_FEATURE = "trailing_return_3min"


def fmt_min(x):
    return "n/a" if pd.isna(x) else f"{x:.1f}"


def fmt_pct(x, digits=1):
    return "n/a" if pd.isna(x) or x is None else f"{x*100:+.{digits}f}%"


def fmt_p(x):
    return "n/a" if pd.isna(x) else f"{x:.4f}"


def feature_value_at(feat, i, series):
    """Causal-only: series[:i] is everything at or before series[i]'s time.
    Identical formulas to pump_timing_research.py's fired_at loop."""
    t, price, liq, vol, bp, vm = series[i]
    if feat == "buy_pressure":
        return bp
    if feat == "volume_mult":
        return vm
    if feat == "trailing_return_3min":
        back3 = nearest_at_or_before(series[:i], t - timedelta(minutes=3))
        return (price / back3[1] - 1) if back3 and back3[1] and back3[1] > 0 else None
    if feat == "trailing_return_5min":
        back5 = nearest_at_or_before(series[:i], t - timedelta(minutes=5))
        return (price / back5[1] - 1) if back5 and back5[1] and back5[1] > 0 else None
    raise ValueError(feat)


def find_signal_time(feat, split_v, series):
    for i in range(2, len(series)):
        t, price, liq, vol, bp, vm = series[i]
        if price is None or price <= 0:
            continue
        val = feature_value_at(feat, i, series)
        if val is not None and val > split_v:
            return t
    return None


def decile_search_validate(df, feat, target_col, split_time_col="primary_signal_time"):
    """Same methodology as pump_timing_research.py's leading-indicator search,
    applied here to is_winner instead of big_move_5min_010."""
    sub = df[[feat, target_col, split_time_col]].dropna()
    if len(sub) < 20:
        return None, dict(feature=feat, reason=f"too few rows overall (n={len(sub)})")
    sub = sub.sort_values(split_time_col)
    split_idx = len(sub) // 2
    split_time = sub.iloc[split_idx][split_time_col]
    train = sub[sub[split_time_col] < split_time]
    test = sub[sub[split_time_col] >= split_time]
    if len(train) < 20 or len(test) < 10:
        return None, dict(feature=feat, reason=f"train/test too small (train={len(train)}, test={len(test)})")

    vals = train[feat].astype(float)
    best = None
    for q in np.arange(0.1, 1.0, 0.1):
        split_v = vals.quantile(q)
        above = train[vals > split_v][target_col].astype(float)
        below = train[vals <= split_v][target_col].astype(float)
        if len(above) < 8 or len(below) < 8:
            continue
        try:
            _, p = stats.mannwhitneyu(above, below, alternative="two-sided")
        except ValueError:
            continue
        effect = above.mean() - below.mean()
        cand = dict(feature=feat, split_quantile=round(q, 1), split_value=split_v,
                    n_above=len(above), n_below=len(below),
                    rate_above=above.mean(), rate_below=below.mean(),
                    effect=effect, p_value=p)
        if best is None or abs(effect) > abs(best["effect"]):
            best = cand
    if best is None:
        return None, dict(feature=feat, reason="no valid split found in train (n too small per side)")

    tvals = test[feat].astype(float)
    t_above = test[tvals > best["split_value"]][target_col].astype(float)
    t_below = test[tvals <= best["split_value"]][target_col].astype(float)
    if len(t_above) < 5 or len(t_below) < 5:
        return None, {**best, "reason": f"test-side n too small ({len(t_above)}/{len(t_below)})"}

    test_effect = t_above.mean() - t_below.mean()
    train_dir = np.sign(best["effect"])
    test_dir = np.sign(test_effect)
    survives = (train_dir == test_dir and train_dir > 0)
    result = dict(**best, test_n_above=len(t_above), test_n_below=len(t_below),
                  test_rate_above=t_above.mean(), test_rate_below=t_below.mean(),
                  test_effect=test_effect, survives=survives)
    return (result if survives else None), (None if survives else result)


async def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    csv_candidates = sorted(OUTPUT_DIR.glob("pump_timing_research_dataset_*.csv"))
    if not csv_candidates:
        print("ERROR: no pump_timing_research_dataset_*.csv found in analysis/output/.")
        return
    src_csv = csv_candidates[-1]
    print(f"Using taxonomy source: {src_csv.name}")
    tax_df = pd.read_csv(src_csv, parse_dates=["discovered_at", "pump_start_time", "peak_time", "first_time"])
    tax_df["token_id"] = tax_df["token_id"].astype(str)
    real_pump_tax = tax_df[tax_df["taxonomy"].isin(["winner", "failed_pump"])].copy()
    print(f"real-pump tokens from source CSV: {len(real_pump_tax)} "
          f"(winner={int((real_pump_tax.taxonomy=='winner').sum())}, "
          f"failed_pump={int((real_pump_tax.taxonomy=='failed_pump').sum())})")

    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        tokens, snaps_by_token, scorer_by_token, shadow_by_token = await load(conn)
    finally:
        await conn.close()
    snaps_str = {str(k): v for k, v in snaps_by_token.items()}

    # ── Step 1: signal_time per rule ──
    rule_fire_counts = {feat: 0 for feat, _ in VALIDATED_RULES}
    rows = []
    for _, trow in real_pump_tax.iterrows():
        tid = trow["token_id"]
        series = snaps_str.get(tid)
        if series is None or len(series) < 5:
            continue
        signal_times = {}
        for feat, split_v in VALIDATED_RULES:
            st = find_signal_time(feat, split_v, series)
            signal_times[feat] = st
            if st is not None:
                rule_fire_counts[feat] += 1
        rows.append(dict(token_id=tid, symbol=trow["symbol"], taxonomy=trow["taxonomy"],
                          discovered_at=trow["discovered_at"], pump_start_time=trow["pump_start_time"],
                          pump_start_price=trow["pump_start_price"], peak_time=trow["peak_time"],
                          **{f"signal_time__{feat}": signal_times[feat] for feat, _ in VALIDATED_RULES}))

    n_pop = len(rows)
    df = pd.DataFrame(rows)
    df["primary_signal_time"] = df[f"signal_time__{PRIMARY_FEATURE}"]
    n_has_primary = int(df["primary_signal_time"].notna().sum())
    print(f"population with series: {n_pop}; primary signal ({PRIMARY_FEATURE}) fired for {n_has_primary}")

    # ── Step 2 & 3: at-signal + delayed-confirmation features ──
    feat_rows = []
    for _, row in df.iterrows():
        tid = row["token_id"]
        sig_t = row["primary_signal_time"]
        rec = dict(token_id=tid, symbol=row["symbol"], taxonomy=row["taxonomy"],
                   primary_signal_time=sig_t)
        if pd.isna(sig_t):
            feat_rows.append(rec)
            continue
        series = snaps_str.get(tid)
        at_or_before = nearest_at_or_before(series, sig_t)
        if at_or_before is None:
            feat_rows.append(rec)
            continue
        _, sig_price, _, _, sig_bp, sig_vm = at_or_before
        pump_start_price = row["pump_start_price"]
        rec["signal_price_over_pump_start"] = (sig_price / pump_start_price
                                                if pump_start_price and pump_start_price > 0 else np.nan)
        rec["minutes_since_pump_start_at_signal"] = minutes_between(row["pump_start_time"], sig_t)
        rec["minutes_since_discovery_at_signal"] = minutes_between(row["discovered_at"], sig_t)
        rec["buy_pressure_at_signal"] = sig_bp
        rec["volume_mult_at_signal"] = sig_vm
        back5 = nearest_at_or_before(series, sig_t - timedelta(minutes=5))
        rec["trailing_return_5min_at_signal"] = (
            sig_price / back5[1] - 1 if back5 and back5[1] and back5[1] > 0 else np.nan
        )
        # confluence: how many of the OTHER 3 rules are also true at sig_t
        n_cofire = 0
        for feat, split_v in VALIDATED_RULES:
            if feat == PRIMARY_FEATURE:
                continue
            val = None
            if feat == "buy_pressure":
                val = sig_bp
            elif feat == "volume_mult":
                val = sig_vm
            elif feat == "trailing_return_5min":
                val = rec["trailing_return_5min_at_signal"]
            if val is not None and not pd.isna(val) and val > split_v:
                n_cofire += 1
        rec["n_rules_cofiring_at_signal"] = n_cofire

        # Step 3: delayed confirmation - strictly AFTER signal, causal for a
        # "wait 3 min then decide" strategy, never used as a zero-delay feature.
        after3 = nearest_at_or_after(series, sig_t + timedelta(minutes=3))
        if after3 is not None and after3[0] > sig_t and sig_price and sig_price > 0:
            rec["confirmation_return_3min"] = after3[1] / sig_price - 1
        else:
            rec["confirmation_return_3min"] = np.nan
        feat_rows.append(rec)

    fdf = pd.DataFrame(feat_rows)
    fdf["is_winner"] = (fdf["taxonomy"] == "winner").astype(int)
    with_signal = fdf[fdf["primary_signal_time"].notna()].copy()
    n_missing_confirmation = int(with_signal["confirmation_return_3min"].isna().sum())

    baseline_n = len(with_signal)
    baseline_winner_rate = with_signal["is_winner"].mean() if baseline_n else np.nan

    # ── Step 4: validate each candidate feature against is_winner ──
    candidate_features = [
        "signal_price_over_pump_start", "minutes_since_pump_start_at_signal",
        "buy_pressure_at_signal", "volume_mult_at_signal",
        "trailing_return_5min_at_signal", "n_rules_cofiring_at_signal",
        "confirmation_return_3min",
    ]
    survived, unvalidated = [], []
    for feat in candidate_features:
        surv, unval = decile_search_validate(with_signal, feat, "is_winner")
        if surv:
            survived.append(surv)
        if unval:
            unvalidated.append(unval)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # split flag for CSV export (chronological, by primary_signal_time)
    sorted_ws = with_signal.dropna(subset=["primary_signal_time"]).sort_values("primary_signal_time")
    split_idx = len(sorted_ws) // 2
    split_time = sorted_ws.iloc[split_idx]["primary_signal_time"] if len(sorted_ws) else None
    if split_time is not None:
        fdf["split"] = np.where(
            fdf["primary_signal_time"].notna(),
            np.where(fdf["primary_signal_time"] < split_time, "train", "test"),
            "n/a",
        )
    else:
        fdf["split"] = "n/a"

    csv_path = OUTPUT_DIR / f"pump_signal_quality_dataset_{ts}.csv"
    fdf.to_csv(csv_path, index=False)

    # ── report ──
    lines = []
    p = lines.append
    p(f"# Pump Signal Quality — {ts}")
    p("")
    p("ANALYSIS-ONLY. Read-only, no trading logic touched.")
    p("")
    p("## Methodology / reuse")
    p("")
    p(f"Taxonomy (winner/failed_pump/rug/flat_no_event) reused AS-IS from `{src_csv.name}`, joined by "
      "token_id - not recomputed. The 4 already-validated leading-indicator rules and their exact "
      "causal feature formulas are reused from `analysis/pump_timing_research.py` "
      "(`trailing_return_3min > 0.053`, `trailing_return_5min > 0.116`, `buy_pressure > 0.667`, "
      "`volume_mult > 1.004`). `trailing_return_3min` is used as `primary_signal_time` (the one the "
      "prior report anchored its 8-minute lead-time finding to).")
    p("")
    p("Step 2 features are computed using ONLY data at or before `primary_signal_time` (zero-delay, "
      "known at the instant the signal fires). Step 3's `confirmation_return_3min` uses data strictly "
      "AFTER `primary_signal_time` - it represents a \"wait 3 minutes before committing\" strategy, "
      "explicitly NOT a zero-delay feature, and is reported separately so it is never confused with "
      "the zero-delay features. `is_winner` label comes only from the pre-computed taxonomy (based on "
      "the token's full eventual lifetime), never fed back into any feature.")
    p("")
    p("## Population and signal-firing coverage")
    p("")
    p(f"Real-pump tokens (winner+failed_pump) from source CSV: n={len(real_pump_tax)}. "
      f"Of these, {n_pop} had a usable snapshot series in this run "
      f"(dataset may have grown/shrunk slightly since the source CSV's snapshot query time).")
    p("")
    p("| rule | n fired (of {}) | miss rate |".format(n_pop))
    p("|---|---|---|")
    for feat, split_v in VALIDATED_RULES:
        n_f = rule_fire_counts[feat]
        miss = 1 - (n_f / n_pop if n_pop else np.nan)
        p(f"| {feat} > {split_v} | {n_f} | {miss*100:.1f}% |")
    p("")
    p(f"Primary signal (`{PRIMARY_FEATURE}`) fired for {n_has_primary} of {n_pop} tokens "
      f"({(1 - n_has_primary/n_pop)*100:.1f}% miss rate if n_pop>0). "
      f"All Step 2-4 analysis below is restricted to these {baseline_n} tokens with a primary signal.")
    p(f"- {n_missing_confirmation} of {baseline_n} tokens had no snapshot ~3 min after the signal "
      "(e.g. signal fired very close to the token's last observed point) - `confirmation_return_3min` "
      "is null for these, not imputed.")
    p("")
    p(f"**Baseline winner rate among all signaled real-pump tokens: {baseline_winner_rate*100:.1f}% "
      f"(n={baseline_n}).**")
    p("")
    p("## At-signal and delayed-confirmation features vs is_winner (time-split validated)")
    p("")
    p("### Survived (same direction, train vs test)")
    p("")
    if survived:
        p("| feature | split_value | train n(above/below) | train winner-rate(above/below) | "
          "test n(above/below) | test winner-rate(above/below) | train p |")
        p("|---|---|---|---|---|---|---|")
        for r in survived:
            p(f"| {r['feature']} | {r['split_value']:.4g} | {r['n_above']}/{r['n_below']} | "
              f"{r['rate_above']:.3f}/{r['rate_below']:.3f} | {r['test_n_above']}/{r['test_n_below']} | "
              f"{r['test_rate_above']:.3f}/{r['test_rate_below']:.3f} | {fmt_p(r['p_value'])} |")
    else:
        p("**None.** No candidate feature - at-signal or delayed-confirmation - held its direction "
          "from train to test on predicting winner-vs-failed-pump. This is a null result and is "
          "reported as such: the causal information available at (or shortly after) pump-start-signal "
          "time tells you a pump IS starting, but not reliably whether THIS pump will hold.")
    p("")
    p("### Did NOT survive / could not be tested (reported for completeness, not hidden)")
    p("")
    if unvalidated:
        p("| feature | reason / train-vs-test |")
        p("|---|---|")
        for r in unvalidated:
            if "reason" in r and "n_above" not in r:
                p(f"| {r['feature']} | {r['reason']} |")
            elif "reason" in r:
                p(f"| {r['feature']} | split={r['split_value']:.4g}, train {r['n_above']}/{r['n_below']} "
                  f"({r['rate_above']:.3f}/{r['rate_below']:.3f}) - {r['reason']} |")
            else:
                p(f"| {r['feature']} | split={r['split_value']:.4g}, train rate "
                  f"{r['rate_above']:.3f}/{r['rate_below']:.3f} (effect {r['effect']:+.3f}), "
                  f"test rate {r['test_rate_above']:.3f}/{r['test_rate_below']:.3f} "
                  f"(effect {r['test_effect']:+.3f}) - reversed in test |")
    else:
        p("(none - every candidate feature either survived or had no data)")
    p("")
    p("## Confluence: does the NUMBER of rules co-firing at signal time matter?")
    p("")
    conf_row = next((r for r in survived if r["feature"] == "n_rules_cofiring_at_signal"), None)
    conf_unval = next((r for r in unvalidated if r.get("feature") == "n_rules_cofiring_at_signal"), None)
    if conf_row:
        p(f"**Survived.** More rules co-firing at the moment `trailing_return_3min` triggers is "
          f"associated with a higher winner rate: above {conf_row['split_value']:.4g} co-firing rules, "
          f"winner rate = {conf_row['rate_above']:.1%} (train, n={conf_row['n_above']}) / "
          f"{conf_row['test_rate_above']:.1%} (test, n={conf_row['test_n_above']}) vs "
          f"{conf_row['rate_below']:.1%} (train, n={conf_row['n_below']}) / "
          f"{conf_row['test_rate_below']:.1%} (test, n={conf_row['test_n_below']}) at/below it.")
    elif conf_unval:
        p("Tested, did not survive time-split validation - see table above (reason: "
          f"{conf_unval.get('reason', 'reversed in test')}).")
    else:
        p("Not enough data to test.")
    p("")
    breakdown = with_signal.groupby("n_rules_cofiring_at_signal")["is_winner"].agg(["mean", "count"])
    if len(breakdown):
        p("Raw (non-split) breakdown for reference:")
        p("")
        p("| n_rules_cofiring | winner rate | n |")
        p("|---|---|---|")
        for idx, r in breakdown.iterrows():
            p(f"| {int(idx)} | {r['mean']*100:.1f}% | {int(r['count'])} |")
    p("")
    p("## Headline")
    p("")
    if survived:
        best = max(survived, key=lambda r: abs(r["test_effect"]))
        p(f"**{len(survived)} feature(s) survived time-split validation.** Best by test-side effect size: "
          f"`{best['feature']} > {best['split_value']:.4g}` - winner rate rises from baseline "
          f"{baseline_winner_rate*100:.1f}% (n={baseline_n}) to {best['rate_above']*100:.1f}% "
          f"(train, n={best['n_above']}) / {best['test_rate_above']*100:.1f}% (test, n={best['test_n_above']}) "
          f"when this holds at/after signal time.")
    else:
        p("**Null result, reported plainly.** Nothing tested here - zero-delay at-signal features, "
          "the delayed 3-minute confirmation check, or the rule-confluence count - reliably "
          "distinguished a future winner from a future failed pump once the causal entry signal fires. "
          "This is itself a useful, real finding: the trailing_return_3min-style signal tells you WHEN "
          "a pump is starting (validated, ~8min median lead time, ~96% coverage per "
          "`pump_timing_research.py`), but the same causal information does not additionally tell you, "
          "at that same moment, whether this particular pump is one worth holding through to a real "
          "win versus one that will dump right back down. A trading strategy built only on this entry "
          "signal would need a SEPARATE exit rule (e.g. a trailing stop) to manage the failed-pump case, "
          "rather than relying on the entry signal itself to filter them out.")
    p("")
    p("## Caveats")
    p("")
    p(f"- Population: {n_pop} real-pump tokens had usable snapshot data; primary signal fired for "
      f"{n_has_primary} ({baseline_n} used in Step 4). Miss-rate for each of the 4 validated rules is "
      "reported above, not hidden.")
    p(f"- {n_missing_confirmation} tokens lack a delayed-confirmation snapshot (~3 min after signal) - "
      "null, not imputed.")
    p("- No-lookahead confirmed: Step 2 features use only data at/before primary_signal_time; Step 3's "
      "confirmation feature uses data strictly after it and is labeled as a delayed strategy, never "
      "conflated with a zero-delay feature; is_winner is the token's already-computed eventual outcome, "
      "never a feature.")
    p("- Train/test split is chronological by primary_signal_time, same discipline as prior scripts "
      "this session.")
    p("- This is descriptive/exploratory pattern-finding, not a proposed rule change. No trading logic, "
      "thresholds, or scorer weights were touched.")

    report_path = OUTPUT_DIR / f"pump_signal_quality_{ts}.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")
    print(f"Wrote {csv_path}")
    print(f"survived={len(survived)} unvalidated={len(unvalidated)}")
    if survived:
        for r in survived:
            print(f"  SURVIVED: {r['feature']} split={r['split_value']:.4g} "
                  f"train {r['rate_above']:.3f}/{r['rate_below']:.3f} "
                  f"test {r['test_rate_above']:.3f}/{r['test_rate_below']:.3f}")


if __name__ == "__main__":
    asyncio.run(main())
