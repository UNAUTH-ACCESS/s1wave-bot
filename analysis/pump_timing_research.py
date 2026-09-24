"""
analysis/pump_timing_research.py
=================================
ANALYSIS-ONLY. Read-only against the live database (SELECT only). Never
imports or touches scorer_worker.py, settings.py, risk.py,
sampling_worker.py, discovery_worker.py, or any other production path.

Purpose
-------
Earlier scripts this session (edge_research.py, shadow_lifetime_followup.py)
asked "does the current scorer/threshold predict outcome" and found the
score>=4.0 shadow population has a -91.5% median LIFETIME return (n=160),
confirmed not a timing artifact. This script asks a different, more basic
question, independent of whether the current scorer is any good: for
tokens that DO have a real pump, when does it start, when does it peak,
what does the run-up look like, and does ANY causal (no-lookahead)
snapshot-level signal give real lead time before the peak. Full
population (all statuses), not just shadow/OBSERVING.

Data-availability caveat (read before trusting the buy/sell section)
----------------------------------------------------------------------
token_snapshots does NOT carry raw buy/sell transaction counts over time.
Those exist only ONCE per token, at discovery, in token_evaluations /
shadow_trades inputs_json (buys_at_discovery etc.) - there is no txn-count
time series to analyze. "Buy-sell / transaction behavior before the pump"
below is measured via buy_pressure (0-1 ratio) and volume_usd/volume_mult
trends across the snapshot series - the only per-snapshot proxies that
actually exist. Wash-trading data likewise only exists as a single
decision-time value (wash_multiplier_at_discovery), not a time series.

Outage handling
----------------
Reuses the exact, already-corrected OUTAGES list from edge_research.py
(two CLOSED intervals - do not use the stale open-ended version still
present in shadow_backtest.py / clean_recap.py).

No-lookahead discipline
-------------------------
Section "leading-indicator search" (Step 6) computes every FEATURE using
only snapshots at or before time t. Outcome labels (forward_return_5min,
forward_return_15min, big_move_5min) use only snapshots strictly after t.
Features and labels are never mixed. The train/test split is chronological
by snapshot time t, not by token, so no future-in-time row is used to
validate a rule applied to an earlier one.

Usage
-----
    PYTHONPATH=. /tmp/s1wave-venv/bin/python analysis/pump_timing_research.py

Outputs
-------
    analysis/output/pump_timing_research_<timestamp>.md
    analysis/output/pump_timing_research_dataset_<timestamp>.csv
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg
from config.settings import settings

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

# Reused verbatim from analysis/edge_research.py - see that file's own
# docstring for why these two intervals (both now closed) are correct.
OUTAGES = [
    (datetime(2026, 9, 20, 12, 47, 5, tzinfo=timezone.utc),
     datetime(2026, 9, 20, 17, 37, 13, tzinfo=timezone.utc)),
    (datetime(2026, 9, 20, 21, 57, 7, tzinfo=timezone.utc),
     datetime(2026, 9, 21, 7, 17, 51, tzinfo=timezone.utc)),
]


def window_overlaps_outage(start, end) -> bool:
    return any(start < o_end and end > o_start for o_start, o_end in OUTAGES)


def _num(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


TOKENS_SQL = """
SELECT id, symbol, discovered_at, status, rejection_reason, observation_started_at
FROM tokens
ORDER BY discovered_at ASC;
"""

SNAPSHOTS_SQL = """
SELECT token_id, sampled_at, price_usd, liquidity_usd, volume_usd, buy_pressure, volume_mult
FROM token_snapshots
ORDER BY token_id, sampled_at ASC;
"""

SCORER_EVALS_SQL = """
SELECT token_id, min(evaluated_at) AS decision_time
FROM token_evaluations
WHERE gate = 'SCORER'
GROUP BY token_id;
"""

SHADOW_SQL = """
SELECT token_id, min(triggered_at) AS shadow_time, max(score) AS max_score
FROM shadow_trades
WHERE experiment_version = 'scorer_v2_threshold_4'
GROUP BY token_id;
"""


async def load(conn):
    tokens = await conn.fetch(TOKENS_SQL)
    snaps = await conn.fetch(SNAPSHOTS_SQL)
    scorer = await conn.fetch(SCORER_EVALS_SQL)
    shadow = await conn.fetch(SHADOW_SQL)

    snaps_by_token = {}
    for r in snaps:
        snaps_by_token.setdefault(r["token_id"], []).append(
            (r["sampled_at"], _num(r["price_usd"]), _num(r["liquidity_usd"]),
             _num(r["volume_usd"]), _num(r["buy_pressure"]), _num(r["volume_mult"]))
        )
    scorer_by_token = {r["token_id"]: r["decision_time"] for r in scorer}
    shadow_by_token = {r["token_id"]: (r["shadow_time"], _num(r["max_score"])) for r in shadow}
    return tokens, snaps_by_token, scorer_by_token, shadow_by_token


def nearest_at_or_before(series, t):
    """series: list of (ts, price, ...) sorted asc. Return the row with the
    largest ts <= t, or None."""
    best = None
    for row in series:
        if row[0] <= t:
            best = row
        else:
            break
    return best


def nearest_at_or_after(series, t):
    for row in series:
        if row[0] >= t:
            return row
    return None


def minutes_between(a, b):
    return (b - a).total_seconds() / 60.0


def reconstruct_token(token_id, symbol, discovered_at, series, scorer_time, shadow_info):
    """series: list of (ts, price, liq, vol, bp, vm) sorted asc, len>=1."""
    n = len(series)
    prices = np.array([s[1] for s in series if s[1] is not None and s[1] > 0])
    if len(prices) < 2:
        return None

    times = [s[0] for s in series if s[1] is not None and s[1] > 0]
    first_time, first_price = times[0], prices[0]
    peak_idx = int(np.argmax(prices))
    peak_time, peak_price = times[peak_idx], prices[peak_idx]

    # pump_start = global min within [first, peak]
    pre_peak_prices = prices[: peak_idx + 1]
    pre_peak_times = times[: peak_idx + 1]
    ps_idx = int(np.argmin(pre_peak_prices))
    pump_start_time, pump_start_price = pre_peak_times[ps_idx], pre_peak_prices[ps_idx]

    pump_magnitude = (peak_price / pump_start_price - 1) if pump_start_price > 0 else np.nan
    naive_pump = (peak_price / first_price - 1) if first_price > 0 else np.nan

    discovery_to_pump_start_min = minutes_between(discovered_at, pump_start_time)
    discovery_to_peak_min = minutes_between(discovered_at, peak_time)
    pump_start_to_peak_min = minutes_between(pump_start_time, peak_time)

    # confirmed reversal: first post-peak snapshot with price <= 0.85*peak
    time_to_confirmed_reversal_min = np.nan
    for t, p in zip(times[peak_idx:], prices[peak_idx:]):
        if p <= 0.85 * peak_price:
            time_to_confirmed_reversal_min = minutes_between(peak_time, t)
            break

    final_price = prices[-1]
    final_lifetime_return = (final_price / first_price - 1) if first_price > 0 else np.nan

    if peak_price > first_price:
        retained_fraction_of_peak_gain = (final_price - first_price) / (peak_price - first_price)
    else:
        retained_fraction_of_peak_gain = np.nan

    # taxonomy
    if naive_pump >= 0.30:
        if retained_fraction_of_peak_gain is not None and not np.isnan(retained_fraction_of_peak_gain) \
           and retained_fraction_of_peak_gain >= 0.20:
            taxonomy = "winner"
        else:
            taxonomy = "failed_pump"
    else:
        if final_lifetime_return <= -0.50:
            taxonomy = "rug"
        else:
            taxonomy = "flat_no_event"

    # pump_fraction_before_decision
    pump_fraction_before_decision = np.nan
    has_scorer = scorer_time is not None
    if has_scorer and pump_start_price > 0 and peak_price > pump_start_price:
        if scorer_time <= pump_start_time:
            pump_fraction_before_decision = 0.0
        else:
            row = nearest_at_or_before(list(zip(times, prices)), scorer_time)
            if row is not None:
                price_at_decision = row[1]
                frac = (price_at_decision - pump_start_price) / (peak_price - pump_start_price)
                pump_fraction_before_decision = float(np.clip(frac, 0.0, 1.0))

    outage_excluded_full = window_overlaps_outage(discovered_at, times[-1])

    return dict(
        token_id=str(token_id), symbol=symbol, discovered_at=discovered_at,
        n_snapshots=n, first_time=first_time, first_price=first_price,
        peak_time=peak_time, peak_price=peak_price,
        pump_start_time=pump_start_time, pump_start_price=pump_start_price,
        pump_magnitude=pump_magnitude, naive_pump=naive_pump,
        discovery_to_pump_start_min=discovery_to_pump_start_min,
        discovery_to_peak_min=discovery_to_peak_min,
        pump_start_to_peak_min=pump_start_to_peak_min,
        time_to_confirmed_reversal_min=time_to_confirmed_reversal_min,
        held_through_end=bool(np.isnan(time_to_confirmed_reversal_min)),
        final_price=final_price, final_lifetime_return=final_lifetime_return,
        retained_fraction_of_peak_gain=retained_fraction_of_peak_gain,
        taxonomy=taxonomy,
        has_scorer_decision=has_scorer,
        scorer_time=scorer_time,
        pump_fraction_before_decision=pump_fraction_before_decision,
        has_shadow=shadow_info is not None,
        shadow_time=shadow_info[0] if shadow_info else None,
        max_score=shadow_info[1] if shadow_info else None,
        outage_excluded_full=outage_excluded_full,
        last_time=times[-1],
    )


def median_iqr(s):
    s = pd.Series(s).dropna()
    if len(s) == 0:
        return (np.nan, np.nan, np.nan, 0)
    return (s.median(), s.quantile(0.25), s.quantile(0.75), len(s))


def fmt_min(x):
    return "n/a" if pd.isna(x) else f"{x:.1f}"


def fmt_pct(x, digits=1):
    return "n/a" if pd.isna(x) else f"{x*100:+.{digits}f}%"


def fmt_p(x):
    return "n/a" if pd.isna(x) else f"{x:.4f}"


async def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        tokens, snaps_by_token, scorer_by_token, shadow_by_token = await load(conn)
    finally:
        await conn.close()

    print(f"tokens={len(tokens)} snapshot-having-tokens={len(snaps_by_token)}")
    global _STR_KEY_MAP
    _STR_KEY_MAP = {str(k): v for k, v in snaps_by_token.items()}

    counts_by_thresh = {5: 0, 10: 0, 20: 0}
    for tid, series in snaps_by_token.items():
        n = len(series)
        for th in counts_by_thresh:
            if n >= th:
                counts_by_thresh[th] += 1

    records = []
    for t in tokens:
        tid = t["id"]
        series = snaps_by_token.get(tid)
        if not series or len(series) < 10:
            continue
        rec = reconstruct_token(
            tid, t["symbol"], t["discovered_at"], series,
            scorer_by_token.get(tid), shadow_by_token.get(tid),
        )
        if rec is None:
            continue
        records.append(rec)

    df = pd.DataFrame(records)
    print(f"qualifying (n_snapshots>=10) tokens with reconstructed timeline: {len(df)}")
    print(df["taxonomy"].value_counts())

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = OUTPUT_DIR / f"pump_timing_research_dataset_{ts}.csv"
    df.to_csv(csv_path, index=False)

    # ── Step 5: snapshot state N minutes before peak, for real-pump tokens ──
    real_pump = df[df["taxonomy"].isin(["winner", "failed_pump"])].copy()
    lookbacks = [1, 3, 5, 10, 15, 30]
    lookback_rows = []
    for X in lookbacks:
        vals_price_ratio, vals_bp, vals_vm, vals_tr3, n_found = [], [], [], [], 0
        for _, row in real_pump.iterrows():
            series = _to_uuid_key(row["token_id"], snaps_by_token)
            if series is None:
                continue
            target_t = row["peak_time"] - timedelta(minutes=X)
            hit = nearest_at_or_before(series, target_t)
            if hit is None or hit[0] < row["first_time"]:
                continue
            n_found += 1
            ts_, price, liq, vol, bp, vm = hit
            vals_price_ratio.append(price / row["peak_price"] if row["peak_price"] else np.nan)
            if bp is not None:
                vals_bp.append(bp)
            if vm is not None:
                vals_vm.append(vm)
            # trailing 3 min return at that point
            back = nearest_at_or_before(series, ts_ - timedelta(minutes=3))
            if back is not None and back[1] and back[1] > 0:
                vals_tr3.append(price / back[1] - 1)
        med_pr, *_ = median_iqr(vals_price_ratio)
        med_bp, *_ = median_iqr(vals_bp)
        med_vm, *_ = median_iqr(vals_vm)
        med_tr3, *_ = median_iqr(vals_tr3)
        lookback_rows.append(dict(X=X, n=n_found, median_price_over_peak=med_pr,
                                   median_buy_pressure=med_bp, median_volume_mult=med_vm,
                                   median_trailing_3min_return=med_tr3))

    # ── Step 6: pooled snapshot-level leading-indicator search ──
    pool_rows = []
    for _, row in real_pump.iterrows():
        series = _to_uuid_key(row["token_id"], snaps_by_token)
        if series is None or len(series) < 5:
            continue
        for i in range(2, len(series)):
            t, price, liq, vol, bp, vm = series[i]
            if price is None or price <= 0:
                continue
            back3 = nearest_at_or_before(series[:i], t - timedelta(minutes=3))
            back5 = nearest_at_or_before(series[:i], t - timedelta(minutes=5))
            tr3 = (price / back3[1] - 1) if back3 and back3[1] and back3[1] > 0 else np.nan
            tr5 = (price / back5[1] - 1) if back5 and back5[1] and back5[1] > 0 else np.nan
            bp_delta3 = (bp - back3[4]) if (bp is not None and back3 and back3[4] is not None) else np.nan
            vm_delta3 = (vm - back5[5]) if (vm is not None and back5 and back5[5] is not None) else np.nan
            fwd5 = nearest_at_or_after(series[i + 1:], t + timedelta(minutes=5))
            fwd15 = nearest_at_or_after(series[i + 1:], t + timedelta(minutes=15))
            fwd_ret5 = (fwd5[1] / price - 1) if fwd5 and fwd5[1] and fwd5[1] > 0 else np.nan
            fwd_ret15 = (fwd15[1] / price - 1) if fwd15 and fwd15[1] and fwd15[1] > 0 else np.nan
            if np.isnan(fwd_ret5):
                continue
            pool_rows.append(dict(
                token_id=row["token_id"], t=t,
                minutes_since_discovery=minutes_between(row["discovered_at"], t),
                trailing_return_3min=tr3, trailing_return_5min=tr5,
                buy_pressure=bp, volume_mult=vm,
                buy_pressure_delta_3min=bp_delta3, volume_mult_delta_3min=vm_delta3,
                forward_return_5min=fwd_ret5, forward_return_15min=fwd_ret15,
                big_move_5min_010=fwd_ret5 >= 0.10,
                big_move_5min_015=fwd_ret5 >= 0.15,
                big_move_5min_020=fwd_ret5 >= 0.20,
            ))
    pool = pd.DataFrame(pool_rows)
    print(f"pooled snapshot-level rows for leading-indicator search: {len(pool)}")

    features = ["trailing_return_3min", "trailing_return_5min", "buy_pressure", "volume_mult",
                "buy_pressure_delta_3min", "volume_mult_delta_3min"]

    survived = []
    unvalidated = []
    if len(pool) >= 40:
        pool_sorted = pool.dropna(subset=["t"]).sort_values("t")
        split_idx = len(pool_sorted) // 2
        split_time = pool_sorted.iloc[split_idx]["t"]
        train = pool_sorted[pool_sorted["t"] < split_time]
        test = pool_sorted[pool_sorted["t"] >= split_time]

        for feat in features:
            for target in ["big_move_5min_010"]:
                sub = train[[feat, target]].dropna()
                if len(sub) < 40:
                    continue
                vals = sub[feat].astype(float)
                best = None
                for q in np.arange(0.1, 1.0, 0.1):
                    split_v = vals.quantile(q)
                    above = sub[vals > split_v][target].astype(float)
                    below = sub[vals <= split_v][target].astype(float)
                    if len(above) < 10 or len(below) < 10:
                        continue
                    try:
                        u, p = stats.mannwhitneyu(above, below, alternative="two-sided")
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
                    continue
                # re-test on test slice with SAME split_value and direction
                tsub = test[[feat, target]].dropna()
                if len(tsub) < 20:
                    unvalidated.append({**best, "reason": "test slice too small"})
                    continue
                tvals = tsub[feat].astype(float)
                t_above = tsub[tvals > best["split_value"]][target].astype(float)
                t_below = tsub[tvals <= best["split_value"]][target].astype(float)
                if len(t_above) < 5 or len(t_below) < 5:
                    unvalidated.append({**best, "reason": "test-side n too small"})
                    continue
                test_effect = t_above.mean() - t_below.mean()
                train_dir = np.sign(best["effect"])
                test_dir = np.sign(test_effect)
                result = dict(**best, test_n_above=len(t_above), test_n_below=len(t_below),
                              test_rate_above=t_above.mean(), test_rate_below=t_below.mean(),
                              test_effect=test_effect, survives=(train_dir == test_dir and train_dir > 0))
                if result["survives"]:
                    survived.append(result)
                else:
                    unvalidated.append(result)

    # For the best surviving rule: earliest-signal lead time distribution
    lead_time_rows = []
    best_rule = survived[0] if survived else None
    if best_rule:
        feat, split_v = best_rule["feature"], best_rule["split_value"]
        n_fired, n_total = 0, 0
        signal_to_pump_start, peak_to_signal = [], []
        for _, row in real_pump.iterrows():
            series = _to_uuid_key(row["token_id"], snaps_by_token)
            if series is None or len(series) < 5:
                continue
            n_total += 1
            fired_at = None
            for i in range(2, len(series)):
                t, price, liq, vol, bp, vm = series[i]
                if price is None or price <= 0:
                    continue
                feat_val = None
                if feat == "buy_pressure":
                    feat_val = bp
                elif feat == "volume_mult":
                    feat_val = vm
                elif feat in ("trailing_return_3min",):
                    back3 = nearest_at_or_before(series[:i], t - timedelta(minutes=3))
                    feat_val = (price / back3[1] - 1) if back3 and back3[1] and back3[1] > 0 else None
                elif feat in ("trailing_return_5min",):
                    back5 = nearest_at_or_before(series[:i], t - timedelta(minutes=5))
                    feat_val = (price / back5[1] - 1) if back5 and back5[1] and back5[1] > 0 else None
                elif feat == "buy_pressure_delta_3min":
                    back3 = nearest_at_or_before(series[:i], t - timedelta(minutes=3))
                    feat_val = (bp - back3[4]) if bp is not None and back3 and back3[4] is not None else None
                elif feat == "volume_mult_delta_3min":
                    back5 = nearest_at_or_before(series[:i], t - timedelta(minutes=5))
                    feat_val = (vm - back5[5]) if vm is not None and back5 and back5[5] is not None else None
                if feat_val is not None and feat_val > split_v:
                    fired_at = t
                    break
            if fired_at is not None:
                n_fired += 1
                signal_to_pump_start.append(minutes_between(row["pump_start_time"], fired_at))
                peak_to_signal.append(minutes_between(fired_at, row["peak_time"]))
        lead_time_rows = dict(feature=feat, split_value=split_v, n_total=n_total, n_fired=n_fired,
                               miss_rate=1 - (n_fired / n_total if n_total else np.nan),
                               signal_minus_pump_start=median_iqr(signal_to_pump_start),
                               peak_minus_signal=median_iqr(peak_to_signal))

    # ── report ──
    lines = []
    p = lines.append
    p(f"# Pump Timing Research — {ts}")
    p("")
    p("ANALYSIS-ONLY. Read-only, no trading logic touched.")
    p("")
    p("## Data-availability caveat")
    p("")
    p("`token_snapshots` has price/liquidity/volume/buy_pressure/volume_mult per snapshot but "
      "NO raw buy/sell transaction counts over time - those exist once, at discovery, in "
      "token_evaluations/shadow_trades inputs_json. \"Buy-sell/transaction behavior before the "
      "pump\" below is measured via buy_pressure and volume_mult trends, the only per-snapshot "
      "proxies that exist. Wash-trading data is likewise a single decision-time value, not a "
      "time series - not analyzed as a trend here.")
    p("")
    p("## Population / threshold sensitivity")
    p("")
    p(f"- Tokens with >=5 snapshots: {counts_by_thresh[5]}")
    p(f"- Tokens with >=10 snapshots (PRIMARY population used below): {counts_by_thresh[10]}")
    p(f"- Tokens with >=20 snapshots: {counts_by_thresh[20]}")
    p("")
    p(f"Reconstructed timelines for {len(df)} qualifying tokens (>=10 snapshots, >=2 valid prices).")
    p("")
    p("## Taxonomy")
    p("")
    p("- **real pump** = peak_price/first_price - 1 >= 30%")
    p("- **winner** = real pump AND retained_fraction_of_peak_gain >= 20% (held at least a fifth "
      "of the peak gain)")
    p("- **failed pump** = real pump but gave back more than 80% of the peak gain")
    p("- **rug** = not a real pump AND final_lifetime_return <= -50%")
    p("- **flat/no-event** = everything else (no meaningful move either way)")
    p("")
    vc = df["taxonomy"].value_counts()
    for k in ["winner", "failed_pump", "rug", "flat_no_event"]:
        p(f"- {k}: n={int(vc.get(k, 0))}")
    p("")
    p("## Timing distributions (median [IQR], n)")
    p("")
    p("| metric | population | median | IQR | n |")
    p("|---|---|---|---|---|")
    for label, sub in [("all real-pump (winner+failed)", real_pump), ("winners only", df[df.taxonomy=="winner"]),
                        ("failed pumps only", df[df.taxonomy=="failed_pump"])]:
        for metric in ["discovery_to_pump_start_min", "discovery_to_peak_min", "pump_start_to_peak_min"]:
            med, q1, q3, n = median_iqr(sub[metric])
            p(f"| {metric} | {label} | {fmt_min(med)} | [{fmt_min(q1)}, {fmt_min(q3)}] | {n} |")
    med, q1, q3, n = median_iqr(real_pump["time_to_confirmed_reversal_min"])
    n_held = int(real_pump["held_through_end"].sum())
    p(f"| time_to_confirmed_reversal_min (>=15% drop from peak) | all real-pump | {fmt_min(med)} | "
      f"[{fmt_min(q1)}, {fmt_min(q3)}] | {n} |")
    p(f"| held_through_end (never confirmed-reversed before data ended) | all real-pump | - | - | "
      f"{n_held} of {len(real_pump)} |")
    p("")
    p("## Pump fraction before scorer decision")
    p("")
    has_dec = df[df["has_scorer_decision"] & df["pump_fraction_before_decision"].notna()]
    for label, sub in [("all with a scorer decision", has_dec),
                        ("also crossed shadow score>=4.0", has_dec[has_dec["has_shadow"]]),
                        ("scored but never crossed shadow threshold", has_dec[~has_dec["has_shadow"]])]:
        med, q1, q3, n = median_iqr(sub["pump_fraction_before_decision"])
        p(f"- {label}: median={('n/a' if pd.isna(med) else f'{med:.2f}')}, "
          f"IQR=[{'n/a' if pd.isna(q1) else f'{q1:.2f}'}, {'n/a' if pd.isna(q3) else f'{q3:.2f}'}], n={n}")
    p("")
    p("(0 = decision came before the pump even started; 1 = decision came at/after the peak, "
      "missing the move entirely.)")
    p("")
    p("## What a token looks like N minutes before its peak (real-pump tokens)")
    p("")
    p("| minutes before peak | n | median price/peak | median buy_pressure | median volume_mult | "
      "median trailing 3min return |")
    p("|---|---|---|---|---|---|")
    def fmt3(x):
        return "n/a" if pd.isna(x) else f"{x:.3f}"

    for r in lookback_rows:
        pr = fmt3(r["median_price_over_peak"])
        bpv = fmt3(r["median_buy_pressure"])
        vmv = fmt3(r["median_volume_mult"])
        p(f"| {r['X']} | {r['n']} | {pr} | {bpv} | {vmv} | {fmt_pct(r['median_trailing_3min_return'])} |")
    p("")
    p("## Leading-indicator search (causal, time-split validated)")
    p("")
    p(f"Pooled snapshot-level dataset: n={len(pool)} rows from {len(real_pump)} real-pump tokens. "
      "Chronological train/test split by snapshot time t (not by token).")
    p("")
    if not len(pool):
        p("No pooled rows available - cannot run the search.")
    elif not survived and not unvalidated:
        p("Pool too small (<40 rows) to run a meaningful decile search.")
    else:
        p("### Rules that SURVIVED time-split validation (same direction train vs test)")
        p("")
        if survived:
            p("| feature | split_value | train n(above/below) | train rate(above/below) | "
              "test n(above/below) | test rate(above/below) | train p |")
            p("|---|---|---|---|---|---|---|")
            for r in survived:
                p(f"| {r['feature']} | {r['split_value']:.4g} | {r['n_above']}/{r['n_below']} | "
                  f"{r['rate_above']:.3f}/{r['rate_below']:.3f} | {r['test_n_above']}/{r['test_n_below']} | "
                  f"{r['test_rate_above']:.3f}/{r['test_rate_below']:.3f} | {fmt_p(r['p_value'])} |")
        else:
            p("**None.** No single-feature decile rule held its direction from train to test. "
              "This is a null result and is reported as such, not hidden.")
        p("")
        p("### Candidates that did NOT survive (in-sample only, unvalidated)")
        p("")
        if unvalidated:
            p("| feature | split_value | train n(above/below) | train rate(above/below) | reason/test |")
            p("|---|---|---|---|---|")
            for r in unvalidated[:10]:
                reason = r.get("reason", f"reversed in test (train effect {r.get('effect', float('nan')):+.3f}, "
                                          f"test effect {r.get('test_effect', float('nan')):+.3f})")
                p(f"| {r['feature']} | {r['split_value']:.4g} | {r['n_above']}/{r['n_below']} | "
                  f"{r['rate_above']:.3f}/{r['rate_below']:.3f} | {reason} |")
        else:
            p("(none)")
    p("")
    p("## Earliest meaningful momentum signal before peak")
    p("")
    if best_rule and lead_time_rows:
        r = lead_time_rows
        sm, sq1, sq3, sn = r["signal_minus_pump_start"]
        pm, pq1, pq3, pn = r["peak_minus_signal"]
        p(f"Best surviving rule: **{r['feature']} > {r['split_value']:.4g}**. "
          f"Fired at least once for {r['n_fired']} of {r['n_total']} real-pump tokens "
          f"(miss rate {r['miss_rate']*100:.1f}%).")
        p("")
        p(f"- Time from pump_start to first signal: median {fmt_min(sm)} min, IQR [{fmt_min(sq1)}, {fmt_min(sq3)}], n={sn} "
          "(positive = signal fired AFTER the dip/pump-start point, i.e. some of the move was already underway)")
        p(f"- Time from first signal to peak: median {fmt_min(pm)} min, IQR [{fmt_min(pq1)}, {fmt_min(pq3)}], n={pn} "
          "(this is the actual usable lead time before the peak, if you entered exactly when the rule fired)")
    else:
        p("No rule survived time-split validation, so there is no earliest-signal lead-time distribution to "
          "report - the honest answer is that this dataset does not yet support a validated leading indicator "
          "from the six features tested.")
    p("")

    # representative timelines
    p("## Representative timelines")
    p("")
    def pick(tax, n_min=15, k=2):
        sub = df[(df.taxonomy == tax) & (df.n_snapshots >= n_min)].sort_values("n_snapshots", ascending=False)
        return sub.head(k)

    for tax, label in [("winner", "Winner"), ("failed_pump", "Failed pump"), ("rug", "Rug")]:
        chosen = pick(tax)
        if len(chosen) == 0:
            chosen = df[df.taxonomy == tax].sort_values("n_snapshots", ascending=False).head(2)
        for _, row in chosen.iterrows():
            p(f"### {label}: {row['symbol']} ({row['token_id'][:8]}...) — n_snapshots={row['n_snapshots']}")
            p("")
            p("| min_since_discovery | price/first_price | buy_pressure | volume_mult | marker |")
            p("|---|---|---|---|---|")
            series = _to_uuid_key(row["token_id"], snaps_by_token)
            if series:
                for t, price, liq, vol, bp, vm in series:
                    if price is None:
                        continue
                    marker = ""
                    if t == row["pump_start_time"]:
                        marker = "pump_start"
                    if t == row["peak_time"]:
                        marker += " peak"
                    mins = minutes_between(row["discovered_at"], t)
                    p(f"| {mins:.1f} | {price/row['first_price']:.3f} | "
                      f"{'n/a' if bp is None else f'{bp:.3f}'} | {'n/a' if vm is None else f'{vm:.3f}'} | {marker} |")
            p("")

    p("## Caveats")
    p("")
    p(f"- Primary population uses n_snapshots>=10 ({len(df)} tokens); sensitivity at >=5 "
      f"({counts_by_thresh[5]}) and >=20 ({counts_by_thresh[20]}) reported above.")
    p(f"- {int(df['outage_excluded_full'].sum())} of {len(df)} qualifying tokens' full lifetime window "
      "overlaps a known outage - included in taxonomy/timing stats but flagged here, not silently dropped "
      "(their raw snapshot data is real, just may have gaps).")
    p("- No-lookahead confirmed: every feature in Step 6 uses only snapshots at or before t; labels use only "
      "snapshots after t; train/test split is chronological by snapshot time.")
    p("- This is descriptive/exploratory pattern-finding, not a proposed rule change. No trading logic, "
      "thresholds, or scorer weights were touched.")

    report_path = OUTPUT_DIR / f"pump_timing_research_{ts}.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")
    print(f"Wrote {csv_path}")


_STR_KEY_MAP = {}


def _to_uuid_key(token_id_str, snaps_by_token):
    """token_id in df is str(uuid); snaps_by_token is keyed by asyncpg UUID objects.
    Uses the module-level string-keyed map built once in main() - O(1), not a scan."""
    return _STR_KEY_MAP.get(token_id_str)


if __name__ == "__main__":
    asyncio.run(main())
