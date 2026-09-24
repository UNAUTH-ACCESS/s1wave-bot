"""
analysis/edge_research.py
==========================
ANALYSIS-ONLY. Read-only against the live database (SELECT only). Never
imports or touches scorer_worker.py, settings.py, risk.py,
sampling_worker.py, discovery_worker.py, or any other production path.
Safe to run at any time, including while main.py is live.

Purpose
-------
Find out whether any decision-time feature (or combination) available to
the S1 Wave / Scorer pipeline actually carries predictive information
about a token's subsequent price behavior - independent of what the
scorer's composite score currently says. This is pure research: no
threshold, weight, or entry/exit rule is read from or written to as a
consequence of anything in this file.

Three candidate groups, analyzed separately (their features/decision-time
anchors are not interchangeable)
----------------------------------------------------------------------
1. Shadow trades (experiment_version='scorer_v2_threshold_4'): every
   token whose composite score crossed 4.0, recorded once per token at
   first crossing. decision_time = triggered_at. Features = shadow_trades
   columns + inputs_json (the SCORER gate's rolling-window components).
   Decision-time price = entry_price (captured at trigger time - no
   lookup needed, no leakage possible).
2. Complete OBSERVING trajectories: every token with
   observation_started_at IS NOT NULL - i.e. every Tier1 reject or
   Scorer discard that was ever tracked as a control-group candidate,
   whether it is still OBSERVING today or has since aged out to
   REJECTED. decision_time = observation_started_at (NOT discovered_at -
   see sampling_worker.py's own reasoning: a Scorer-discarded token can
   have been discovered long before the moment it was actually
   discarded). Features = the LATEST token_evaluations row with
   passed=false and evaluated_at <= observation_started_at for that
   token (gate is TIER1 or SCORER depending on why it entered
   OBSERVING). Decision-time price = the first token_snapshot at or
   after observation_started_at.
   NOTE: groups 1 and 2 are NOT mutually exclusive - a token can cross
   score>=4.0 (shadow-flagged) and later get Scorer-discarded into
   OBSERVING. Both are analyzed and the overlap is reported explicitly,
   since it affects how independent the two groups' findings really are.
3. The 2 real paper trades: reported as individual case studies only,
   never pooled into any n-based statistic. decision_time = entry_time.
   trade_price_observations (phase13 instrumentation) has ZERO rows for
   both - both predate that fix (entry_time 2026-09-20, phase13 landed
   later) - so their price paths fall back to token_snapshots, same as
   every other candidate, not the denser per-second feed.

Look-ahead discipline
----------------------
Every feature value used comes from a row whose own timestamp is <=
decision_time (shadow_trades.triggered_at / observation_started_at /
trades.entry_time respectively). Every outcome value comes exclusively
from token_snapshots rows with sampled_at >= decision_time. There is no
mixing of the two directions anywhere in this script.

Outage handling - READ THIS BEFORE REUSING THE OUTAGES LIST ELSEWHERE
-----------------------------------------------------------------------
shadow_backtest.py and clean_recap.py define a second outage as
OPEN-ENDED: (2026-09-20 21:57:07 UTC, datetime.now()). That was correct
when written but is now STALE. As of 2026-09-21T07:17:51Z, a separate,
independent SolanaTracker key went live for the discovery worker, and
discovery has been covering (writing snapshots for) a meaningful subset
of WATCHING/OBSERVING tokens on its own ~60s poll cadence ever since -
this is real, uncontaminated data. The original sampling worker's own
key remains separately exhausted (still 403s), so coverage after
2026-09-21T07:17:51Z is SPARSE for some tokens (or entirely absent once
a token ages past discovery's 30-minute lookback), never absent for the
whole population, and never wrong when present. This script treats
2026-09-20T21:57:07Z-2026-09-21T07:17:51Z as a CLOSED outage interval
(both discovery and sampling were fully dead - verified via hourly
bucket counts on token_evaluations/shadow_trades/token_snapshots showing
a hard gap in that exact window) and treats everything after
2026-09-21T07:17:51Z as valid-but-sparse, handled via reporting
n_observations_used per horizon rather than exclusion. A future script
that copies this OUTAGES list should use THIS closed version, not the
open-ended one in the older scripts.

Statistical methodology (pandas/numpy/scipy.stats only - no sklearn:
scikit-learn failed to install in this environment due to a disk quota,
and everything here is doable without it)
----------------------------------------------------------------------
- Individual numeric feature vs continuous outcome: Spearman correlation
  + quintile-binned mean-outcome table (catches non-monotonic shapes a
  single r would hide).
- Individual feature vs rug_flag: Mann-Whitney U between rug/non-rug
  groups.
- Nonlinear threshold search: decile grid search per feature against a
  primary target (peak_return_30m, rug_flag), best split kept and
  explicitly labeled SEARCHED (multiple-comparisons risk - the real test
  is whether it survives the time-split, not its raw p-value).
- Feature interactions: 2x2 median-split Kruskal-Wallis across the top
  individually-promising features.
- Score value-add: regress peak_return_30m on the raw rolling-window
  components (bp/vm/vlr p1-3) via numpy.linalg.lstsq, take residuals;
  regress score on the same components, take residuals; Spearman-
  correlate the two residual series. Near-zero means score isn't adding
  information the raw components didn't already carry.
- Time-split validation: pool groups 1+2 (tagged), sort by decision_time,
  split at the chronological median. Every threshold/interaction finding
  is re-evaluated on the test slice alone and reported side-by-side. A
  finding that reverses sign or vanishes in test is reported as such and
  excluded from the final shortlist.
- Every reported number carries its n. n<20 is labeled "LOW CONFIDENCE
  (n<20)" directly in the prose, not just a footnote.

Usage
-----
    PYTHONPATH=. /tmp/s1wave-venv/bin/python analysis/edge_research.py

Outputs
-------
    analysis/output/edge_research_<timestamp>.md
    analysis/output/edge_research_dataset_<timestamp>.csv
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg  # noqa: E402

from config.settings import settings  # noqa: E402

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

OUTAGES = [
    (datetime(2026, 9, 20, 12, 47, 5, tzinfo=timezone.utc),
     datetime(2026, 9, 20, 17, 37, 13, tzinfo=timezone.utc)),
    (datetime(2026, 9, 20, 21, 57, 7, tzinfo=timezone.utc),
     datetime(2026, 9, 21, 7, 17, 51, tzinfo=timezone.utc)),
]

HORIZONS = [5, 15, 30, 60]
EXPERIMENT_VERSION = "scorer_v2_threshold_4"


def window_overlaps_outage(start, end) -> bool:
    return any(start < o_end and end > o_start for o_start, o_end in OUTAGES)


def _num(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ── SQL ─────────────────────────────────────────────────────────────────

SHADOW_SQL = """
SELECT st.token_id, tok.symbol, st.triggered_at, st.score, st.tier1_passed,
       st.entry_price, st.liquidity_usd, st.market_cap_usd, st.buys,
       st.sells, st.wash_multiplier, st.inputs_json
FROM shadow_trades st
JOIN tokens tok ON tok.id = st.token_id
WHERE st.experiment_version = $1
ORDER BY st.triggered_at ASC;
"""

OBSERVING_TOKENS_SQL = """
SELECT id, symbol, discovered_at, observation_started_at,
       observation_exit_reason, rejection_reason, status
FROM tokens
WHERE observation_started_at IS NOT NULL
ORDER BY observation_started_at ASC;
"""

# All passed=false evaluations for OBSERVING-anchored tokens; the latest
# one at/before observation_started_at is picked in Python per token
# (window functions would work too, but this keeps the "latest <= anchor"
# rule visible and easy to audit here rather than buried in SQL).
EVALS_SQL = """
SELECT token_id, evaluated_at, gate, passed, reason_code, inputs_json
FROM token_evaluations
WHERE passed = false
ORDER BY token_id, evaluated_at ASC;
"""

TRADES_SQL = """
SELECT id, token_id, token_mint, entry_price, entry_time,
       entry_composite_score, entry_bp_trend, entry_vm_trend,
       entry_vm_zone, entry_liquidity_flag, entry_liquidity_trend,
       exit_price, exit_time, exit_reason, pnl_pct, hold_duration_seconds,
       status, entry_source
FROM trades
ORDER BY entry_time ASC;
"""

TRADE_OBS_SQL = """
SELECT trade_id, observed_at, price_usd
FROM trade_price_observations
ORDER BY trade_id, observed_at ASC;
"""

SNAPSHOTS_SQL = """
SELECT token_id, sampled_at, price_usd
FROM token_snapshots
ORDER BY token_id, sampled_at ASC;
"""


# ── outcome computation ────────────────────────────────────────────────

def compute_outcomes(snaps, decision_time):
    """
    snaps: list of (sampled_at, price) for one token, any order.
    decision_time: anchor to measure "after" from.

    Returns dict per horizon key (5,15,30,60,"full") ->
      {forward_return, peak_return, max_drawdown, n_observations_used,
       outage_excluded (bool)}
    Baseline price = first snapshot AT OR AFTER decision_time (never
    before - that would leak pre-decision information as if it were the
    entry price).
    """
    after = sorted(
        [(t, p) for t, p in snaps if t is not None and p is not None and p > 0
         and t >= decision_time],
        key=lambda x: x[0],
    )
    result = {}
    if not after:
        for h in HORIZONS + ["full"]:
            result[h] = dict(forward_return=None, peak_return=None,
                              max_drawdown=None, n_observations_used=0,
                              outage_excluded=False)
        return result

    baseline = after[0][1]
    last_ts = after[-1][0]

    for h in HORIZONS:
        cutoff = decision_time + timedelta(minutes=h)
        excluded = window_overlaps_outage(decision_time, cutoff)
        window = [(t, p) for t, p in after if t <= cutoff]
        if excluded or not window:
            result[h] = dict(forward_return=None, peak_return=None,
                              max_drawdown=None,
                              n_observations_used=len(window),
                              outage_excluded=excluded)
            continue
        prices = [p for _, p in window]
        result[h] = dict(
            forward_return=prices[-1] / baseline - 1,
            peak_return=max(prices) / baseline - 1,
            max_drawdown=min(prices) / baseline - 1,
            n_observations_used=len(window),
            outage_excluded=False,
        )

    excluded_full = window_overlaps_outage(decision_time, last_ts)
    prices = [p for _, p in after]
    result["full"] = dict(
        forward_return=prices[-1] / baseline - 1,
        peak_return=max(prices) / baseline - 1,
        max_drawdown=min(prices) / baseline - 1,
        n_observations_used=len(after),
        outage_excluded=excluded_full,
    )
    return result


# ── data loading ────────────────────────────────────────────────────────

async def load_all(conn):
    shadow_rows = await conn.fetch(SHADOW_SQL, EXPERIMENT_VERSION)
    obs_tokens = await conn.fetch(OBSERVING_TOKENS_SQL)
    eval_rows = await conn.fetch(EVALS_SQL)
    trade_rows = await conn.fetch(TRADES_SQL)
    trade_obs_rows = await conn.fetch(TRADE_OBS_SQL)
    snap_rows = await conn.fetch(SNAPSHOTS_SQL)

    snaps_by_token = {}
    for r in snap_rows:
        snaps_by_token.setdefault(r["token_id"], []).append((r["sampled_at"], _num(r["price_usd"])))

    evals_by_token = {}
    for r in eval_rows:
        evals_by_token.setdefault(r["token_id"], []).append(r)

    obs_by_trade = {}
    for r in trade_obs_rows:
        obs_by_trade.setdefault(r["trade_id"], []).append((r["observed_at"], _num(r["price_usd"])))

    return shadow_rows, obs_tokens, evals_by_token, trade_rows, obs_by_trade, snaps_by_token


def parse_scorer_inputs(inputs_json_str):
    """Common inputs_json shape for SCORER-gate rows (shadow_trades and
    token_evaluations gate='SCORER' alike)."""
    if not inputs_json_str:
        return {}
    d = json.loads(inputs_json_str) if isinstance(inputs_json_str, str) else inputs_json_str
    out = {}
    for k in ("bp_p1", "bp_p2", "bp_p3", "vm_p1", "vm_p2", "vm_p3",
              "vlr_p1", "vlr_p2", "vlr_p3", "score", "age_minutes",
              "bp_component", "vm_component", "vlr_component",
              "buys_at_discovery", "sells_at_discovery",
              "total_txns_at_discovery", "liquidity_usd_at_discovery",
              "market_cap_usd_at_discovery", "wash_multiplier_at_discovery",
              "buy_sell_ratio_at_discovery", "prev_composite_score"):
        out[k] = _num(d.get(k))
    for k in ("bp_trend", "vm_trend", "vlr_trend", "vm_zone", "signal",
              "scorer_version"):
        out[k] = d.get(k)
    return out


def parse_tier1_inputs(inputs_json_str):
    if not inputs_json_str:
        return {}
    d = json.loads(inputs_json_str) if isinstance(inputs_json_str, str) else inputs_json_str
    return {
        "wash_multiplier": _num(d.get("wash_multiplier")),
        "age_minutes": _num(d.get("age_minutes")),
        "liquidity_usd": _num(d.get("liquidity_usd")),
        "market_cap_usd": _num(d.get("market_cap_usd")),
        "buys": d.get("buys"),
        "sells": d.get("sells"),
    }


NUMERIC_FEATURES = [
    "score", "bp_p1", "bp_p2", "bp_p3", "vm_p1", "vm_p2", "vm_p3",
    "vlr_p1", "vlr_p2", "vlr_p3", "liquidity_usd_at_discovery",
    "wash_multiplier_at_discovery", "buy_sell_ratio_at_discovery",
    "age_minutes",
]

RAW_COMPONENT_FEATURES = ["bp_p1", "bp_p2", "bp_p3", "vm_p1", "vm_p2", "vm_p3",
                           "vlr_p1", "vlr_p2", "vlr_p3"]


def build_group1_shadow(shadow_rows, snaps_by_token):
    recs = []
    for r in shadow_rows:
        feats = parse_scorer_inputs(r["inputs_json"])
        feats["score"] = _num(r["score"])  # column is authoritative over inputs_json copy
        feats["liquidity_usd_at_discovery"] = feats.get("liquidity_usd_at_discovery") or _num(r["liquidity_usd"])
        feats["market_cap_usd_at_discovery"] = feats.get("market_cap_usd_at_discovery") or _num(r["market_cap_usd"])
        feats["wash_multiplier_at_discovery"] = feats.get("wash_multiplier_at_discovery") or _num(r["wash_multiplier"])
        decision_time = r["triggered_at"]
        entry_price = _num(r["entry_price"])
        snaps = list(snaps_by_token.get(r["token_id"], []))
        # entry_price is already the decision-time price; prepend it so
        # compute_outcomes' baseline is exactly this value, not whatever
        # the nearest snapshot happens to say (avoids a second source of
        # truth for the same instant).
        snaps_with_entry = [(decision_time, entry_price)] + [
            (t, p) for t, p in snaps if t > decision_time
        ]
        outcomes = compute_outcomes(snaps_with_entry, decision_time)
        rec = dict(
            group="shadow", token_id=str(r["token_id"]), symbol=r["symbol"],
            decision_time=decision_time, decision_price=entry_price,
            **feats,
        )
        for h in HORIZONS + ["full"]:
            for k, v in outcomes[h].items():
                rec[f"{k}_{h}"] = v
        rug_gone = False  # shadow_trades table has no exit-reason column
        rug_price = (outcomes["full"]["max_drawdown"] is not None
                     and outcomes["full"]["max_drawdown"] <= -0.80)
        rec["rug_flag_gone"] = rug_gone
        rec["rug_flag_price"] = rug_price
        rec["rug_flag"] = rug_gone or bool(rug_price)
        recs.append(rec)
    return recs


def build_group2_observing(obs_tokens, evals_by_token, snaps_by_token):
    recs = []
    for t in obs_tokens:
        token_id = t["id"]
        anchor = t["observation_started_at"]
        candidates = [e for e in evals_by_token.get(token_id, []) if e["evaluated_at"] <= anchor]
        if not candidates:
            continue  # no decision-time feature row available - skip, don't guess
        latest = max(candidates, key=lambda e: e["evaluated_at"])
        gate = latest["gate"]
        if gate == "SCORER":
            feats = parse_scorer_inputs(latest["inputs_json"])
        elif gate == "TIER1":
            feats = parse_tier1_inputs(latest["inputs_json"])
            # Tier1-level rows don't carry bp/vm/vlr rolling-window
            # components (those don't exist until sampling starts) -
            # leave them as None rather than fabricating zeros.
            for k in ("score",) + tuple(RAW_COMPONENT_FEATURES):
                feats.setdefault(k, None)
        else:
            continue

        snaps = list(snaps_by_token.get(token_id, []))
        decision_snaps = [(ts, p) for ts, p in snaps if ts >= anchor]
        if not decision_snaps:
            continue  # no post-decision data at all - can't compute any outcome
        outcomes = compute_outcomes(snaps, anchor)

        rec = dict(
            group="observing", token_id=str(token_id), symbol=t["symbol"],
            decision_time=anchor, decision_gate=gate,
            **feats,
        )
        for h in HORIZONS + ["full"]:
            for k, v in outcomes[h].items():
                rec[f"{k}_{h}"] = v
        exit_reason = (t["observation_exit_reason"] or "") + " " + (t["rejection_reason"] or "")
        rug_gone = "TOKEN_GONE" in exit_reason
        rug_price = (outcomes["full"]["max_drawdown"] is not None
                     and outcomes["full"]["max_drawdown"] <= -0.80)
        rec["rug_flag_gone"] = rug_gone
        rec["rug_flag_price"] = rug_price
        rec["rug_flag"] = rug_gone or bool(rug_price)
        recs.append(rec)
    return recs


def build_group3_trades(trade_rows, obs_by_trade, snaps_by_token):
    cases = []
    for r in trade_rows:
        trade_id = r["id"]
        entry_time = r["entry_time"]
        entry_price = _num(r["entry_price"])
        obs = obs_by_trade.get(trade_id, [])
        snaps = list(snaps_by_token.get(r["token_id"], []))
        source = "trade_price_observations" if obs else "token_snapshots (fallback - predates phase13)"
        path = obs if obs else snaps
        after = sorted([(t, p) for t, p in path if p and t >= entry_time], key=lambda x: x[0])
        highest = max((p for _, p in after), default=None)
        lowest = min((p for _, p in after), default=None)
        mfe = (highest / entry_price - 1) if highest and entry_price else None
        mae = (lowest / entry_price - 1) if lowest and entry_price else None
        cases.append(dict(
            trade_id=str(trade_id), token_id=str(r["token_id"]),
            entry_source=r["entry_source"], entry_time=entry_time,
            entry_price=entry_price, entry_composite_score=_num(r["entry_composite_score"]),
            entry_bp_trend=r["entry_bp_trend"], entry_vm_trend=r["entry_vm_trend"],
            entry_vm_zone=r["entry_vm_zone"], entry_liquidity_flag=r["entry_liquidity_flag"],
            entry_liquidity_trend=r["entry_liquidity_trend"],
            exit_price=_num(r["exit_price"]), exit_time=r["exit_time"],
            exit_reason=r["exit_reason"], pnl_pct=_num(r["pnl_pct"]),
            hold_duration_seconds=r["hold_duration_seconds"], status=r["status"],
            price_path_source=source, n_price_points=len(after),
            mfe_pct=mfe, mae_pct=mae,
        ))
    return cases


# ── statistics ──────────────────────────────────────────────────────────

def quintile_table(df, feature, outcome):
    sub = df[[feature, outcome]].dropna()
    n = len(sub)
    if n < 10:
        return None, n
    try:
        sub = sub.copy()
        sub["_bin"] = pd.qcut(sub[feature], q=min(5, sub[feature].nunique()), duplicates="drop")
    except ValueError:
        return None, n
    table = sub.groupby("_bin", observed=True)[outcome].agg(["count", "mean", "median"])
    return table, n


def spearman_safe(x, y):
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    mask = ~(np.isnan(x) | np.isnan(y))
    x, y = x[mask], y[mask]
    n = len(x)
    if n < 5 or np.all(x == x[0]) or np.all(y == y[0]):
        return None, None, n
    rho, p = stats.spearmanr(x, y)
    return rho, p, n


def feature_vs_outcome_screen(df, features, outcome):
    rows = []
    for feat in features:
        if feat not in df.columns:
            continue
        rho, p, n = spearman_safe(df[feat], df[outcome])
        rows.append(dict(feature=feat, outcome=outcome, spearman_rho=rho, p_value=p, n=n))
    return pd.DataFrame(rows)


def rug_flag_screen(df, features):
    rows = []
    for feat in features:
        if feat not in df.columns:
            continue
        sub = df[[feat, "rug_flag"]].dropna()
        rug = sub[sub["rug_flag"]][feat].astype(float)
        non_rug = sub[~sub["rug_flag"]][feat].astype(float)
        if len(rug) < 5 or len(non_rug) < 5:
            rows.append(dict(feature=feat, n_rug=len(rug), n_non_rug=len(non_rug),
                              u_stat=None, p_value=None, note="n<5 in one group"))
            continue
        u, p = stats.mannwhitneyu(rug, non_rug, alternative="two-sided")
        rows.append(dict(feature=feat, n_rug=len(rug), n_non_rug=len(non_rug),
                          median_rug=rug.median(), median_non_rug=non_rug.median(),
                          u_stat=u, p_value=p, note=None))
    return pd.DataFrame(rows)


def decile_threshold_search(df, feature, target, target_is_binary):
    sub = df[[feature, target]].dropna()
    n = len(sub)
    if n < 20:
        return None
    vals = sub[feature].astype(float)
    best = None
    for q in np.arange(0.1, 1.0, 0.1):
        split = vals.quantile(q)
        above = sub[vals > split][target]
        below = sub[vals <= split][target]
        if len(above) < 5 or len(below) < 5:
            continue
        if target_is_binary:
            above_v = above.astype(float)
            below_v = below.astype(float)
        else:
            above_v = above.astype(float)
            below_v = below.astype(float)
        try:
            u, p = stats.mannwhitneyu(above_v, below_v, alternative="two-sided")
        except ValueError:
            continue
        effect = above_v.mean() - below_v.mean()
        cand = dict(feature=feature, target=target, split_quantile=round(q, 1),
                    split_value=split, n_above=len(above), n_below=len(below),
                    mean_above=above_v.mean(), mean_below=below_v.mean(),
                    effect=effect, p_value=p)
        if best is None or abs(effect) > abs(best["effect"]):
            best = cand
    return best


def kruskal_interaction(df, feat_a, feat_b, target):
    sub = df[[feat_a, feat_b, target]].dropna()
    n = len(sub)
    if n < 40:
        return None
    med_a, med_b = sub[feat_a].median(), sub[feat_b].median()
    cells = {}
    for a_hi in (True, False):
        for b_hi in (True, False):
            mask = ((sub[feat_a] > med_a) == a_hi) & ((sub[feat_b] > med_b) == b_hi)
            cells[(a_hi, b_hi)] = sub[mask][target].astype(float)
    ns = {k: len(v) for k, v in cells.items()}
    if min(ns.values()) < 10:
        return dict(feat_a=feat_a, feat_b=feat_b, target=target, n=n,
                    cell_n=ns, note="cell n<10 - unreliable", stat=None, p_value=None)
    groups = [v for v in cells.values()]
    stat, p = stats.kruskal(*groups)
    means = {f"{'hi' if k[0] else 'lo'}_{feat_a}_{'hi' if k[1] else 'lo'}_{feat_b}": round(v.mean(), 4)
             for k, v in cells.items()}
    return dict(feat_a=feat_a, feat_b=feat_b, target=target, n=n, cell_n=ns,
                cell_means=means, stat=stat, p_value=p, note=None)


def score_value_add(df, outcome="peak_return_30"):
    sub = df[RAW_COMPONENT_FEATURES + ["score", outcome]].dropna()
    n = len(sub)
    if n < 20:
        return dict(n=n, note="n<20 - too small to fit a partial-correlation model")
    X = sub[RAW_COMPONENT_FEATURES].astype(float).values
    X1 = np.column_stack([X, np.ones(len(X))])
    y_outcome = sub[outcome].astype(float).values
    y_score = sub["score"].astype(float).values

    coef_o, *_ = np.linalg.lstsq(X1, y_outcome, rcond=None)
    resid_outcome = y_outcome - X1 @ coef_o
    coef_s, *_ = np.linalg.lstsq(X1, y_score, rcond=None)
    resid_score = y_score - X1 @ coef_s

    rho, p = stats.spearmanr(resid_score, resid_outcome)
    raw_rho, raw_p, _ = spearman_safe(sub["score"], sub[outcome])
    return dict(n=n, partial_spearman_rho=rho, partial_p_value=p,
                raw_score_spearman_rho=raw_rho, raw_score_p_value=raw_p,
                outcome=outcome)


def confidence_tag(n):
    return " **[LOW CONFIDENCE n<20]**" if n is not None and n < 20 else ""


# ── report assembly ────────────────────────────────────────────────────

def fmt_pct(x, digits=1):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "n/a"
    return f"{x*100:+.{digits}f}%"


def fmt_p(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "n/a"
    return f"{x:.4f}"


async def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        shadow_rows, obs_tokens, evals_by_token, trade_rows, obs_by_trade, snaps_by_token = await load_all(conn)
    finally:
        await conn.close()

    g1 = build_group1_shadow(shadow_rows, snaps_by_token)
    g2 = build_group2_observing(obs_tokens, evals_by_token, snaps_by_token)
    g3 = build_group3_trades(trade_rows, obs_by_trade, snaps_by_token)

    df1 = pd.DataFrame(g1)
    df2 = pd.DataFrame(g2)

    # Rename horizon-keyed outcome columns to flat names for pandas ease:
    # forward_return_5, peak_return_30, max_drawdown_60, etc. (already the
    # naming used by build_group*; horizon "full" stays as-is).
    combined = pd.concat([df1, df2], ignore_index=True, sort=False) if len(df1) or len(df2) else pd.DataFrame()

    overlap_ids = set(df1["token_id"]) & set(df2["token_id"]) if len(df1) and len(df2) else set()

    now = datetime.now(timezone.utc)
    ts_tag = now.strftime("%Y%m%d_%H%M%S")
    lines = []

    def p(s=""):
        lines.append(s)

    p("# S1 Wave — Edge Research Report")
    p(f"Generated: {now.isoformat()}")
    p("")
    p("**Research only. No scorer, threshold, entry, exit, or risk logic was changed. "
      "No candidate hypothesis below has been implemented.**")
    p("")
    p("## Methodology")
    p("")
    p(f"- Group 1 (shadow trades, `{EXPERIMENT_VERSION}`): decision_time = `triggered_at`. n={len(df1)}.")
    p(f"- Group 2 (complete OBSERVING trajectories): decision_time = `observation_started_at`. n={len(df2)} "
      f"(of {len(obs_tokens)} tokens with observation_started_at set — the rest were dropped for having no "
      "decision-time evaluation row at/before the anchor, or zero post-decision snapshots).")
    p(f"- Overlap between group 1 and group 2 by token_id: {len(overlap_ids)} tokens appear in both "
      "(shadow-flagged AND later Scorer-discarded into OBSERVING) — findings from the two groups are not fully independent to that extent.")
    p(f"- Group 3 (real paper trades): n=2, reported as case studies only, never pooled into a statistic.")
    p("- Outage handling: two windows excluded per-horizon where they overlap a candidate's "
      f"[decision_time, decision_time+horizon] span — "
      f"{OUTAGES[0][0].isoformat()} to {OUTAGES[0][1].isoformat()} (20-token-limit lockup), and "
      f"{OUTAGES[1][0].isoformat()} to {OUTAGES[1][1].isoformat()} (full discovery+sampling blackout, "
      "verified via hourly bucket gaps). Data after the second window's end is real but sparser for some "
      "tokens (see module docstring) — not excluded, just carries lower n_observations_used.")

    # time-split boundary
    combined_sorted = combined.dropna(subset=["decision_time"]).sort_values("decision_time") if len(combined) else combined
    if len(combined_sorted) >= 20:
        split_idx = len(combined_sorted) // 2
        split_time = combined_sorted.iloc[split_idx]["decision_time"]
        train = combined_sorted.iloc[:split_idx]
        test = combined_sorted.iloc[split_idx:]
    else:
        split_time, train, test = None, combined_sorted, pd.DataFrame()
    p(f"- Time-split boundary (chronological median of groups 1+2 pooled, n={len(combined_sorted)}): "
      f"{split_time.isoformat() if split_time is not None else 'n/a — too few rows to split'}. "
      f"Train n={len(train)}, test n={len(test)}.")
    p("")

    # ── per-feature screening: continuous outcomes ─────────────────────
    p("## Individual feature screening — continuous outcomes")
    p("")
    for group_name, gdf in (("Shadow trades", df1), ("OBSERVING trajectories", df2)):
        if not len(gdf):
            continue
        p(f"### {group_name} (n={len(gdf)})")
        p("")
        for outcome in ["peak_return_30", "forward_return_30", "max_drawdown_30", "peak_return_60"]:
            if outcome not in gdf.columns:
                continue
            p(f"**vs `{outcome}`**")
            p("")
            p("| feature | spearman rho | p | n |")
            p("|---|---|---|---|")
            screen = feature_vs_outcome_screen(gdf, NUMERIC_FEATURES, outcome)
            screen = screen.sort_values("spearman_rho", key=lambda s: s.abs(), ascending=False, na_position="last")
            for _, row in screen.iterrows():
                tag = confidence_tag(row["n"])
                rho = row["spearman_rho"]
                rho_str = "n/a" if rho is None or (isinstance(rho, float) and np.isnan(rho)) else f"{rho:+.3f}"
                p(f"| {row['feature']} | {rho_str} | {fmt_p(row['p_value'])} | {row['n']}{tag} |")
            p("")

    # ── rug flag screening ───────────────────────────────────────────
    p("## Individual feature screening — rug/failure outcome")
    p("")
    p("`rug_flag` = observation_exit_reason/rejection_reason containing TOKEN_GONE (data-availability rug "
      "signal) OR max_drawdown_full <= -80% (price-based rug signal). Both counted as failure but they measure "
      "different things — see the breakdown below before the combined-flag table. **Read the base rate before "
      "the p-values below**: for shadow trades, rug_flag_price alone is true for the MAJORITY of the group "
      "(see per-group counts) — an 80%+ intra-lifetime drawdown is the typical outcome for a memecoin here, "
      "not a rare tail event, so this section is really answering \"what predicts survival\" (the minority "
      "class), not \"what predicts a rare failure.\"")
    p("")
    p("**Feature collinearity warning**: bp_p1/bp_p2/bp_p3 are ~0.92-0.98 correlated with each other in this "
      "dataset (same underlying buy-pressure metric across adjacent rolling windows, not independent signals), "
      "and vlr_p1/p2/p3 likely behave similarly. `wash_multiplier_at_discovery` and `buy_sell_ratio_at_discovery` "
      "are correlated at ~1.0 — they are the same buys/sells ratio computed twice, not two signals; the "
      "shortlist below treats them as one finding. Several \"different\" findings below and in the threshold "
      "sections that name different p1/p2/p3 variants of the same base metric are effectively re-detections of "
      "one signal, not independent confirmations — treat a cluster of bp_p* or vlr_p* hits together as ONE "
      "candidate finding, not three.")
    p("")
    for group_name, gdf in (("Shadow trades", df1), ("OBSERVING trajectories", df2)):
        if not len(gdf):
            continue
        n_gone = int(gdf["rug_flag_gone"].sum())
        n_price = int(gdf["rug_flag_price"].sum())
        n_either = int(gdf["rug_flag"].sum())
        p(f"### {group_name} (n={len(gdf)}): rug_flag_gone={n_gone}, rug_flag_price={n_price} "
          f"({n_price/len(gdf)*100:.0f}% of group), either={n_either}")
        p("")
        p("| feature | n rug | n non-rug | median rug | median non-rug | p |")
        p("|---|---|---|---|---|---|")
        screen = rug_flag_screen(gdf, NUMERIC_FEATURES)
        for _, row in screen.iterrows():
            if row.get("note") == "n<5 in one group":
                p(f"| {row['feature']} | {row['n_rug']} | {row['n_non_rug']} | n/a | n/a | insufficient n |")
                continue
            tag = confidence_tag(min(row["n_rug"], row["n_non_rug"]))
            p(f"| {row['feature']} | {row['n_rug']} | {row['n_non_rug']} | {row['median_rug']:.4g} | "
              f"{row['median_non_rug']:.4g} | {fmt_p(row['p_value'])}{tag} |")
        p("")

    # ── nonlinear threshold search ──────────────────────────────────
    p("## Nonlinear threshold search (SEARCHED — see time-split validation below before trusting any of these)")
    p("")
    threshold_findings = []
    for group_name, gdf in (("shadow", df1), ("observing", df2)):
        if not len(gdf):
            continue
        for feat in NUMERIC_FEATURES:
            if feat not in gdf.columns:
                continue
            for target, is_bin in (("peak_return_30", False), ("rug_flag", True)):
                if target not in gdf.columns:
                    continue
                best = decile_threshold_search(gdf, feat, target, is_bin)
                if best:
                    best["group"] = group_name
                    threshold_findings.append(best)
    tf_df = pd.DataFrame(threshold_findings)
    if len(tf_df):
        tf_df = tf_df.sort_values("p_value")
        p("Top 15 by p-value (uncorrected for multiple comparisons — treat as leads, not conclusions):")
        p("")
        p("| group | feature | target | split (q) | n above / below | mean above / below | effect | p |")
        p("|---|---|---|---|---|---|---|---|")
        for _, row in tf_df.head(15).iterrows():
            tag = confidence_tag(min(row["n_above"], row["n_below"]))
            p(f"| {row['group']} | {row['feature']} | {row['target']} | {row['split_value']:.4g} (q{row['split_quantile']}) "
              f"| {row['n_above']} / {row['n_below']} | {row['mean_above']:.4g} / {row['mean_below']:.4g} "
              f"| {row['effect']:+.4g} | {fmt_p(row['p_value'])}{tag} |")
        p("")
    else:
        p("No threshold candidates met the minimum n (5 per side) for a decile split.")
        p("")

    # ── interactions ───────────────────────────────────────────────
    p("## Feature interactions (top individually-promising features, 2x2 median split, Kruskal-Wallis)")
    p("")
    interaction_findings = []
    for group_name, gdf in (("shadow", df1), ("observing", df2)):
        if not len(gdf) or "peak_return_30" not in gdf.columns:
            continue
        screen = feature_vs_outcome_screen(gdf, NUMERIC_FEATURES, "peak_return_30")
        screen = screen.dropna(subset=["spearman_rho"]).sort_values("spearman_rho", key=lambda s: s.abs(), ascending=False)
        top_feats = screen.head(5)["feature"].tolist()
        for i in range(len(top_feats)):
            for j in range(i + 1, len(top_feats)):
                for target in ("peak_return_30", "rug_flag"):
                    res = kruskal_interaction(gdf, top_feats[i], top_feats[j], target)
                    if res:
                        res["group"] = group_name
                        interaction_findings.append(res)
    if interaction_findings:
        for res in interaction_findings:
            p(f"**[{res['group']}] {res['feat_a']} x {res['feat_b']} -> {res['target']}** (n={res['n']})")
            if res["note"]:
                p(f"  - {res['note']} (cell n: {res['cell_n']})")
            else:
                p(f"  - cell n: {res['cell_n']}")
                p(f"  - cell means: {res['cell_means']}")
                p(f"  - Kruskal-Wallis stat={res['stat']:.3f}, p={fmt_p(res['p_value'])}{confidence_tag(min(res['cell_n'].values()))}")
            p("")
    else:
        p("No interaction candidates met the minimum sample size (n>=40 pooled, n>=10 per cell).")
        p("")

    # ── score value-add ─────────────────────────────────────────────
    p("## Does the composite score add information beyond the raw rolling-window components?")
    p("")
    for group_name, gdf in (("Shadow trades", df1), ("OBSERVING trajectories", df2)):
        if not len(gdf):
            continue
        res = score_value_add(gdf, outcome="peak_return_30")
        p(f"**{group_name}** (n={res['n']})")
        if res.get("note"):
            p(f"  - {res['note']}")
        else:
            p(f"  - Raw score vs peak_return_30: spearman rho={res['raw_score_spearman_rho']:+.3f}, "
              f"p={fmt_p(res['raw_score_p_value'])}")
            p(f"  - Score's residual (after regressing both score and outcome on bp/vm/vlr p1-3) vs outcome's "
              f"residual: partial spearman rho={res['partial_spearman_rho']:+.3f}, p={fmt_p(res['partial_p_value'])}"
              f"{confidence_tag(res['n'])}")
            p("  - Interpretation: a partial rho near zero means score is not adding predictive information "
              "beyond what bp/vm/vlr already carry linearly — it would just be re-deriving the same signal.")
        p("")

    # ── time-split validation ────────────────────────────────────────
    p("## Time-split validation")
    p("")
    if split_time is None:
        p("Too few pooled rows to perform a meaningful time split — skipped.")
        p("")
    else:
        p(f"Train = decisions before {split_time.isoformat()} (n={len(train)}); "
          f"test = decisions at/after it (n={len(test)}).")
        p("")
        validated = []
        for _, row in tf_df.sort_values("p_value").head(10).iterrows() if len(tf_df) else []:
            feat, target, split_val = row["feature"], row["target"], row["split_value"]
            for label, part in (("train", train), ("test", test)):
                sub = part[part["group"] == row["group"]] if "group" in part.columns else part
                sub = sub[[feat, target]].dropna() if feat in sub.columns and target in sub.columns else pd.DataFrame()
                if len(sub) < 10:
                    continue
                above = sub[sub[feat].astype(float) > split_val][target].astype(float)
                below = sub[sub[feat].astype(float) <= split_val][target].astype(float)
                if len(above) < 5 or len(below) < 5:
                    continue
                effect = above.mean() - below.mean()
                validated.append(dict(feature=feat, target=target, group=row["group"],
                                       split=label, n_above=len(above), n_below=len(below),
                                       effect=effect))
        if validated:
            vdf = pd.DataFrame(validated)
            p("| feature | target | group | split | n above/below | effect |")
            p("|---|---|---|---|---|---|")
            for _, row in vdf.iterrows():
                p(f"| {row['feature']} | {row['target']} | {row['group']} | {row['split']} | "
                  f"{row['n_above']}/{row['n_below']} | {row['effect']:+.4g} |")
            p("")
            p("A finding whose effect changes sign between train and test rows above should be treated as "
              "noise, not a real edge — check each feature's train/test pair before adding it to the shortlist below.")
            p("")
        else:
            p("No train/test finding had enough rows on both sides (n>=5 per group) to compare.")
            p("")

    # ── real trade case studies ──────────────────────────────────────
    p("## Case studies: the 2 real paper trades (n=2 — never pooled into any statistic above)")
    p("")
    for c in g3:
        p(f"### Trade {c['trade_id'][:8]} ({c['entry_source']})")
        p(f"- entry_time={c['entry_time'].isoformat()}, entry_price={c['entry_price']}, "
          f"entry_composite_score={c['entry_composite_score']}")
        p(f"- entry_bp_trend={c['entry_bp_trend']}, entry_vm_trend={c['entry_vm_trend']}, "
          f"entry_vm_zone={c['entry_vm_zone']}, entry_liquidity_flag={c['entry_liquidity_flag']}, "
          f"entry_liquidity_trend={c['entry_liquidity_trend']}")
        p(f"- exit_time={c['exit_time'].isoformat() if c['exit_time'] else 'n/a'}, exit_price={c['exit_price']}, "
          f"exit_reason={c['exit_reason']}, pnl_pct={fmt_pct(c['pnl_pct'])}, "
          f"hold_duration_seconds={c['hold_duration_seconds']}")
        p(f"- price path source: {c['price_path_source']} ({c['n_price_points']} points)")
        p(f"- MFE={fmt_pct(c['mfe_pct'])}, MAE={fmt_pct(c['mae_pct'])}")
        p("")

    # ── shortlist ──────────────────────────────────────────────────
    p("## Candidate entry hypotheses for prospective paper testing")
    p("")
    p("Selection rule applied: a candidate below survived (a) the decile threshold search, (b) had the same "
      "sign of effect in both train and test slices in the validation table above, and (c) both sides had "
      "n>=5. Nothing here has been implemented — this is a shortlist to test prospectively, not a change.")
    p("")
    shortlisted = []
    if validated:
        vdf = pd.DataFrame(validated)
        for (feat, target, group), sub in vdf.groupby(["feature", "target", "group"]):
            if set(sub["split"]) != {"train", "test"}:
                continue
            tr = sub[sub["split"] == "train"].iloc[0]
            te = sub[sub["split"] == "test"].iloc[0]
            if tr["effect"] == 0 or te["effect"] == 0:
                continue
            same_sign = (tr["effect"] > 0) == (te["effect"] > 0)
            if same_sign:
                shortlisted.append((feat, target, group, tr, te))

    # Collapse correlated feature-family duplicates (bp_p1/p2/p3,
    # vlr_p1/p2/p3, vm_p1/p2/p3 are the same underlying rolling-window
    # metric at adjacent time steps, not independent signals — see the
    # collinearity warning in the rug-flag section) into one shortlist
    # entry per (family, target, group) instead of listing near-identical
    # re-detections separately.
    # wash_multiplier_at_discovery IS buy_sell_ratio_at_discovery (both are
    # buys/sells from the same Tier1-time counts) — confirmed correlation
    # ~1.0 in this dataset, not two signals.
    ALIAS = {"wash_multiplier_at_discovery": "buy_sell_ratio_at_discovery"}
    seen_families = set()
    deduped = []
    for feat, target, group, tr, te in shortlisted:
        canon = ALIAS.get(feat, feat)
        fam_key = (canon.split("_p")[0] if "_p" in canon and canon[-1].isdigit() else canon, target, group)
        if fam_key in seen_families:
            continue
        seen_families.add(fam_key)
        deduped.append((feat, target, group, tr, te))
    n_collapsed = len(shortlisted) - len(deduped)
    if n_collapsed:
        p(f"({n_collapsed} additional threshold hit(s) omitted here as correlated re-detections of the same "
          f"family — e.g. bp_p1/bp_p2/bp_p3 collapsed to one entry. Full list is in the dataset CSV / validation table above.)")
        p("")
    shortlisted = deduped

    if shortlisted:
        for feat, target, group, tr, te in shortlisted:
            p(f"- **{group}: {feat} split -> {target}**. Train effect {tr['effect']:+.4g} "
              f"(n={tr['n_above']}/{tr['n_below']}), test effect {te['effect']:+.4g} "
              f"(n={te['n_above']}/{te['n_below']}). Same sign in both halves.")
            p(f"  - Rationale: higher/lower {feat} at decision time associates with different {target} "
              f"in both the earlier and later half of the dataset, not just in-sample.")
            p(f"  - What a prospective test needs to track: composite score AND raw {feat} at decision time "
              f"for every future candidate, plus realized {target}-equivalent outcome, kept separate from "
              "any change to actual entry logic.")
            p("")
    else:
        p("**No candidate survived train/test validation with the same sign on both sides.** "
          "Every threshold pattern found in the full-sample search either reversed sign, vanished, or lacked "
          "enough rows on one side of the time split. This is itself a finding: on the current sample size, "
          "there is no feature/threshold combination that looks like a robust, non-overfit edge yet.")
        p("")

    report_path = OUTPUT_DIR / f"edge_research_{ts_tag}.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")

    # ── CSV dump ──────────────────────────────────────────────────
    csv_path = OUTPUT_DIR / f"edge_research_dataset_{ts_tag}.csv"
    if len(combined):
        export = combined.copy()
        if split_time is not None:
            export["split"] = np.where(export["decision_time"] < split_time, "train", "test")
        export.to_csv(csv_path, index=False)
    else:
        csv_path.write_text("no data\n")

    print(f"Report: {report_path}")
    print(f"Dataset CSV: {csv_path}")
    print(f"Group sizes: shadow={len(df1)}, observing={len(df2)}, trades={len(g3)}")
    print(f"Shortlisted hypotheses: {len(shortlisted)}")


if __name__ == "__main__":
    asyncio.run(main())
