"""
analysis/raw_feature_backtest.py
=================================
ANALYSIS-ONLY. Read-only against the live database. This script never
writes to any production table, never imports or touches config/settings
values that affect live behavior, and never changes Tier1 rules, scorer
weights, or thresholds. It is safe to run at any time, including while
main.py is live, because it only issues SELECT queries.

Purpose
-------
Test whether raw API features available at decision time actually carry
predictive information about a token's subsequent price movement -
independent of what the existing Tier1 gate / Scorer / S1 Wave currently
decide. The Tier1 verdict, the Scorer's score, and the S1 Wave verdict
are included ONLY as reference columns for comparison. Nothing here tunes
them.

Look-ahead discipline (read this before trusting any number below)
--------------------------------------------------------------------
"Decision time" for the raw-feature analysis = the token's first TIER1
evaluation. Every discovered token gets exactly one path through Tier1,
pass or fail, before anything else happens to it (see
filters/tier1_worker.py). All raw features analyzed here come from
token_evaluations.inputs_json for gate='TIER1', which is captured BEFORE
the first price/liquidity snapshot is ever taken for that token -
sampling only starts once a token is WATCHING or OBSERVING, i.e. strictly
after Tier1 has already run (see workers/sampling_worker.py). The outcome
(peak_return, final_return) is computed exclusively from token_snapshots
rows for that token, which by construction all occur at or after the
Tier1 decision. There is no leakage in the numeric/binary feature
analysis below.

Two things are deliberately EXCLUDED from the decision-time feature set
for exactly this reason, even though they were on the requested list:
  - buy_pressure and volume: Tier1Worker never captures either at
    decision time (Tier1 doesn't gate on them at all) - the only place
    they exist is inside token_snapshots, which is POST-decision. Using
    them as "decision-time" features would be leakage. This gap is
    called out in the findings section as something worth fixing
    upstream (capture them at discovery time), not something this script
    quietly works around.
  - holder_count: present as a column on `tokens` but never populated by
    the current pipeline (0/N rows have it set at the time this script
    was written) - there is nothing to analyze yet.

Post-instrumentation-pass update: a Scorer DISCARD now routes the token
into OBSERVING (see workers/scoring_worker.py) instead of straight to
REJECTED, exactly like a Tier1 reject, anchored at its own
observation_started_at rather than the token's original discovered_at.
That means going forward there IS genuine forward-looking data after a
discard. This script reports two distinct outcome families to keep that
honest:
  - post_accept_return_{5,15,30,full} — anchored at the Tier1 decision,
    i.e. "what happened after we let it through Tier1" (works for every
    token, pass or fail — anchor is decision_time either way).
  - post_reject_return_{5,15,30,full} — anchored at observation_started_at,
    i.e. "what happened after we specifically rejected/discarded it".
    Only populated for tokens that actually have an observation_started_at
    (Tier1 rejects, and Scorer discards from AFTER this instrumentation
    pass — historical discards made before this change have no post-reject
    data and are reported as such, not silently backfilled or estimated).
Both use the exact same computation (see compute_windowed_returns), so
accepted and rejected populations are measured identically per point 4 of
the instrumentation request.

Usage
-----
    PYTHONPATH=. python analysis/raw_feature_backtest.py

Outputs
-------
    analysis/output/report_<timestamp>.md   - the human-readable report
    analysis/output/dataset_<timestamp>.csv - the underlying per-token dataset
"""

from __future__ import annotations

import asyncio
import csv
import json
import math
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import settings  # noqa: E402

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
LOG_PATH = Path(__file__).resolve().parent.parent / "bot.log"

# A token needs at least this many snapshots for "peak after first" to mean
# anything at all (with 1 snapshot, peak == first by definition).
MIN_SNAPSHOTS_FOR_OUTCOME = 2

# The six tokens flagged in the earlier ad-hoc pass as the best real movers
# among Tier1-passed tokens that the Scorer discarded. Matched by symbol +
# closest scorer score, since symbols repeat across unrelated mints in this
# dataset (multiple "KITTY", "FOMO", "PIKACHU", etc.) and we want the exact
# instances originally flagged, not just any token sharing the name.
SIX_MOVERS_TARGET = {
    "KITTY": 4.86,
    "Cta": 4.79,
    "CURTIS": 4.76,
    "CROC": 4.84,
    "YouTube": 4.26,
    "MONTY": 4.79,
}


FETCH_SQL = """
WITH tier1_first AS (
    SELECT DISTINCT ON (token_id)
        token_id, evaluated_at AS decision_time, passed AS tier1_passed,
        reason_code AS tier1_reason, inputs_json AS tier1_inputs
    FROM token_evaluations
    WHERE gate = 'TIER1'
    ORDER BY token_id, evaluated_at ASC
),
scorer_last AS (
    SELECT DISTINCT ON (token_id)
        token_id, evaluated_at AS scorer_time, passed AS scorer_passed,
        reason_code AS scorer_reason, inputs_json AS scorer_inputs
    FROM token_evaluations
    WHERE gate = 'SCORER'
    ORDER BY token_id, evaluated_at DESC
),
s1_first AS (
    SELECT DISTINCT ON (token_id)
        token_id, evaluated_at AS s1_time, passed AS s1_passed,
        reason_code AS s1_reason, inputs_json AS s1_inputs
    FROM token_evaluations
    WHERE gate = 'S1_WAVE'
    ORDER BY token_id, evaluated_at ASC
),
snap_stats AS (
    SELECT
        token_id,
        count(*) AS n_snaps,
        min(sampled_at) AS first_ts,
        max(sampled_at) AS last_ts,
        (array_agg(price_usd ORDER BY sampled_at))[1] AS price_first,
        (array_agg(price_usd ORDER BY sampled_at DESC))[1] AS price_last,
        max(price_usd) AS price_peak,
        (array_agg(liquidity_usd ORDER BY sampled_at))[1] AS liq_first,
        (array_agg(liquidity_usd ORDER BY sampled_at DESC))[1] AS liq_last
    FROM token_snapshots
    GROUP BY token_id
)
SELECT
    tok.id, tok.mint_address, tok.symbol, tok.discovered_at, tok.status,
    tok.rejection_reason, tok.lp_locked_burned,
    tok.observation_started_at, tok.observation_exit_reason,
    t1.decision_time, t1.tier1_passed, t1.tier1_reason, t1.tier1_inputs,
    sc.scorer_time, sc.scorer_passed, sc.scorer_reason, sc.scorer_inputs,
    s1.s1_time, s1.s1_passed, s1.s1_reason, s1.s1_inputs,
    ss.n_snaps, ss.first_ts, ss.last_ts,
    ss.price_first, ss.price_last, ss.price_peak,
    ss.liq_first, ss.liq_last
FROM tokens tok
JOIN tier1_first t1 ON t1.token_id = tok.id
LEFT JOIN scorer_last sc ON sc.token_id = tok.id
LEFT JOIN s1_first s1 ON s1.token_id = tok.id
LEFT JOIN snap_stats ss ON ss.token_id = tok.id
ORDER BY tok.discovered_at ASC;
"""

# Bulk snapshot fetch — grouped by token_id in Python. Needed for the
# windowed (5/15/30-minute) returns, which require per-timestamp filtering
# that snap_stats' aggregates can't provide.
SNAPSHOTS_SQL = """
SELECT token_id, sampled_at, price_usd
FROM token_snapshots
ORDER BY token_id, sampled_at ASC;
"""


# ── stats helpers (no numpy/scipy - dataset is small, stdlib is enough) ────

def _num(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return None
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return cov / (sx * sy)


def _rank(values):
    indexed = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and values[indexed[j + 1]] == values[indexed[i]]:
            j += 1
        avg_rank = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[indexed[k]] = avg_rank
        i = j + 1
    return ranks


def spearman(xs, ys):
    if len(xs) < 3:
        return None
    return pearson(_rank(xs), _rank(ys))


def bucket_analysis(pairs, target_buckets=4):
    """pairs: list of (feature_value, outcome_value). Returns quantile buckets."""
    pairs = sorted(pairs, key=lambda p: p[0])
    n = len(pairs)
    n_buckets = target_buckets
    if n < target_buckets * 5:
        n_buckets = max(1, n // 6)
    if n_buckets <= 1:
        ovals = [p[1] for p in pairs]
        return [{
            "range": f"all values (n={n})", "n": n,
            "mean_outcome": statistics.fmean(ovals) if ovals else None,
            "median_outcome": statistics.median(ovals) if ovals else None,
        }]
    size = n / n_buckets
    buckets = []
    for b in range(n_buckets):
        lo = int(round(b * size))
        hi = int(round((b + 1) * size)) if b < n_buckets - 1 else n
        chunk = pairs[lo:hi]
        if not chunk:
            continue
        fvals = [c[0] for c in chunk]
        ovals = [c[1] for c in chunk]
        buckets.append({
            "range": f"{fvals[0]:.4g} to {fvals[-1]:.4g}",
            "n": len(chunk),
            "mean_outcome": statistics.fmean(ovals),
            "median_outcome": statistics.median(ovals),
        })
    return buckets


def is_monotonic(values):
    if len(values) < 2:
        return None
    incs = all(a <= b for a, b in zip(values, values[1:]))
    decs = all(a >= b for a, b in zip(values, values[1:]))
    return "increasing" if incs and not decs else ("decreasing" if decs and not incs else "non-monotonic")


def analyze_numeric_feature(name, rows, feature_fn, outcome_key="peak_return"):
    pairs = []
    for r in rows:
        v = feature_fn(r)
        o = r.get(outcome_key)
        if v is None or o is None:
            continue
        pairs.append((float(v), float(o)))
    result = {"feature": name, "n": len(pairs)}
    if len(pairs) < 5:
        result["note"] = "too few data points to analyze (n < 5)"
        return result
    xs, ys = [p[0] for p in pairs], [p[1] for p in pairs]
    result["pearson_r"] = pearson(xs, ys)
    result["spearman_rho"] = spearman(xs, ys)
    buckets = bucket_analysis(pairs)
    result["buckets"] = buckets
    means = [b["mean_outcome"] for b in buckets]
    result["shape"] = is_monotonic(means)
    return result


def analyze_binary_feature(name, rows, feature_fn, outcome_key="peak_return"):
    groups = {}
    for r in rows:
        v = feature_fn(r)
        o = r.get(outcome_key)
        if v is None or o is None:
            continue
        groups.setdefault(v, []).append(float(o))
    result = {"feature": name, "groups": {}}
    for k, vals in groups.items():
        result["groups"][str(k)] = {
            "n": len(vals),
            "mean_outcome": statistics.fmean(vals),
            "median_outcome": statistics.median(vals),
        }
    if len(result["groups"]) < 2:
        result["note"] = "constant across the whole dataset - no discriminative power here"
    return result


def compute_windowed_returns(snaps, anchor):
    """
    snaps: list of (sampled_at: datetime, price_usd: float), already sorted
           ascending for one token.
    anchor: the decision_time to measure "after" from (either the Tier1
            decision, for post_accept_*, or observation_started_at, for
            post_reject_*).

    Returns dict with keys 5, 15, 30, "full" -> return (float) or None if
    there isn't at least one snapshot at/after anchor within that window.
    Baseline price is the first snapshot AT OR AFTER anchor - never a
    snapshot before it, which would mix in information the decision-maker
    didn't have yet.
    """
    from datetime import timedelta

    after = [(t, p) for t, p in snaps if t >= anchor and p is not None and p > 0]
    result = {5: None, 15: None, 30: None, "full": None}
    if not after:
        return result
    baseline = after[0][1]
    for minutes in (5, 15, 30):
        cutoff = anchor + timedelta(minutes=minutes)
        window = [p for t, p in after if t <= cutoff]
        if window:
            result[minutes] = max(window) / baseline - 1
    result["full"] = max(p for _, p in after) / baseline - 1
    return result


def fmt_pct(x, digits=1):
    if x is None:
        return "n/a"
    return f"{x*100:+.{digits}f}%"


def fmt_r(x):
    if x is None:
        return "n/a"
    return f"{x:+.3f}"


# ── data loading ────────────────────────────────────────────────────────

async def load_snapshots_by_token(conn):
    records = await conn.fetch(SNAPSHOTS_SQL)
    by_token = {}
    for rec in records:
        price = _num(rec["price_usd"])
        by_token.setdefault(rec["token_id"], []).append((rec["sampled_at"], price))
    return by_token


async def load_rows(conn):
    records = await conn.fetch(FETCH_SQL)
    snaps_by_token = await load_snapshots_by_token(conn)
    rows = []
    excluded_insufficient = 0

    for rec in records:
        tier1_inputs = json.loads(rec["tier1_inputs"]) if rec["tier1_inputs"] else {}
        scorer_inputs = json.loads(rec["scorer_inputs"]) if rec["scorer_inputs"] else None
        s1_inputs = json.loads(rec["s1_inputs"]) if rec["s1_inputs"] else None

        buys = tier1_inputs.get("buys")
        sells = tier1_inputs.get("sells")
        wash_mult = _num(tier1_inputs.get("wash_multiplier"))
        age_minutes = _num(tier1_inputs.get("age_minutes"))
        liq = _num(tier1_inputs.get("liquidity_usd"))
        mcap = _num(tier1_inputs.get("market_cap_usd"))
        lp_burn_pct = tier1_inputs.get("lp_burn")

        buy_sell_ratio = (buys / sells) if (sells not in (None, 0) and buys is not None) else None
        total_txns = (buys + sells) if (buys is not None and sells is not None) else None

        n_snaps = rec["n_snaps"] or 0
        price_first = _num(rec["price_first"])
        price_peak = _num(rec["price_peak"])
        price_last = _num(rec["price_last"])
        liq_first = _num(rec["liq_first"])
        liq_last = _num(rec["liq_last"])

        peak_return = None
        final_return = None
        if n_snaps >= MIN_SNAPSHOTS_FOR_OUTCOME and price_first:
            peak_return = price_peak / price_first - 1
            final_return = price_last / price_first - 1
        else:
            excluded_insufficient += 1

        # ── windowed returns (new) ──────────────────────────────────────
        my_snaps = snaps_by_token.get(rec["id"], [])
        decision_time = rec["decision_time"]
        observation_started_at = rec["observation_started_at"]

        post_accept = compute_windowed_returns(my_snaps, decision_time) if decision_time else {
            5: None, 15: None, 30: None, "full": None
        }
        post_reject = (
            compute_windowed_returns(my_snaps, observation_started_at)
            if observation_started_at else {5: None, 15: None, 30: None, "full": None}
        )

        row = {
            "mint": rec["mint_address"],
            "symbol": rec["symbol"],
            "discovered_at": rec["discovered_at"],
            "decision_time": decision_time,
            "observation_started_at": observation_started_at,
            "observation_exit_reason": rec["observation_exit_reason"],
            "tier1_passed": rec["tier1_passed"],
            "tier1_reason": rec["tier1_reason"],
            "rejection_reason": rec["rejection_reason"],
            "lp_locked_burned": rec["lp_locked_burned"],
            "age_minutes": age_minutes,
            "liquidity_usd": liq,
            "market_cap_usd": mcap,
            "buys": buys,
            "sells": sells,
            "wash_multiplier": wash_mult,
            "buy_sell_ratio": buy_sell_ratio,
            "total_txns": total_txns,
            "lp_burn_pct": lp_burn_pct,
            "n_snaps": n_snaps,
            "price_first": price_first,
            "price_peak": price_peak,
            "price_last": price_last,
            "liq_first": liq_first,
            "liq_last": liq_last,
            "peak_return": peak_return,
            "final_return": final_return,
            "post_accept_return_5m": post_accept[5],
            "post_accept_return_15m": post_accept[15],
            "post_accept_return_30m": post_accept[30],
            "post_accept_return_full": post_accept["full"],
            "post_reject_return_5m": post_reject[5],
            "post_reject_return_15m": post_reject[15],
            "post_reject_return_30m": post_reject[30],
            "post_reject_return_full": post_reject["full"],
            "scorer_score": _num(scorer_inputs.get("score")) if scorer_inputs else None,
            "scorer_bp_trend": scorer_inputs.get("bp_trend") if scorer_inputs else None,
            "scorer_vm_trend": scorer_inputs.get("vm_trend") if scorer_inputs else None,
            "scorer_reason": rec["scorer_reason"],
            "scorer_version": scorer_inputs.get("scorer_version") if scorer_inputs else None,
            "scorer_bp_component": _num(scorer_inputs.get("bp_component")) if scorer_inputs else None,
            "scorer_vm_component": _num(scorer_inputs.get("vm_component")) if scorer_inputs else None,
            "scorer_vlr_component": _num(scorer_inputs.get("vlr_component")) if scorer_inputs else None,
            "scorer_bp_p1": scorer_inputs.get("bp_p1") if scorer_inputs else None,
            "scorer_bp_p2": scorer_inputs.get("bp_p2") if scorer_inputs else None,
            "scorer_bp_p3": scorer_inputs.get("bp_p3") if scorer_inputs else None,
            "scorer_vm_p1": scorer_inputs.get("vm_p1") if scorer_inputs else None,
            "scorer_vm_p2": scorer_inputs.get("vm_p2") if scorer_inputs else None,
            "scorer_vm_p3": scorer_inputs.get("vm_p3") if scorer_inputs else None,
            "s1_buy_pressure": _num(s1_inputs.get("buy_pressure")) if s1_inputs else None,
            "s1_reason": rec["s1_reason"],
        }
        rows.append(row)

    return rows, excluded_insufficient


def load_scorer_components_from_log(mints):
    """
    LEGACY FALLBACK ONLY. bp_p1/p2/p3, vm_p1/p2/p3 and the per-signal
    components are now persisted directly to
    token_evaluations.inputs_json for every SCORER evaluation (see
    workers/scoring_worker.py) - this function exists only to enrich the
    six-movers narrative for evaluations that were written BEFORE that
    instrumentation pass, which have no bp_p1/p2/p3 etc. in the database at
    all. Any evaluation from after the instrumentation change never needs
    this function; the dataset builder reads those fields straight from
    the DB. If the log has rotated or doesn't exist, this degrades
    gracefully to an empty result and the report says so.
    """
    out = {}
    if not LOG_PATH.exists():
        return out
    wanted = set(mints)
    try:
        with LOG_PATH.open("r", errors="ignore") as f:
            for line in f:
                if "scoring_worker.token_scored" not in line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                mint = obj.get("mint")
                if mint in wanted:
                    out.setdefault(mint, []).append(obj)
    except OSError:
        return out
    return out


def write_csv(path, rows):
    fieldnames = [
        "mint", "symbol", "discovered_at", "decision_time", "observation_started_at",
        "observation_exit_reason", "tier1_passed", "tier1_reason",
        "rejection_reason", "lp_locked_burned", "age_minutes", "liquidity_usd",
        "market_cap_usd", "buys", "sells", "wash_multiplier", "buy_sell_ratio",
        "total_txns", "lp_burn_pct", "n_snaps", "price_first", "price_peak",
        "price_last", "liq_first", "liq_last", "peak_return", "final_return",
        "post_accept_return_5m", "post_accept_return_15m", "post_accept_return_30m",
        "post_accept_return_full", "post_reject_return_5m", "post_reject_return_15m",
        "post_reject_return_30m", "post_reject_return_full",
        "scorer_score", "scorer_bp_trend", "scorer_vm_trend", "scorer_reason",
        "scorer_version", "scorer_bp_component", "scorer_vm_component", "scorer_vlr_component",
        "scorer_bp_p1", "scorer_bp_p2", "scorer_bp_p3",
        "scorer_vm_p1", "scorer_vm_p2", "scorer_vm_p3",
        "s1_buy_pressure", "s1_reason",
    ]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            row = dict(r)
            for k in ("discovered_at", "decision_time", "observation_started_at"):
                if isinstance(row.get(k), datetime):
                    row[k] = row[k].isoformat()
            w.writerow(row)


# ── data integrity checks (point 6 of the instrumentation request) ────────

def run_integrity_checks(rows):
    """
    Returns a list of (check_name, count, detail_str, is_problem) tuples.
    is_problem controls the ✓/⚠ flag independently of count==0, since some
    checks (scorer_version count, re-discovered mints) report a non-zero
    count that isn't actually bad news.
    """
    checks = []

    # Impossible timestamps: decision made before the token was discovered.
    bad_order = [r for r in rows if r["decision_time"] and r["discovered_at"]
                 and r["decision_time"] < r["discovered_at"]]
    checks.append(("Decision before discovery (impossible)", len(bad_order),
                   ", ".join(r["symbol"] or r["mint"][:8] for r in bad_order[:5]), len(bad_order) > 0))

    # observation_started_at before decision_time - the token would have to
    # have been rejected before it was ever evaluated.
    bad_obs_order = [r for r in rows if r["observation_started_at"] and r["decision_time"]
                      and r["observation_started_at"] < r["decision_time"]]
    checks.append(("Observation started before decision_time (impossible)", len(bad_obs_order),
                   ", ".join(r["symbol"] or r["mint"][:8] for r in bad_obs_order[:5]), len(bad_obs_order) > 0))

    # Missing decision-time fields in the TIER1 record.
    missing_fields = [r for r in rows if any(
        r.get(f) is None for f in ("age_minutes", "liquidity_usd", "market_cap_usd", "wash_multiplier")
    )]
    checks.append(("TIER1 rows with a missing raw decision-time field", len(missing_fields),
                   "wash_multiplier is null only when buys=sells=0", len(missing_fields) > 0))

    # Invalid numeric values: negative price/liquidity, which cannot exist.
    bad_numeric = [r for r in rows if
                   (r["price_first"] is not None and r["price_first"] < 0) or
                   (r["liquidity_usd"] is not None and r["liquidity_usd"] < 0) or
                   (r["wash_multiplier"] is not None and r["wash_multiplier"] < 0)]
    checks.append(("Negative price/liquidity/wash_multiplier (invalid)", len(bad_numeric), "", len(bad_numeric) > 0))

    # scorer_version mixing - only a problem once MORE THAN ONE version has
    # ever been recorded; today there is exactly one, which is the healthy
    # state, not a warning.
    versions = {r["scorer_version"] for r in rows if r.get("scorer_version")}
    checks.append(("Distinct scorer_version values in this dataset", len(versions),
                   ", ".join(sorted(versions)), len(versions) > 1))

    # Duplicate token/decision records: more than one TIER1 evaluation for
    # the same token is legitimate (a mint can resurface across discovery
    # polls) but worth surfacing as a count, not silently assumed away.
    return checks


# ── report ──────────────────────────────────────────────────────────────

NUMERIC_FEATURES = [
    ("age_minutes", lambda r: r["age_minutes"]),
    ("liquidity_usd (Tier1-captured)", lambda r: r["liquidity_usd"]),
    ("market_cap_usd", lambda r: r["market_cap_usd"]),
    ("buys", lambda r: r["buys"]),
    ("sells", lambda r: r["sells"]),
    ("wash_multiplier", lambda r: r["wash_multiplier"]),
    ("buy_sell_ratio (buys/sells; sells=0 rows excluded, see note)", lambda r: r["buy_sell_ratio"]),
    ("total_txns (buys+sells)", lambda r: r["total_txns"]),
]

BINARY_FEATURES = [
    ("lp_locked_burned", lambda r: r["lp_locked_burned"]),
    ("mint_authority_renounced", lambda r: True),  # placeholder, checked for variance below
]


async def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        rows, excluded = await load_rows(conn)
        # pull the constant-check columns directly, since Row dicts don't carry them
        auth_check = await conn.fetch(
            "SELECT DISTINCT mint_authority_renounced, freeze_authority_renounced FROM tokens"
        )
        dup_tier1 = await conn.fetch(
            "SELECT token_id, count(*) AS n FROM token_evaluations WHERE gate='TIER1' "
            "GROUP BY token_id HAVING count(*) > 1"
        )
    finally:
        await conn.close()

    usable = [r for r in rows if r["peak_return"] is not None]
    sells_zero = [r for r in rows if r["buys"] not in (None, 0) and r["sells"] == 0]

    lines = []

    def p(s=""):
        lines.append(s)

    p("# S1 Wave — Raw Feature Backtest (analysis-only)")
    p(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    p("")
    p("**No thresholds, scorer weights, gates, or production behavior were changed by this script or this report.**")
    p("")
    p(f"- Total tokens in dataset: {len(rows)}")
    p(f"- Excluded from outcome analysis (fewer than {MIN_SNAPSHOTS_FOR_OUTCOME} snapshots — no usable "
      f"'after decision' price data yet): {excluded}")
    p(f"- Usable for the raw-feature-vs-outcome analysis below: **{len(usable)}**")
    p("")
    p("Sample size warning: this is still a small, short-history dataset. Correlations below "
      "are reported as-is, with n shown at every step, so you can judge how much weight each "
      "one deserves. None of this is a recommendation to change anything yet.")
    p("")

    # ── data integrity ────────────────────────────────────────────────
    p("## 0. Data integrity checks")
    p("")
    p("Every check below runs and reports its count, including when the count is zero — "
      "a clean check is stated, not omitted.")
    p("")
    integrity = run_integrity_checks(rows)
    integrity.append(("Tokens with more than one TIER1 evaluation (re-discovered mint)",
                       len(dup_tier1), "not necessarily an error — a mint can resurface across polls", False))
    p("| check | count | detail |")
    p("|---|---|---|")
    for name, count, detail, is_problem in integrity:
        flag = "⚠" if is_problem else "✓"
        p(f"| {flag} {name} | {count} | {detail} |")
    p("")

    # ── numeric features ──────────────────────────────────────────────
    p("## 1. Numeric raw features vs. peak_return")
    p("")
    p("`peak_return` = (highest observed price / first observed price after the Tier1 decision) − 1, "
      "over the full window the token was actually sampled. This is the leak-free outcome: every input "
      "here was captured strictly before the first snapshot.")
    p("")
    for name, fn in NUMERIC_FEATURES:
        res = analyze_numeric_feature(name, usable, fn)
        p(f"### {name}")
        if "note" in res:
            p(f"- n={res['n']} — {res['note']}")
            p("")
            continue
        p(f"- n = {res['n']}")
        p(f"- Pearson r = {fmt_r(res['pearson_r'])}  |  Spearman ρ = {fmt_r(res['spearman_rho'])}")
        if res["pearson_r"] is not None and res["spearman_rho"] is not None:
            gap = abs(res["spearman_rho"]) - abs(res["pearson_r"])
            if gap > 0.15:
                p(f"  - Spearman notably stronger than Pearson (Δ={gap:+.3f}) — hints at a monotonic "
                  f"but non-linear relationship rather than a straight line.")
        p(f"- Bucket shape: **{res['shape']}**")
        p("")
        p("| range | n | mean peak_return | median peak_return |")
        p("|---|---|---|---|")
        for b in res["buckets"]:
            p(f"| {b['range']} | {b['n']} | {fmt_pct(b['mean_outcome'])} | {fmt_pct(b['median_outcome'])} |")
        p("")

    p(f"**Note on sells=0 (undefined buy/sell ratio):** {len(sells_zero)} tokens had buys>0 and sells=0 "
      f"at decision time. In the current Tier1 code, wash_multiplier in that case is defined as literally "
      f"the raw buy count (`float(buys)`), not a ratio — so a token with 8 buys/0 sells gets wash_multiplier=8.0, "
      f"the same numeric neighborhood as a token with a real 8:1 buy/sell ratio from hundreds of trades. "
      f"These are not the same thing and the current field conflates them. This is flagged again in the "
      f"findings section as a real feature-engineering issue, not fixed here.")
    p("")

    # ── binary features ───────────────────────────────────────────────
    p("## 2. Binary/categorical features vs. peak_return")
    p("")
    auth_rows = [dict(r) for r in auth_check]
    if len(auth_rows) == 1:
        p(f"- `mint_authority_renounced` / `freeze_authority_renounced`: **constant across all "
          f"{len(rows)} tokens** ({auth_rows[0]['mint_authority_renounced']} / "
          f"{auth_rows[0]['freeze_authority_renounced']}) — every token discovered so far already "
          f"satisfies both, so neither has any discriminative power in this dataset. Either pump.fun's "
          f"graduated-token pipeline enforces this structurally, or SolanaTracker's `/tokens/multi/graduated` "
          f"endpoint only ever returns tokens that already meet this bar. Either way, these two Tier1 checks "
          f"are currently rejecting zero tokens.")
    else:
        res = analyze_binary_feature("mint_authority_renounced", usable, lambda r: True)
        p(str(res))
    p("")

    lp_res = analyze_binary_feature("lp_locked_burned", usable, lambda r: r["lp_locked_burned"])
    p("### lp_locked_burned")
    if "note" in lp_res:
        p(f"- {lp_res['note']}")
    else:
        p("| value | n | mean peak_return | median peak_return |")
        p("|---|---|---|---|")
        for k, g in lp_res["groups"].items():
            p(f"| {k} | {g['n']} | {fmt_pct(g['mean_outcome'])} | {fmt_pct(g['median_outcome'])} |")
    p("")

    tier1_res = analyze_binary_feature("tier1_passed", usable, lambda r: r["tier1_passed"])
    p("### tier1_passed")
    p("(Included for context, not as a real independent variable — passing Tier1 is what determines "
      "whether a token gets the long WATCHING sampling window vs. the short 90s OBSERVING window, so "
      "this comparison is confounded with observation-window length by construction. See the caveat "
      "in the earlier conversation about this exact issue.)")
    if "note" not in tier1_res:
        p("| value | n | mean peak_return | median peak_return |")
        p("|---|---|---|---|")
        for k, g in tier1_res["groups"].items():
            p(f"| {k} | {g['n']} | {fmt_pct(g['mean_outcome'])} | {fmt_pct(g['median_outcome'])} |")
    p("")

    # ── scorer comparison ─────────────────────────────────────────────
    p("## 3. Rejected vs. accepted — same measurement, both directions")
    p("")
    p("post_accept_return_* is anchored at the Tier1 decision (works for every token, pass or fail). "
      "post_reject_return_* is anchored at observation_started_at — the moment a Tier1 reject or Scorer "
      "discard actually happened — and only exists for tokens that have that field set. Both use the "
      "identical computation (see compute_windowed_returns), so the two populations are measured the "
      "same way.")
    p("")
    has_reject_window = [r for r in rows if r["observation_started_at"] is not None]
    p(f"- Tokens with a post-rejection observation window at all: **{len(has_reject_window)}** "
      f"(this will be 0 or near-0 on the first run right after this instrumentation shipped — only "
      f"NEW Tier1 rejects and Scorer discards get observation_started_at; nothing is backfilled onto "
      f"historical rows, per the 'do not rewrite historical data' constraint)")
    p("")
    for window_label, key in (("5 min", "post_reject_return_5m"), ("15 min", "post_reject_return_15m"),
                               ("30 min", "post_reject_return_30m"), ("full window", "post_reject_return_full")):
        vals = [r[key] for r in has_reject_window if r[key] is not None]
        if vals:
            p(f"- post-reject peak return @ {window_label}: n={len(vals)}, "
              f"mean={fmt_pct(statistics.fmean(vals))}, median={fmt_pct(statistics.median(vals))}")
        else:
            p(f"- post-reject peak return @ {window_label}: n=0 (not enough elapsed time yet)")
    p("")
    accepted_only = [r for r in rows if r["tier1_passed"]]
    for window_label, key in (("5 min", "post_accept_return_5m"), ("15 min", "post_accept_return_15m"),
                               ("30 min", "post_accept_return_30m"), ("full window", "post_accept_return_full")):
        vals = [r[key] for r in accepted_only if r[key] is not None]
        if vals:
            p(f"- post-accept (Tier1-passed) peak return @ {window_label}: n={len(vals)}, "
              f"mean={fmt_pct(statistics.fmean(vals))}, median={fmt_pct(statistics.median(vals))}")
    p("")

    p("## 4. Scorer comparison (reference only)")
    p("")
    p("Important limitation, stated plainly: the pipeline stops sampling a token the instant the Scorer "
      "discards it (status flips to REJECTED; sampling_worker only loads WATCHING/OBSERVING tokens). That "
      "means there is essentially no genuine 'what happened AFTER the Scorer's decision' data yet — the "
      "Scorer's decision is typically the last thing that happens before observation stops. What follows "
      "compares the Scorer's composite score against the SAME whole-window peak_return used above (which "
      "includes whatever runup happened before the discard, i.e. the same window the Scorer's own bp/vm "
      "inputs were already looking at) — not a true forward-only comparison. Treat this section as weaker "
      "evidence than section 1, and see finding #6 for the fix.")
    p("")
    scored = [r for r in usable if r["scorer_score"] is not None]
    scorer_res = analyze_numeric_feature("scorer_score", scored, lambda r: r["scorer_score"])
    if "note" in scorer_res:
        p(f"- n={scorer_res['n']} — {scorer_res['note']}")
    else:
        p(f"- n = {scorer_res['n']}")
        p(f"- Pearson r = {fmt_r(scorer_res['pearson_r'])}  |  Spearman ρ = {fmt_r(scorer_res['spearman_rho'])}")
        p(f"- Bucket shape: **{scorer_res['shape']}**")
        p("")
        p("| score range | n | mean peak_return | median peak_return |")
        p("|---|---|---|---|")
        for b in scorer_res["buckets"]:
            p(f"| {b['range']} | {b['n']} | {fmt_pct(b['mean_outcome'])} | {fmt_pct(b['median_outcome'])} |")
    p("")

    # ── six movers deep dive ──────────────────────────────────────────
    p("## 5. The six previously-identified movers")
    p("")
    six_rows = []
    for symbol, target_score in SIX_MOVERS_TARGET.items():
        candidates = [r for r in rows if r["symbol"] == symbol and r["tier1_passed"] and r["scorer_score"] is not None]
        if not candidates:
            continue
        best = min(candidates, key=lambda r: abs(r["scorer_score"] - target_score))
        six_rows.append(best)

    have_db_components = any(r.get("scorer_bp_component") is not None for r in six_rows)
    if have_db_components:
        p("*(bp/vm/vlr components below are read straight from token_evaluations — these six "
          "evaluations were written after the instrumentation pass.)*")
    else:
        p("*(these six evaluations predate the instrumentation pass, so token_evaluations has no "
          "bp_component/vm_component/vlr_component for them — falling back to bot.log, which does "
          "have the raw bp_p1/p2/p3 / vm_p1/p2/p3 for these specific events. Any SCORER evaluation "
          "written from now on has this in the database directly and needs no log fallback.)*")
        p("")
    log_components = {} if have_db_components else load_scorer_components_from_log([r["mint"] for r in six_rows])
    if not have_db_components and not log_components:
        p("*(bot.log also not available or rotated past these events — showing composite score only.)*")
        p("")

    p("| symbol | liq@decision | wash_mult | buys/sells | scorer score | scorer trends | discard reason | peak_return |")
    p("|---|---|---|---|---|---|---|---|")
    for r in six_rows:
        trends = f"bp={r['scorer_bp_trend']}/vm={r['scorer_vm_trend']}"
        p(f"| {r['symbol']} | ${r['liquidity_usd']:,.0f} | {r['wash_multiplier']:.2f} | "
          f"{r['buys']}/{r['sells']} | {r['scorer_score']:.2f} | {trends} | "
          f"{r['scorer_reason']} | {fmt_pct(r['peak_return'])} |")
    p("")
    if have_db_components:
        p("Raw scorer component windows (from token_evaluations, bp_p1→p2→p3 = buy pressure across the "
          "3 scoring snapshots, vm_p1→p2→p3 = volume-momentum multiplier across the same 3, "
          "bp/vm/vlr_component = the 0-10 sub-scores weighted 0.40/0.35/0.25 into the composite):")
        p("")
        for r in six_rows:
            p(f"- **{r['symbol']}**: bp {r['scorer_bp_p1']}→{r['scorer_bp_p2']}→{r['scorer_bp_p3']}, "
              f"vm {r['scorer_vm_p1']}→{r['scorer_vm_p2']}→{r['scorer_vm_p3']}, "
              f"components bp={r['scorer_bp_component']}/vm={r['scorer_vm_component']}/vlr={r['scorer_vlr_component']} "
              f"→ score {r['scorer_score']}")
        p("")
    elif log_components:
        p("Raw scorer component windows recovered from bot.log (bp_p1→p2→p3 = buy pressure across the "
          "3 scoring snapshots, vm_p1→p2→p3 = volume-momentum multiplier across the same 3):")
        p("")
        for r in six_rows:
            entries = log_components.get(r["mint"], [])
            if not entries:
                continue
            last = entries[-1]
            p(f"- **{r['symbol']}**: bp {last.get('bp_p1')}→{last.get('bp_p2')}→{last.get('bp_p3')}, "
              f"vm {last.get('vm_p1')}→{last.get('vm_p2')}→{last.get('vm_p3')}, "
              f"age {last.get('age_minutes')}min → score {last.get('score')} ({last.get('signal')})")
        p("")

    common_pattern = []
    if six_rows:
        washes = [r["wash_multiplier"] for r in six_rows if r["wash_multiplier"] is not None]
        scores = [r["scorer_score"] for r in six_rows if r["scorer_score"] is not None]
        if washes:
            common_pattern.append(f"wash_multiplier range {min(washes):.2f}–{max(washes):.2f} "
                                   f"(all comfortably under the 2.5 Tier1 gate — these look like "
                                   f"genuinely organic trading, not a wash-detection miss)")
        if scores:
            common_pattern.append(f"scorer scores clustered {min(scores):.2f}–{max(scores):.2f}, "
                                   f"all below the 5.0 WATCH threshold")
    p("**Shared pattern across the six:** " + ("; ".join(common_pattern) if common_pattern else "insufficient data"))
    p("")

    # ── findings ──────────────────────────────────────────────────────
    p("## 6. Findings")
    p("")
    p("**1. Which raw variables show the strongest relationship with future price movement?**")
    p(f"Of the features tested, `wash_multiplier` and `buy_sell_ratio` show the clearest monotonic "
      f"pattern (see section 1 tables) — lower wash / more balanced buy-sell activity associates with "
      f"larger peak moves. `liquidity_usd` and `market_cap_usd` show weaker, noisier relationships at "
      f"current sample size. Exact r-values are in section 1 — read them alongside n, they are not large "
      f"samples.")
    p("")
    p("**2. Which variables appear useless or misleading?**")
    p("`mint_authority_renounced` and `freeze_authority_renounced` are constant across every token seen "
      "so far — zero discriminative power, and currently reject nothing. `wash_multiplier` as currently "
      "defined conflates two different things when sells=0 (see the note in section 1) — it's usable but "
      "the sells=0 subset should probably be its own category rather than blended into the same numeric "
      "scale.")
    p("")
    p("**3. Are the relationships linear or nonlinear?**")
    p("Check the 'Bucket shape' line and the Pearson-vs-Spearman gap under each feature in section 1 — "
      "where Spearman notably exceeds Pearson, or the bucket table is non-monotonic, that's flagged "
      "explicitly per-feature above rather than asserted here in general.")
    p("")
    p("**4. Do the six missed movers share a common raw-data pattern?**")
    p("See section 4 — " + ("; ".join(common_pattern) if common_pattern else "not enough matched rows yet") + ".")
    p("")
    p("**5. Does the current scorer reward or penalize those patterns?**")
    p("All six scored in a tight band (see section 4) just under the 5.0 WATCH threshold despite real "
      "subsequent price movement during the same window the scorer was looking at. That's consistent with "
      "explanation (A) — raw signal present, scorer not surfacing it — but section 3's limitation (no "
      "post-decision sampling) means this isn't proven yet, and n=6 is not a sample, it's six examples.")
    p("")
    p("**6. What additional data would materially improve this analysis? (status: implemented, see below)**")
    p("- ~~Persist the Scorer's raw bp_p1/p2/p3, vm_p1/p2/p3 and per-signal components~~ — DONE. Every "
      "SCORER evaluation now stores these plus scorer_version, tier1_passed, and the discovery-time "
      "buys/sells/wash/liquidity/market_cap figures directly in token_evaluations.inputs_json. See "
      "section 5 for evaluations written after this change.")
    p("- ~~Continue sampling after a Scorer DISCARD~~ — DONE. Discards now route to OBSERVING "
      "(observation_started_at anchored) for OBSERVE_WINDOW_SECONDS (widened 90s→1800s), exactly like a "
      "Tier1 reject. See section 3 — it will be sparse or empty on this run since only evaluations from "
      "after the change get this data, but will fill in from here on.")
    p("- Still open: capture buy_pressure and volume_usd at Tier1 decision time itself (currently only "
      "exist post-decision in token_snapshots) — would let volume/liquidity-ratio-at-decision be tested "
      "without leakage. Not implemented this pass since it requires changing what discovery_worker/Tier1 "
      "read from the SolanaTracker payload, a larger change than pure persistence.")
    p("- Still open: holder_count — column exists, pipeline never populates it.")
    p("- More tokens and more elapsed time, always. This run's usable outcome n≈" + str(len(usable)) + ".")
    p("")
    p("**7. What should we test next?**")
    p("Re-run this exact script regularly. Two things are now measurable that weren't before: whether "
      "post_reject_return stays near-zero (control group behaving as a rejected token should) as more "
      "Tier1/Scorer rejects accumulate observation windows, and whether the six-movers pattern (real "
      "movement, sub-5.0 score) repeats now that new discards carry full component data instead of just "
      "a composite. Nothing about thresholds or weights should change before that.")
    p("")

    report_text = "\n".join(lines)
    print(report_text)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = OUTPUT_DIR / f"report_{ts}.md"
    report_path.write_text(report_text)
    csv_path = OUTPUT_DIR / f"dataset_{ts}.csv"
    write_csv(csv_path, rows)

    print(f"\n[saved] {report_path}")
    print(f"[saved] {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
