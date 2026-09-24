"""
analysis/winners_profile.py
============================
ANALYSIS-ONLY, READ-ONLY. Builds directly on analysis/edge_research.py's
output (analysis/output/edge_research_dataset_*.csv) — does not touch any
production/trading file, does not write to the database.

Purpose
-------
edge_research.py asked "does any single feature predict outcome across the
whole population" and found mostly noise. This script asks a narrower,
more concrete question the user asked directly: pull out the actual
cluster of tokens that did well after being discovered — not just "high
peak return" (a token that spiked +300% for one snapshot then rugged to
zero counts as a spike, not a winner) but ones that (a) ended up net
positive relative to their decision-time price and (b) got there via a
comparatively smooth, sustained move rather than a violent round-trip —
then profiles what, if anything, those winners have in common at
decision time.

"Straight movement" definition
-------------------------------
Two path-shape metrics computed from each candidate's raw token_snapshots
price path (decision_time -> last available snapshot):
  - path_efficiency = (last_price - first_price) / sum(|consecutive price
    deltas|). Ranges -1..1. A value near +1 means nearly every step moved
    in the same net-positive direction (a "straight" run up); a value
    near 0 means the price round-tripped a lot to end up roughly where it
    started; negative means it net moved down despite possibly choppy
    up-ticks along the way.
  - reversal_rate = fraction of consecutive snapshot-to-snapshot steps
    that flip sign relative to the previous step. Lower = straighter.

"Did well" definition
----------------------
forward_return_full (realized return from decision-time price to the
LAST available snapshot, i.e. did it actually end up ahead, not just
peak ahead) in the top quartile of the filtered population. This
deliberately does NOT use peak_return_full alone, because peak return
rewards a token that spiked once and then collapsed exactly the same as
one that spiked and held — that is precisely the "not a real winner"
case the user is asking to exclude by saying "straight movements".

Filter applied before ranking: n_observations_used_full >= 5 (need an
actual path to call anything "straight" or "not") and not
outage_excluded_full (edge_research.py already flags candidates whose
outcome window overlapped a known data-blackout window).

No look-ahead beyond what edge_research.py already guaranteed: all
decision-time features here are the exact same ones edge_research.py
extracted at decision_time, no different sourcing.

Usage
-----
    PYTHONPATH=. /tmp/s1wave-venv/bin/python analysis/winners_profile.py <edge_research_dataset_csv>

Outputs
-------
    analysis/output/winners_profile_<timestamp>.md
    analysis/output/winners_list_<timestamp>.csv   (the actual winners, one row each)
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg
from config.settings import settings

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

DECISION_FEATURES = [
    "score", "bp_p1", "bp_p2", "bp_p3", "vm_p1", "vm_p2", "vm_p3",
    "vlr_p1", "vlr_p2", "vlr_p3", "age_minutes",
    "liquidity_usd_at_discovery", "market_cap_usd_at_discovery",
    "wash_multiplier_at_discovery", "buy_sell_ratio_at_discovery",
]
CATEGORICAL_FEATURES = ["bp_trend", "vm_trend", "vlr_trend", "vm_zone", "decision_gate", "group"]


def path_metrics(prices: np.ndarray) -> dict:
    if len(prices) < 3:
        return dict(path_efficiency=np.nan, reversal_rate=np.nan, time_to_peak_frac=np.nan)
    deltas = np.diff(prices)
    nonzero = deltas[deltas != 0]
    net = prices[-1] - prices[0]
    total_abs = np.sum(np.abs(deltas))
    path_efficiency = net / total_abs if total_abs > 0 else 0.0
    if len(nonzero) >= 2:
        signs = np.sign(nonzero)
        reversals = np.sum(signs[1:] != signs[:-1])
        reversal_rate = reversals / (len(nonzero) - 1)
    else:
        reversal_rate = np.nan
    peak_idx = int(np.argmax(prices))
    time_to_peak_frac = peak_idx / (len(prices) - 1)
    return dict(path_efficiency=path_efficiency, reversal_rate=reversal_rate,
                time_to_peak_frac=time_to_peak_frac)


async def fetch_paths(token_ids: list[str]) -> dict[str, np.ndarray]:
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT token_id, sampled_at, price_usd FROM token_snapshots
            WHERE token_id = ANY($1::uuid[]) ORDER BY token_id, sampled_at ASC
            """,
            token_ids,
        )
    finally:
        await conn.close()
    out: dict[str, list[float]] = {}
    for r in rows:
        out.setdefault(str(r["token_id"]), []).append(float(r["price_usd"]))
    return {k: np.array(v) for k, v in out.items()}


def mann_whitney_row(feature: str, winners: pd.Series, rest: pd.Series) -> dict:
    w = winners.dropna()
    r = rest.dropna()
    if len(w) < 3 or len(r) < 3:
        return dict(feature=feature, n_winners=len(w), n_rest=len(r),
                    median_winners=w.median() if len(w) else np.nan,
                    median_rest=r.median() if len(r) else np.nan,
                    p_value=np.nan, note="too few non-null values")
    stat, p = stats.mannwhitneyu(w, r, alternative="two-sided")
    return dict(feature=feature, n_winners=len(w), n_rest=len(r),
                median_winners=w.median(), median_rest=r.median(), p_value=p, note="")


async def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else None
    if not csv_path:
        candidates = sorted(OUTPUT_DIR.glob("edge_research_dataset_*.csv"))
        if not candidates:
            print("No edge_research_dataset_*.csv found in analysis/output/.")
            return
        csv_path = str(candidates[-1])
    print(f"Loading {csv_path}")
    df = pd.read_csv(csv_path)
    df["decision_time"] = pd.to_datetime(df["decision_time"], utc=True)

    # Filter: need a real path, and not contaminated by a known outage.
    pool = df[
        (df["n_observations_used_full"] >= 5)
        & (~df["outage_excluded_full"].astype(bool))
        & df["forward_return_full"].notna()
    ].copy()
    print(f"Filtered pool: {len(pool)} of {len(df)} candidates "
          f"(n_observations_used_full>=5, not outage-excluded, has forward_return_full)")

    # Fetch raw price paths for the whole filtered pool (need path metrics
    # for everyone to compare winners vs rest fairly, not just winners).
    token_ids = pool["token_id"].astype(str).tolist()
    paths = await fetch_paths(token_ids)
    metrics = {tid: path_metrics(prices) for tid, prices in paths.items()}
    for col in ("path_efficiency", "reversal_rate", "time_to_peak_frac"):
        pool[col] = pool["token_id"].astype(str).map(lambda t: metrics.get(t, {}).get(col, np.nan))

    # "Did well" = top quartile of forward_return_full within the filtered pool.
    threshold = pool["forward_return_full"].quantile(0.75)
    winners = pool[pool["forward_return_full"] >= threshold].copy()
    rest = pool[pool["forward_return_full"] < threshold].copy()
    winners = winners.sort_values("forward_return_full", ascending=False)

    print(f"Winners (top quartile, forward_return_full >= {threshold:.3f}): {len(winners)}")
    print(f"Rest: {len(rest)}")

    # Feature comparison table: winners vs rest.
    comparison_rows = []
    for feat in DECISION_FEATURES:
        comparison_rows.append(mann_whitney_row(feat, winners[feat], rest[feat]))
    for feat in ("path_efficiency", "reversal_rate", "time_to_peak_frac"):
        comparison_rows.append(mann_whitney_row(feat, winners[feat], rest[feat]))
    comparison = pd.DataFrame(comparison_rows)

    # Categorical feature composition: winners vs rest, as proportions.
    cat_tables = {}
    for feat in CATEGORICAL_FEATURES:
        wc = winners[feat].fillna("NULL").value_counts(normalize=True).rename("winners_pct")
        rc = rest[feat].fillna("NULL").value_counts(normalize=True).rename("rest_pct")
        cat_tables[feat] = pd.concat([wc, rc], axis=1).fillna(0.0)

    # Within winners: are the "straight movers" a further sub-cluster?
    straight_threshold = winners["path_efficiency"].median()
    straight_winners = winners[winners["path_efficiency"] >= straight_threshold]
    choppy_winners = winners[winners["path_efficiency"] < straight_threshold]

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    winners_csv = OUTPUT_DIR / f"winners_list_{ts}.csv"
    out_cols = (["group", "symbol", "decision_time", "decision_price", "forward_return_full",
                 "peak_return_full", "max_drawdown_full", "path_efficiency", "reversal_rate",
                 "time_to_peak_frac", "n_observations_used_full"] + DECISION_FEATURES + CATEGORICAL_FEATURES)
    winners[out_cols].to_csv(winners_csv, index=False)

    lines = []
    p = lines.append
    p(f"# Winners Profile — {ts}")
    p("")
    p("ANALYSIS-ONLY. Read-only, no trading logic touched. Built on top of "
      f"`{Path(csv_path).name}`.")
    p("")
    p("## Method")
    p("")
    p(f"- Started from {len(df)} candidates in the edge-research dataset (shadow trades + "
      "OBSERVING trajectories).")
    p(f"- Filtered to {len(pool)} with a real price path (n_observations_used_full >= 5) and no "
      "outage overlap.")
    p("- \"Did well\" = top quartile of `forward_return_full` (realized return from decision-time "
      "price to the LAST available snapshot — not peak return, so a spike-then-rug does not count "
      "as a winner just because it touched a high peak).")
    p(f"- Quartile threshold: forward_return_full >= {threshold:.1%}. "
      f"Winners: n={len(winners)}. Rest: n={len(rest)}.")
    p("- \"Straight movement\" = `path_efficiency` (net move / sum of absolute step-to-step moves, "
      "computed from the token's actual snapshot price path). Near +1 = moved steadily in one "
      "direction; near 0 = round-tripped a lot to end up where it started.")
    p("")
    p("## Headline: what the winners actually look like")
    p("")
    p(f"- Median forward_return_full among winners: **{winners['forward_return_full'].median():.1%}** "
      f"(rest: {rest['forward_return_full'].median():.1%})")
    p(f"- Median path_efficiency among winners: **{winners['path_efficiency'].median():.3f}** "
      f"(rest: {rest['path_efficiency'].median():.3f})")
    p(f"- Median reversal_rate among winners: **{winners['reversal_rate'].median():.3f}** "
      f"(rest: {rest['reversal_rate'].median():.3f})")
    p(f"- Median time_to_peak_frac among winners: **{winners['time_to_peak_frac'].median():.3f}** "
      "(0 = peaked immediately, 1 = peaked at the very last observed point)")
    p("")
    p("## Decision-time feature comparison: winners vs rest (Mann-Whitney U)")
    p("")
    p("| feature | n_winners | n_rest | median_winners | median_rest | p_value | note |")
    p("|---|---|---|---|---|---|---|")
    for _, row in comparison.iterrows():
        flag = " **LOW CONFIDENCE (n<20)**" if min(row["n_winners"], row["n_rest"]) < 20 else ""
        sig = " **" if pd.notna(row["p_value"]) and row["p_value"] < 0.05 else ""
        p(f"| {row['feature']} | {row['n_winners']} | {row['n_rest']} | "
          f"{row['median_winners']:.4g} | {row['median_rest']:.4g} | "
          f"{'' if pd.isna(row['p_value']) else f'{row['p_value']:.4f}'}{sig} | {row['note']}{flag} |")
    p("")
    p("Rows marked `**` after the p-value are significant at p<0.05 — treat as a lead, not a "
      "conclusion (this is one comparison among many, no multiple-comparison correction applied).")
    p("")
    p("## Categorical composition: winners vs rest")
    p("")
    for feat, table in cat_tables.items():
        p(f"### {feat}")
        p("")
        p("| value | winners_pct | rest_pct |")
        p("|---|---|---|")
        for val, r in table.iterrows():
            p(f"| {val} | {r['winners_pct']:.1%} | {r['rest_pct']:.1%} |")
        p("")
    p("## Within winners: straight movers vs choppy winners")
    p("")
    p(f"Split winners at their own median path_efficiency ({straight_threshold:.3f}): "
      f"{len(straight_winners)} straight movers, {len(choppy_winners)} choppy winners "
      "(both groups are still in the top-quartile-return winners set — this asks whether a "
      "*subset* of winners got there smoothly vs violently).")
    p("")
    p(f"- Straight movers: median forward_return_full = {straight_winners['forward_return_full'].median():.1%}, "
      f"median score = {straight_winners['score'].median():.2f}, "
      f"median age_minutes = {straight_winners['age_minutes'].median():.1f}")
    p(f"- Choppy winners: median forward_return_full = {choppy_winners['forward_return_full'].median():.1%}, "
      f"median score = {choppy_winners['score'].median():.2f}, "
      f"median age_minutes = {choppy_winners['age_minutes'].median():.1f}")
    p("")
    p("## The actual winners list")
    p("")
    p(f"Full list with every decision-time feature and path metric: `{winners_csv.name}`. "
      f"Top 15 by forward_return_full shown here:")
    p("")
    p("| symbol | group | forward_return_full | path_efficiency | reversal_rate | score | age_min |")
    p("|---|---|---|---|---|---|---|")
    for _, row in winners.head(15).iterrows():
        p(f"| {row['symbol']} | {row['group']} | {row['forward_return_full']:.1%} | "
          f"{row['path_efficiency']:.3f} | {row['reversal_rate']:.3f} | {row['score']:.2f} | "
          f"{row['age_minutes']:.1f} |")
    p("")
    p("## Caveats")
    p("")
    p(f"- Winners n={len(winners)}, several sub-comparisons here have n<20 per side — flagged above, "
      "not strong enough to call a rule.")
    p("- This is descriptive/exploratory, not out-of-sample validated like edge_research.py's "
      "shortlist — treat this as \"what did the winners look like\", not a proven predictive rule.")
    p("- No trading logic, thresholds, or scorer weights were touched. Nothing implemented.")

    report_path = OUTPUT_DIR / f"winners_profile_{ts}.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")
    print(f"Wrote {winners_csv}")


if __name__ == "__main__":
    asyncio.run(main())
