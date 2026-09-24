# Shadow Lifetime Follow-up — 20260921_182559

ANALYSIS-ONLY. Read-only, no trading logic touched. Re-anchors analysis/edge_research.py's shadow-vs-OBSERVING comparison to discovery time instead of decision time, using that script's own `compute_outcomes` and `OUTAGES` unchanged (imported directly, not reimplemented).

## Methodology notes

- Lifetime anchor = `tokens.discovered_at`. Decision anchor = `shadow_trades.triggered_at` (shadow) / `tokens.observation_started_at` (OBSERVING).
- `max_drawdown` = min(price)/baseline - 1, i.e. baseline-to-trough, NOT peak-to-trough (inherited convention from edge_research.py's compute_outcomes - kept identical here).
- Substantial-upside threshold: pre-decision peak return >= 20% (a token that already moved meaningfully before the bot ever scored it). Also reported: a median-split version at the shadow population's own median pre-decision peak return (5.9%) as a robustness check against the 20% figure being an arbitrary pick.
- Outage-excluded (lifetime, full horizon): shadow n=0, OBSERVING n=82. Outage-excluded (decision, full horizon): shadow n=1, OBSERVING n=7.

## Missing-data coverage

| group | n total | n zero snapshots | n dropped (no post-decision data) | n analyzed | n with pre-decision data | median discovery-to-first-snapshot latency |
|---|---|---|---|---|---|---|
| shadow | 160 | 0 | 0 | 160 | 160 | 0.991 (IQR 0.446 to 1) min |
| observing | 911 | 186 | 1 | 724 | 304 | 0.996 (IQR 0.434 to 1) min |

Note: OBSERVING's "n dropped" here only reflects tokens with zero snapshots or zero post-decision snapshots (this script's own filters) - edge_research.py additionally drops OBSERVING tokens with no matching token_evaluations row before decision_time, which this script does not need (it doesn't use decision-time features here, only price).

## Does the -91.5%-style result survive when measured from discovery?

**Shadow population, decision-anchored median return: -89.5% (n=160).**
**Shadow population, lifetime-anchored median return: -91.5% (n=160).**
**Verdict: **Survives, but weakened** - the lifetime view is less severe than the decision-anchored view, consistent with part (not all) of the effect being a timing artifact.**

## Headline comparison: shadow vs OBSERVING control, both anchors

| metric | shadow median (n) | observing median (n) | Mann-Whitney p |
|---|---|---|---|
| lifetime_return_full | -91.5% (n=160) | -0.1% (n=718) | 0.0000 |
| decision_return_full | -89.5% (n=160) | -0.4% (n=718) | 0.0000 |
| lifetime max upside (peak_return) | 28.2% (n=160) | 18.6% (n=718) | 0.4556 |
| lifetime max drawdown | -91.9% (n=160) | -14.2% (n=718) | 0.0000 |
| time_discovery_to_decision_min | 3.1 (n=160) | 0.0 (n=724) | 0.0000 |
| time_discovery_to_peak_min | 4.0 (n=160) | 7.1 (n=718) | 0.0000 |
| time_decision_to_peak_min | 1.9 (n=160) | 5.1 (n=718) | 0.0000 |

## Score-bucket stratification (shadow only)

Note: `TIER2_WATCH_THRESHOLD=5.0` in production settings, so the 5.0+ bucket below overlaps with tokens that also crossed the live WATCH signal - context, not a caveat.

| bucket | n | median lifetime_return_full | median decision_return_full | median lifetime_max_drawdown | median time_discovery_to_decision_min |
|---|---|---|---|---|---|
| 4.0-4.49 | 21 | -91.0% | -87.0% | -91.9% | 2.9 |
| 4.5-4.99 | 69 | -91.6% | -86.1% | -93.7% | 3.2 |
| 5.0+ | 70 | -91.6% | -92.3% | -91.8% | 3.1 |

The most negative decision-anchored median sits in the **5.0+** bucket (-92.3% if applicable) - the effect looks broadly spread across buckets, not concentrated in one.

## Substantial-upside split (shadow, tokens with pre-decision data only)

### 20% threshold

| segment | n | median lifetime_return_full | median decision_return_full |
|---|---|---|---|
| already pumped pre-decision | 44 | -88.4% | -92.9% |
| had not yet pumped | 116 | -93.4% | -89.0% |

### median split (>= 5.9%)

| segment | n | median lifetime_return_full | median decision_return_full |
|---|---|---|---|
| already pumped pre-decision | 80 | -94.3% | -95.1% |
| had not yet pumped | 80 | -88.7% | -81.9% |

**Diagnostic: Both subsets show poor LIFETIME returns too - this points to a more fundamental issue with what the scorer flags, not purely a timing/entry-point artifact.**

## Caveats

- Shadow n analyzed = 160, OBSERVING n analyzed = 724. Several sub-comparisons above have one or both sides under n=20 - flagged inline.
- This is descriptive, not out-of-sample validated (unlike edge_research.py's shortlist). Treat this as a direct diagnostic of the -91.5%-style finding, not a new predictive claim.
- No trading logic, thresholds, or scorer weights were touched. Nothing implemented.