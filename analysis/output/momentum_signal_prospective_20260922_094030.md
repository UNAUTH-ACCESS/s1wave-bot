# Momentum Confluence — Prospective Check — 20260922_094030

ANALYSIS-ONLY. Read-only, no trading logic touched. First live-data checkpoint of the `momentum_confluence_v1` forward-tracking experiment deployed 2026-09-22.

- Total signals recorded so far: **145**
- Time span: 2026-09-22T03:59:26.954928+00:00 to 2026-09-22T09:36:27.615756+00:00 (~5.7h)
- This is an EARLY checkpoint, not a conclusion — the retrospective validation used 420 real-pump tokens; this prospective sample is a fraction of that. Read every number here as "first look", not "confirmed".

## Coverage: how many signals are mature enough to judge at each horizon

| horizon | mature (enough time passed) | has data | no snapshot despite maturity |
|---|---|---|---|
| 5min | 144/145 | 144 | 0 |
| 15min | 143/145 | 143 | 0 |
| 30min | 140/145 | 140 | 0 |
| 60min | 129/145 | 129 | 0 |

## Outcome by confluence bucket (0-1 rules vs 2+ rules) — the exact split the retrospective work validated

n=145 < MIN_FOR_SPLIT=200 — too early for a chronological time-split. Pooled comparison only (same caveat as the very first checkpoint): every number below could still be an early-sample artifact, not yet a validated effect.

### 5-minute forward return

- POOLED — 0-1 rules: n=72, median=-22.8%, win-rate=24/72 (33%) | 2+ rules: n=72, median=6.6%, win-rate=49/72 (68%)

### 15-minute forward return

- POOLED — 0-1 rules: n=71, median=-86.3%, win-rate=17/71 (24%) | 2+ rules: n=72, median=2.8%, win-rate=39/72 (54%)

### 30-minute forward return

- POOLED — 0-1 rules: n=69, median=-94.2%, win-rate=11/69 (16%) | 2+ rules: n=71, median=0.0%, win-rate=35/71 (49%)

### 60-minute forward return

- POOLED — 0-1 rules: n=62, median=-90.5%, win-rate=11/62 (18%) | 2+ rules: n=67, median=1.1%, win-rate=34/67 (51%)

## Raw distribution: n_rules_cofiring counts in this sample

- 0 rules co-firing: n=20
- 1 rules co-firing: n=53
- 2 rules co-firing: n=71
- 3 rules co-firing: n=1

## Still-open signals (status WATCHING/OBSERVING) vs concluded (REJECTED/other)

- REJECTED: 135
- OBSERVING: 10

## Caveats

- Sample is small (n=145) and young (oldest signal ~5.7h) — this is a checkpoint to establish the tracking is working correctly and watch the trend, not a validated result. Time-split activates automatically once n>=200 — just re-run this same script.
- No trading logic, thresholds, or scorer weights touched. Nothing implemented from this checkpoint.