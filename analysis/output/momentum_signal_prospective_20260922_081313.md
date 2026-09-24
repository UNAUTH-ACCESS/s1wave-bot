# Momentum Confluence — Prospective Check — 20260922_081313

ANALYSIS-ONLY. Read-only, no trading logic touched. First live-data checkpoint of the `momentum_confluence_v1` forward-tracking experiment deployed 2026-09-22.

- Total signals recorded so far: **115**
- Time span: 2026-09-22T03:59:26.954928+00:00 to 2026-09-22T08:10:27.439266+00:00 (~4.2h)
- This is an EARLY checkpoint, not a conclusion — the retrospective validation used 420 real-pump tokens; this prospective sample is a fraction of that. Read every number here as "first look", not "confirmed".

## Coverage: how many signals are mature enough to judge at each horizon

| horizon | mature (enough time passed) | has data | no snapshot despite maturity |
|---|---|---|---|
| 5min | 110/115 | 110 | 0 |
| 15min | 107/115 | 107 | 0 |
| 30min | 102/115 | 102 | 0 |
| 60min | 93/115 | 93 | 0 |

## Outcome by confluence bucket (0-1 rules vs 2+ rules) — the exact split the retrospective work validated

### 5-minute forward return

- 0-1 rules co-firing: n=51, median=-15.6%, positive-return rate=18/51 (35%)
- 2+ rules co-firing: n=59, median=6.2%, positive-return rate=40/59 (68%)

### 15-minute forward return

- 0-1 rules co-firing: n=49, median=-71.4%, positive-return rate=13/49 (27%)
- 2+ rules co-firing: n=58, median=1.3%, positive-return rate=30/58 (52%)

### 30-minute forward return

- 0-1 rules co-firing: n=49, median=-88.4%, positive-return rate=8/49 (16%)
- 2+ rules co-firing: n=53, median=0.0%, positive-return rate=26/53 (49%)

### 60-minute forward return

- 0-1 rules co-firing: n=44, median=-93.3%, positive-return rate=6/44 (14%)
- 2+ rules co-firing: n=49, median=0.0%, positive-return rate=24/49 (49%)

## Raw distribution: n_rules_cofiring counts in this sample

- 0 rules co-firing: n=16
- 1 rules co-firing: n=38
- 2 rules co-firing: n=61

## Still-open signals (status WATCHING/OBSERVING) vs concluded (REJECTED/other)

- REJECTED: 102
- OBSERVING: 13

## Caveats

- Sample is small (n=115) and young (oldest signal ~4.2h) — this is a checkpoint to establish the tracking is working correctly and to watch the trend, not a validated result.
- No time-split validation performed here — not enough data yet for a meaningful train/test split. Revisit this once n is large enough (recommend re-running once n>=200-300, matching the retrospective sample's order of magnitude).
- No trading logic, thresholds, or scorer weights touched. Nothing implemented from this checkpoint.