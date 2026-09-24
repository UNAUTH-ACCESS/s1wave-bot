# Momentum Confluence — Prospective Check — 20260922_113801

ANALYSIS-ONLY. Read-only, no trading logic touched. First live-data checkpoint of the `momentum_confluence_v1` forward-tracking experiment deployed 2026-09-22.

- Total signals recorded so far: **193**
- Time span: 2026-09-22T03:59:26.954928+00:00 to 2026-09-22T11:31:27.862568+00:00 (~7.6h)
- This is an EARLY checkpoint, not a conclusion — the retrospective validation used 420 real-pump tokens; this prospective sample is a fraction of that. Read every number here as "first look", not "confirmed".

## Coverage: how many signals are mature enough to judge at each horizon

| horizon | mature (enough time passed) | has data | no snapshot despite maturity |
|---|---|---|---|
| 5min | 193/193 | 193 | 0 |
| 15min | 189/193 | 189 | 0 |
| 30min | 181/193 | 181 | 0 |
| 60min | 163/193 | 163 | 0 |

## Outcome by confluence bucket (0-1 rules vs 2+ rules) — the exact split the retrospective work validated

n=193 >= MIN_FOR_SPLIT=193 — chronological time-split now active. Split at 2026-09-22T07:26:27.358969+00:00: train n=96, test n=97.

### 5-minute forward return

- TRAIN — 0-1 rules: n=46, median=-8.2%, win-rate=18/46 (39%) | 2+ rules: n=50, median=6.4%, win-rate=36/50 (72%)
- TEST  — 0-1 rules: n=46, median=-25.9%, win-rate=17/46 (37%) | 2+ rules: n=51, median=9.8%, win-rate=36/51 (71%)
- **Direction holds in test.**

### 15-minute forward return

- TRAIN — 0-1 rules: n=46, median=-69.1%, win-rate=12/46 (26%) | 2+ rules: n=50, median=5.4%, win-rate=29/50 (58%)
- TEST  — 0-1 rules: n=44, median=-95.9%, win-rate=10/44 (23%) | 2+ rules: n=49, median=13.2%, win-rate=29/49 (59%)
- **Direction holds in test.**

### 30-minute forward return

- TRAIN — 0-1 rules: n=46, median=-90.5%, win-rate=7/46 (15%) | 2+ rules: n=50, median=0.7%, win-rate=25/50 (50%)
- TEST  — 0-1 rules: n=40, median=-96.9%, win-rate=6/40 (15%) | 2+ rules: n=45, median=6.0%, win-rate=24/45 (53%)
- **Direction holds in test.**

### 60-minute forward return

- TRAIN — 0-1 rules: n=46, median=-90.5%, win-rate=7/46 (15%) | 2+ rules: n=50, median=0.7%, win-rate=25/50 (50%)
- TEST  — 0-1 rules: n=34, median=-96.9%, win-rate=6/34 (18%) | 2+ rules: n=33, median=1.1%, win-rate=17/33 (52%)
- **Direction holds in test.**

## Raw distribution: n_rules_cofiring counts in this sample

- 0 rules co-firing: n=23
- 1 rules co-firing: n=69
- 2 rules co-firing: n=97
- 3 rules co-firing: n=4

## Still-open signals (status WATCHING/OBSERVING) vs concluded (REJECTED/other)

- REJECTED: 175
- OBSERVING: 18

## Caveats

- Sample (n=193, ~7.6h old) is now large enough for a real chronological time-split — see the train/test comparison above. Still worth growing further before treating this as final; the retrospective study used n=420.
- No trading logic, thresholds, or scorer weights touched. Nothing implemented from this checkpoint.