# Shadow experiment backtest: scorer_v2_threshold_4
Generated: 2026-09-20T18:53:50.426496+00:00

**Analysis-only. shadow_trades is never read by CapitalEngine, RiskEngine, or execution.py. TIER2_WATCH_THRESHOLD (5.0) and TIER2_STRONG_BUY_THRESHOLD (8.0) are unchanged and this script never touches them. Real `trades` table: 0 rows from this or any recent activity — confirmed separately.**

## 1. Dataset
- shadow_trades for scorer_v2_threshold_4: 5 total, 0 excluded (decided during outage), **5 clean**
- Tier1-passed, scored below 4.0 (reference group, no shadow row by design): 75 total, **75 clean**

## 2. Bucketed comparison

### Bucket 1: score 4.0-4.49 (n=3)
- @ 5min: n=0 (not enough elapsed clean time yet)
- @ 15min: n=0 (not enough elapsed clean time yet)
- @ 30min: n=0 (not enough elapsed clean time yet)
- @ 60min: n=0 (not enough elapsed clean time yet)

### Bucket 2: score 4.5-4.99 (n=2)
- @ 5min: n=0 (not enough elapsed clean time yet)
- @ 15min: n=0 (not enough elapsed clean time yet)
- @ 30min: n=0 (not enough elapsed clean time yet)
- @ 60min: n=0 (not enough elapsed clean time yet)

### Bucket 3: score 5.0+ (n=0)
- no observations yet

### Bucket 4: Tier1-passed, scored below 4.0 (reference) (n=75)
- @ 5min: n=69, median=+0.1%, mean=+3.5%, hit≥10%=12%, hit≥20%=7%
- @ 15min: n=67, median=+0.1%, mean=+8.9%, hit≥10%=15%, hit≥20%=12%
- @ 30min: n=67, median=+0.1%, mean=+9.6%, hit≥10%=15%, hit≥20%=12%
- @ 60min: n=66, median=+0.1%, mean=+9.6%, hit≥10%=14%, hit≥20%=12%

## 3. Simulated exit rules (exact engine/risk.py logic, replayed over observed prices)

- 1 of 5 shadow trades hit a simulated exit condition so far (the rest either haven't moved enough or haven't been observed long enough)
- Breakdown: {'TAKE_PROFIT': 1}
- Simulated win rate (of those that hit an exit): 1/1 (100%)

- Max observed gain across all shadow trades: median +3.8%, worst +2.3%, best +39.6%
- Max observed drawdown across all shadow trades: median +0.0%, worst +0.0%

## 4. Every shadow trade so far

| symbol | score | entry_price | 30m return | max_gain | max_drawdown | sim. exit |
|---|---|---|---|---|---|---|
| Greenland | 4.18 | 0.0010549651 | pending | +2.9% | +0.0% | no exit yet |
| CALI | 4.86 | 0.0000813457 | pending | +39.6% | +0.0% | TAKE_PROFIT |
| Stamp | 4.29 | 0.0014655075 | pending | +20.2% | +0.0% | no exit yet |
| BEAST | 4.22 | 0.0005575680 | pending | +3.8% | +0.0% | no exit yet |
| BASKET | 4.56 | 0.0005650511 | pending | +2.3% | +0.0% | no exit yet |

## 5. Reminder
This is n=5 shadow trades. The goal per your instructions is NOT to conclude 4.0 is correct — it's to accumulate enough of these to eventually answer that. No thresholds were changed. Re-run this script as more data accumulates.