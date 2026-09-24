# Shadow experiment backtest: scorer_v2_threshold_4
Generated: 2026-09-21T02:03:04.052042+00:00

**Analysis-only. shadow_trades is never read by CapitalEngine, RiskEngine, or execution.py. TIER2_WATCH_THRESHOLD (5.0) and TIER2_STRONG_BUY_THRESHOLD (8.0) are unchanged and this script never touches them. Real `trades` table: 0 rows from this or any recent activity — confirmed separately.**

## 1. Dataset
- shadow_trades for scorer_v2_threshold_4: 65 total, 1 excluded (decided during outage), **64 clean**
- Tier1-passed, scored below 4.0 (reference group, no shadow row by design): 96 total, **96 clean**

## 2. Bucketed comparison

### Bucket 1: score 4.0-4.49 (n=13)
- @ 5min: n=12, median=+23.9%, mean=+35.6%, hit≥10%=67%, hit≥20%=58%
- @ 15min: n=12, median=+37.4%, mean=+55.3%, hit≥10%=75%, hit≥20%=67%
- @ 30min: n=11, median=+42.6%, mean=+63.9%, hit≥10%=82%, hit≥20%=82%
- @ 60min: n=8, median=+52.8%, mean=+85.4%, hit≥10%=100%, hit≥20%=100%

### Bucket 2: score 4.5-4.99 (n=24)
- @ 5min: n=24, median=+16.4%, mean=+28.0%, hit≥10%=71%, hit≥20%=42%
- @ 15min: n=24, median=+19.6%, mean=+40.8%, hit≥10%=71%, hit≥20%=50%
- @ 30min: n=21, median=+18.2%, mean=+49.4%, hit≥10%=71%, hit≥20%=48%
- @ 60min: n=16, median=+17.1%, mean=+41.7%, hit≥10%=62%, hit≥20%=44%

### Bucket 3: score 5.0+ (n=27)
- @ 5min: n=26, median=+37.8%, mean=+82.8%, hit≥10%=62%, hit≥20%=58%
- @ 15min: n=26, median=+57.0%, mean=+158.5%, hit≥10%=62%, hit≥20%=62%
- @ 30min: n=24, median=+57.0%, mean=+176.7%, hit≥10%=67%, hit≥20%=62%
- @ 60min: n=17, median=+55.7%, mean=+163.6%, hit≥10%=71%, hit≥20%=65%

### Bucket 4: Tier1-passed, scored below 4.0 (reference) (n=96)
- @ 5min: n=88, median=+0.1%, mean=+11.3%, hit≥10%=23%, hit≥20%=18%
- @ 15min: n=86, median=+0.2%, mean=+21.3%, hit≥10%=28%, hit≥20%=24%
- @ 30min: n=83, median=+0.2%, mean=+24.2%, hit≥10%=28%, hit≥20%=24%
- @ 60min: n=80, median=+0.2%, mean=+20.6%, hit≥10%=26%, hit≥20%=24%

## 3. Simulated exit rules (exact engine/risk.py logic, replayed over observed prices)

- 63 of 64 shadow trades hit a simulated exit condition so far (the rest either haven't moved enough or haven't been observed long enough)
- Breakdown: {'TAKE_PROFIT': 28, 'HARD_FLOOR': 32, 'STOP_LOSS': 3}
- Simulated win rate (of those that hit an exit): 28/63 (44%)

- Max observed gain across all shadow trades: median +43.7%, worst +0.0%, best +1717.2%
- Max observed drawdown across all shadow trades: median -92.5%, worst -99.8%

## 4. Every shadow trade so far

| symbol | score | entry_price | 30m return | max_gain | max_drawdown | sim. exit |
|---|---|---|---|---|---|---|
| Greenland | 4.18 | 0.0010549651 | +50.5% | +81.0% | -99.8% | TAKE_PROFIT |
| CALI | 4.86 | 0.0000813457 | +0.0% | +39.6% | -97.5% | TAKE_PROFIT |
| Stamp | 4.29 | 0.0014655075 | +158.3% | +210.5% | -26.9% | TAKE_PROFIT |
| BEAST | 4.22 | 0.0005575680 | +25.2% | +28.1% | -99.6% | HARD_FLOOR |
| BASKET | 4.56 | 0.0005650511 | +16.0% | +18.6% | -99.7% | HARD_FLOOR |
| JEANWIFHAT | 7.00 | 0.0000169554 | +170.9% | +186.7% | -86.0% | TAKE_PROFIT |
| WHEREJP | 6.40 | 0.0000934479 | +192.3% | +164.6% | -97.1% | HARD_FLOOR |
| FOMO LISA | 4.84 | 0.0001668638 | +0.0% | +59.3% | -98.2% | TAKE_PROFIT |
| zkSNARK | 5.62 | 0.0000668262 | +0.3% | +77.5% | -83.5% | TAKE_PROFIT |
| ZERO | 4.80 | 0.0000354804 | +0.0% | +0.0% | -89.4% | HARD_FLOOR |
| Lobby | 4.56 | 0.0005689452 | +24.8% | +28.6% | -99.7% | HARD_FLOOR |
| ORANGE | 4.82 | 0.0000466430 | +245.5% | +277.1% | -94.9% | TAKE_PROFIT |
| anon | 5.52 | 0.0000085729 | +143.7% | +190.9% | -21.1% | STOP_LOSS |
| anon | 6.66 | 0.0000945440 | +16.0% | +12.1% | -80.3% | HARD_FLOOR |
| CLIPPED | 4.84 | 0.0001067267 | +30.6% | +161.6% | -94.7% | TAKE_PROFIT |
| TIM | 4.81 | 0.0000530132 | +6.3% | +47.1% | -87.8% | TAKE_PROFIT |
| JEANDOOMER | 7.63 | 0.0001118582 | +58.3% | +213.5% | -97.6% | TAKE_PROFIT |
| JeanWhale | 5.69 | 0.0000446213 | +55.7% | +85.5% | -93.5% | TAKE_PROFIT |
| JEANTROLL | 6.67 | 0.0000588267 | +4.6% | +0.7% | -3.8% | no exit yet |
| MEALS | 5.60 | 0.0000261036 | +523.1% | +272.3% | -89.0% | HARD_FLOOR |
| PHILSEM | 5.72 | 0.0000560912 | +8.2% | +9.2% | -96.0% | HARD_FLOOR |
| Stamp | 4.17 | 0.0001277140 | +39.5% | +52.4% | -98.4% | TAKE_PROFIT |
| fomopay | 4.56 | 0.0005683772 | +11.9% | +16.8% | -99.6% | HARD_FLOOR |
| GFM | 4.79 | 0.0000087063 | +140.2% | +228.3% | -64.2% | TAKE_PROFIT |
| JEV | 4.06 | 0.0005356536 | +35.2% | +40.3% | -99.6% | TAKE_PROFIT |
| uoaoa | 5.70 | 0.0000775556 | +290.4% | +276.4% | -96.6% | HARD_FLOOR |
| GIVE | 7.59 | 0.0001880355 | +0.0% | +34.1% | -96.5% | TAKE_PROFIT |
| ONKEY | 4.49 | 0.0000100478 | +238.5% | +194.6% | -57.3% | HARD_FLOOR |
| FLORK | 4.69 | 0.0000055806 | +18.2% | +11.5% | -20.8% | STOP_LOSS |
| JEANPILL | 4.43 | 0.0001813494 | +63.0% | +73.7% | -98.9% | TAKE_PROFIT |
| Pigeon | 4.56 | 0.0005572099 | +54.3% | +62.1% | -99.6% | TAKE_PROFIT |
| TablePhil | 4.79 | 0.0000322894 | +54.1% | +102.6% | -93.3% | TAKE_PROFIT |
| CASINO | 4.73 | 0.0000186940 | +0.0% | +17.7% | -82.4% | HARD_FLOOR |
| Launchpad | 4.90 | 0.0000583032 | +0.0% | +0.0% | -95.1% | HARD_FLOOR |
| STC | 5.63 | 0.0000451420 | +0.0% | +28.3% | -91.3% | HARD_FLOOR |
| 1M | 5.61 | 0.0000206100 | +34.4% | +0.0% | -86.3% | HARD_FLOOR |
| FUNDED | 5.50 | 0.0000136511 | +1187.2% | +1717.2% | -28.4% | TAKE_PROFIT |
| SoLLM | 5.68 | 0.0000326259 | +34.0% | +62.6% | -92.1% | TAKE_PROFIT |
| FLY4003488 | 4.06 | 0.0007574929 | +42.6% | +47.4% | -99.7% | TAKE_PROFIT |
| imagine | 6.87 | 0.0000107456 | +62.3% | +75.7% | -73.2% | TAKE_PROFIT |
| PERC | 4.65 | 0.0000173365 | +65.5% | +130.8% | -86.1% | TAKE_PROFIT |
| PEPEPHIL | 5.94 | 0.0000354320 | +2.1% | +0.0% | -93.0% | HARD_FLOOR |
| Sanders | 4.91 | 0.0001012402 | +21.1% | +24.5% | -97.4% | HARD_FLOOR |
| bud | 6.93 | 0.0000408931 | +85.3% | +191.5% | -94.1% | TAKE_PROFIT |
| TF | 4.92 | 0.0000319344 | +13.4% | +34.1% | -90.6% | TAKE_PROFIT |
| CROWD | 4.38 | 0.0000183044 | +0.0% | +21.2% | -83.3% | HARD_FLOOR |
| INU | 5.71 | 0.0000650445 | +1060.6% | +383.9% | -94.9% | HARD_FLOOR |
| Tender | 4.82 | 0.0000479424 | +164.1% | +133.5% | -93.2% | HARD_FLOOR |
| fomopad | 4.22 | 0.0000089110 | +3.4% | +0.0% | -56.5% | HARD_FLOOR |
| minijean | 4.72 | 0.0000724985 | +159.4% | +149.4% | -96.6% | HARD_FLOOR |
| Simple | 4.82 | 0.0000269987 | +11.9% | +0.0% | -86.6% | HARD_FLOOR |
| Doge | 5.52 | 0.0000153557 | +0.0% | +0.0% | -76.6% | HARD_FLOOR |
| worldwar | 5.70 | 0.0000328371 | +107.7% | +166.9% | +0.0% | TAKE_PROFIT |
| DOGE | 5.64 | 0.0001868867 | +0.0% | +0.0% | -89.9% | HARD_FLOOR |
| ZEBRA | 5.17 | 0.0000429229 | +203.0% | +505.4% | +0.0% | TAKE_PROFIT |
| Aiden | 4.07 | 0.0005268668 | +47.0% | +0.0% | -99.6% | HARD_FLOOR |
| ZCAT | 4.75 | 0.0000189416 | pending | +19.2% | -38.3% | HARD_FLOOR |
| StonkBankr | 4.65 | 0.0000096631 | pending | +39.9% | -60.0% | TAKE_PROFIT |
| fomo | 7.64 | 0.0000998692 | pending | +204.8% | -97.7% | STOP_LOSS |
| SUSHINU | 5.72 | 0.0000712883 | pending | +33.3% | -96.8% | HARD_FLOOR |
| OpenAI | 4.06 | 0.0004420457 | pending | +5.2% | -99.5% | HARD_FLOOR |
| ZEAL | 4.83 | 0.0000299066 | pending | +9.3% | -82.7% | HARD_FLOOR |
| IKUN | 4.10 | 0.0005273147 | pending | +25.1% | -79.2% | HARD_FLOOR |
| GATSBY | 5.61 | 0.0000260695 | pending | +5.3% | -64.1% | HARD_FLOOR |

## 5. Reminder
This is n=64 shadow trades. The goal per your instructions is NOT to conclude 4.0 is correct — it's to accumulate enough of these to eventually answer that. No thresholds were changed. Re-run this script as more data accumulates.