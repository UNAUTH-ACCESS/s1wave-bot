# confluence_entry_v1 — Go/No-Go Evaluation — 20260922_163936

ANALYSIS-ONLY. No simulation. No trading logic touched. Strategy frozen per explicit instruction — this evaluation observes, it does not tune.

- Closed positions: **38** (target: 100+, or run early if SolanaTracker's account exhausted again — see caveats).
- **NOTE: n=38 is below the 100 target.** Every number below is real, but treat this as an interim read, not the final evaluation, unless this run was explicitly triggered early by API exhaustion.

## Core metrics

- **Net cumulative P&L (additive, R-multiples): +5.63R** (sum of pnl_pct across all 38 trades, equal unit risk assumed per trade)
- Net cumulative P&L (compounding, full-reinvestment reference only): +31.3%
- **Expectancy: +14.82%** per trade
- **Win rate: 65.8%** (25/38)
- **Profit factor: 2.38**
- Median winner: +31.1%
- Mean winner: +38.9%
- Median loser: -32.3%
- Mean loser: -31.5%
- **Maximum drawdown: 0.98R** (peak +3.31R -> trough +2.34R, on the chronological additive equity curve)

## Rug frequency and loss magnitude

- Total HARD_FLOOR-magnitude exits: 12/38 (32%)
- Of those, "normal" floor breaches (worse than -6% but not below -40%): 7, mean -18.1%
- **Confirmed rugs (<= -40%): 5/38 (13.2%)**, mean -55.3%, worst -97.6%

## Performance by confluence count

- 2 rules co-firing: n=36, win_rate=69%, expectancy=+17.6%, profit_factor=2.87, median=+30.6%
- 3 rules co-firing: n=2, win_rate=0%, expectancy=-35.5%, profit_factor=0.00, median=-35.5%

## Performance over chronological quarters (does the edge persist?)

### Quarter 1 (2026-09-22T12:43:30.682576+00:00 to 2026-09-22T13:57:51.842422+00:00)
- quarter: n=9, win_rate=56%, expectancy=+17.0%, profit_factor=2.92, median=+30.5%

### Quarter 2 (2026-09-22T13:57:53.558801+00:00 to 2026-09-22T14:43:54.015832+00:00)
- quarter: n=9, win_rate=67%, expectancy=+12.9%, profit_factor=2.34, median=+30.9%

### Quarter 3 (2026-09-22T14:48:03.728045+00:00 to 2026-09-22T15:48:35.686109+00:00)
- quarter: n=9, win_rate=78%, expectancy=+23.9%, profit_factor=3.06, median=+30.8%

### Quarter 4 (2026-09-22T15:48:35.686109+00:00 to 2026-09-22T16:29:32.214489+00:00)
- quarter: n=11, win_rate=64%, expectancy=+7.2%, profit_factor=1.57, median=+30.2%

## Out-of-sample check: does the pooled expectancy survive a chronological split?

- TRAIN (earlier half): n=19, win_rate=63%, expectancy=+15.8%, profit_factor=2.81, median=+30.9%
- TEST (later half): n=19, win_rate=68%, expectancy=+13.8%, profit_factor=2.08, median=+30.3%
- Mann-Whitney U test (train vs test distributions): p=0.559 (no significant difference — consistent with a stable edge)
- **Expectancy sign survives out-of-sample (positive in both halves).**

## Exit reason breakdown

- TAKE_PROFIT: n=25 (66%), mean=+38.9%
- HARD_FLOOR: n=12 (32%), mean=-33.6%
- STOP_LOSS: n=1 (3%), mean=-6.5%

## Caveats

- All figures are real, live 1-second-monitored outcomes — no simulation.
- Position sizing is not yet decided; additive (R-multiple) cumulative P&L and drawdown assume equal risk per trade, not a specific dollar amount.
- No trading logic, thresholds, or scorer weights were touched to produce this evaluation.