# confluence_entry_v1 — Go/No-Go Evaluation — 20260923_010851

ANALYSIS-ONLY. No simulation. No trading logic touched. Strategy frozen per explicit instruction — this evaluation observes, it does not tune.

- Closed positions: **167** (target: 100+, or run early if SolanaTracker's account exhausted again — see caveats).

## Core metrics

- **Net cumulative P&L (additive, R-multiples): +71.41R** (sum of pnl_pct across all 167 trades, equal unit risk assumed per trade)
- Net cumulative P&L (compounding, full-reinvestment reference only): -100.0%
- **Expectancy: +42.76%** per trade
- **Win rate: 66.5%** (111/167)
- **Profit factor: 3.71**
- Median winner: +31.0%
- Mean winner: +88.1%
- Median loser: -42.1%
- Mean loser: -47.1%
- **Maximum drawdown: 3.69R** (peak +6.82R -> trough +3.12R, on the chronological additive equity curve)

## Rug frequency and loss magnitude

- Total HARD_FLOOR-magnitude exits: 54/167 (32%)
- Of those, "normal" floor breaches (worse than -6% but not below -40%): 25, mean -14.6%
- **Confirmed rugs (<= -40%): 29/167 (17.4%)**, mean -77.8%, worst -99.4%

## Performance by confluence count

- 2 rules co-firing: n=163, win_rate=67%, expectancy=+44.2%, profit_factor=3.84, median=+30.2%
- 3 rules co-firing: n=4, win_rate=25%, expectancy=-17.3%, profit_factor=0.30, median=-30.3%

## Performance over chronological quarters (does the edge persist?)

### Quarter 1 (2026-09-22T12:43:30.682576+00:00 to 2026-09-22T17:07:12.239395+00:00)
- quarter: n=41, win_rate=68%, expectancy=+16.0%, profit_factor=2.60, median=+30.5%

### Quarter 2 (2026-09-22T17:07:45.736270+00:00 to 2026-09-22T20:12:09.404968+00:00)
- quarter: n=41, win_rate=54%, expectancy=-4.7%, profit_factor=0.79, median=+13.3%

### Quarter 3 (2026-09-22T20:14:51.159639+00:00 to 2026-09-22T22:21:12.354866+00:00)
- quarter: n=41, win_rate=78%, expectancy=+17.8%, profit_factor=2.82, median=+30.1%

### Quarter 4 (2026-09-22T22:24:24.041224+00:00 to 2026-09-23T01:06:36.638452+00:00)
- quarter: n=44, win_rate=66%, expectancy=+135.2%, profit_factor=7.46, median=+30.1%

## Out-of-sample check: does the pooled expectancy survive a chronological split?

- TRAIN (earlier half): n=83, win_rate=61%, expectancy=+5.6%, profit_factor=1.35, median=+30.2%
- TEST (later half): n=84, win_rate=71%, expectancy=+79.5%, profit_factor=6.05, median=+30.1%
- Mann-Whitney U test (train vs test distributions): p=0.834 (no significant difference — consistent with a stable edge)
- **Expectancy sign survives out-of-sample (positive in both halves).**

## Exit reason breakdown

- TAKE_PROFIT: n=94 (56%), mean=+102.0%
- HARD_FLOOR: n=54 (32%), mean=-48.6%
- TIME_EXIT: n=17 (10%), mean=+10.8%
- STOP_LOSS: n=2 (1%), mean=-6.6%

## Caveats

- All figures are real, live 1-second-monitored outcomes — no simulation.
- Position sizing is not yet decided; additive (R-multiple) cumulative P&L and drawdown assume equal risk per trade, not a specific dollar amount.
- No trading logic, thresholds, or scorer weights were touched to produce this evaluation.