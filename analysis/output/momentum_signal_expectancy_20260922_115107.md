# Momentum Confluence — Entry-to-Exit Expectancy — 20260922_115107

ANALYSIS-ONLY. Read-only, no trading logic touched. Simulates a REAL entry-to-exit round trip for every momentum_confluence_v1 signal using engine/risk.py's exact, unmodified exit priority (via analysis/shadow_backtest.py's simulate_exit(), imported not reimplemented) — answers "is expectancy actually better", not just "is the win rate higher".

- Exit rule replayed: HARD_FLOOR -7% > STOP_LOSS -6% > TAKE_PROFIT +30% > TIME_EXIT 6h
- Total signals: 198. Resolved (hit an exit condition): 159. Unresolved (still open, too young or data ran out before any exit fired): 39.

## Expectancy by confluence bucket (0-1 rules vs 2+ rules) — resolved trades only

n=198 >= MIN_FOR_SPLIT=193 — chronological time-split active. Split at 2026-09-22T07:35:27.401114+00:00.

### TRAIN
- 0-1 rules: n=45, win_rate=27%, **expectancy=-21.3%**, avg_win=+42.9%, avg_loss=-44.7%
- 2+ rules  : n=32, win_rate=66%, **expectancy=-10.9%**, avg_win=+33.2%, avg_loss=-95.0%

### TEST
- 0-1 rules: n=47, win_rate=32%, **expectancy=-16.8%**, avg_win=+55.6%, avg_loss=-50.7%
- 2+ rules  : n=35, win_rate=69%, **expectancy=+12.3%**, avg_win=+47.3%, avg_loss=-64.2%

**Expectancy advantage for 2+ rules holds in test.**

## Exit-reason breakdown by confluence bucket (resolved trades only)

- 0-1 rules (n=92): HARD_FLOOR=63 (68%), TAKE_PROFIT=27 (29%), STOP_LOSS=2 (2%)
- 2+ rules (n=67): TAKE_PROFIT=45 (67%), HARD_FLOOR=22 (33%)

## Unresolved signals by confluence bucket (excluded from expectancy — not a win, not a loss)

- 0-1 rules: n=2 still open (median age 364min)
- 2+ rules: n=37 still open (median age 283min)

## Caveats

- `pnl_pct` here is the exact simulated round-trip result of engine/risk.py's real exit rules — not a fixed-horizon forward return like the earlier prospective checkpoint used. This is the correct number for "would this have been a good trade", the earlier checkpoint's win-rate/median was not.
- Unresolved signals are excluded, not treated as 0% or as losses — a young signal that hasn't hit any exit condition yet is genuinely undetermined, and folding it into either bucket would bias the result.
- No trading logic, thresholds, or scorer weights touched. Nothing implemented from this checkpoint.