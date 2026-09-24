# Confluence Entry v1 — Real Entry-to-Exit Expectancy — 20260922_162014

ANALYSIS-ONLY. No simulation — every number below comes directly from confluence_shadow_positions rows already closed live by workers/confluence_shadow_worker.py using real 1-second DexScreener monitoring and engine/risk.py's real exit priority.

- Total positions: 47. Closed: 34. Still open: 13.
- Every row here has n_rules_cofiring >= 2 by construction (the entry rule itself) — there is no 0-1-vs-2+ split possible in this table, unlike the earlier proxy checkpoints.

## Real expectancy (closed positions only)

- n = 34
- win rate = 68%
- **expectancy (mean pnl_pct) = +16.5%**
- median pnl_pct = +30.6%
- avg win = +39.6%
- avg loss = -31.9%

## Comparison: real (this table) vs the earlier WATCHING-tier proxy backtest

- Proxy backtest (analysis/momentum_signal_expectancy.py, n=198 signals, 30-60s cadence): train expectancy -10.9%, test expectancy +12.3%
- Real (this table, 1-second cadence): expectancy +16.5% (n=34)
- **closes the gap toward positive.**

## Exit reason breakdown

- TAKE_PROFIT: n=23 (68%), mean pnl_pct=+39.6%
- HARD_FLOOR: n=10 (29%), mean pnl_pct=-34.4%
- STOP_LOSS: n=1 (3%), mean pnl_pct=-6.5%

## Hold time (closed positions)

- median hold: 6.4 min, min=0.0, max=15.1

## All closed positions

| symbol | n_cofiring | exit_reason | pnl_pct | hold_min |
|---|---|---|---|---|
| BUFFETT | 2 | TAKE_PROFIT | +107.1% | 3.7 |
| Pump | 2 | HARD_FLOOR | -47.2% | 0.0 |
| MrBeast | 2 | TAKE_PROFIT | +31.4% | 11.1 |
| Huawei | 2 | STOP_LOSS | -6.5% | 0.0 |
| TRUMPONS | 2 | TAKE_PROFIT | +33.3% | 12.2 |
| BITTEDDY | 2 | TAKE_PROFIT | +30.5% | 5.7 |
| NVIDIA | 2 | TAKE_PROFIT | +30.9% | 11.1 |
| $KFCAT | 2 | HARD_FLOOR | -16.0% | 0.0 |
| BUTTFUCK | 2 | HARD_FLOOR | -10.1% | 0.0 |
| FOMOMODE | 2 | TAKE_PROFIT | +32.8% | 1.1 |
| PLOI | 2 | TAKE_PROFIT | +31.3% | 6.1 |
| NFL | 2 | TAKE_PROFIT | +30.9% | 8.8 |
| NTDA | 3 | HARD_FLOOR | -32.3% | 0.0 |
| X | 2 | HARD_FLOOR | -45.6% | 4.1 |
| x7 | 2 | TAKE_PROFIT | +31.7% | 14.2 |
| purplecat | 2 | TAKE_PROFIT | +45.2% | 9.2 |
| Huddle | 2 | HARD_FLOOR | -8.6% | 0.0 |
| FOMO | 2 | TAKE_PROFIT | +30.5% | 5.9 |
| FOMO | 2 | TAKE_PROFIT | +31.0% | 10.1 |
| SpaceX | 2 | TAKE_PROFIT | +30.8% | 7.3 |
| NASA | 2 | HARD_FLOOR | -97.6% | 10.7 |
| Amazon | 2 | TAKE_PROFIT | +134.5% | 0.6 |
| tsmc | 2 | TAKE_PROFIT | +31.1% | 15.1 |
| fomopay | 2 | TAKE_PROFIT | +30.3% | 13.1 |
| Amazon | 2 | TAKE_PROFIT | +31.8% | 11.1 |
| CRCL | 2 | HARD_FLOOR | -7.1% | 0.1 |
| QI | 2 | HARD_FLOOR | -40.8% | 10.1 |
| fomocoin | 2 | TAKE_PROFIT | +30.5% | 10.1 |
| PETERPAN | 2 | TAKE_PROFIT | +30.2% | 6.7 |
| Pochi | 2 | TAKE_PROFIT | +31.5% | 5.7 |
| NTDA | 3 | HARD_FLOOR | -38.7% | 0.0 |
| NFL | 2 | TAKE_PROFIT | +30.6% | 12.6 |
| OpenAI | 2 | TAKE_PROFIT | +31.7% | 1.6 |
| TikTok | 2 | TAKE_PROFIT | +30.3% | 11.7 |

## Caveats

- Small, young sample — treat direction as a lead, not a confirmed result, until n is much larger.
- Two early positions (X7, CENTS) that closed on a DexScreener bad tick before the _best_pair volume-preference fix and tick-confirmation guard landed were manually reset to 'open' with corrupted observations removed — this reflects the corrected table, not a script-side filter. If they appear above as closed, it is on their real, post-fix outcome.
- No trading logic, thresholds, or scorer weights touched. Nothing implemented from this checkpoint.