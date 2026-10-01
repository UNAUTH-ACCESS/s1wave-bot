# S1Wave calibration handbook

For the next AI (or human) continuing calibration. Read `CLAUDE.md` HANDOFF first for the standing rules (no secrets in output, no live-logic changes without the user's go-ahead, no history rewrites). This file covers only **what we measure, how, and how to decide**.

Start with:

```
cd /home/solana/s1wave-bot/solanabot && source .venv/bin/activate
S1WAVE_ENV_FILE=.env PYTHONPATH=. python analysis/calibration_report.py     # base account
# .env.second / .env.efetobo for the other accounts (each has its own DB)
```

It is read-only, prints Markdown, and saves `analysis/output/calibration_<UTC>.md`. Save one per session so later sessions can diff against it.

## 1. The question calibration answers

The strategy (`confluence_entry_v1`) buys fresh pump.fun-style tokens when a momentum signal fires, filtered by three entry rules, and exits on a layered stop. Calibration asks, in priority order:

1. **Is there real edge after real costs?** Measured only on live (real-money) trades.
2. **Where does paper (shadow) disagree with live, and why?** The shadow dataset is 5x larger than live, so it is only useful to the extent it matches live.
3. **Do the entry filters and exit parameters still discriminate?** Re-test thresholds on new data.

The recurring lesson: **paper numbers flatter the strategy; live numbers are the truth.** As of 2026-10-01, shadow shows ~80% win rate and a +16% capped mean while verified live shows 1 win in 38 and a -44% mean. Closing that gap is the whole job.

## 2. Source of truth hierarchy (never mix these up)

| Rank | Measure | Where | Trust |
|---|---|---|---|
| 1 | On-chain realized P&L | `confluence_live_trades.real_pnl_usd`, `entry_real_sol_lamports`, `exit_real_sol_lamports` (read from the real transactions; trades closed since 2026-09-28) | Ground truth |
| 2 | Intended live P&L | `pnl_usd`, `position_usd` (older trades) | Understates real spend about 6x in aggregate; use only as fallback and flag it |
| 3 | Live executable proceeds | `real_pnl_pct`, `real_price` (a Jupiter sell quote for the exact held size at check time) | Good for guard decisions, not a realized result |
| 4 | Shadow modeled-executable | `confluence_shadow_positions.exec_pnl_pct` (since 2026-10-01 10:58Z) | Optimistic until calibrated, see 5 |
| 5 | Shadow snapshot | `pnl_pct` (DexScreener price) | Optimistic: no fees or slippage, and thin pools freeze at stale prices |

Jupiter `priceImpactPct` is **not a measurement** on thin routes: it returns the sentinel `"1"`. Always judge by proceeds (`outAmount`).

## 3. Data catalog

One Postgres DB per account (base, second, efetobo). Row counts are for the base DB at 2026-10-01. Migrations in `migrations/` are applied by hand (`create_all` only creates new tables, never new columns).

| Table | What | Rows | Key columns | Notes |
|---|---|---|---|---|
| `tokens` | Every discovered token | 8.1k | `mint_address`, `status`, `liquidity_usd`, `market_cap_usd` | |
| `token_snapshots` | Time series per token (~30-60s) | 209k | `sampled_at`, price, volume, buys/sells, liquidity | Written by sampling + discovery. Stops when SolanaTracker keys run out |
| `token_evaluations` | Gate verdicts with inputs | 10k | `gate` (`TIER1`...), `inputs_json` (age, liquidity, mcap, lp_burn, wash_multiplier, authorities, buy_pressure, volume_mult), `rejection reason` | Source of the wash-trading and liquidity filters |
| `momentum_signal_events` | One row per token on first signal | 4.4k | `triggered_at`, `trigger_price`, `trailing_return_3min/5min`, `buy_pressure`, `volume_mult`, `n_rules_cofiring`, `liquidity_usd`, `market_cap_usd` | Experiment `momentum_confluence_v1`. Entry needs `n_rules_cofiring >= 2` |
| `confluence_shadow_positions` | Paper trades | 1.6k (532 closed) | `entry_*`, `exit_*`, `exit_reason`, `pnl_pct`, `status`; modeled fills: `exec_status` (`modeled`/`no_quote`), `exec_notional_usd`, `exec_entry_liq_usd`, `exec_pnl_pct` | Skips are rows too: `wash_skipped`, `high_liq_skip`, `low_bp_skip` (748/80/209). `status` is VARCHAR(16): keep new values short |
| `confluence_shadow_observations` | 1-second price series of open shadow positions | 1.5M | `position_id`, `observed_at`, `price_usd` | Enables exit-rule replays |
| `confluence_shadow_exec_checks` | Modeled guard/exit checks | 0 so far | `kind` (entry/guard/exit), `exec_pnl_pct`, `price_impact_raw` (modeled fraction) | Fills once signals fire again |
| `confluence_shadow_variant_positions` | Exit-rule variants riding on each shadow position (same entry/ticks, own stop and hold rules, modeled fills) | 0 until signals fire | `variant`, `status`, `exit_reason`, `pnl_pct`, `exec_pnl_pct` | Compare with `analysis/variant_report.py live` |
| `confluence_live_trades` | Real trades | 908 (109 closed) | everything in the hierarchy above, plus fees (`entry/exit_network_fee_lamports`, rent, `reclaim_*`), `exit_reason`, `error_detail` | Same skip statuses, plus `buy_failed` (38) |
| `confluence_live_observations` | Per-check price record of open live trades | 103k | | Guard audit trail |
| `confluence_notifications` | Alerts and events | 413 | `event` | Includes key exhaustion, guard exits, halts |
| `balance_history`, `circuit_breaker_state`, `daily_loss_state` | Capital and halt state | | | Used for drawdown and halt analysis |

Exit reasons: `TIME_EXIT` (6h), `HARD_FLOOR` (-7%, or velocity breaker at a -25% single tick), `STOP_LOSS`/`TRAILING_STOP` (staircase from -6%, ratchets in 10% steps), `LIQUIDITY_GUARD`, `MANUAL_CLOSE`, `RECONCILED_ONCHAIN`.

### Data-quality caveats (apply to every analysis)

- **Phantom peaks.** A couple of shadow trades show single-tick jumps to prices with $0 recorded liquidity. Raw shadow means are meaningless (the base DB's raw mean is over 3000%). **Always cap each trade's counted gain at +100%** (`CAP` in the report) and quote the capped mean.
- **Frozen snapshots.** Thin pools can sit at a stale DexScreener price, so `TIME_EXIT` in shadow wins 99.6% of the time. Treat that as the most suspect cell in the dataset.
- **Sampling outages.** When SolanaTracker keys are exhausted there are no new snapshots or signals. Check report section 1 first: if the latest snapshot is old, nothing below is fresh. Outage windows must be excluded from time-series work (see `analysis/clean_recap.py` for how a confirmed window was handled).
- **Regimes.** Compare only within a regime. Boundaries: entry filters all live **2026-09-24**; real on-chain audit trail **2026-09-28**; liquidity-guard fix (judges proceeds, not impact) **2026-10-01 06:11Z**; modeled shadow fills **2026-10-01 10:58Z**. The constants are at the top of `calibration_report.py`.
- **Live cohort is small.** Double-digit swings from noise alone are normal at n under 50. Report n next to every rate.

## 4. Standard metrics (define them the same way every time)

- **Return per trade** = realized P&L / capital deployed in that trade (live: `real_pnl_usd` / real entry spend if known, else `position_usd`).
- **Win rate** = share of trades with return > 0. Never quote it without the **mean and median**: live wins are about $0.008 and losses about $0.15, so a 33% win rate still loses money.
- **Capped mean** = mean of `min(return, +100%)`. **Rug rate** = share with return ≤ -40%.
- **Expectancy** = capped mean after fees. Win rate alone is the wrong measure (see `analysis/momentum_signal_expectancy.py`).
- **Max drawdown** and the cumulative P&L curve are on the dashboard ("Live performance") and `GET /confluence/live/performance`.
- **Paired gap** = shadow return minus live return on the same token (report section 4). This is the model's calibration error; track whether it shrinks.

## 5. Analysis types, and which script does each

| Type | Question | Script |
|---|---|---|
| Standard battery | Where do we stand? | `analysis/calibration_report.py` |
| Guard audit | Is the liquidity guard liquidating healthy trades? Live vs shadow since the fix | `analysis/liquidity_guard_false_positives.py` |
| Bucket / filter test | Does buy-pressure, liquidity, wash-trading, rule-count still discriminate? | `calibration_report.py` section 5; `engine/filter_calibration.py` runs a weekly 14-day version and alerts on drift |
| Strategy go/no-go | Expectancy of `confluence_entry_v1` | `confluence_entry_v1_evaluation.py`, `confluence_shadow_expectancy.py` |
| Exit-rule replay | Would other stops or sizing have done better on the recorded 1s paths? | `sl_tp_and_concurrency_sweep.py`, `trailing_stop_retroactive_replay.py` |
| Signal research | Does the momentum signal precede peaks (time-split validated)? | `pump_timing_research.py`, `pump_signal_quality.py`, `momentum_signal_prospective.py`, `momentum_signal_expectancy.py` |
| Population / lifecycle | Survival curves, sweet spots, winner profiles | `token_lifecycle_analysis.py`, `full_lifecycle_report.py`, `winners_profile.py`, `edge_research.py`, `raw_feature_backtest.py` |

All are read-only. Outputs go to `analysis/output/` (large CSVs and Markdown snapshots; cheap to regenerate).

## 6. The calibration loop

1. **Run the battery** and read section 1 (data freshness) before anything else.
2. **Fix one thing at a time.** The 2026-10-01 guard bug was found because shadow (no guard) and live (guard) differed by one variable.
3. **Prospective, not retrospective.** Every change gets a start timestamp constant; compare only data after it. Retrospective fits on the dataset that produced a rule overstate it (the signal research used a chronological train/test split for this reason).
4. **Bucket, then cross-tab.** Several apparent filters (liquidity, LP burn, wash trading) were the same population. Check overlap before claiming independence.
5. **Sanity-check with a real quote** before acting on a DexScreener price.
6. **Decide with guardrails.** Change live parameters only when: n is adequate, the effect holds out-of-sample, the change is a narrow tested condition (template: `workers/entry_filters.py`), and the user approves. Thresholds are never auto-tuned; the calibration check only flags drift.
7. **Deploy and watch rate limits**, not only correctness. Jupiter's free tier is one shared per-IP budget for all live trading; background checks have already starved it once.
8. **Document** the finding, the regime start date, and how to reproduce it (script + command) in `CLAUDE.md`, and save a new `calibration_*.md`.

### Parallel exit variants (2026-10-01)

`engine/exit_variants.py` parameterises the live exit stack (`base` reproduces it exactly; tested). Each new shadow entry also opens one row per variant in `confluence_shadow_variant_positions` (wide_stop, tight_stop, fine_step, coarse_step, no_trail, hold_1h, hold_20m, tp_50), driven by the same ticks and modeled fills, so every day is a controlled experiment on exits. Add or change a variant by editing `variant_specs()`; old rows keep their name, so rename rather than redefine. Exit rules only: entry-filter variants would need price tracking of skipped candidates (not done).

- `analysis/variant_report.py live`: paired difference vs base, bootstrap 95% CI, verdict (`candidate` only if n >= 30 paired and CI above 0). A flag for human review, never an auto-promotion.
- `analysis/variant_report.py replay`: same variants over recorded 1s paths of closed positions. Available now but censored (paths end at the primary's exit), so trust the "resolved only" column and live mode.
- Setting: `SHADOW_VARIANTS_ENABLED`.

## 7. Open calibration questions (highest value first)

1. **Does the guard fix work?** Needs about 30 closed live trades after 2026-10-01 06:11Z (`liquidity_guard_false_positives.py` prints the prospective line). Before the fix, 59 of 109 live exits were `LIQUIDITY_GUARD`.
2. **Manual closes beat automatic exits.** Live `MANUAL_CLOSE`: 28 trades, 68% win, +17.5% mean, versus `LIQUIDITY_GUARD` 59 trades, 14% win, -36.5% mean (n small, selection effect likely since the operator chooses when). Worth studying what the operator sees.
3. **Calibrate the shadow pool model** against live: compare `exec_pnl_pct` to live return on the same tokens (report section 4), then adjust `SHADOW_EXEC_DEX_FEE_PCT` and consider a rug-risk term (the smooth AMM understates sudden liquidity pulls). Needs working sampling keys so signals fire.
4. **Shadow `TIME_EXIT` realism.** 244 shadow time exits at 99.6% win is implausible; test with sell-quote or pool-model proceeds at the hold-expiry price.
5. **Entry cost.** Live pays rent, network fees, and later fills. Quantify the per-trade fixed cost against the $0.40 average position; at that size fixed costs may exceed the edge. Consider whether position size, not signal quality, is the binding constraint.
6. **Sampling dependency.** All signals depend on SolanaTracker keys that are lifetime-capped. No key, no data, no calibration. A paid plan or an alternative snapshot source is a prerequisite for sustained work.

## 8. Extending the dataset

- Add columns with an additive `migrations/phaseNN_*.sql` and apply it to **all three** account DBs by hand; add the matching ORM column.
- Prefer recording raw inputs over derived verdicts (the `inputs_json` pattern) so a later analysis can recompute rules.
- Record skipped candidates as rows with a short status; they are the counterfactual that tells you whether a filter helps.
- Never gate behavior on a new field until it has a prospective track record.

## 9. Exporting the dataset (dashboard "Data export" card)

- `GET /export/index` lists exportable tables with row counts. `GET /export/<table>.csv.gz` streams one table as gzip CSV (tokens, snapshots, evaluations, signals, shadow_positions, shadow_observations, shadow_exec_checks, live_trades, live_observations). Code: `api/export.py`.
- `GET /export/handoff.zip` is the **AI handoff pack**: MANIFEST.md, a freshly generated calibration report, schema.json, 300-row samples of each table, CLAUDE.md, CALIBRATION.md, README.md, and the key code (`code/`, plus `code/analysis/`). It holds no env files, keys or notebook. Give it to a new AI first; pair it with the full `.csv.gz` files when it needs the raw data.
- All routes sit behind the dashboard Basic Auth. Each account's dashboard exports its own DB.
