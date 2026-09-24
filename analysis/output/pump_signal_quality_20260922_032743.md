# Pump Signal Quality — 20260922_032743

ANALYSIS-ONLY. Read-only, no trading logic touched.

## Methodology / reuse

Taxonomy (winner/failed_pump/rug/flat_no_event) reused AS-IS from `pump_timing_research_dataset_20260922_030234.csv`, joined by token_id - not recomputed. The 4 already-validated leading-indicator rules and their exact causal feature formulas are reused from `analysis/pump_timing_research.py` (`trailing_return_3min > 0.053`, `trailing_return_5min > 0.116`, `buy_pressure > 0.667`, `volume_mult > 1.004`). `trailing_return_3min` is used as `primary_signal_time` (the one the prior report anchored its 8-minute lead-time finding to).

Step 2 features are computed using ONLY data at or before `primary_signal_time` (zero-delay, known at the instant the signal fires). Step 3's `confirmation_return_3min` uses data strictly AFTER `primary_signal_time` - it represents a "wait 3 minutes before committing" strategy, explicitly NOT a zero-delay feature, and is reported separately so it is never confused with the zero-delay features. `is_winner` label comes only from the pre-computed taxonomy (based on the token's full eventual lifetime), never fed back into any feature.

## Population and signal-firing coverage

Real-pump tokens (winner+failed_pump) from source CSV: n=438. Of these, 438 had a usable snapshot series in this run (dataset may have grown/shrunk slightly since the source CSV's snapshot query time).

| rule | n fired (of 438) | miss rate |
|---|---|---|
| trailing_return_3min > 0.053 | 420 | 4.1% |
| trailing_return_5min > 0.116 | 378 | 13.7% |
| buy_pressure > 0.667 | 210 | 52.1% |
| volume_mult > 1.004 | 436 | 0.5% |

Primary signal (`trailing_return_3min`) fired for 420 of 438 tokens (4.1% miss rate if n_pop>0). All Step 2-4 analysis below is restricted to these 420 tokens with a primary signal.
- 0 of 420 tokens had no snapshot ~3 min after the signal (e.g. signal fired very close to the token's last observed point) - `confirmation_return_3min` is null for these, not imputed.

**Baseline winner rate among all signaled real-pump tokens: 41.9% (n=420).**

## At-signal and delayed-confirmation features vs is_winner (time-split validated)

### Survived (same direction, train vs test)

| feature | split_value | train n(above/below) | train winner-rate(above/below) | test n(above/below) | test winner-rate(above/below) | train p |
|---|---|---|---|---|---|---|
| minutes_since_pump_start_at_signal | 3.001 | 146/63 | 0.507/0.302 | 128/83 | 0.398/0.386 | 0.0063 |
| trailing_return_5min_at_signal | 0.0824 | 14/9 | 0.714/0.444 | 11/12 | 0.545/0.500 | 0.2193 |
| n_rules_cofiring_at_signal | 1 | 96/113 | 0.729/0.204 | 78/133 | 0.577/0.286 | 0.0000 |

### Did NOT survive / could not be tested (reported for completeness, not hidden)

| feature | reason / train-vs-test |
|---|---|
| signal_price_over_pump_start | split=1.11, train rate 0.383/0.690 (effect -0.307), test rate 0.354/0.484 (effect -0.131) - reversed in test |
| buy_pressure_at_signal | split=0.979, train 21/188 (0.952/0.388) - test-side n too small (2/209) |
| volume_mult_at_signal | split=2.119, train rate 0.143/0.479 (effect -0.336), test rate 0.423/0.389 (effect +0.034) - reversed in test |
| confirmation_return_3min | split=-1, train 182/27 (0.363/1.000) - test-side n too small (211/0) |

## Confluence: does the NUMBER of rules co-firing at signal time matter?

**Survived.** More rules co-firing at the moment `trailing_return_3min` triggers is associated with a higher winner rate: above 1 co-firing rules, winner rate = 72.9% (train, n=96) / 57.7% (test, n=78) vs 20.4% (train, n=113) / 28.6% (test, n=133) at/below it.

Raw (non-split) breakdown for reference:

| n_rules_cofiring | winner rate | n |
|---|---|---|
| 0 | 11.1% | 45 |
| 1 | 27.9% | 201 |
| 2 | 65.5% | 165 |
| 3 | 77.8% | 9 |

## Headline

**3 feature(s) survived time-split validation.** Best by test-side effect size: `n_rules_cofiring_at_signal > 1` - winner rate rises from baseline 41.9% (n=420) to 72.9% (train, n=96) / 57.7% (test, n=78) when this holds at/after signal time.

## Caveats

- Population: 438 real-pump tokens had usable snapshot data; primary signal fired for 420 (420 used in Step 4). Miss-rate for each of the 4 validated rules is reported above, not hidden.
- 0 tokens lack a delayed-confirmation snapshot (~3 min after signal) - null, not imputed.
- No-lookahead confirmed: Step 2 features use only data at/before primary_signal_time; Step 3's confirmation feature uses data strictly after it and is labeled as a delayed strategy, never conflated with a zero-delay feature; is_winner is the token's already-computed eventual outcome, never a feature.
- Train/test split is chronological by primary_signal_time, same discipline as prior scripts this session.
- This is descriptive/exploratory pattern-finding, not a proposed rule change. No trading logic, thresholds, or scorer weights were touched.