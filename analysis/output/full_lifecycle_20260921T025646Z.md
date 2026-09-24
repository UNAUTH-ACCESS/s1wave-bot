# S1 Wave — Full Lifecycle & Paper-Trading Forensic Report
Generated: 2026-09-21T02:56:46.438907+00:00

**Analysis-only. No trading logic, thresholds, entries, exits, or risk controls were changed.**

## 0. Data-availability facts (read this before the numbers below)

1. **Paper execution has zero modeled slippage/delay by design.** `execution.py::_simulate_buy()` returns `actual_price=None` — the caller always fills at the snapshot price it already had. Verified against both real trades below: entry_price matches the signal snapshot's price to 12 decimal places, exactly, both times.
2. **No price-tick persistence exists for ENTERED tokens.** Once a token enters a trade, sampling_worker deliberately stops sampling it (a websocket-fed live monitor takes over, and nothing about that feed is written to any table). For the 2 real trades this means: maximum favorable/adverse excursion WHILE HOLDING, and what the token did AFTER exit, cannot be reconstructed from stored data — reported as 'not available', not estimated.
3. Symbols collide across unrelated mints throughout this dataset — every lookup below is by token_id, never by symbol text.
4. Known contamination windows excluded: 2026-09-20T12:47:05+00:00 to 2026-09-20T17:37:13+00:00; 2026-09-20T21:57:07+00:00 to 2026-09-21T02:56:45.961710+00:00

## 1-4 & 7. The real paper trades — full forensic reconstruction

**2 real paper trades exist. This is the complete set — not a sample.**

### Trade: JEANTROLL (6o69QeCQ9wUqFUHvSTReaYBj9HXvTj9UDByVzCZfpump)

**Token lifecycle**
- Discovered: 2026-09-20T19:35:14.940260+00:00
- Tier1 decision: PASS at 2026-09-20T19:35:14.940260+00:00 (age_at_discovery=0.36min, liquidity=$17,387)
- Signal gate: SCORER, fired at 2026-09-20T19:38:28.004572+00:00, reason=strong_buy
- Score: 9.08 (bp_component=7.70830, vm_component=10, vlr_component=10)

**Entry analysis**
- Signal generated: 2026-09-20T19:38:28.004572+00:00 @ price 0.0000592222 (nearest snapshot at or before signal time)
- Actual entry: 2026-09-20T19:38:29.017574+00:00 @ price 0.0000592222
- Delay signal→entry: 1.01s
- Price movement signal→entry: +0.0% (0% is expected — see fact #1 above, not a data error)
- Local peak in the snapshots leading up to entry: 0.0000592222 (entry WAS at the local peak)

**Exit analysis**
- Exit: 2026-09-20T19:38:45.138699+00:00 @ price 0.0000555912, reason=STOP_LOSS
- Hold duration: 16s
- Realized P&L: -6.1% ($-3.07)
- Max favorable/adverse excursion while holding: **not available** (see fact #2 above — no tick persistence for ENTERED tokens)
- What happened after exit: **not available** (same reason — sampling stops once a token leaves WATCHING/OBSERVING, CLOSED tokens are never re-sampled)

**Signal-vs-actual comparison**
- If entered at signal price (0.0000592222): identical to actual entry (fact #1 — no slippage modeled)
  - Hypothetical exit @ 5min: would need post-entry ticks, not available
  - Hypothetical exit @ 15min: would need post-entry ticks, not available
  - Hypothetical exit @ 30min: would need post-entry ticks, not available
  - Hypothetical exit @ 60min: would need post-entry ticks, not available
- Actual strategy exit: STOP_LOSS after 16s, -6.1%
- **What caused the outcome**: entry price was fairly and immediately filled at the exact signal price (not a stale/delayed fill) — the loss came from the token reversing within 16s of a fresh EMERGING/ACCELERATING read, not from execution mechanics.

**Timeline**
```
DISCOVERY  2026-09-20T19:35:14.940260+00:00
TIER1      2026-09-20T19:35:14.940260+00:00  PASS
SIGNAL     2026-09-20T19:38:28.004572+00:00  price=0.0000592222
ENTRY      2026-09-20T19:38:29.017574+00:00  price=0.0000592222
EXIT       2026-09-20T19:38:45.138699+00:00  price=0.0000555912  (STOP_LOSS)
POST-EXIT  not available (no post-close price data persisted)
```

### Trade: SOLSTAMP (AvtCoZt4ApaFqCBz31AdSrdRS3WKWxvPjTvUyd4Npump)

**Token lifecycle**
- Discovered: 2026-09-20T21:40:15.242570+00:00
- Tier1 decision: PASS at 2026-09-20T21:40:15.242570+00:00 (age_at_discovery=0.03min, liquidity=$32,942)
- Signal gate: S1_WAVE, fired at 2026-09-20T21:40:29.253617+00:00, reason=entry_signal

**Entry analysis**
- Signal generated: 2026-09-20T21:40:29.253617+00:00 @ price 0.0001420737 (nearest snapshot at or before signal time)
- Actual entry: 2026-09-20T21:40:29.571414+00:00 @ price 0.0001420737
- Delay signal→entry: 0.32s
- Price movement signal→entry: +0.0% (0% is expected — see fact #1 above, not a data error)
- Local peak in the snapshots leading up to entry: 0.0001420737 (entry WAS at the local peak)

**Exit analysis**
- Exit: 2026-09-20T21:41:30.601217+00:00 @ price 0.0001348648, reason=TRAILING_STOP
- Hold duration: 61s
- Realized P&L: -5.1% ($-2.53)
- Max favorable/adverse excursion while holding: **not available** (see fact #2 above — no tick persistence for ENTERED tokens)
- What happened after exit: **not available** (same reason — sampling stops once a token leaves WATCHING/OBSERVING, CLOSED tokens are never re-sampled)

**Signal-vs-actual comparison**
- If entered at signal price (0.0001420737): identical to actual entry (fact #1 — no slippage modeled)
  - Hypothetical exit @ 5min: would need post-entry ticks, not available
  - Hypothetical exit @ 15min: would need post-entry ticks, not available
  - Hypothetical exit @ 30min: would need post-entry ticks, not available
  - Hypothetical exit @ 60min: would need post-entry ticks, not available
- Actual strategy exit: TRAILING_STOP after 61s, -5.1%
- **What caused the outcome**: entry price was fairly and immediately filled at the exact signal price (not a stale/delayed fill) — the loss came from the token reversing shortly after entry, not from execution mechanics.

**Timeline**
```
DISCOVERY  2026-09-20T21:40:15.242570+00:00
TIER1      2026-09-20T21:40:15.242570+00:00  PASS
SIGNAL     2026-09-20T21:40:29.253617+00:00  price=0.0001420737
ENTRY      2026-09-20T21:40:29.571414+00:00  price=0.0001420737
EXIT       2026-09-20T21:41:30.601217+00:00  price=0.0001348648  (TRAILING_STOP)
POST-EXIT  not available (no post-close price data persisted)
```

## 5. Non-traded candidates — what would have happened

Every token that was scored but never became a real trade, because it never reached the real 8.0 STRONG_BUY bar (or the real 0.80 S1 buy-pressure bar). Simulated using the exact same exit-rule replay as the shadow-threshold experiment (engine/risk.py's logic, unmodified).

- 243 non-traded, scored candidates (clean, outage-excluded)
- Why no trade: score never reached 8.0 (real max seen: 4.94) — this IS the reason, not a risk-gate block or insufficient balance (checked directly: every SCORER/S1_WAVE evaluation that passed its real threshold DID become a trade — 1-for-1, no blocked signals in this dataset)

- Of these, simulated outcome: 56 would have hit TAKE_PROFIT, 80 would have hit a stop, 107 no exit triggered / not enough data yet

**Comparison: actual trades vs. best non-traded candidates**
| symbol | score | max_gain (before sim exit) | sim exit |
|---|---|---|---|
| INU | 4.62 | +396.8% | TAKE_PROFIT |
| Mentor | 3.75 | +343.7% | TAKE_PROFIT |
| PERC | 4.65 | +130.8% | TAKE_PROFIT |
| JEANWIFHAT | 3.57 | +125.2% | TAKE_PROFIT |
| FreeBots | 3.46 | +119.2% | TAKE_PROFIT |
| (actual) JEANTROLL | 9.08 | n/a (real trade) | STOP_LOSS |

## 6. Performance by score bucket

| bucket | candidates | actual trades | sim. win rate | median sim. P&L | mean sim. P&L | avg hold-to-exit |
|---|---|---|---|---|---|---|
| <4.0 | 96 | 0 | 14/32 | -6.8% | +6.2% | 145s |
| 4.0-4.99 | 147 | 0 | 42/104 | -11.4% | -4.1% | 138s |
| 5.0-7.99 | 0 | 0 | n/a | n/a | n/a | n/a |
| 8.0+ | 0 | 1 | n/a | n/a | n/a | n/a |

Note: the 1 real S1-path trade has no composite score (S1 fires on raw buy-pressure, not the scorer) and is excluded from this table by construction — reported separately above.

## Final answer to the actual question

**"If S1 Wave had real money following it, exactly where did it buy, where did it sell, how long did it hold, how much did it actually make/lose, and what happened to the token before and after the trade?"**

- **JEANTROLL**: bought at 0.0000592222 on 2026-09-20T19:38:29.017574+00:00, sold at 0.0000555912 on 2026-09-20T19:38:45.138699+00:00 (STOP_LOSS), held 16s, lost $3.07 (-6.1%). Before the trade: price was rising into the entry. After the trade: **unknown — not recorded** (see fact #2).
- **SOLSTAMP**: bought at 0.0001420737 on 2026-09-20T21:40:29.571414+00:00, sold at 0.0001348648 on 2026-09-20T21:41:30.601217+00:00 (TRAILING_STOP), held 61s, lost $2.53 (-5.1%). Before the trade: price was rising into the entry. After the trade: **unknown — not recorded** (see fact #2).

Both real trades so far are losses, both closed in under a minute, and — critically — neither loss can be attributed to a bad fill, a stale price, or execution delay: the paper engine filled both at the exact signal price, instantly, by design. What we can't yet say is whether the exit was well-timed or premature, because no price data exists for either token after the trade closed. That is the single most useful thing to fix next if you want this kind of report to be complete for future trades.