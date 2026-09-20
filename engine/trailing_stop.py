"""
engine/trailing_stop.py
========================
Trailing stop staircase engine for S1 wave positions.

Manages the progressive stop floor that locks in profit at each 10%
increment above entry. Pure logic — no DB access, no I/O.

Staircase rules
---------------
Entry: stop floor = entry_price * (1 - 0.06) = entry * 0.94

At each 10% profit increment above entry, the floor moves up to lock
in the previous stage:

  current_price >= entry * 1.10  →  floor = entry * 1.00  (breakeven)
  current_price >= entry * 1.20  →  floor = entry * 1.10  (+10% locked)
  current_price >= entry * 1.30  →  floor = entry * 1.20  (+20% locked)
  current_price >= entry * 1.40  →  floor = entry * 1.30  (+30% locked)
  ... and so on at every subsequent 10% increment

The floor NEVER moves down. If the market pulls back through the floor,
the position is closed and the locked profit is booked.

Design
------
- Stateless functions only — callers own all state (Trade fields)
- TradeRiskWorker calls update_trailing_stop() on every MarketEvent
- Returns (new_floor, should_close) — caller writes to DB if changed
- No ExitReason returned — caller creates TRAILING_STOP exit
"""

from __future__ import annotations

from decimal import Decimal


# The initial stop distance below entry (-6%)
_INITIAL_STOP_PCT = Decimal("0.06")

# Step size for staircase increments (5%)
# Data-validated: 5% steps outperform 10% on all 11 actual trades
# Total PnL improvement: +28.80% vs +12.01% — win rate 45% vs 27%
_STEP = Decimal("0.10")


def initial_floor(entry_price: Decimal) -> Decimal:
    """
    Compute the initial stop floor for a new S1 wave position.
    Set at -6% from entry, consistent with the fixed stop loss.
    """
    return (entry_price * (1 - _INITIAL_STOP_PCT)).quantize(Decimal("0.000000000001"))


def compute_floor(entry_price: Decimal, high_watermark: Decimal) -> Decimal:
    """
    Compute the correct stop floor given the current high watermark.

    The floor locks in the previous 10% increment below the watermark's
    stage. Examples with entry = 1.00:

      hwm = 1.05  →  floor = 0.94  (initial, no stage reached)
      hwm = 1.10  →  floor = 1.00  (breakeven locked)
      hwm = 1.15  →  floor = 1.00  (still in +10% stage)
      hwm = 1.20  →  floor = 1.10  (+10% locked)
      hwm = 1.35  →  floor = 1.20  (+20% locked, not yet at +30% trigger)
      hwm = 1.40  →  floor = 1.30  (+30% locked)

    Returns the floor price (never below initial_floor).
    """
    gain_pct = (high_watermark - entry_price) / entry_price

    if gain_pct < _STEP:
        # Haven't reached first increment — initial floor applies
        return initial_floor(entry_price)

    # How many full 10% steps have been cleared?
    # e.g. gain=0.25 → steps_cleared=2 (cleared 10%, 20%)
    # floor locks the previous step: steps_cleared - 1 steps above entry
    steps_cleared = int(gain_pct / _STEP)
    locked_steps = steps_cleared - 1  # one step behind the current stage

    if locked_steps <= 0:
        # At exactly +10% — floor moves to breakeven (entry)
        return entry_price.quantize(Decimal("0.000000000001"))

    floor = entry_price * (1 + _STEP * locked_steps)
    return floor.quantize(Decimal("0.000000000001"))


def update_trailing_stop(
    entry_price: Decimal,
    current_price: Decimal,
    current_floor: Decimal,
    current_hwm: Decimal,
) -> tuple[Decimal, Decimal, bool]:
    """
    Process one price tick for an S1 wave position.

    Parameters
    ----------
    entry_price   : trade.entry_price
    current_price : MarketEvent.price_usd
    current_floor : trade.trailing_stop_floor
    current_hwm   : trade.high_watermark_price

    Returns
    -------
    (new_floor, new_hwm, should_close)

    new_floor     : updated stop floor (>= current_floor, never lower)
    new_hwm       : updated high watermark (>= current_hwm, never lower)
    should_close  : True if current_price has breached the stop floor
    """
    # Update high watermark — never goes down
    new_hwm = max(current_hwm, current_price)

    # Recompute floor based on new watermark
    candidate_floor = compute_floor(entry_price, new_hwm)

    # Floor never goes down
    new_floor = max(current_floor, candidate_floor)

    # Check if current price has breached the floor
    should_close = current_price <= new_floor

    return new_floor, new_hwm, should_close
