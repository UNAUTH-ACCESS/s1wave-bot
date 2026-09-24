"""
analysis/trailing_stop_retroactive_replay.py
================================================
ANALYSIS-ONLY, READ-ONLY. Never writes to any table, never touches
CONFLUENCE_LIVE_ENABLED, never executes anything.

Answers: if the confluence_entry_v1 shadow positions closed so far had
used the new layered exit (engine/trailing_stop.py, replacing the old
fixed -6%/+30% pair) and the new compounding position sizing
(ConfluenceLiveWorker._compute_position_usd(), $10 stake, 50% of profit
above it redeployed, $50 ceiling) instead of the old rules and flat $10
sizing, what would the equity curve, max drawdown, longest losing streak,
and final bankroll have been?

Methodology and its real limitation
------------------------------------
Every closed position's actual recorded price path (confluence_shadow_
observations) only extends up to the tick that triggered its OLD exit.
ConfluenceShadowWorker stops recording observations the instant a
position is marked closed. For a position the old rule force-sold at
+30% (TAKE_PROFIT), we have ZERO data on what the price did next — the
new rule would not have sold there, so we genuinely cannot know the true
outcome for those trades.

What we CAN say rigorously for those cases: engine/trailing_stop.py's
floor only ratchets UP and is a function of the high watermark reached so
far. At the exact instant a position's high watermark reaches +30%, the
floor has already locked +20% (see compute_floor's own worked example:
hwm=1.30 -> floor=1.20). So even in the worst case — the position
instantly reverses on the very next (unrecorded) tick — the new rule
would not have exited below roughly that +20%-locked level. This script
reports such cases as a LOWER BOUND, not a true replay, and keeps them
visibly separate from exactly-replayed trades throughout.

Trades whose old exit was HARD_FLOOR or the velocity breaker replay
EXACTLY — those thresholds are unchanged by this update, so the new rule
fires at the identical tick. STOP_LOSS trades (old fixed -6%) also
replay exactly whenever the position never made a new high above entry
(the new rule's initial floor is also -6%, so if the ratchet never
engaged, the two rules are the same threshold). TIME_EXIT trades replay
exactly whenever the floor at the moment of timeout hadn't moved past
entry, and yield a currently-open state under the new rule when it had.

Single-position-at-a-time constraint
-------------------------------------
The live/compounding design (CONFLUENCE_LIVE_MAX_CONCURRENT=1) only ever
holds one position. The shadow experiment has no such limit and often has
several positions open at once. To simulate what the COMPOUNDING BANKROLL
would actually have done, trades are replayed in entry_time order and a
later-entering trade is SKIPPED (not taken) if its entry_time falls
before the currently-held simulated trade's exit_time — exactly mirroring
_safe_to_enter()'s concurrency gate. This means the equity curve reflects
a real subset of the 196 closed signals, not all of them.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from database.engine import get_session
from engine.trailing_stop import initial_floor, update_trailing_stop
from models.orm import ConfluenceShadowObservation, ConfluenceShadowPosition

VELOCITY_BREAKER_PCT = Decimal("-0.25")
HARD_FLOOR_PCT = Decimal("-0.07")
MAX_HOLD_SECONDS = 6 * 3600

STARTING_STAKE = Decimal("10.0")
COMPOUND_PCT = Decimal("0.5")
MAX_POSITION_USD = Decimal("50.0")


def check_exit(entry_price, entry_time, current_price, now, current_floor, current_hwm):
    if entry_price is None or entry_price <= Decimal("0"):
        return None, current_floor, current_hwm
    pnl_pct = (current_price - entry_price) / entry_price
    if pnl_pct <= VELOCITY_BREAKER_PCT:
        return "HARD_FLOOR", current_floor, max(current_hwm, current_price)
    if pnl_pct <= HARD_FLOOR_PCT:
        return "HARD_FLOOR", current_floor, max(current_hwm, current_price)
    new_floor, new_hwm, should_close = update_trailing_stop(entry_price, current_price, current_floor, current_hwm)
    if should_close:
        reason = "STOP_LOSS" if new_floor <= initial_floor(entry_price) else "TRAILING_STOP"
        return reason, new_floor, new_hwm
    hold_seconds = (now - entry_time).total_seconds()
    if hold_seconds >= MAX_HOLD_SECONDS:
        return "TIME_EXIT", new_floor, new_hwm
    return None, new_floor, new_hwm


def compute_position_usd(equity: Decimal) -> Decimal:
    stake = STARTING_STAKE
    if equity <= stake:
        position_usd = max(Decimal("0"), equity)
    else:
        profit = equity - stake
        position_usd = stake + profit * COMPOUND_PCT
    return min(position_usd, MAX_POSITION_USD)


async def load_closed_positions():
    async with get_session() as session:
        result = await session.execute(
            select(ConfluenceShadowPosition)
            .where(ConfluenceShadowPosition.status == "closed")
            .order_by(ConfluenceShadowPosition.entry_time.asc())
        )
        positions = result.scalars().all()

        obs_by_position = {}
        for pos in positions:
            obs_result = await session.execute(
                select(ConfluenceShadowObservation.observed_at, ConfluenceShadowObservation.price_usd)
                .where(ConfluenceShadowObservation.position_id == pos.id)
                .order_by(ConfluenceShadowObservation.observed_at.asc())
            )
            obs_by_position[pos.id] = obs_result.all()
    return positions, obs_by_position


def replay_one(pos, observations):
    """Returns dict with new_exit_reason, new_exit_time, new_exit_price,
    new_pnl_pct, is_lower_bound_only, old_exit_reason, old_pnl_pct."""
    floor = initial_floor(pos.entry_price)
    hwm = pos.entry_price
    reason = None
    exit_time = None
    exit_price = None

    for observed_at, price_usd in observations:
        reason, floor, hwm = check_exit(pos.entry_price, pos.entry_time, price_usd, observed_at, floor, hwm)
        if reason is not None:
            exit_time = observed_at
            exit_price = price_usd
            break

    old_pnl_pct = float((pos.exit_price - pos.entry_price) / pos.entry_price) if pos.exit_price else None

    if reason is not None:
        new_pnl_pct = float((exit_price - pos.entry_price) / pos.entry_price)
        return dict(
            position_id=pos.id, entry_time=pos.entry_time,
            new_exit_reason=reason, new_exit_time=exit_time, new_pnl_pct=new_pnl_pct,
            is_lower_bound_only=False, old_exit_reason=pos.exit_reason, old_pnl_pct=old_pnl_pct,
        )

    # No exit condition reached within the recorded data. Data ran out —
    # report the floor-locked lower bound instead of a true outcome.
    last_time = observations[-1][0] if observations else pos.entry_time
    lower_bound_pnl_pct = float((floor - pos.entry_price) / pos.entry_price)
    return dict(
        position_id=pos.id, entry_time=pos.entry_time,
        new_exit_reason="INSUFFICIENT_DATA", new_exit_time=last_time, new_pnl_pct=lower_bound_pnl_pct,
        is_lower_bound_only=True, old_exit_reason=pos.exit_reason, old_pnl_pct=old_pnl_pct,
    )


def simulate_bankroll(replays: list[dict]):
    """Single-position-at-a-time compounding simulation, mirroring
    CONFLUENCE_LIVE_MAX_CONCURRENT=1's _safe_to_enter() gate."""
    replays_sorted = sorted(replays, key=lambda r: r["entry_time"])

    equity = STARTING_STAKE
    equity_curve = [("start", equity)]
    taken = []
    skipped_overlap = 0
    next_available_time = None

    for r in replays_sorted:
        if next_available_time is not None and r["entry_time"] < next_available_time:
            skipped_overlap += 1
            continue
        position_usd = compute_position_usd(equity)
        pnl_usd = position_usd * Decimal(str(r["new_pnl_pct"]))
        equity += pnl_usd
        equity_curve.append((r["position_id"], equity))
        taken.append(dict(r, position_usd=position_usd, pnl_usd=pnl_usd, equity_after=equity))
        next_available_time = r["new_exit_time"]

    # Max drawdown (peak-to-trough on the equity curve).
    peak = equity_curve[0][1]
    max_dd = Decimal("0")
    for _, eq in equity_curve:
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak if peak > 0 else Decimal("0")
        if dd > max_dd:
            max_dd = dd

    # Longest losing streak (consecutive taken trades with pnl_usd < 0).
    longest_streak = 0
    current_streak = 0
    for t in taken:
        if t["pnl_usd"] < 0:
            current_streak += 1
            longest_streak = max(longest_streak, current_streak)
        else:
            current_streak = 0

    return dict(
        final_bankroll=equity, taken=taken, skipped_overlap=skipped_overlap,
        max_drawdown_pct=max_dd, longest_losing_streak=longest_streak,
        equity_curve=equity_curve,
    )


async def main():
    positions, obs_by_position = await load_closed_positions()
    print(f"Loaded {len(positions)} closed confluence_shadow_positions rows "
          f"(experiment_version=confluence_entry_v1).\n")

    replays = [replay_one(pos, obs_by_position[pos.id]) for pos in positions]

    exact = [r for r in replays if not r["is_lower_bound_only"]]
    lower_bound_only = [r for r in replays if r["is_lower_bound_only"]]
    reason_counts = Counter(r["old_exit_reason"] for r in lower_bound_only)
    print(f"Exactly replayed under the new rule: {len(exact)} / {len(replays)}")
    print(f"Lower-bound only (old exit cut off the data before the new rule "
          f"would have acted): {len(lower_bound_only)} / {len(replays)}")
    print(f"  by old exit_reason: {dict(reason_counts)}\n")

    new_reason_counts = Counter(r["new_exit_reason"] for r in exact)
    print(f"New-rule exit reasons among exactly-replayed trades: {dict(new_reason_counts)}\n")

    sim = simulate_bankroll(replays)
    print("=== Single-position-at-a-time compounding simulation ===")
    print(f"Trades taken (respecting the 1-at-a-time concurrency gate): {len(sim['taken'])}")
    print(f"Trades skipped as overlapping with an already-open simulated position: {sim['skipped_overlap']}")
    print(f"Starting bankroll: ${STARTING_STAKE}")
    print(f"Final bankroll:    ${sim['final_bankroll']:.2f}  "
          f"(CONSERVATIVE — includes floor-locked lower bounds for cut-short winners)")
    print(f"Max drawdown:      {sim['max_drawdown_pct']*100:.1f}%")
    print(f"Longest losing streak: {sim['longest_losing_streak']} trades in a row\n")

    lower_bound_taken = [t for t in sim["taken"] if t["is_lower_bound_only"]]
    print(f"Of the {len(sim['taken'])} trades actually taken in the simulation, "
          f"{len(lower_bound_taken)} are lower-bound-only (their true new-rule pnl is >= what's shown here).")

    print("\n=== First 10 and last 10 taken trades ===")
    for t in (sim["taken"][:10] + (["..."] if len(sim["taken"]) > 20 else []) + sim["taken"][-10:]):
        if t == "...":
            print("...")
            continue
        flag = " [LOWER BOUND]" if t["is_lower_bound_only"] else ""
        print(f"  entry={t['entry_time'].isoformat()}  pos=${t['position_usd']:.2f}  "
              f"pnl={t['new_pnl_pct']*100:+.1f}%  (${t['pnl_usd']:+.2f})  "
              f"reason={t['new_exit_reason']}  equity_after=${t['equity_after']:.2f}{flag}")


if __name__ == "__main__":
    asyncio.run(main())
