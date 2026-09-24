"""
analysis/sl_tp_and_concurrency_sweep.py
==========================================
ANALYSIS-ONLY, READ-ONLY. Never writes to any table, never touches
CONFLUENCE_LIVE_ENABLED, never executes anything.

Two questions, both against every closed confluence_shadow_positions row
(200 as of 2026-09-23, up from the 196 used in the previous replay):

1. What initial stop / staircase-step / hard-floor combination actually
   performs best against the real recorded price paths, instead of
   assuming the currently-deployed -6% / 10% steps / -7% is right?

2. If we allow N concurrent positions instead of 1, how many of the real
   signals actually become tradeable (fewer skipped for overlap), and
   what per-trade allocation keeps total capital at risk unchanged?

Methodology, and its limits (same honesty rules as
analysis/trailing_stop_retroactive_replay.py)
------------------------------------------------------------------------
Every closed position's recorded observation history only extends to
whatever rule ACTUALLY closed it historically (the old fixed -6%/+30%
pair for trades closed before 2026-09-23's restart, the new trailing
staircase for trades closed after). To compare CANDIDATE parameter sets
fairly against each other, every candidate is replayed against the same
raw recorded price path, regardless of which rule really closed the
trade. A candidate that would have wanted to hold PAST where the data
ends is marked a lower bound (its true pnl is >= what's shown), exactly
as before. This makes every candidate's numbers conservative by the same
amount in the same direction, so relative comparisons between candidates
stay fair even though none of the absolute numbers are exact.

Concurrency simulation: N independent "slots", each behaving like the
current single-slot compounding design but started with STARTING_STAKE/N.
A signal is assigned to the first slot that is free (its previously
assigned trade has already exited) at that signal's entry_time; if no
slot is free, the signal is skipped — exactly mirroring
_safe_to_enter()'s concurrency gate generalized to N.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from itertools import product

from sqlalchemy import select

from database.engine import get_session
from engine.trailing_stop import initial_floor as default_initial_floor
from models.orm import ConfluenceShadowObservation, ConfluenceShadowPosition

VELOCITY_BREAKER_PCT = Decimal("-0.25")
MAX_HOLD_SECONDS = 6 * 3600
STARTING_STAKE = Decimal("10.0")
COMPOUND_PCT = Decimal("0.5")


def make_initial_floor(stop_pct: Decimal):
    def f(entry_price: Decimal) -> Decimal:
        return (entry_price * (1 + stop_pct)).quantize(Decimal("0.000000000001"))
    return f


def make_compute_floor(stop_pct: Decimal, step_pct: Decimal):
    def f(entry_price: Decimal, hwm: Decimal) -> Decimal:
        gain_pct = (hwm - entry_price) / entry_price
        if gain_pct < step_pct:
            return (entry_price * (1 + stop_pct)).quantize(Decimal("0.000000000001"))
        steps_cleared = int(gain_pct / step_pct)
        locked_steps = steps_cleared - 1
        if locked_steps <= 0:
            return entry_price.quantize(Decimal("0.000000000001"))
        return (entry_price * (1 + step_pct * locked_steps)).quantize(Decimal("0.000000000001"))
    return f


def make_check_exit(stop_pct: Decimal, step_pct: Decimal, hard_floor_pct: Decimal):
    initial_floor_fn = make_initial_floor(stop_pct)
    compute_floor_fn = make_compute_floor(stop_pct, step_pct)

    def check_exit(entry_price, entry_time, current_price, now, current_floor, current_hwm):
        pnl_pct = (current_price - entry_price) / entry_price
        if pnl_pct <= VELOCITY_BREAKER_PCT:
            return "HARD_FLOOR", current_floor, max(current_hwm, current_price)
        if pnl_pct <= hard_floor_pct:
            return "HARD_FLOOR", current_floor, max(current_hwm, current_price)
        new_hwm = max(current_hwm, current_price)
        candidate_floor = compute_floor_fn(entry_price, new_hwm)
        new_floor = max(current_floor, candidate_floor)
        should_close = current_price <= new_floor
        if should_close:
            reason = "STOP_LOSS" if new_floor <= initial_floor_fn(entry_price) else "TRAILING_STOP"
            return reason, new_floor, new_hwm
        hold_seconds = (now - entry_time).total_seconds()
        if hold_seconds >= MAX_HOLD_SECONDS:
            return "TIME_EXIT", new_floor, new_hwm
        return None, new_floor, new_hwm

    return check_exit, initial_floor_fn


async def load_all():
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


def replay_one(pos, observations, stop_pct, step_pct, hard_floor_pct):
    check_exit, initial_floor_fn = make_check_exit(stop_pct, step_pct, hard_floor_pct)
    floor = initial_floor_fn(pos.entry_price)
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

    if reason is not None:
        pnl_pct = float((exit_price - pos.entry_price) / pos.entry_price)
        return dict(position_id=pos.id, entry_time=pos.entry_time, exit_time=exit_time,
                    exit_reason=reason, pnl_pct=pnl_pct, is_lower_bound=False)

    last_time = observations[-1][0] if observations else pos.entry_time
    lower_bound_pnl_pct = float((floor - pos.entry_price) / pos.entry_price)
    return dict(position_id=pos.id, entry_time=pos.entry_time, exit_time=last_time,
                exit_reason="INSUFFICIENT_DATA", pnl_pct=lower_bound_pnl_pct, is_lower_bound=True)


REALISTIC_FILL_CAP = 1.0  # +100% — see module docstring's "thin liquidity" note


def aggregate_stats(replays: list[dict]) -> dict:
    """
    Reports both the raw average and a REALISTIC_FILL_CAP-capped average.
    Two trades in this dataset (Banana, +6050%; a second token, +6633%)
    are single-tick jumps to a price DexScreener still reports hours later
    with real ongoing volume — not reverted, so not a pure data glitch —
    but both show liquidity.usd=0 at that price level. A real sell order
    into near-zero recorded liquidity would face slippage far beyond the
    quoted price (same real phenomenon already found for HARD_FLOOR exits
    this session: median -63% realized vs the nominal -7%). Taking these
    prints at face value for a real-money expectancy estimate would be
    dangerously optimistic, so the capped average clips any single
    trade's contribution at REALISTIC_FILL_CAP — a defensible ceiling for
    what a real market order could plausibly still achieve, not a claim
    that anything above it is fake.
    """
    n = len(replays)
    wins = [r for r in replays if r["pnl_pct"] > 0]
    losses = [r for r in replays if r["pnl_pct"] <= 0]
    win_rate = len(wins) / n if n else 0.0
    avg_pnl_pct = sum(r["pnl_pct"] for r in replays) / n if n else 0.0
    gross_win = sum(r["pnl_pct"] for r in wins)
    gross_loss = -sum(r["pnl_pct"] for r in losses)
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
    lower_bound_count = sum(1 for r in replays if r["is_lower_bound"])

    capped_pnls = [min(r["pnl_pct"], REALISTIC_FILL_CAP) for r in replays]
    avg_pnl_pct_capped = sum(capped_pnls) / n if n else 0.0
    capped_wins = [p for p in capped_pnls if p > 0]
    capped_losses = [p for p in capped_pnls if p <= 0]
    gross_win_c = sum(capped_wins)
    gross_loss_c = -sum(capped_losses)
    profit_factor_capped = (gross_win_c / gross_loss_c) if gross_loss_c > 0 else float("inf")

    return dict(n=n, win_rate=win_rate, avg_pnl_pct=avg_pnl_pct, profit_factor=profit_factor,
                lower_bound_count=lower_bound_count, avg_pnl_pct_capped=avg_pnl_pct_capped,
                profit_factor_capped=profit_factor_capped)


def simulate_slots(replays: list[dict], n_slots: int, capped: bool = False) -> dict:
    """N independent compounding slots, each starting at STARTING_STAKE/n_slots.
    capped=True clips each trade's pnl_pct at REALISTIC_FILL_CAP — see
    aggregate_stats' docstring for why."""
    replays_sorted = sorted(replays, key=lambda r: r["entry_time"])
    slot_equity = [STARTING_STAKE / n_slots] * n_slots
    slot_next_available = [None] * n_slots
    taken = 0
    skipped = 0
    all_time_pnl = Decimal("0")
    peak_total_equity = sum(slot_equity)
    max_dd = Decimal("0")
    losing_streak = 0
    longest_losing_streak = 0

    for r in replays_sorted:
        free_slot = None
        for i in range(n_slots):
            if slot_next_available[i] is None or r["entry_time"] >= slot_next_available[i]:
                free_slot = i
                break
        if free_slot is None:
            skipped += 1
            continue

        stake = STARTING_STAKE / n_slots
        equity = slot_equity[free_slot]
        if equity <= stake:
            position_usd = max(Decimal("0"), equity)
        else:
            profit = equity - stake
            position_usd = stake + profit * COMPOUND_PCT
        pnl_pct = min(r["pnl_pct"], REALISTIC_FILL_CAP) if capped else r["pnl_pct"]
        pnl_usd = position_usd * Decimal(str(pnl_pct))
        slot_equity[free_slot] = equity + pnl_usd
        slot_next_available[free_slot] = r["exit_time"]
        taken += 1

        if pnl_usd < 0:
            losing_streak += 1
            longest_losing_streak = max(longest_losing_streak, losing_streak)
        else:
            losing_streak = 0

        total_equity = sum(slot_equity)
        if total_equity > peak_total_equity:
            peak_total_equity = total_equity
        dd = (peak_total_equity - total_equity) / peak_total_equity if peak_total_equity > 0 else Decimal("0")
        if dd > max_dd:
            max_dd = dd

    return dict(n_slots=n_slots, taken=taken, skipped=skipped,
                final_bankroll=sum(slot_equity), max_drawdown_pct=max_dd,
                longest_losing_streak=longest_losing_streak)


async def main():
    positions, obs_by_position = await load_all()
    print(f"Loaded {len(positions)} closed confluence_shadow_positions rows.\n")

    print("=" * 96)
    print("PART 1 — Stop-loss / staircase-step / hard-floor sweep")
    print(f"(raw = every recorded print taken at face value; capped = clipped at "
          f"+{REALISTIC_FILL_CAP*100:.0f}% per trade — see aggregate_stats' docstring: "
          f"2 trades in this dataset are single-tick jumps to a price still live hours "
          f"later with real volume, so not reverted glitches, but both show $0 recorded "
          f"liquidity at that level — a real sell order there would not realistically "
          f"fill anywhere near the quoted price)")
    print("=" * 96)
    print(f"{'stop':>6} {'step':>6} {'floor':>7} | {'n':>4} {'win%':>6} "
          f"{'raw_avg%':>9} {'raw_PF':>7} | {'cap_avg%':>9} {'cap_PF':>7} {'lower_bd':>8}")

    grid_results = []
    for stop_pct, step_pct, hard_floor_pct in product(
        [Decimal("-0.05"), Decimal("-0.06"), Decimal("-0.08"), Decimal("-0.10")],
        [Decimal("0.10"), Decimal("0.15"), Decimal("0.20")],
        [Decimal("-0.07"), Decimal("-0.10"), Decimal("-0.15")],
    ):
        if hard_floor_pct >= stop_pct:
            continue  # hard floor must be strictly below the initial stop
        replays = [replay_one(pos, obs_by_position[pos.id], stop_pct, step_pct, hard_floor_pct)
                   for pos in positions]
        stats = aggregate_stats(replays)
        grid_results.append((stop_pct, step_pct, hard_floor_pct, stats, replays))
        print(f"{float(stop_pct)*100:>5.0f}% {float(step_pct)*100:>5.0f}% {float(hard_floor_pct)*100:>6.0f}% | "
              f"{stats['n']:>4} {stats['win_rate']*100:>5.1f}% "
              f"{stats['avg_pnl_pct']*100:>8.2f}% {stats['profit_factor']:>7.2f} | "
              f"{stats['avg_pnl_pct_capped']*100:>8.2f}% {stats['profit_factor_capped']:>7.2f} "
              f"{stats['lower_bound_count']:>8}")

    best = max(grid_results, key=lambda g: g[3]["avg_pnl_pct_capped"])
    print(f"\nBest REALISTIC (capped) average pnl per trade: stop={float(best[0])*100:.0f}% "
          f"step={float(best[1])*100:.0f}% hard_floor={float(best[2])*100:.0f}% -> "
          f"avg={best[3]['avg_pnl_pct_capped']*100:.2f}%, win_rate={best[3]['win_rate']*100:.1f}%, "
          f"PF={best[3]['profit_factor_capped']:.2f}")

    current = next(g for g in grid_results if g[0] == Decimal("-0.06") and g[1] == Decimal("0.10")
                    and g[2] == Decimal("-0.07"))
    print(f"Currently deployed (-6%/10%-steps/-7%): capped_avg={current[3]['avg_pnl_pct_capped']*100:.2f}%, "
          f"win_rate={current[3]['win_rate']*100:.1f}%, capped_PF={current[3]['profit_factor_capped']:.2f}")

    print("\n" + "=" * 96)
    print("PART 2 — Concurrency: how many slots to actually use most of the signal flow")
    print("(bankroll figures use the REALISTIC capped pnl per trade, not the raw prints)")
    print("=" * 96)
    best_stop, best_step, best_floor = best[0], best[1], best[2]
    best_replays = best[4]
    print(f"\n(using the best stop/step/floor combo found above: {float(best_stop)*100:.0f}%/"
          f"{float(best_step)*100:.0f}%/{float(best_floor)*100:.0f}%)")
    print(f"{'slots':>6} {'taken':>6} {'skipped':>8} {'final_$':>10} {'max_dd':>8} {'streak':>7}")
    for n_slots in (1, 2, 3, 5, 8, 12):
        sim = simulate_slots(best_replays, n_slots, capped=True)
        print(f"{n_slots:>6} {sim['taken']:>6} {sim['skipped']:>8} "
              f"${float(sim['final_bankroll']):>9.2f} {float(sim['max_drawdown_pct'])*100:>7.1f}% "
              f"{sim['longest_losing_streak']:>7}")

    print("\n(using the CURRENTLY DEPLOYED -6%/10%-steps/-7% for comparison)")
    current_replays = current[4]
    print(f"{'slots':>6} {'taken':>6} {'skipped':>8} {'final_$':>10} {'max_dd':>8} {'streak':>7}")
    for n_slots in (1, 2, 3, 5, 8, 12):
        sim = simulate_slots(current_replays, n_slots, capped=True)
        print(f"{n_slots:>6} {sim['taken']:>6} {sim['skipped']:>8} "
              f"${float(sim['final_bankroll']):>9.2f} {float(sim['max_drawdown_pct'])*100:>7.1f}% "
              f"{sim['longest_losing_streak']:>7}")


if __name__ == "__main__":
    asyncio.run(main())
