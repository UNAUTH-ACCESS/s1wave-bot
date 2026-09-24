"""
engine/sell_coordination.py
============================
In-process guard preventing the automated worker loop and a manual close
request from both submitting a real on-chain sell for the same trade at
the same time.

Found live 2026-09-24 (user: "there are positions active but cant close
there"): confluence_live_worker.py's own ~1s _cycle() and the dashboard's
POST /confluence/live/trades/{id}/close both call ExecutionEngine.sell()
independently for whatever mint/trade a position is holding — nothing
stopped them from doing this for the SAME trade at the same moment. When
a position trips its own exit condition (TIME_EXIT/HARD_FLOOR/liquidity
guard) right as a human clicks Close, both submit a real swap for the
same token balance. Whichever lands second is rejected on-chain — real
custom program errors (0x1788/6024, 0x1789/6025) — because the first
swap already changed the account's real balance out from under it.
Confirmed live: Mercedes and Coinbase both hit exactly this pattern
during one incident, with the manual close endpoint reporting
SELL_FAILED_CRITICAL to the user for a position the worker's own loop
was simultaneously closing out successfully underneath it — a scary,
confusing "can't close" experience for a position that, moments later,
usually isn't actually stuck at all.

manual_actions.py's docstring used to claim "the two can never double-
process the same trade since both check status == 'open' ... right up to
the DB write" — true for the DB ROW (only one writer's update ever lands),
false for the real on-chain SWAP (both still submit). This module closes
that second gap.

confluence_live_worker.py's own automatic loop and api/app.py's manual-
close endpoint both run inside the SAME single asyncio event loop (see
main.py: one process, asyncio.gather over both). A plain set is therefore
sufficient — there is never an `await` between the membership check and
the add below, so this is race-free without needing a real asyncio.Lock.
"""

from __future__ import annotations

_IN_FLIGHT_SELLS: set[str] = set()


def try_start_sell(trade_id: str) -> bool:
    """
    Claims the right to submit a real sell for `trade_id`. Returns True and
    marks it in-flight if nothing else is currently selling it; returns
    False (does not mark it) if another sell attempt is already in progress
    — the caller must NOT submit a swap in that case.
    """
    if trade_id in _IN_FLIGHT_SELLS:
        return False
    _IN_FLIGHT_SELLS.add(trade_id)
    return True


def finish_sell(trade_id: str) -> None:
    """Releases the claim — always call this in a `finally` block after
    try_start_sell() returned True, whether the sell succeeded or failed."""
    _IN_FLIGHT_SELLS.discard(trade_id)
