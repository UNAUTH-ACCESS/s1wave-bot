"""
workers/dexscreener_client.py
================================
Shared DexScreener constants/helpers used by both confluence workers
(confluence_shadow_worker.py, confluence_live_worker.py).

Extracted 2026-09-23 from workers/trade_monitor_worker.py during the
removal of the old scorer/S1Wave/CapitalEngine paper-trading pipeline —
that worker (and the real `trades` table it monitored) is gone, but
_best_pair()'s pool-selection logic is real, previously-live-tested
code (see its docstring for the 2026-09-22 incident) that both
confluence workers depend on, so it moved here instead of being
duplicated or deleted with the rest of that pipeline.
"""

from __future__ import annotations

_DEXSCREENER_BASE = "https://api.dexscreener.com/latest/dex/tokens"


def _best_pair(pairs: list[dict]) -> dict | None:
    """
    DexScreener returns every pair a token trades on (a graduated pump.fun
    token typically has 2+: the old bonding-curve pool plus the live AMM
    pool it migrated to, sometimes a third dead/copycat pool too).

    Prefer recent trading VOLUME (h6, falling back to h24), not
    liquidity.usd. Found live 2026-09-22, via a real confluence-shadow
    position closing on an 80x-wrong price: DexScreener reported the
    genuinely active pumpswap pool (3858 in 6h volume, thousands of real
    txns) with liquidity.usd=0 (an indexing lag on their side, not a real
    zero), while an essentially dead meteora pool ($0.94 total volume
    ever, ~50 lifetime txns) reported a nonzero liquidity.usd=0.01 and
    was wrongly selected under the old liquidity-only rule, since a
    reported 0 was filtered out entirely. Volume is a far more reliable
    "is this the pool people are actually trading on" signal than a
    liquidity figure that can silently read 0 for an active pool. Falls
    back to liquidity.usd only if no returned pair has any recorded
    volume at all (a genuinely quiet market, not a data gap).
    """
    def _volume(p: dict) -> float:
        v = p.get("volume") or {}
        return v.get("h6") or v.get("h24") or 0

    with_volume = [p for p in pairs if _volume(p) > 0]
    if with_volume:
        return max(with_volume, key=_volume)
    with_liquidity = [p for p in pairs if (p.get("liquidity") or {}).get("usd")]
    if with_liquidity:
        return max(with_liquidity, key=lambda p: p["liquidity"]["usd"])
    return pairs[0] if pairs else None
