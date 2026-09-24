"""
tests/test_dexscreener_client.py
===================================
Coverage for workers/dexscreener_client.py's _best_pair() — the pool
selection logic shared by ConfluenceShadowWorker and ConfluenceLiveWorker.

Moved 2026-09-23 from tests/test_trade_monitor_best_pair.py when the old
TradeMonitorWorker (and the rest of the scorer/S1Wave paper-trading
pipeline) was removed — this function was extracted into its own module
first specifically so this regression coverage didn't have to go with it.

Real bug this closes (found live 2026-09-22): a confluence-shadow position
closed on an 80x-wrong price because the old liquidity-only rule picked an
essentially dead pool ($0.94 total volume ever) over the genuinely active
one (thousands of real txns, $3858 in 6h volume) — the active pool's
liquidity.usd happened to read 0 (a DexScreener indexing lag, not a real
zero), and the old rule filtered any liquidity=0 pair out entirely. Since
this function now feeds real, on-chain trade exit decisions via
ConfluenceLiveWorker, this is real production risk, not just a shadow-
experiment issue.
"""

from __future__ import annotations

from workers.dexscreener_client import _best_pair


def pair(dex_id, price, liquidity_usd=None, vol_h6=0, vol_h24=0):
    return {
        "dexId": dex_id,
        "priceUsd": price,
        "liquidity": {"usd": liquidity_usd} if liquidity_usd is not None else {},
        "volume": {"h6": vol_h6, "h24": vol_h24},
    }


def test_real_2026_09_22_case_prefers_the_actually_traded_pool():
    """The exact CENTS case: active pool reports liquidity=0 despite huge
    volume; a near-dead pool reports nonzero liquidity but almost no volume.
    Volume must win."""
    active = pair("pumpswap", "0.00008365", liquidity_usd=0, vol_h6=3858.21, vol_h24=3858.21)
    dead = pair("meteora", "0.006456", liquidity_usd=0.01, vol_h6=0.94, vol_h24=0.94)
    result = _best_pair([active, dead])
    assert result["dexId"] == "pumpswap"


def test_prefers_higher_volume_over_higher_liquidity():
    low_liq_high_vol = pair("a", "1.0", liquidity_usd=100, vol_h6=50_000)
    high_liq_low_vol = pair("b", "2.0", liquidity_usd=1_000_000, vol_h6=10)
    result = _best_pair([low_liq_high_vol, high_liq_low_vol])
    assert result["dexId"] == "a"

def test_falls_back_to_liquidity_when_no_pair_has_any_volume():
    p1 = pair("a", "1.0", liquidity_usd=500, vol_h6=0, vol_h24=0)
    p2 = pair("b", "2.0", liquidity_usd=5000, vol_h6=0, vol_h24=0)
    result = _best_pair([p1, p2])
    assert result["dexId"] == "b"


def test_falls_back_to_h24_volume_when_h6_is_zero():
    p1 = pair("a", "1.0", liquidity_usd=100, vol_h6=0, vol_h24=0)
    p2 = pair("b", "2.0", liquidity_usd=100, vol_h6=0, vol_h24=999)
    result = _best_pair([p1, p2])
    assert result["dexId"] == "b"


def test_single_pair_is_returned_regardless():
    only = pair("a", "1.0", liquidity_usd=None, vol_h6=0, vol_h24=0)
    assert _best_pair([only]) is only


def test_empty_list_returns_none():
    assert _best_pair([]) is None
