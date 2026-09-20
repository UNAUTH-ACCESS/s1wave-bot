"""
dexscreener/client.py
=====================
DEX Screener API client.

Two endpoints are used in v3 (per PRD §3.1):
  1. /token-profiles/latest/v1   — discovery poll (called every 60s)
  2. /tokens/{chainId}/{address} — single token lookup (called once per
                                   new token during enrichment)

Both are public, no auth required.  Rate limit is not published by DEX
Screener — we stay conservative (1 req/s burst, exponential backoff on
429/5xx).

All responses are parsed into typed dataclasses so callers never touch
raw dicts.  Field absence is handled explicitly — DEX Screener omits
fields for tokens with insufficient trading history.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.logging import get_logger

log = get_logger(__name__)

_BASE_URL = "https://api.dexscreener.com"
_TIMEOUT = httpx.Timeout(timeout=15.0, connect=5.0)

# DEX Screener returns Solana pairs under this chain ID
_SOLANA_CHAIN = "solana"


# ── Response types ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class DexTokenProfile:
    """
    One item from /token-profiles/latest/v1.

    Used by the discovery_worker to identify new tokens.  Only the fields
    needed for deduplication and enrichment queuing are captured here.
    """
    chain_id: str
    token_address: str
    # These may be absent on very new tokens
    symbol: str | None = None
    name: str | None = None
    icon_url: str | None = None


@dataclass(frozen=True)
class DexPairSnapshot:
    """
    Price/liquidity snapshot from a single-pair response.

    Populated during enrichment and also during the 30s sampling loop.
    All Decimal fields are None when DEX Screener omits them.

    Two buy pressure windows are captured:
        buy_pressure_h1 — 1-hour window, used for Tier 1 enrichment gate.
                          More resistant to single-whale noise than m5.
                          A single large buy in 5 minutes can push m5 BP
                          to 0.95 and make a rug look organic; h1 smooths
                          that out.
        buy_pressure_m5 — 5-minute window, used by the sampling_worker
                          for the rolling 30s momentum scorer (Phase 4).
                          Fine-grained recency is what matters there.

    The sampling_worker writes buy_pressure_m5 into TokenSnapshot.buy_pressure.
    The enrichment_worker uses buy_pressure_h1 only for logging/context;
    the Tier 1 gate does not currently gate on buy pressure — that is a
    Tier 2 signal.  Both fields are captured here so Phase 4 has both
    available without re-fetching.
    """
    pair_address: str
    base_token_address: str
    base_token_symbol: str | None
    base_token_name: str | None

    price_usd: Decimal | None
    liquidity_usd: Decimal | None
    market_cap_usd: Decimal | None

    # m5 = last 5 minutes — used by sampling_worker for rolling scorer
    volume_m5_usd: Decimal | None

    # h1 = last 1 hour — used for enrichment context (noise-resistant)
    volume_h1_usd: Decimal | None

    # m5 buy pressure: buys / (buys + sells) over 5 min
    # Used by sampling_worker → TokenSnapshot.buy_pressure
    buy_pressure_m5: Decimal | None

    # h1 buy pressure: buys / (buys + sells) over 1 hour
    # More reliable for enrichment-time context on new tokens
    buy_pressure_h1: Decimal | None

    # Token creation time (Unix ms) — used for age calculation
    pair_created_at_ms: int | None

    # Raw m5 counts — kept for wash multiplier calculation in enrichment
    txns_m5_buys: int
    txns_m5_sells: int

    # Raw h1 counts — available for richer wash multiplier analysis
    txns_h1_buys: int
    txns_h1_sells: int

    # Back-compat alias: sampling_worker reads this field name
    @property
    def buy_pressure(self) -> Decimal | None:
        """Returns m5 buy pressure — the value written to TokenSnapshot."""
        return self.buy_pressure_m5


@dataclass
class DexTokenDetail:
    """
    Aggregated result for a single token across all its pairs.

    We take the pair with the highest liquidity as the canonical snapshot —
    meme coins often have multiple pools, we want the deepest one.
    """
    token_address: str
    symbol: str | None
    name: str | None
    best_pair: DexPairSnapshot | None  # highest liquidity pair
    all_pairs: list[DexPairSnapshot] = field(default_factory=list)


# ── Parser helpers ────────────────────────────────────────────────────────────

def _dec(value: Any) -> Decimal | None:
    """Safely convert a value to Decimal, returning None on failure."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _parse_pair(pair: dict[str, Any]) -> DexPairSnapshot:
    txns = pair.get("txns", {})

    txns_m5 = txns.get("m5", {})
    buys_m5 = int(txns_m5.get("buys", 0))
    sells_m5 = int(txns_m5.get("sells", 0))
    total_m5 = buys_m5 + sells_m5
    bp_m5: Decimal | None = None
    if total_m5 > 0:
        bp_m5 = Decimal(str(buys_m5 / total_m5)).quantize(Decimal("0.0001"))

    txns_h1 = txns.get("h1", {})
    buys_h1 = int(txns_h1.get("buys", 0))
    sells_h1 = int(txns_h1.get("sells", 0))
    total_h1 = buys_h1 + sells_h1
    bp_h1: Decimal | None = None
    if total_h1 > 0:
        bp_h1 = Decimal(str(buys_h1 / total_h1)).quantize(Decimal("0.0001"))

    liq = pair.get("liquidity", {})
    base = pair.get("baseToken", {})
    volume = pair.get("volume", {})

    return DexPairSnapshot(
        pair_address=pair.get("pairAddress", ""),
        base_token_address=base.get("address", ""),
        base_token_symbol=base.get("symbol"),
        base_token_name=base.get("name"),
        price_usd=_dec(pair.get("priceUsd")),
        liquidity_usd=_dec(liq.get("usd")),
        market_cap_usd=_dec(pair.get("marketCap")),
        volume_m5_usd=_dec(volume.get("m5")),
        volume_h1_usd=_dec(volume.get("h1")),
        buy_pressure_m5=bp_m5,
        buy_pressure_h1=bp_h1,
        pair_created_at_ms=pair.get("pairCreatedAt"),
        txns_m5_buys=buys_m5,
        txns_m5_sells=sells_m5,
        txns_h1_buys=buys_h1,
        txns_h1_sells=sells_h1,
    )


def _parse_token_detail(token_address: str, pairs: list[dict]) -> DexTokenDetail:
    solana_pairs = [p for p in pairs if p.get("chainId") == _SOLANA_CHAIN]
    parsed = [_parse_pair(p) for p in solana_pairs]

    # Pick canonical pair: highest liquidity_usd
    best: DexPairSnapshot | None = None
    for p in parsed:
        if p.liquidity_usd is None:
            continue
        if best is None or p.liquidity_usd > (best.liquidity_usd or Decimal("0")):
            best = p

    symbol = best.base_token_symbol if best else None
    name = best.base_token_name if best else None

    return DexTokenDetail(
        token_address=token_address,
        symbol=symbol,
        name=name,
        best_pair=best,
        all_pairs=parsed,
    )


# ── Client ────────────────────────────────────────────────────────────────────

class DexScreenerClient:
    """
    Async DEX Screener client.

    Intended to be created once and reused across the process lifetime.
    Uses a shared httpx.AsyncClient with connection pooling.

    Usage
    -----
        client = DexScreenerClient()
        await client.start()

        profiles = await client.get_latest_token_profiles()
        detail   = await client.get_token_detail("mint_address_here")

        await client.close()

    Or use as an async context manager:
        async with DexScreenerClient() as client:
            ...
    """

    def __init__(self) -> None:
        self._http: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=_BASE_URL,
            timeout=_TIMEOUT,
            headers={"Accept": "application/json"},
            follow_redirects=True,
        )

    async def close(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None

    async def __aenter__(self) -> "DexScreenerClient":
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            raise RuntimeError("DexScreenerClient not started — call start() first.")
        return self._http

    # ── Public methods ────────────────────────────────────────────────────

    @retry(
        retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    async def get_latest_token_profiles(self) -> list[DexTokenProfile]:
        """
        Poll /token-profiles/latest/v1.

        Returns all Solana token profiles in the latest batch.
        Called by discovery_worker every DEXSCREENER_POLL_INTERVAL seconds.
        """
        resp = await self._client().get("/token-profiles/latest/v1")

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "5"))
            log.warning("dexscreener.rate_limited", retry_after=retry_after)
            await asyncio.sleep(retry_after)

            resp = await self._client().get("/token-profiles/latest/v1")
            resp.raise_for_status()
        data = resp.json()

        profiles: list[DexTokenProfile] = []
        items = data if isinstance(data, list) else data.get("data", [])
        seen = set()
        for item in items:
            if item.get("chainId") != _SOLANA_CHAIN:
                continue
            addr = item.get("tokenAddress", "")
            if not addr or addr in seen:
                continue
            seen.add(addr)
            profiles.append(DexTokenProfile(
                chain_id="solana",
                token_address=addr,
                symbol=item.get("symbol"),
                name=item.get("name"),
                icon_url=item.get("icon"),
            ))

        return profiles

    @retry(
        retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    async def get_token_detail(self, mint_address: str) -> DexTokenDetail | None:
        """
        Fetch price/liquidity detail for a single token.

        Returns None if the token is not found (404) or has no pairs.
        Called once per new token during enrichment.
        """
        resp = await self._client().get(
            f"/token-pairs/v1/{_SOLANA_CHAIN}/{mint_address}"
        )

        if resp.status_code == 404:
            log.debug("dexscreener.token_not_found", mint=mint_address)
            return None

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "5"))
            await asyncio.sleep(retry_after)
            resp = await self._client().get(f"/token-pairs/v1/{_SOLANA_CHAIN}/{mint_address}")

        resp.raise_for_status()
        data = resp.json()

        pairs = data if isinstance(data, list) else data.get("pairs") or []
        if not pairs:
            log.debug("dexscreener.no_pairs", mint=mint_address)
            return None

        detail = _parse_token_detail(mint_address, pairs)
        log.debug(
            "dexscreener.token_detail_fetched",
            mint=mint_address,
            pairs=len(detail.all_pairs),
            liquidity=str(detail.best_pair.liquidity_usd) if detail.best_pair else None,
        )
        return detail

    async def get_token_snapshot(self, mint_address: str) -> DexPairSnapshot | None:
        """
        Lightweight wrapper used by the sampling_worker.

        Returns only the best-pair snapshot (highest liquidity).
        Returns None if the token has disappeared from DEX Screener.
        """
        detail = await self.get_token_detail(mint_address)
        if detail is None:
            return None
        return detail.best_pair
