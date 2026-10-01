"""
Wallet-less Jupiter quotes for paper trading (shadow worker).

Answers: "at this exact size, how much would Jupiter actually pay/give
right now?" Read-only, no keys, no signing. Calls are serialized through a
module-level gate because lite-api.jup.ag shares one per-IP budget with the
real trader (see confluence_live_worker's 429 history); a failed or
rate-limited call returns a reason, never a fake number.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal

import httpx

from config.logging import get_logger
from config.settings import settings

log = get_logger(__name__)

SOL_MINT = "So11111111111111111111111111111111111111112"
_MIN_INTERVAL_S = 0.5
_TIMEOUT = httpx.Timeout(timeout=6.0, connect=3.0)

_gate = asyncio.Lock()
_last_call = 0.0


@dataclass(frozen=True)
class Quote:
    in_amount: int
    out_amount: int
    price_impact_raw: str  # exactly as reported; "1" is a sentinel on thin routes
    price_impact: Decimal


@dataclass(frozen=True)
class QuoteResult:
    quote: Quote | None
    reason: str  # 'ok' | 'no_route' | 'rate_limited' | 'error'


async def get_quote(input_mint: str, output_mint: str, amount: int) -> QuoteResult:
    global _last_call
    url = f"{settings.JUPITER_API_URL}/quote"
    async with _gate:
        wait = _MIN_INTERVAL_S - (time.monotonic() - _last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.get(url, params={
                    "inputMint": input_mint, "outputMint": output_mint,
                    "amount": amount, "slippageBps": settings.SLIPPAGE_BPS,
                })
        except Exception as exc:
            log.warning("jupiter_quotes.error", error=str(exc))
            return QuoteResult(None, "error")
        finally:
            _last_call = time.monotonic()

    if resp.status_code == 429:
        return QuoteResult(None, "rate_limited")
    if resp.status_code in (400, 404):
        return QuoteResult(None, "no_route")
    if resp.status_code != 200:
        return QuoteResult(None, "error")
    try:
        data = resp.json()
        raw = str(data.get("priceImpactPct", "0"))
        return QuoteResult(Quote(int(data["inAmount"]), int(data["outAmount"]), raw, Decimal(raw)), "ok")
    except Exception:
        return QuoteResult(None, "error")


async def quote_buy(mint: str, sol_lamports: int) -> QuoteResult:
    return await get_quote(SOL_MINT, mint, sol_lamports)


async def quote_sell(mint: str, token_raw: int) -> QuoteResult:
    return await get_quote(mint, SOL_MINT, token_raw)
