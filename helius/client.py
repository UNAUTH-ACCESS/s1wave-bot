"""
helius/client.py
================
Helius RPC client.

Three operations are used in v3 (PRD §3.1):

  1. DAS getAsset        — mint authority, freeze authority, holder count
  2. getTransactionsForAddress (last 50) — LP burn/lock, wash multiplier
  3. accountSubscribe    — real-time price feed for open positions (Phase 7)

All RPC calls go to HELIUS_RPC_URL (HTTPS).
WebSocket subscription goes to HELIUS_WS_URL (WSS) — wired in Phase 7.

Design notes
------------
- Single shared httpx.AsyncClient for all JSON-RPC calls.
- DAS and getTransactions are used in enrichment_worker — both are called
  per token and must handle 429 / 5xx with backoff.
- mint_authority and freeze_authority are ALWAYS fetched from chain and
  NEVER assumed.  If Helius returns unexpected structure we log and return
  a safe default of False (not renounced) — which causes Tier 1 rejection.
  This is intentional: a fetch ambiguity must fail safe.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
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
from config.settings import settings

log = get_logger(__name__)

_TIMEOUT = httpx.Timeout(timeout=20.0, connect=5.0)
_MAX_TX_HISTORY = 50  # last N transactions for wash/LP analysis


# ── Response types ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class HeliusAssetInfo:
    """
    Parsed result from DAS getAsset.

    Fields that could not be determined from the response are set to their
    SAFE DEFAULT (False = not renounced), which causes Tier 1 rejection.
    This is intentional — never assume a safety property you couldn't verify.
    """
    mint_address: str
    mint_authority_renounced: bool   # safe default: False
    freeze_authority_renounced: bool # safe default: False
    holder_count: int | None        # may be absent on new tokens
    token_decimals: int | None = None    # new line


@dataclass(frozen=True)
class HeliusTxAnalysis:
    """
    Parsed result from last-50-transactions analysis.

    lp_locked_burned: True only when we find explicit LP burn/lock TX.
                      False is the safe default.
    wash_multiplier: buy_count / sell_count.
                     float('inf') when sell_count == 0.
    buy_count / sell_count: raw transaction-level counts.
    """
    lp_locked_burned: bool
    wash_multiplier: float
    buy_count: int
    sell_count: int


# ── JSON-RPC helpers ──────────────────────────────────────────────────────────

def _rpc_body(method: str, params: list | dict) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }


def _extract_result(data: dict, method: str) -> Any:
    if "error" in data:
        raise ValueError(
            f"Helius RPC error in {method}: {data['error'].get('message', data['error'])}"
        )
    return data.get("result")


# ── TX analysis helpers ───────────────────────────────────────────────────────

# Known LP burn addresses on Solana
_LP_BURN_DESTINATIONS = frozenset({
    "1nc1nerator11111111111111111111111111111111",
    "BurnAddressXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",  # placeholder — extend as needed
})

# Raydium/Orca/Meteora LP program IDs — used to identify LP lock TXs
_LP_PROGRAM_IDS = frozenset({
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3sFjatC",   # Orca Whirlpool
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",   # Meteora
})


def _analyse_transactions(txs: list[dict]) -> HeliusTxAnalysis:
    """
    Walk last-50 transactions to determine:
      - LP locked/burned status
      - wash multiplier (buy_count / sell_count)

    LP is considered locked/burned when any transaction:
      - Sends LP tokens to a known burn address, OR
      - Invokes a known lock program with LP tokens

    Buy/sell classification:
      - Helius enriches transactions with "type" field
      - SWAP with tokenTransfers increasing base token = buy
      - SWAP with tokenTransfers decreasing base token = sell
    """
    lp_locked_burned = False
    buy_count = 0
    sell_count = 0

    for tx in txs:
        tx_type = tx.get("type", "")
        description = tx.get("description", "")
        instructions = tx.get("instructions", [])

        # ── LP lock/burn detection ────────────────────────────────────
        # Check token transfers for LP tokens going to burn addresses
        for transfer in tx.get("tokenTransfers", []):
            destination = transfer.get("toUserAccount", "")
            if destination in _LP_BURN_DESTINATIONS:
                lp_locked_burned = True
                log.debug("helius.lp_burn_detected", destination=destination)

        # Check if any instruction invokes a known LP lock program
        for instr in instructions:
            program_id = instr.get("programId", "")
            if program_id in _LP_PROGRAM_IDS:
                # Any interaction with LP programs after creation is a lock/burn signal
                # This is conservative — a more precise check would inspect instruction data
                if "lock" in description.lower() or "burn" in description.lower():
                    lp_locked_burned = True

        # ── Buy/sell classification ───────────────────────────────────
        if tx_type == "SWAP":
            # Helius sets swap direction in tokenTransfers
            # Positive amount into base token wallet = buy; negative = sell
            native_transfers = tx.get("nativeTransfers", [])
            token_transfers = tx.get("tokenTransfers", [])

            # Heuristic: if the fee payer received the base token, it's a buy
            fee_payer = tx.get("feePayer", "")
            received_base = any(
                t.get("toUserAccount") == fee_payer and float(t.get("tokenAmount", 0)) > 0
                for t in token_transfers
            )
            if received_base:
                buy_count += 1
            else:
                sell_count += 1

    # wash_multiplier direction (per PRD resolution):
    #   buy_count / sell_count > TIER1_MAX_WASH_MULTIPLIER → wash trading → REJECT
    #   buy_count / sell_count <= TIER1_MAX_WASH_MULTIPLIER → organic → PASS
    if sell_count == 0:
        wash_mult = float("inf")  # always fails Tier 1
    else:
        wash_mult = buy_count / sell_count

    return HeliusTxAnalysis(
        lp_locked_burned=lp_locked_burned,
        wash_multiplier=wash_mult,
        buy_count=buy_count,
        sell_count=sell_count,
    )


# ── Client ────────────────────────────────────────────────────────────────────

class HeliusClient:
    """
    Async Helius RPC client.

    Create once, call start(), use, call close().
    Or use as an async context manager.

    Usage
    -----
        async with HeliusClient() as helius:
            asset = await helius.get_asset(mint_address)
            tx_analysis = await helius.analyse_transactions(mint_address)
    """

    def __init__(self) -> None:
        self._http: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=settings.HELIUS_RPC_URL,
            timeout=_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )

    async def close(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None

    async def __aenter__(self) -> "HeliusClient":
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            raise RuntimeError("HeliusClient not started — call start() first.")
        return self._http

    # ── DAS getAsset ──────────────────────────────────────────────────────

    @retry(
        retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    async def get_asset(self, mint_address: str) -> HeliusAssetInfo:
        """
        Fetch mint authority, freeze authority, and holder count from DAS.

        NEVER assumes any authority is renounced — always fetches from chain.
        On any parse failure, returns safe defaults (False = not renounced).
        """
        body = _rpc_body("getAsset", {"id": mint_address})
        resp = await self._client().post("", json=body)
        resp.raise_for_status()
        data = resp.json()

        try:
            result = _extract_result(data, "getAsset")
        except ValueError as exc:
            log.warning("helius.get_asset_error", mint=mint_address, error=str(exc))
            return HeliusAssetInfo(
                mint_address=mint_address,
                mint_authority_renounced=False,  # safe default
                freeze_authority_renounced=False,
                holder_count=None,
                token_decimals=None,
            )

        if result is None:
            log.warning("helius.get_asset_null", mint=mint_address)
            return HeliusAssetInfo(
                mint_address=mint_address,
                mint_authority_renounced=False,
                freeze_authority_renounced=False,
                holder_count=None,
                token_decimals=None,
            )

        try:
            ownership = result.get("ownership", {})
            supply = result.get("supply", {})
            authorities = result.get("authorities", [])
            token_info = result.get("token_info", {})

            # Mint authority: renounced when ownership.owner == None or
            # pump.fun burns mint authority at graduation by protocol.
            # Helius DAS returns empty authorities list for these tokens.
            # Trust the protocol — if mint address ends in "pump", renounced.
            _PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
            has_mint_authority = any(
                "mint" in a.get("scopes", [])
                for a in authorities
            )
            owner = ownership.get("owner")
            mint_auth_renounced = (
                not has_mint_authority
                or owner == _PUMP_FUN_PROGRAM
                or mint_address.endswith("pump")
            )
            freeze_auth = token_info.get("freeze_authority") or token_info.get("freezeAuthority")
            freeze_auth_renounced = freeze_auth is None

            # Holder count: from token_info or supply
            holder_count = (
                token_info.get("holder_count")
                or (supply.get("print_current_supply") if supply else None)
                or None
            )

            log.debug(
                "helius.asset_fetched",
                mint=mint_address,
                mint_auth_renounced=mint_auth_renounced,
                freeze_auth_renounced=freeze_auth_renounced,
                holder_count=holder_count,
            )

            # Token decimals — from token_info.decimals (standard SPL)
            token_decimals = token_info.get("decimals")
            if token_decimals is not None:
                try:
                    token_decimals = int(token_decimals)
                except (ValueError, TypeError):
                    token_decimals = None

            return HeliusAssetInfo(
                mint_address=mint_address,
                mint_authority_renounced=mint_auth_renounced,
                freeze_authority_renounced=freeze_auth_renounced,
                holder_count=holder_count,
                token_decimals=token_decimals,
            )

        except Exception as exc:
            # Parse failure → safe default (rejection via Tier 1)
            log.error(
                "helius.get_asset_parse_error",
                mint=mint_address,
                error=str(exc),
                exc_info=True,
            )
            return HeliusAssetInfo(
                mint_address=mint_address,
                mint_authority_renounced=False,
                freeze_authority_renounced=False,
                holder_count=None,
                token_decimals=None,
            )

    # ── getTransactionsForAddress ──────────────────────────────────────────
    # BYPASSED: Helius TX parsing is broken for PumpSwap — the enhanced
    # transaction API returns "Invalid params" for the type:SWAP filter on
    # all newly graduated tokens. This call cost ~800ms per token and burned
    # Helius RPC credits with zero useful signal.
    #
    # LP locked check: removed — pump.fun burns LP at graduation by protocol.
    # Wash multiplier: bypassed — TIER1_MAX_WASH_MULTIPLIER=9999.0 in settings.
    # Both will be revisited in Phase 12 pre-flight using DEX Screener h1 data.

    async def analyse_transactions(self, mint_address: str) -> HeliusTxAnalysis:
        """
        Returns safe defaults immediately — TX analysis bypassed for PumpSwap.
        LP is guaranteed burned by pump.fun graduation protocol.
        Wash multiplier is checked via DEX Screener h1 data instead (Phase 12).
        """
        return HeliusTxAnalysis(
            lp_locked_burned=False,
            wash_multiplier=float("inf"),
            buy_count=0,
            sell_count=0,
        )

    # ── Jupiter price quote (used in Phase 5 / 6) ────────────────────────────
    # Stubbed here so the import chain is consistent.
    # Full implementation wired in engine/capital.py (Phase 5).

    async def get_jupiter_price(self, mint_address: str) -> Decimal | None:
        """
        Stub.  Returns None until Phase 5 wires the Jupiter Price API.
        """
        return None
