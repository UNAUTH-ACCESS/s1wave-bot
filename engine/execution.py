"""
engine/execution.py
====================
Jupiter swap execution engine.

Handles all real on-chain swaps. Controlled by PAPER_TRADING flag in
settings — when True, returns a simulated success result and touches
no real funds. When False, executes real Jupiter swaps.

Buy policy:   Zero retries. A failed buy is a missed trade — not a risk.
              Log and abort. Do not retry.

Sell policy:  Up to MAX_SELL_RETRIES retries with SELL_RETRY_DELAY_S
              delay between attempts. A failed sell means a stuck
              position. After all retries exhausted, emit
              execution.sell_failed_critical — triggers immediate
              Telegram alert and keeps position open for manual
              intervention.

Jupiter flow:
  1. GET /quote  — best route for the swap
  2. POST /swap  — build the versioned transaction
  3. Sign        — with wallet keypair
  4. Submit      — via Solana RPC sendTransaction
  5. Confirm     — poll for 'confirmed' commitment

Logging contract
----------------
info:
  execution.buy_simulated      — paper trading mode, no real swap
  execution.sell_simulated     — paper trading mode, no real swap
  execution.buy_submitted      — tx signature, input/output amounts
  execution.sell_submitted     — tx signature, input/output amounts
  execution.buy_confirmed      — actual fill price, token amount
  execution.sell_confirmed     — actual fill price, proceeds
warning:
  execution.sell_retry         — attempt N of MAX_SELL_RETRIES
error:
  execution.buy_failed         — full error context, no retry
  execution.sell_failed        — single attempt failure
  execution.sell_failed_critical — all retries exhausted, manual intervention needed
"""

from __future__ import annotations

import asyncio
import base64
import base58
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx
from solders.keypair import Keypair               # type: ignore
from solders.transaction import VersionedTransaction  # type: ignore
from solana.rpc.async_api import AsyncClient       # type: ignore
from solana.rpc.commitment import Confirmed        # type: ignore
from solana.rpc.models import TxOpts               # type: ignore

from config.logging import get_logger
from config.settings import settings
from workers.telegram_alerts import notify_sell_failed_critical

log = get_logger(__name__)

_JUPITER_QUOTE_URL = "{base}/quote"
_JUPITER_SWAP_URL  = "{base}/swap"
_SOL_MINT          = "So11111111111111111111111111111111111111112"
_CONFIRM_TIMEOUT_S = 60
_CONFIRM_POLL_S    = 2
_MAX_SELL_RETRIES  = 3
_SELL_RETRY_DELAY_S = 2.0


@dataclass
class ExecutionResult:
    """
    Result of a swap attempt.

    success=True means the transaction was confirmed on-chain.
    actual_price is the real fill price computed from amounts.
    actual_amount is the exact token amount bought (for sell sizing).
    tx_signature is the on-chain transaction ID.
    """
    success:        bool
    tx_signature:   str | None = None
    actual_price:   Decimal | None = None
    actual_amount:  Decimal | None = None   # token lamports bought
    error_type:     str | None = None
    error_detail:   str | None = None


class ExecutionEngine:
    """
    Jupiter swap execution with paper/live toggle.

    Parameters are read from settings at construction time.
    Call buy() to open a position, sell() to close one.
    """

    def __init__(self) -> None:
        self._paper = settings.PAPER_TRADING
        self._slippage_bps = settings.SLIPPAGE_BPS
        self._jupiter_base = settings.JUPITER_API_URL.rstrip("/")
        self._http_timeout = httpx.Timeout(timeout=30.0, connect=10.0)

        if not self._paper:
            raw_key = settings.WALLET_PRIVATE_KEY
            key_bytes = base58.b58decode(raw_key)
            self._keypair = Keypair.from_bytes(key_bytes)
            self._rpc = AsyncClient(settings.SOLANA_RPC_URL, commitment=Confirmed)
            log.info("execution.live_mode",
                     wallet=str(self._keypair.pubkey())[:8] + "...",
                     slippage_bps=self._slippage_bps)
        else:
            self._keypair = None
            self._rpc = None
            log.info("execution.paper_mode")

    # ── Public API ─────────────────────────────────────────────────────────

    async def buy(
        self,
        mint: str,
        position_usd: Decimal,
        sol_price_usd: Decimal,
    ) -> ExecutionResult:
        """
        Buy `mint` token using SOL worth `position_usd`.

        In paper mode: returns simulated success with estimated amounts.
        In live mode:  executes real Jupiter swap, zero retries.

        Returns ExecutionResult with actual_amount = exact token lamports
        received — store this on Trade.token_amount_bought.
        """
        if self._paper:
            return self._simulate_buy(mint, position_usd, sol_price_usd)

        sol_amount_lamports = int(
            float(position_usd / sol_price_usd) * 1e9
        )

        try:
            return await self._execute_buy(mint, sol_amount_lamports, position_usd)
        except Exception as exc:
            log.error(
                "execution.buy_failed",
                mint=mint,
                position_usd=str(position_usd),
                error_type=type(exc).__name__,
                error=str(exc),
                exc_info=True,
            )
            return ExecutionResult(
                success=False,
                error_type=type(exc).__name__,
                error_detail=str(exc),
            )

    async def sell(
        self,
        mint: str,
        token_amount: Decimal,
        trade_id: str,
        symbol: str | None = None,
    ) -> ExecutionResult:
        """
        Sell exact `token_amount` of `mint` back to SOL.

        In paper mode: returns simulated success.
        In live mode:  up to MAX_SELL_RETRIES retries.
        On total failure: emits sell_failed_critical and Telegram alert.
        Position stays open — manual intervention required.
        """
        if self._paper:
            return self._simulate_sell(mint, token_amount)

        token_lamports = int(token_amount)
        last_error = None

        for attempt in range(1, _MAX_SELL_RETRIES + 1):
            try:
                result = await self._execute_sell(mint, token_lamports)
                if result.success:
                    return result
                last_error = result.error_detail
            except Exception as exc:
                last_error = str(exc)
                log.error(
                    "execution.sell_failed",
                    mint=mint,
                    trade_id=trade_id,
                    attempt=attempt,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )

            if attempt < _MAX_SELL_RETRIES:
                log.warning(
                    "execution.sell_retry",
                    mint=mint,
                    trade_id=trade_id,
                    attempt=attempt,
                    next_attempt=attempt + 1,
                    delay_s=_SELL_RETRY_DELAY_S,
                )
                await asyncio.sleep(_SELL_RETRY_DELAY_S)

        # All retries exhausted — critical failure
        log.error(
            "execution.sell_failed_critical",
            mint=mint,
            trade_id=trade_id,
            symbol=symbol,
            attempts=_MAX_SELL_RETRIES,
            last_error=last_error,
            action_required="MANUAL_INTERVENTION",
        )

        # Fire immediate Telegram alert — position is stuck
        await notify_sell_failed_critical(
            mint=mint,
            trade_id=trade_id,
            symbol=symbol,
            attempts=_MAX_SELL_RETRIES,
            last_error=last_error,
        )

        return ExecutionResult(
            success=False,
            error_type="SELL_FAILED_CRITICAL",
            error_detail=last_error,
        )

    # ── Paper trading ──────────────────────────────────────────────────────

    def _simulate_buy(
        self,
        mint: str,
        position_usd: Decimal,
        sol_price_usd: Decimal,
    ) -> ExecutionResult:
        """Return a simulated buy result — no on-chain activity."""
        estimated_lamports = int(float(position_usd / sol_price_usd) * 1e9)
        log.info(
            "execution.buy_simulated",
            mint=mint,
            position_usd=str(position_usd),
            estimated_sol_lamports=estimated_lamports,
        )
        return ExecutionResult(
            success=True,
            tx_signature="PAPER_TRADE",
            actual_price=None,       # caller uses snapshot price
            actual_amount=Decimal(str(estimated_lamports)),
        )

    def _simulate_sell(
        self,
        mint: str,
        token_amount: Decimal,
    ) -> ExecutionResult:
        """Return a simulated sell result — no on-chain activity."""
        log.info(
            "execution.sell_simulated",
            mint=mint,
            token_amount=str(token_amount),
        )
        return ExecutionResult(
            success=True,
            tx_signature="PAPER_TRADE",
            actual_price=None,       # caller uses monitor price
            actual_amount=token_amount,
        )

    # ── Live execution ─────────────────────────────────────────────────────

    async def _execute_buy(
        self,
        mint: str,
        sol_lamports: int,
        position_usd: Decimal,
    ) -> ExecutionResult:
        """Execute a real SOL → token swap via Jupiter."""
        async with httpx.AsyncClient(timeout=self._http_timeout) as client:
            # Step 1: Get quote
            quote = await self._get_quote(
                client,
                input_mint=_SOL_MINT,
                output_mint=mint,
                amount=sol_lamports,
            )
            if not quote:
                return ExecutionResult(
                    success=False,
                    error_type="quote_failed",
                    error_detail="No route found",
                )

            out_amount = int(quote["outAmount"])

            # Step 2: Build swap transaction
            tx_bytes = await self._build_swap_tx(client, quote)
            if not tx_bytes:
                return ExecutionResult(
                    success=False,
                    error_type="swap_build_failed",
                    error_detail="Failed to build swap transaction",
                )

            # Step 3: Sign and submit
            tx_sig = await self._sign_and_submit(tx_bytes)
            if not tx_sig:
                return ExecutionResult(
                    success=False,
                    error_type="submit_failed",
                    error_detail="Transaction submission failed",
                )

            log.info(
                "execution.buy_submitted",
                mint=mint,
                tx_signature=tx_sig,
                sol_in=sol_lamports,
                token_out_estimated=out_amount,
            )

            # Step 4: Confirm
            confirmed = await self._confirm_tx(tx_sig)
            if not confirmed:
                return ExecutionResult(
                    success=False,
                    error_type="confirm_timeout",
                    error_detail=f"Transaction {tx_sig} not confirmed within {_CONFIRM_TIMEOUT_S}s",
                    tx_signature=tx_sig,
                )

            # Compute actual fill price from amounts
            actual_price = Decimal(str(sol_lamports)) / Decimal(str(out_amount))

            log.info(
                "execution.buy_confirmed",
                mint=mint,
                tx_signature=tx_sig,
                sol_in_lamports=sol_lamports,
                token_out_lamports=out_amount,
                actual_price=str(actual_price),
            )

            return ExecutionResult(
                success=True,
                tx_signature=tx_sig,
                actual_price=actual_price,
                actual_amount=Decimal(str(out_amount)),
            )

    async def _execute_sell(
        self,
        mint: str,
        token_lamports: int,
    ) -> ExecutionResult:
        """Execute a real token → SOL swap via Jupiter."""
        async with httpx.AsyncClient(timeout=self._http_timeout) as client:
            quote = await self._get_quote(
                client,
                input_mint=mint,
                output_mint=_SOL_MINT,
                amount=token_lamports,
            )
            if not quote:
                return ExecutionResult(
                    success=False,
                    error_type="quote_failed",
                    error_detail="No route found for sell",
                )

            sol_out = int(quote["outAmount"])

            tx_bytes = await self._build_swap_tx(client, quote)
            if not tx_bytes:
                return ExecutionResult(
                    success=False,
                    error_type="swap_build_failed",
                    error_detail="Failed to build sell transaction",
                )

            tx_sig = await self._sign_and_submit(tx_bytes)
            if not tx_sig:
                return ExecutionResult(
                    success=False,
                    error_type="submit_failed",
                    error_detail="Sell transaction submission failed",
                )

            log.info(
                "execution.sell_submitted",
                mint=mint,
                tx_signature=tx_sig,
                token_in=token_lamports,
                sol_out_estimated=sol_out,
            )

            confirmed = await self._confirm_tx(tx_sig)
            if not confirmed:
                return ExecutionResult(
                    success=False,
                    error_type="confirm_timeout",
                    error_detail=f"Sell tx {tx_sig} not confirmed within {_CONFIRM_TIMEOUT_S}s",
                    tx_signature=tx_sig,
                )

            actual_price = Decimal(str(sol_out)) / Decimal(str(token_lamports))

            log.info(
                "execution.sell_confirmed",
                mint=mint,
                tx_signature=tx_sig,
                token_in_lamports=token_lamports,
                sol_out_lamports=sol_out,
                actual_price=str(actual_price),
            )

            return ExecutionResult(
                success=True,
                tx_signature=tx_sig,
                actual_price=actual_price,
                actual_amount=Decimal(str(sol_out)),
            )

    # ── Jupiter helpers ────────────────────────────────────────────────────

    async def _get_quote(
        self,
        client: httpx.AsyncClient,
        input_mint: str,
        output_mint: str,
        amount: int,
    ) -> dict[str, Any] | None:
        url = _JUPITER_QUOTE_URL.format(base=self._jupiter_base)
        try:
            resp = await client.get(url, params={
                "inputMint":   input_mint,
                "outputMint":  output_mint,
                "amount":      amount,
                "slippageBps": self._slippage_bps,
                "onlyDirectRoutes": False,
            })
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.error("execution.quote_error",
                      input=input_mint, output=output_mint,
                      error=str(exc))
            return None

    async def _build_swap_tx(
        self,
        client: httpx.AsyncClient,
        quote: dict,
    ) -> bytes | None:
        url = _JUPITER_SWAP_URL.format(base=self._jupiter_base)
        try:
            resp = await client.post(url, json={
                "quoteResponse":         quote,
                "userPublicKey":         str(self._keypair.pubkey()),
                "wrapAndUnwrapSol":      True,
                "computeUnitPriceMicroLamports": "auto",
            })
            resp.raise_for_status()
            data = resp.json()
            return base64.b64decode(data["swapTransaction"])
        except Exception as exc:
            log.error("execution.swap_build_error", error=str(exc))
            return None

    async def _sign_and_submit(self, tx_bytes: bytes) -> str | None:
        try:
            tx = VersionedTransaction.from_bytes(tx_bytes)
            tx.sign([self._keypair])
            opts = TxOpts(
                skip_preflight=False,
                preflight_commitment=Confirmed,
            )
            result = await self._rpc.send_transaction(tx, opts=opts)
            return str(result.value)
        except Exception as exc:
            log.error("execution.submit_error", error=str(exc))
            return None

    async def _confirm_tx(self, tx_sig: str) -> bool:
        deadline = time.monotonic() + _CONFIRM_TIMEOUT_S
        while time.monotonic() < deadline:
            try:
                resp = await self._rpc.get_transaction(
                    tx_sig,
                    commitment=Confirmed,
                    max_supported_transaction_version=0,
                )
                if resp.value is not None:
                    if resp.value.transaction.meta.err is None:
                        return True
                    else:
                        log.error("execution.tx_error_on_chain",
                                  tx_sig=tx_sig,
                                  err=str(resp.value.transaction.meta.err))
                        return False
            except Exception:
                pass
            await asyncio.sleep(_CONFIRM_POLL_S)
        return False
