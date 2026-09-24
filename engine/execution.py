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
from solders.signature import Signature           # type: ignore
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

    Parameters are read from settings at construction time, UNLESS an
    override is passed — added 2026-09-23 so a second, fully isolated
    execution path (workers/confluence_live_worker.py) can run live
    against its own dedicated wallet without ever touching the global
    settings.PAPER_TRADING flag or settings.WALLET_PRIVATE_KEY that every
    other caller (CapitalEngine, RiskEngine, S1WaveWorker, TradeRiskWorker
    — all of which call ExecutionEngine() with no arguments) relies on.
    Passing no arguments reproduces the exact original behavior; this is
    purely additive.

    Call buy() to open a position, sell() to close one.
    """

    def __init__(
        self,
        paper_override: bool | None = None,
        wallet_private_key_override: str | None = None,
        rpc_url_override: str | None = None,
    ) -> None:
        self._paper = settings.PAPER_TRADING if paper_override is None else paper_override
        self._slippage_bps = settings.SLIPPAGE_BPS
        self._jupiter_base = settings.JUPITER_API_URL.rstrip("/")
        self._http_timeout = httpx.Timeout(timeout=30.0, connect=10.0)

        if not self._paper:
            raw_key = settings.WALLET_PRIVATE_KEY if wallet_private_key_override is None else wallet_private_key_override
            key_bytes = base58.b58decode(raw_key)
            self._keypair = Keypair.from_bytes(key_bytes)
            rpc_url = settings.SOLANA_RPC_URL if rpc_url_override is None else rpc_url_override
            self._rpc = AsyncClient(rpc_url, commitment=Confirmed)
            log.info("execution.live_mode",
                     wallet=str(self._keypair.pubkey())[:8] + "...",
                     slippage_bps=self._slippage_bps)
        else:
            self._keypair = None
            self._rpc = None
            log.info("execution.paper_mode")

    # ── Public API ─────────────────────────────────────────────────────────

    async def get_wallet_balance_sol(self) -> Decimal | None:
        """
        Live on-chain SOL balance of this engine's own wallet — the ground
        truth for equity-based position sizing (2026-09-24, see
        workers/confluence_live_worker.py's _get_equity_usd()). Returns
        None in paper mode (no real wallet) or on an RPC error; callers
        must treat None as "unknown balance", never as zero — a transient
        RPC hiccup must not read as "wallet is empty" and trip a halt.
        """
        if self._paper or self._rpc is None or self._keypair is None:
            return None
        try:
            resp = await self._rpc.get_balance(self._keypair.pubkey())
            return Decimal(resp.value) / Decimal(1_000_000_000)
        except Exception as exc:
            log.warning("execution.get_balance_failed", error=str(exc))
            return None

    async def get_sell_quote(self, mint: str, token_lamports: int) -> dict | None:
        """
        A real, executable Jupiter sell quote for `token_lamports` of
        `mint` — read-only, submits and signs nothing. Added 2026-09-24
        after finding live that a DexScreener-sourced "current price" can
        be badly stale (reflecting a pool the market has already
        abandoned) while showing an unrealistic paper gain: BLK's
        dashboard showed +19.5% unrealized off a stale pumpswap-pool
        price, while a real Jupiter quote for the exact held size —
        routed through the pool actually trading now (Meteora DAMM v2) —
        showed 100% price impact and a real -76.5% loss. The snapshot
        price a position is monitored against can diverge arbitrarily
        from what the market will actually pay for the size actually
        held; this is the only way to know the difference before
        committing to a real sell.

        Returns {"out_lamports": int, "price_impact_pct": Decimal} or
        None on any failure (no route, network error) — callers must
        treat None as "unknown", never as "safe".
        """
        if self._paper or self._rpc is None or self._keypair is None:
            return None
        try:
            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                quote = await self._get_quote(
                    client, input_mint=mint, output_mint=_SOL_MINT, amount=token_lamports,
                )
            if not quote:
                return None
            return {
                "out_lamports": int(quote["outAmount"]),
                "price_impact_pct": Decimal(str(quote.get("priceImpactPct", "0"))),
            }
        except Exception as exc:
            log.warning("execution.liquidity_quote_failed", mint=mint, error=str(exc))
            return None

    async def get_token_balance_raw(self, mint: str) -> tuple[int, int] | None:
        """
        Real, on-chain (raw_amount, decimals) for this wallet's holding of
        `mint` — added 2026-09-24 to fix a real bug found live: the fill
        price used to be derived from swap-execution math (SOL lamports in
        / raw token units out), which is NOT the same unit as the USD-per-
        whole-token price everything else (DexScreener, the exit-decision
        logic) uses — confirmed on a real trade: the old formula produced
        0.00427 against that token's real $0.0005419 DexScreener price, 8x
        off and dimensionally meaningless. Querying the actual post-trade
        balance (whatever program owns the mint — Token or Token-2022, this
        filters by mint, not program, so both resolve) gives a real amount
        and its real decimals to compute price = position_usd / (raw_amount
        / 10**decimals), which IS in the same units as everything else.
        Retries once after a short delay — a brief post-confirmation read
        lag is more likely than the amount genuinely not being there yet.
        Returns None (not zero) on failure; callers must not treat that as
        "received nothing".
        """
        if self._paper or self._rpc is None or self._keypair is None:
            return None
        from solana.rpc.models import TokenAccountOpts
        from solders.pubkey import Pubkey
        for attempt in (1, 2):
            try:
                resp = await self._rpc.get_token_accounts_by_owner_json_parsed(
                    self._keypair.pubkey(), TokenAccountOpts(mint=Pubkey.from_string(mint)),
                )
                if resp.value:
                    info = resp.value[0].account.data.parsed["info"]["tokenAmount"]
                    return int(info["amount"]), int(info["decimals"])
            except Exception as exc:
                log.warning("execution.get_token_balance_failed", mint=mint, attempt=attempt, error=str(exc))
            if attempt == 1:
                await asyncio.sleep(2.0)
        return None

    async def close_token_account(self, mint: str) -> str | None:
        """
        Reclaim the ~0.0015-0.0021 SOL rent locked in a fully-drained token
        account by closing it back to this wallet's main SOL balance.

        Added 2026-09-24 after finding, live, that every entry creates a
        token account (rent-exempt, ~0.0015-0.0021 SOL) via Jupiter's swap
        builder, and nothing ever closed it after the matching exit sold
        the position to zero — confirmed on-chain: 5 already-fully-sold
        positions had left 5 dead, still-rent-bearing accounts behind,
        totaling ~$0.86 that never showed up in equity_usd, in open-
        position value, or in realized P&L, because none of those three
        fields track token-account rent at all. This closes the loop for
        every future exit; the 5 pre-existing stranded accounts needed a
        one-off reclaim script instead, since this method only ever runs
        right after a sell this engine itself just executed.

        Deliberately best-effort: queries the real on-chain balance right
        before closing (refuses if it isn't exactly zero — CloseAccount
        fails on-chain for a nonzero balance, but checking first avoids
        wasting a transaction and gives a clearer log line), and any
        failure here is logged and swallowed, never raised — this is a
        bonus recovery on top of an already-successful sell, not a
        condition of one. Returns the closing tx signature, or None if
        there was nothing to close, the balance wasn't zero, or the close
        itself failed.
        """
        if self._paper or self._rpc is None or self._keypair is None:
            return None
        from solana.rpc.models import TokenAccountOpts
        from solders.pubkey import Pubkey
        from solders.instruction import Instruction, AccountMeta
        from solders.message import MessageV0

        try:
            resp = await self._rpc.get_token_accounts_by_owner_json_parsed(
                self._keypair.pubkey(), TokenAccountOpts(mint=Pubkey.from_string(mint)),
            )
            if not resp.value:
                return None
            entry = resp.value[0]
            info = entry.account.data.parsed["info"]["tokenAmount"]
            if int(info["amount"]) != 0:
                log.warning("execution.close_account_skipped_nonzero_balance",
                            mint=mint, amount=info["amount"])
                return None

            account_pubkey = entry.pubkey
            program_id = entry.account.owner

            ix = Instruction(
                program_id,
                bytes([9]),  # CloseAccount — identical opcode/layout on both
                             # the classic Token program and Token-2022.
                [
                    AccountMeta(account_pubkey, False, True),        # account to close
                    AccountMeta(self._keypair.pubkey(), False, True),  # rent destination
                    AccountMeta(self._keypair.pubkey(), True, False),  # owner (signer)
                ],
            )
            blockhash_resp = await self._rpc.get_latest_blockhash()
            msg = MessageV0.try_compile(
                self._keypair.pubkey(), [ix], [], blockhash_resp.value.blockhash,
            )
            tx = VersionedTransaction(msg, [self._keypair])
            opts = TxOpts(skip_preflight=False, preflight_commitment=Confirmed)
            result = await self._rpc.send_transaction(tx, opts=opts)
            tx_sig = str(result.value)

            confirmed = await self._confirm_tx(tx_sig)
            if not confirmed:
                log.warning("execution.close_account_not_confirmed", mint=mint, tx_signature=tx_sig)
                return None

            log.info("execution.rent_reclaimed", mint=mint,
                      account=str(account_pubkey), tx_signature=tx_sig)
            return tx_sig
        except Exception as exc:
            log.warning("execution.close_account_failed", mint=mint,
                        error_type=type(exc).__name__, error=str(exc))
            return None

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

            # Real fill price and amount from the actual post-trade balance
            # (2026-09-24 fix — see get_token_balance_raw()'s docstring for
            # why: sol_lamports/out_amount is NOT a USD-per-token price, it's
            # SOL-lamports-per-raw-unit, a different unit entirely from what
            # the exit-decision logic (DexScreener's price_usd) compares it
            # against. position_usd is the one number here already known in
            # real USD, dividing it by the real whole-token amount received
            # gives a price in the same units as everything else.
            balance = await self.get_token_balance_raw(mint)
            if balance is not None:
                raw_amount, decimals = balance
                actual_amount = Decimal(raw_amount)
                actual_price = position_usd / (Decimal(raw_amount) / (Decimal(10) ** decimals))
            else:
                # Real, on-chain-confirmed tokens exist regardless — falling
                # back to the quote's estimate rather than losing track of
                # them. decimals unknown here without the balance lookup;
                # nearly all pump.fun-originated SPL tokens use 6, which is
                # the best available approximation, clearly logged as such.
                log.error("execution.post_trade_balance_unavailable", mint=mint, tx_signature=tx_sig,
                          fallback="using quoted out_amount with an assumed 6 decimals")
                actual_amount = Decimal(str(out_amount))
                actual_price = position_usd / (Decimal(str(out_amount)) / Decimal(10 ** 6))

            log.info(
                "execution.buy_confirmed",
                mint=mint,
                tx_signature=tx_sig,
                sol_in_lamports=sol_lamports,
                token_out_lamports=int(actual_amount),
                actual_price=str(actual_price),
            )

            return ExecutionResult(
                success=True,
                tx_signature=tx_sig,
                actual_price=actual_price,
                actual_amount=actual_amount,
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

            # NOTE (2026-09-24): same dimensional issue as the old buy-side
            # actual_price (SOL-lamports per raw-token-unit, not a USD-per-
            # token price) — NOT fixed here because, unlike the buy side,
            # nothing currently reads this field: _maybe_exit() computes
            # pnl_usd directly from actual_amount (real SOL received) *
            # SOL_PRICE_USD, and uses the DexScreener-sourced current_price
            # for exit_price, never this value. Left as informational only;
            # fix the same way (real post-trade balance) before anything
            # ever starts relying on it for a real decision.
            actual_price = Decimal(str(sol_out)) / Decimal(str(token_lamports))

            log.info(
                "execution.sell_confirmed",
                mint=mint,
                tx_signature=tx_sig,
                token_in_lamports=token_lamports,
                sol_out_lamports=sol_out,
                actual_price=str(actual_price),
            )

            # Reclaim the token account's rent now that the position is
            # fully sold (2026-09-24 — see close_token_account()'s
            # docstring). A full-exit sell always empties the account, so
            # this is safe to attempt unconditionally; best-effort and
            # never allowed to affect the sell's own success/failure —
            # close_token_account() already swallows its own errors, but
            # this belt-and-suspenders try/except keeps that guarantee
            # true even if that ever changes.
            try:
                await self.close_token_account(mint)
            except Exception as exc:
                log.warning("execution.close_account_call_site_error", mint=mint,
                            error_type=type(exc).__name__, error=str(exc))

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
        """
        2026-09-24 — found live, during the first real trade attempts after
        the Jupiter URL fix: solders.VersionedTransaction has no in-place
        .sign() method in the installed version (0.29.0) — it's an
        immutable, Rust-backed object. Every attempt was failing here with
        "'VersionedTransaction' object has no attribute 'sign'", caught by
        the except below and reported as the generic "Transaction
        submission failed". The correct pattern (confirmed against solders'
        own constructor docstring, then verified end-to-end with the real
        wallet keypair before deploying): build a NEW VersionedTransaction
        from the unsigned one's .message and the real signer list, rather
        than mutating the deserialized object.
        """
        try:
            unsigned = VersionedTransaction.from_bytes(tx_bytes)
            tx = VersionedTransaction(unsigned.message, [self._keypair])
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
        """
        2026-09-24 — found live, on the first real trade after the signing
        fix: get_transaction() requires a solders.Signature object, not a
        plain str — every single poll attempt raised TypeError, silently
        swallowed by the bare `except: pass` below (itself a second bug:
        it never logged anything, so this was invisible in the logs and
        only found by independently querying the chain directly). The real
        transaction had ALREADY succeeded on-chain the whole time; this
        loop just never once successfully checked, spun for the full
        timeout, and reported a false "not confirmed" — leaving a real,
        filled position with no ConfluenceLiveTrade row to track it.
        """
        sig = Signature.from_string(tx_sig)
        deadline = time.monotonic() + _CONFIRM_TIMEOUT_S
        while time.monotonic() < deadline:
            try:
                resp = await self._rpc.get_transaction(
                    sig,
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
            except Exception as exc:
                log.warning("execution.confirm_poll_error", tx_sig=tx_sig,
                            error_type=type(exc).__name__, error=str(exc))
            await asyncio.sleep(_CONFIRM_POLL_S)
        return False
