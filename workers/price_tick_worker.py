"""
workers/price_tick_worker.py
============================
Price tick worker — Phase 7

Two responsibilities:
  1. Subscription manager — watches ws_subscribe_queue for new trade opens,
     looks up pool address + decimals, calls HeliusWebSocket.subscribe()
     Watches ws_unsubscribe_queue for trade closes, calls unsubscribe()

  2. Price queue drainer — drains price_queue every 500ms, calls
     RiskEngine.evaluate_trade_at_price() for each tick

This worker owns the bridge between the WebSocket layer and the risk engine.
The WebSocket handler is intentionally non-blocking — it only pushes ticks.
All risk evaluation happens here, in a separate async loop.

Queue contracts
---------------
ws_subscribe_queue:   asyncio.Queue[str]  — mint addresses of newly opened trades
ws_unsubscribe_queue: asyncio.Queue[str]  — pool addresses of closed trades
price_queue:          asyncio.Queue[PriceTick] — raw price ticks from WS handler

PumpSwap pool discovery
-----------------------
Given a token mint address, we need to find the PumpSwap pool account.
PumpSwap pool addresses are deterministic PDAs derived from:
  seeds = [b"pool", base_mint, quote_mint]
  program_id = PumpSwap program

The quote mint is always wrapped SOL (So11111111111111111111111111111111111111112).
We derive the PDA using the known PumpSwap program ID.

If PDA derivation fails, we fall back to fetching the pool via DEX Screener
token-pairs endpoint which includes the pairAddress field.

Token decimals
--------------
Fetched from tokens.token_decimals (stored at enrichment time from Helius DAS).
Falls back to 6 (standard pump.fun token) if not stored.
SOL (quote token) is always 9 decimals.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

from sqlalchemy import select

from config.logging import get_logger
from config.settings import settings
from database.engine import get_session
from engine.risk import RiskEngine
from helius.websocket import HeliusWebSocket, PriceTick, SubscriptionEntry
from models.orm import Token, TokenStatus, Trade, TradeStatus

log = get_logger(__name__)

# PumpSwap program ID (mainnet)
_PUMPSWAP_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

# Wrapped SOL mint — always the quote token in PumpSwap pools
_WSOL_MINT = "So11111111111111111111111111111111111111112"
_WSOL_DECIMALS = 9

# Fallback token decimals if not stored in DB (standard pump.fun = 6)
_DEFAULT_TOKEN_DECIMALS = 6

# How often to drain the price queue (seconds)
_DRAIN_INTERVAL = 0.5

# Max ticks to process per drain cycle (prevents runaway on tick burst)
_MAX_TICKS_PER_DRAIN = 50


class PriceTickWorker:
    """
    Bridges HeliusWebSocket price ticks to the RiskEngine.

    Parameters
    ----------
    ws              : HeliusWebSocket — already running
    price_queue     : asyncio.Queue[PriceTick] — filled by WS handler
    ws_subscribe_queue   : asyncio.Queue[str] — mint addresses to subscribe
    ws_unsubscribe_queue : asyncio.Queue[str] — pool addresses to unsubscribe
    risk_engine     : RiskEngine
    shutdown_event  : asyncio.Event
    """

    def __init__(
        self,
        ws: HeliusWebSocket,
        price_queue: asyncio.Queue,
        ws_subscribe_queue: asyncio.Queue,
        ws_unsubscribe_queue: asyncio.Queue,
        risk_engine: RiskEngine,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._ws               = ws
        self._price_queue      = price_queue
        self._sub_queue        = ws_subscribe_queue
        self._unsub_queue      = ws_unsubscribe_queue
        self._risk             = risk_engine
        self._shutdown         = shutdown_event

        # trade_id → Trade (for risk evaluation)
        self._active_trades: dict[str, Trade] = {}

    async def run(self) -> None:
        log.info("price_tick_worker.started")

        while not self._shutdown.is_set():
            t0 = time.monotonic()

            # Process new trade subscriptions
            await self._process_subscribe_queue()

            # Process trade close unsubscriptions
            await self._process_unsubscribe_queue()

            # Drain price ticks and evaluate exits
            await self._drain_price_queue()

            # Maintain 500ms cadence
            elapsed = time.monotonic() - t0
            sleep_for = max(0.0, _DRAIN_INTERVAL - elapsed)
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=sleep_for
                )
            except asyncio.TimeoutError:
                pass

        log.info("price_tick_worker.stopped")

    # ── Subscribe queue ───────────────────────────────────────────────────

    async def _process_subscribe_queue(self) -> None:
        """Drain ws_subscribe_queue. For each new mint, look up pool and subscribe."""
        while not self._sub_queue.empty():
            try:
                mint = self._sub_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            try:
                await self._subscribe_trade(mint)
            except Exception as exc:
                log.error(
                    "price_tick_worker.subscribe_error",
                    mint=mint,
                    exc_type=type(exc).__name__,
                    error=str(exc),
                )
            finally:
                self._sub_queue.task_done()

    async def _subscribe_trade(self, mint: str) -> None:
        """
        Look up token + open trade, find vault addresses, subscribe to WS.
        Subscribes to BOTH vault token accounts for correct SPL balance decoding.
        """
        async with get_session() as session:
            token_result = await session.execute(
                select(Token).where(Token.mint_address == mint)
            )
            token = token_result.scalar_one_or_none()

            if token is None:
                log.warning("price_tick_worker.token_not_found", mint=mint)
                return

            trade_result = await session.execute(
                select(Trade).where(
                    Trade.token_mint == mint,
                    Trade.status == TradeStatus.OPEN,
                )
            )
            trade = trade_result.scalar_one_or_none()

        if trade is None:
            log.warning("price_tick_worker.no_open_trade", mint=mint)
            return

        decimals_a = token.token_decimals if token.token_decimals is not None \
                     else _DEFAULT_TOKEN_DECIMALS

        # Get pool address + vault addresses from DEX Screener
        pool_info = await self._fetch_pool_info(mint)
        if pool_info is None:
            log.warning(
                "price_tick_worker.pool_not_found",
                mint=mint,
                trade_id=str(trade.id),
            )
            return

        pool_address, base_vault, quote_vault = pool_info

        entry = SubscriptionEntry(
            token_mint=mint,
            trade_id=str(trade.id),
            entry_price=trade.entry_price,
            decimals_a=decimals_a,
            decimals_b=_WSOL_DECIMALS,
            pool_address=pool_address,
            base_vault=base_vault,
            quote_vault=quote_vault,
        )

        self._active_trades[str(trade.id)] = trade

        await self._ws.subscribe(entry)
        log.info(
            "price_tick_worker.subscribed",
            mint=mint,
            trade_id=str(trade.id),
            pool=pool_address,
            base_vault=base_vault,
            quote_vault=quote_vault,
            decimals_a=decimals_a,
        )

        # Schedule a confirmation check — if no ticks arrive within 30s,
        # log a warning so we know the subscription is silent
        asyncio.create_task(
            self._confirm_subscription(mint, str(trade.id), pool_address)
        )

    # ── Unsubscribe queue ─────────────────────────────────────────────────

    async def _process_unsubscribe_queue(self) -> None:
        """Drain ws_unsubscribe_queue. Unsubscribe closed trades from WS."""
        while not self._unsub_queue.empty():
            try:
                pool_address = self._unsub_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            try:
                await self._ws.unsubscribe(pool_address)
            except Exception as exc:
                log.error(
                    "price_tick_worker.unsubscribe_error",
                    pool=pool_address,
                    error=str(exc),
                )
            finally:
                self._unsub_queue.task_done()

    # ── Price queue drainer ───────────────────────────────────────────────

    async def _drain_price_queue(self) -> None:
        """
        Drain up to MAX_TICKS_PER_DRAIN ticks. Deduplicates by token_mint —
        only one DEX Screener call per token per drain cycle (every 500ms).

        This is the only place that awaits risk engine operations.
        The WS handler never awaits anything — it only pushes.
        """
        # Collect all ticks — keep only the latest tick per token_mint
        latest: dict[str, PriceTick] = {}
        processed = 0

        while processed < _MAX_TICKS_PER_DRAIN:
            try:
                tick: PriceTick = self._price_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            latest[tick.token_mint] = tick
            self._price_queue.task_done()
            processed += 1

        # One DEX Screener call per unique token, not per tick
        for tick in latest.values():
            try:
                await self._evaluate_tick(tick)
            except Exception as exc:
                log.error(
                    "price_tick_worker.tick_eval_error",
                    token_mint=tick.token_mint,
                    exc_type=type(exc).__name__,
                    error=str(exc),
                )

        if processed > 0:
            log.debug("price_tick_worker.drain_complete", ticks=processed, unique=len(latest))

    async def _evaluate_tick(self, tick: PriceTick) -> None:
        """
        Evaluate one price tick against exit rules.

        The WS tick signals that the pool account changed — we use it as a
        trigger, but fetch the actual price from DEX Screener. This avoids
        the broken Borsh offset decoder which was producing garbage prices.
        """
        # Load trade from cache or DB
        trade = self._active_trades.get(tick.trade_id)

        if trade is None:
            # Not in cache — load from DB
            async with get_session() as session:
                result = await session.execute(
                    select(Trade).where(Trade.id == tick.trade_id)
                )
                trade = result.scalar_one_or_none()

            if trade is None or trade.status != TradeStatus.OPEN:
                # Trade already closed — unsubscribe
                await self._ws.unsubscribe(tick.pool_address)
                self._active_trades.pop(tick.trade_id, None)
                return

            self._active_trades[tick.trade_id] = trade

        if trade.status != TradeStatus.OPEN:
            await self._ws.unsubscribe(tick.pool_address)
            self._active_trades.pop(tick.trade_id, None)
            return

        # Fetch real price from DEX Screener — Borsh decode is unreliable
        spot_price = await self._fetch_dexscreener_price(tick.token_mint)
        if spot_price is None or spot_price <= Decimal("0"):
            log.debug(
                "price_tick_worker.dexscreener_price_missing",
                token_mint=tick.token_mint,
            )
            return

        # Evaluate exit conditions at DEX Screener spot price
        await self._risk.evaluate_trade_at_price(trade, spot_price)

        # If trade was just closed by risk engine, unsubscribe
        async with get_session() as session:
            result = await session.execute(
                select(Trade).where(Trade.id == trade.id)
            )
            refreshed = result.scalar_one_or_none()

        if refreshed is None or refreshed.status != TradeStatus.OPEN:
            await self._ws.unsubscribe(tick.pool_address)
            self._active_trades.pop(tick.trade_id, None)
            log.info(
                "price_tick_worker.trade_closed_unsubscribed",
                trade_id=tick.trade_id,
                pool=tick.pool_address,
            )

    async def _confirm_subscription(
        self, mint: str, trade_id: str, pool_address: str
    ) -> None:
        """
        Wait 30s after subscribing. If no WS ticks have arrived, start a
        REST polling fallback that fetches DEX Screener price every 10s and
        runs the full risk evaluation until the trade closes or WS resumes.

        This is the fix for the TROLLTRUMP -15.56% bleed: the WS subscription
        was silent for the full trade window and no fallback activated.
        """
        await asyncio.sleep(30)

        trade = self._active_trades.get(trade_id)
        if trade is None:
            return  # trade already closed — all good

        # Check if any ticks came in during the 30s window by inspecting
        # the subscription entry's last_tick_at timestamp
        sub_id = self._ws._pool_to_sub.get(pool_address)
        entry = self._ws._subscriptions.get(sub_id) if sub_id else None
        ticks_received = (
            entry is not None
            and (time.monotonic() - entry.last_tick_at) < 29.0
        )

        if ticks_received:
            log.debug(
                "price_tick_worker.subscription_confirmed",
                mint=mint,
                trade_id=trade_id,
            )
            return

        log.warning(
            "price_tick_worker.subscription_silent_starting_fallback",
            mint=mint,
            trade_id=trade_id,
            pool=pool_address,
        )

        # REST fallback loop — poll DEX Screener every 10s
        while not self._shutdown.is_set():
            # Stop if trade closed (removed from active cache)
            if trade_id not in self._active_trades:
                break

            # Stop if WS ticks have now started arriving
            sub_id = self._ws._pool_to_sub.get(pool_address)
            entry = self._ws._subscriptions.get(sub_id) if sub_id else None
            if entry is not None and (time.monotonic() - entry.last_tick_at) < 15.0:
                log.info(
                    "price_tick_worker.fallback_ws_resumed",
                    mint=mint,
                    trade_id=trade_id,
                )
                break

            # Fetch live price and evaluate stop loss / take profit / floor
            spot_price = await self._fetch_dexscreener_price(mint)
            if spot_price is not None and spot_price > Decimal("0"):
                current_trade = self._active_trades.get(trade_id)
                if current_trade is not None:
                    try:
                        await self._risk.evaluate_trade_at_price(current_trade, spot_price)
                        log.debug(
                            "price_tick_worker.fallback_tick_evaluated",
                            mint=mint,
                            trade_id=trade_id,
                            price=str(spot_price),
                        )
                    except Exception as exc:
                        log.error(
                            "price_tick_worker.fallback_eval_error",
                            mint=mint,
                            trade_id=trade_id,
                            error=str(exc),
                        )

            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pass

        log.debug("price_tick_worker.fallback_stopped", mint=mint, trade_id=trade_id)

    async def _fetch_dexscreener_price(self, token_mint: str) -> Decimal | None:
        """
        Fetch current spot price from DEX Screener.
        Used instead of Borsh decode which produces garbage prices.
        """
        import httpx
        url = f"https://api.dexscreener.com/token-pairs/v1/solana/{token_mint}"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                pairs = resp.json()

            if not isinstance(pairs, list):
                pairs = pairs.get("pairs", [])

            for pair in pairs:
                price_str = pair.get("priceUsd") or pair.get("priceNative")
                if price_str:
                    try:
                        return Decimal(str(price_str))
                    except Exception:
                        continue

        except Exception as exc:
            log.warning(
                "price_tick_worker.dexscreener_price_fetch_failed",
                token_mint=token_mint,
                error=str(exc),
            )
        return None

    # ── Pool address discovery ────────────────────────────────────────────

    async def _fetch_pool_info(self, token_mint: str) -> tuple[str, str, str] | None:
        """
        Fetch pool address + vault addresses from DEX Screener.

        Returns (pool_address, base_vault, quote_vault) or None.

        DEX Screener token-pairs response includes:
          pairAddress  — the pool account pubkey
          liquidity.base/quote token accounts can be derived, but
          the simplest approach is to use the Helius RPC to read the
          pool account and extract the vault pubkeys from bytes 139-202
          (post-discriminator positions for pool_base_token_account and
          pool_quote_token_account in the Pool struct).

        Pool struct layout (post 8-byte discriminator):
          pool_bump     u8       offset 0   (1 byte)
          index         u16      offset 1   (2 bytes)
          creator       Pubkey   offset 3   (32 bytes)
          base_mint     Pubkey   offset 35  (32 bytes)
          quote_mint    Pubkey   offset 67  (32 bytes)
          lp_mint       Pubkey   offset 99  (32 bytes)
          pool_base_token_account  Pubkey   offset 131 (32 bytes)
          pool_quote_token_account Pubkey   offset 163 (32 bytes)
          lp_supply     u64      offset 195 (8 bytes)
        """
        import httpx
        import base64
        import struct
        import base58  # standard on solana tooling

        # Step 1: get pool address from DEX Screener
        url = f"https://api.dexscreener.com/token-pairs/v1/solana/{token_mint}"
        pool_address = None
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                pairs = resp.json()

            if not isinstance(pairs, list):
                pairs = pairs.get("pairs", [])

            for pair in pairs:
                dex = pair.get("dexId", "")
                if "pumpswap" in dex.lower() or "pump" in dex.lower():
                    pool_address = pair.get("pairAddress")
                    if pool_address:
                        break

        except Exception as exc:
            log.warning("price_tick_worker.dexscreener_lookup_failed",
                       token_mint=token_mint, error=str(exc))
            return None

        if not pool_address:
            return None

        # Step 2: read pool account to extract vault pubkeys
        body = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getAccountInfo",
            "params": [pool_address, {"encoding": "base64", "commitment": "confirmed"}],
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(settings.HELIUS_RPC_URL, json=body)
                resp.raise_for_status()
                data = resp.json()

            result = data.get("result", {})
            value  = result.get("value") if result else None
            if not value:
                return None

            account_data = value.get("data")
            if not account_data or not isinstance(account_data, list):
                return None

            raw = base64.b64decode(account_data[0])

            # Skip 8-byte discriminator, then read pubkeys at offsets 131 and 163
            _DISC = 8
            _BASE_VAULT_OFFSET  = _DISC + 131  # = 139
            _QUOTE_VAULT_OFFSET = _DISC + 163  # = 171

            if len(raw) < _QUOTE_VAULT_OFFSET + 32:
                log.warning("price_tick_worker.pool_account_too_short",
                           pool=pool_address, length=len(raw))
                return None

            base_vault_bytes  = raw[_BASE_VAULT_OFFSET  : _BASE_VAULT_OFFSET  + 32]
            quote_vault_bytes = raw[_QUOTE_VAULT_OFFSET : _QUOTE_VAULT_OFFSET + 32]

            # Convert bytes to base58 pubkey string
            try:
                base_vault  = base58.b58encode(base_vault_bytes).decode()
                quote_vault = base58.b58encode(quote_vault_bytes).decode()
            except Exception:
                # Fallback: use solders if available
                try:
                    from solders.pubkey import Pubkey
                    base_vault  = str(Pubkey.from_bytes(base_vault_bytes))
                    quote_vault = str(Pubkey.from_bytes(quote_vault_bytes))
                except Exception as exc2:
                    log.warning("price_tick_worker.vault_pubkey_decode_failed",
                               error=str(exc2))
                    return None

            log.debug("price_tick_worker.pool_info_fetched",
                     pool=pool_address, base_vault=base_vault, quote_vault=quote_vault)
            return (pool_address, base_vault, quote_vault)

        except Exception as exc:
            log.warning("price_tick_worker.pool_account_fetch_failed",
                       pool=pool_address, error=str(exc))
            return None
