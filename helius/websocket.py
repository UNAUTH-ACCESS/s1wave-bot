"""
helius/websocket.py
===================
Helius WebSocket connection manager — Phase 7

Manages a single persistent WSS connection to Helius. Subscribes to
PumpSwap pool accounts for open trade positions and decodes price ticks
in real time.

Design decisions (locked in per spec)
--------------------------------------
- Encoding: base64  — jsonParsed doesn't work for custom Anchor accounts
- Commitment: processed  — fastest available; not used for accounting,
  only for stop-loss triggering. Slots can roll back.
- Discriminator: first 8 bytes skipped before BorshCoder decode
- Price formula: spot price only
  price = (reserve_b / 10^decimals_b) / (reserve_a / 10^decimals_a)
- Handler: non-blocking — pushes onto price_queue, never awaits TX
- Stale detection: 30s no-tick threshold → NO_PRICE_UPDATES notification
- Reconnect: exponential backoff 1s→2s→4s→8s→...→60s cap
  On reconnect: re-subscribe all active accounts from registry
  On reconnect: fetch current account state via getAccountInfo as baseline
  (do not wait for first notification)

Subscription registry
---------------------
subscriptions: dict[int, SubscriptionEntry]
  key  = subscription_id (numeric, returned by accountSubscribe)
  value = SubscriptionEntry(token_mint, trade_id, entry_price,
                            decimals_a, decimals_b, pool_address)

The registry is the single source of truth for active WS subscriptions.
It survives reconnects — on reconnect all entries are re-subscribed with
new subscription IDs.

PumpSwap pool account layout (Anchor, post-discriminator)
---------------------------------------------------------
After skipping the 8-byte discriminator, the relevant fields are:
  pool_bump       : u8       (1 byte)
  index           : u16      (2 bytes)
  creator         : Pubkey   (32 bytes)
  base_mint       : Pubkey   (32 bytes)
  quote_mint      : Pubkey   (32 bytes)
  lp_mint         : Pubkey   (32 bytes)
  pool_base_token_account  : Pubkey (32 bytes)
  pool_quote_token_account : Pubkey (32 bytes)
  ... (other fields)
  reserve_base    : u64      (at fixed offset after above)
  reserve_quote   : u64

We use the actual token vault accounts (pool_base_token_account /
pool_quote_token_account) from getAccountInfo to read current balances
rather than storing reserve offsets which may shift between IDL versions.
This approach is IDL-version agnostic.
"""

from __future__ import annotations

import asyncio
import base64
import json
import struct
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import httpx

from config.logging import get_logger
from config.settings import settings
from models.orm import NotificationEvent

log = get_logger(__name__)

# Reconnect backoff: 1, 2, 4, 8, 16, 32, 60, 60, ... seconds
_BACKOFF_BASE    = 1.0
_BACKOFF_MAX     = 60.0
_STALE_THRESHOLD = 30.0   # seconds before NO_PRICE_UPDATES fires
_PRICE_QUEUE_MAX = 1000   # max buffered ticks before dropping oldest

# SPL Token account layout (standard, 165 bytes):
#   mint     pubkey   offset 0   len 32
#   owner    pubkey   offset 32  len 32
#   amount   u64      offset 64  len 8   ← token balance
#   ...
#
# The PumpSwap Pool account does NOT store reserves directly.
# Reserves live in the two SPL vault token accounts:
#   pool_base_token_account  (base token vault)
#   pool_quote_token_account (quote token vault)
#
# We subscribe to BOTH vaults. Each tick gives us one vault balance.
# When we have both, we compute price = (quote/10^9) / (base/10^6)
_SPL_AMOUNT_OFFSET = 64   # u64 balance in SPL token account
_SPL_ACCOUNT_LEN   = 165  # minimum valid SPL token account size
_DISCRIMINATOR_LEN = 8    # unused for SPL accounts, kept for reference


@dataclass
class SubscriptionEntry:
    """One active WS subscription for an open trade position."""
    token_mint:        str
    trade_id:          str             # UUID string
    entry_price:       Decimal
    decimals_a:        int             # base token decimals (default 6 for pump.fun)
    decimals_b:        int             # quote token decimals (SOL = 9)
    pool_address:      str             # PumpSwap pool account pubkey (for reference)
    base_vault:        str             # pool_base_token_account (SPL token account)
    quote_vault:       str             # pool_quote_token_account (SPL token account)
    last_tick_at:      float = field(default_factory=time.monotonic)
    # Cached vault balances — updated on each tick
    reserve_base:      int = 0
    reserve_quote:     int = 0


@dataclass
class PriceTick:
    """A decoded price update from a WS notification."""
    token_mint:      str
    trade_id:        str
    price:           Decimal
    pool_address:    str
    timestamp:       float = field(default_factory=time.monotonic)


class HeliusWebSocket:
    """
    Manages the Helius WSS connection and subscription registry.

    Parameters
    ----------
    price_queue       : asyncio.Queue[PriceTick]
        Shared with PriceTickWorker. Non-blocking push only.
    notification_queue: asyncio.Queue
        For Telegram WS health events.
    shutdown_event    : asyncio.Event
    """

    def __init__(
        self,
        price_queue: asyncio.Queue,
        notification_queue: asyncio.Queue,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._price_queue = price_queue
        self._notif_queue = notification_queue
        self._shutdown    = shutdown_event

        # subscription_id → SubscriptionEntry
        self._subscriptions: dict[int, SubscriptionEntry] = {}

        # pool_address → subscription_id (reverse lookup for unsubscribe)
        self._pool_to_sub: dict[str, int] = {}

        self._ws = None           # websockets.WebSocketClientProtocol
        self._send_lock = asyncio.Lock()
        self._req_id    = 0
        self._connected = False
        self._reconnect_attempts = 0

        # pending subscribe responses: req_id → SubscriptionEntry (before we get sub_id)
        self._pending: dict[int, SubscriptionEntry] = {}

        # sub_id → role ("base" or "quote") for vault subscriptions
        self._vault_to_role: dict[int, str] = {}
        # pending vault role: req_id → (entry, role)
        self._pending_vault: dict[int, tuple] = {}

    # ── Public API ────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main loop — connect, subscribe, handle messages, reconnect."""
        log.info("helius_ws.starting")

        while not self._shutdown.is_set():
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._reconnect_attempts += 1
                backoff = min(_BACKOFF_BASE * (2 ** (self._reconnect_attempts - 1)), _BACKOFF_MAX)
                log.warning(
                    "helius_ws.disconnected",
                    exc_type=type(exc).__name__,
                    error=str(exc),
                    attempt=self._reconnect_attempts,
                    backoff_s=backoff,
                )
                await self._queue_notif(NotificationEvent.WS_DISCONNECTED, {
                    "reason": str(exc),
                    "attempt": self._reconnect_attempts,
                })
                await asyncio.sleep(backoff)

        log.info("helius_ws.stopped")

    async def subscribe(self, entry: SubscriptionEntry) -> None:
        """
        Subscribe to a PumpSwap pool account for an open trade.
        Called by PriceTickWorker when a new trade opens.
        Thread-safe via asyncio.
        """
        if not self._connected or self._ws is None:
            # WS not ready yet — store for re-subscribe on connect
            self._pending_subscribe = getattr(self, "_pending_subscribe", {})
            self._pending_subscribe[entry.pool_address] = entry
            log.debug("helius_ws.subscribe_deferred", pool=entry.pool_address)
            return

        await self._send_subscribe(entry)

    async def unsubscribe(self, pool_address: str) -> None:
        """Unsubscribe from a pool account when a trade closes."""
        sub_id = self._pool_to_sub.pop(pool_address, None)
        if sub_id is None:
            return

        self._subscriptions.pop(sub_id, None)

        if self._connected and self._ws is not None:
            await self._send_unsubscribe(sub_id)

        log.info("helius_ws.unsubscribed", pool=pool_address, sub_id=sub_id)

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def _connect_and_run(self) -> None:
        """Open WS connection, re-subscribe all active accounts, handle messages."""
        import websockets

        url = settings.HELIUS_WS_URL
        log.info("helius_ws.connecting", url=url[:40] + "...")

        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            self._connected = True
            self._reconnect_attempts = 0

            log.info("helius_ws.connected")
            await self._queue_notif(NotificationEvent.WS_CONNECTED, {})

            # Re-subscribe all active entries
            await self._resubscribe_all()

            # Fetch baseline prices immediately — don't wait for first tick
            await self._fetch_baseline_prices()

            # Start stale monitor alongside message loop
            stale_task = asyncio.create_task(self._stale_monitor())

            try:
                async for raw_msg in ws:
                    await self._handle_message(raw_msg)
            finally:
                stale_task.cancel()
                self._connected = False
                self._ws = None

    async def _resubscribe_all(self) -> None:
        """Re-subscribe all entries after reconnect. Clears old sub IDs."""
        # Merge deferred pending subscriptions
        pending = getattr(self, "_pending_subscribe", {})
        for entry in pending.values():
            if entry.pool_address not in self._pool_to_sub:
                self._subscriptions[entry.pool_address] = entry  # temp key

        # Re-subscribe using pool_address as staging key
        active_entries = list(self._subscriptions.values())
        self._subscriptions.clear()
        self._pool_to_sub.clear()

        for entry in active_entries:
            await self._send_subscribe(entry)

        if active_entries:
            log.info("helius_ws.resubscribed", count=len(active_entries))

    async def _fetch_baseline_prices(self) -> None:
        """
        Immediately fetch current account state for all subscribed pools.
        This seeds the price baseline so the risk engine has a price even
        before the first on-chain notification arrives.
        """
        entries = list(self._subscriptions.values())
        if not entries:
            return

        for entry in entries:
            try:
                price = await self._fetch_pool_price(entry)
                if price is not None:
                    tick = PriceTick(
                        token_mint=entry.token_mint,
                        trade_id=entry.trade_id,
                        price=price,
                        pool_address=entry.pool_address,
                    )
                    await self._push_tick(tick)
                    log.info(
                        "helius_ws.baseline_price_fetched",
                        token_mint=entry.token_mint,
                        price=str(price),
                        pool=entry.pool_address,
                    )
            except Exception as exc:
                log.warning(
                    "helius_ws.baseline_fetch_failed",
                    pool=entry.pool_address,
                    error=str(exc),
                )

    # ── Message handling ──────────────────────────────────────────────────

    async def _handle_message(self, raw: str | bytes) -> None:
        """
        Non-blocking message handler. Parses, decodes, pushes to queue.
        Never awaits transaction confirmation or DB operations.
        """
        try:
            msg = json.loads(raw)
        except Exception:
            return

        # Subscribe confirmation: {"jsonrpc":"2.0","result":<sub_id>,"id":<req_id>}
        if "result" in msg and "id" in msg and isinstance(msg["result"], int):
            req_id = msg["id"]
            sub_id = msg["result"]
            entry  = self._pending.pop(req_id, None)
            if entry is not None:
                self._subscriptions[sub_id] = entry
                self._pool_to_sub[entry.pool_address] = sub_id
                log.info(
                    "helius_ws.subscribed",
                    token_mint=entry.token_mint,
                    pool=entry.pool_address,
                    sub_id=sub_id,
                )
            return

        # Account notification
        method = msg.get("method")
        if method != "accountNotification":
            return

        params = msg.get("params", {})
        sub_id = params.get("subscription")
        entry  = self._subscriptions.get(sub_id)
        if entry is None:
            return

        # Update stale tracker
        entry.last_tick_at = time.monotonic()

        # Decode SPL token account data — read u64 balance at offset 64
        result_data  = params.get("result", {})
        value        = result_data.get("value", {})
        account_data = value.get("data")

        if not account_data or not isinstance(account_data, list):
            return

        raw_b64 = account_data[0]
        try:
            raw_bytes = base64.b64decode(raw_b64)
        except Exception:
            return

        if len(raw_bytes) < _SPL_AMOUNT_OFFSET + 8:
            return

        try:
            balance, = struct.unpack_from("<Q", raw_bytes, _SPL_AMOUNT_OFFSET)
        except struct.error:
            return

        # Determine which vault this notification is for
        # We need to look up the sub_id in a vault→entry mapping
        price = self._compute_price_from_vault_tick(sub_id, balance, entry)
        if price is None or price <= Decimal("0"):
            return

        tick = PriceTick(
            token_mint=entry.token_mint,
            trade_id=entry.trade_id,
            price=price,
            pool_address=entry.pool_address,
        )
        await self._push_tick(tick)

    # ── Price decoding ────────────────────────────────────────────────────

    def _compute_price_from_vault_tick(
        self,
        sub_id: int,
        balance: int,
        entry: SubscriptionEntry,
    ) -> Decimal | None:
        """
        Update one vault balance and compute price if both vaults are known.

        The WS notifies us when either vault changes. We cache the balance
        and compute price when both reserves are non-zero.

        Price formula:
          price_usd = (reserve_quote / 10^decimals_b) / (reserve_base / 10^decimals_a)

        For pump.fun tokens: decimals_a=6, decimals_b=9 (SOL)
        Result is in SOL per token — multiply by SOL/USD for USD price.

        Note: We store SOL price in the entry for USD conversion, or we
        return the SOL price and let the risk engine handle USD conversion
        using its cached SOL price.
        """
        # Identify which vault this sub_id belongs to
        # base_vault_sub and quote_vault_sub are stored in _vault_to_role
        role = self._vault_to_role.get(sub_id)
        if role == "base":
            entry.reserve_base = balance
        elif role == "quote":
            entry.reserve_quote = balance
        else:
            return None

        if entry.reserve_base == 0 or entry.reserve_quote == 0:
            return None

        normalised_base  = Decimal(entry.reserve_base)  / Decimal(10 ** entry.decimals_a)
        normalised_quote = Decimal(entry.reserve_quote) / Decimal(10 ** entry.decimals_b)

        price = normalised_quote / normalised_base
        return price.quantize(Decimal("0.000000000001"))

    async def _fetch_vault_balance(self, vault_address: str) -> int | None:
        """
        Fetch current SPL token account balance via getAccountInfo.
        Used on baseline seed after reconnect.
        """
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                vault_address,
                {"encoding": "base64", "commitment": "processed"},
            ],
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

            raw_bytes = base64.b64decode(account_data[0])
            if len(raw_bytes) < _SPL_AMOUNT_OFFSET + 8:
                return None

            balance, = struct.unpack_from("<Q", raw_bytes, _SPL_AMOUNT_OFFSET)
            return balance
        except Exception as exc:
            log.warning("helius_ws.vault_fetch_failed", vault=vault_address, error=str(exc))
            return None

    async def _fetch_pool_price(self, entry: SubscriptionEntry) -> Decimal | None:
        """
        Fetch current price by reading both vault balances via getAccountInfo.
        Used on baseline seed after reconnect — not in the hot path.
        """
        base_bal  = await self._fetch_vault_balance(entry.base_vault)
        quote_bal = await self._fetch_vault_balance(entry.quote_vault)

        if not base_bal or not quote_bal or base_bal == 0:
            return None

        entry.reserve_base  = base_bal
        entry.reserve_quote = quote_bal

        normalised_base  = Decimal(base_bal)  / Decimal(10 ** entry.decimals_a)
        normalised_quote = Decimal(quote_bal) / Decimal(10 ** entry.decimals_b)

        price = normalised_quote / normalised_base
        return price.quantize(Decimal("0.000000000001"))

    def has_subscription_for(self, pool_address: str) -> bool:
        """Return True if there is an active subscription for this pool address."""
        return pool_address in self._pool_to_sub

    # ── Subscribe / unsubscribe ───────────────────────────────────────────

    async def _send_subscribe(self, entry: SubscriptionEntry) -> None:
        """Subscribe to BOTH vault token accounts for price monitoring."""
        for vault, role in [(entry.base_vault, "base"), (entry.quote_vault, "quote")]:
            self._req_id += 1
            req_id = self._req_id
            self._pending_vault[req_id] = (entry, role)

            msg = json.dumps({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "accountSubscribe",
                "params": [
                    vault,
                    {
                        "encoding": "base64",
                        "commitment": "processed",
                    },
                ],
            })
            async with self._send_lock:
                await self._ws.send(msg)

            log.debug(
                "helius_ws.subscribe_sent",
                vault=vault,
                role=role,
                token_mint=entry.token_mint,
                req_id=req_id,
            )

    async def _send_unsubscribe(self, sub_id: int) -> None:
        self._req_id += 1
        msg = json.dumps({
            "jsonrpc": "2.0",
            "id": self._req_id,
            "method": "accountUnsubscribe",
            "params": [sub_id],
        })
        async with self._send_lock:
            await self._ws.send(msg)

    # ── Stale monitor ─────────────────────────────────────────────────────

    async def _stale_monitor(self) -> None:
        """
        Runs alongside the message loop. Detects if a subscribed pool
        hasn't sent a tick in STALE_THRESHOLD seconds and queues a
        Telegram notification. Does NOT disconnect or unsubscribe.
        """
        while True:
            await asyncio.sleep(10.0)
            now = time.monotonic()
            for sub_id, entry in list(self._subscriptions.items()):
                stale_s = now - entry.last_tick_at
                if stale_s >= _STALE_THRESHOLD:
                    log.warning(
                        "helius_ws.stale_price",
                        token_mint=entry.token_mint,
                        pool=entry.pool_address,
                        stale_seconds=round(stale_s, 1),
                    )
                    await self._queue_notif(NotificationEvent.NO_PRICE_UPDATES, {
                        "symbol": entry.token_mint[:8],
                        "stale_seconds": round(stale_s, 1),
                        "pool": entry.pool_address,
                    })

    # ── Helpers ───────────────────────────────────────────────────────────

    async def _push_tick(self, tick: PriceTick) -> None:
        """Non-blocking push to price queue. Drops oldest if full."""
        if self._price_queue.qsize() >= _PRICE_QUEUE_MAX:
            try:
                self._price_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            self._price_queue.put_nowait(tick)
        except asyncio.QueueFull:
            pass  # bounded queue, drop if full

    async def _queue_notif(self, event: NotificationEvent, payload: dict) -> None:
        """Queue a Telegram health notification. Never blocks."""
        import orjson
        from datetime import datetime, timezone
        from models.orm import NotificationQueue, NotificationStatus

        payload["timestamp"] = datetime.now(timezone.utc).isoformat()

        try:
            from database.engine import get_session
            async with get_session() as session:
                notif = NotificationQueue(
                    event_type=event,
                    payload=orjson.dumps(payload).decode(),
                    status=NotificationStatus.PENDING,
                )
                session.add(notif)
        except Exception as exc:
            log.error("helius_ws.notif_queue_failed", error=str(exc))
