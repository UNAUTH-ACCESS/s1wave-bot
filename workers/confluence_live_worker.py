"""
workers/confluence_live_worker.py
====================================
REAL, on-chain live trading for confluence_entry_v1 ONLY.

Built 2026-09-23 after the user reviewed a mixed
confluence_shadow_positions evaluation (n=167 real, 1-second-monitored
paper trades: a real edge, but inconsistent across time — 2 of 4
chronological periods were flat/negative once a single dominant outlier
was excluded — and a 17% rug rate averaging -78%) and explicitly chose to
proceed anyway, with $10 total capital at risk. This worker is that
decision, implemented as cautiously as the user asked.

ISOLATION — read this before touching anything here or in the files it
touches
-----------------------------------------------------------------------
This worker, and the two-parameter change to engine/execution.py's
ExecutionEngine that makes it possible, are the ONLY things in this
codebase that can move real money as of 2026-09-23. Specifically:
  - Uses its OWN dedicated wallet (settings.CONFLUENCE_LIVE_WALLET_
    PRIVATE_KEY), never settings.WALLET_PRIVATE_KEY.
  - The old scorer/S1Wave/CapitalEngine paper-trading pipeline (the only
    other thing that ever called ExecutionEngine()) was removed entirely
    on 2026-09-23 — this is now the only production caller of
    ExecutionEngine, period.
  - Never touches the real `trades` table (also removed) or
    MAX_CONCURRENT_TRADES — its own confluence_live_trades table is
    fully separate.
  - Gated behind settings.CONFLUENCE_LIVE_ENABLED, which defaults to
    False. New entries stop the instant this is False, though open
    positions are still monitored/exited regardless — see main.py.

Entry rule — IDENTICAL to confluence_shadow_worker.py, on purpose
---------------------------------------------------------------------
Same signal (momentum_signal_events, n_rules_cofiring >= 2 for
experiment_version='momentum_confluence_v1') as the paper experiment this
was validated against. Not extended, not re-tuned — the whole point of
freezing the strategy per the user's own instruction was to test THIS
exact rule with real execution risk added, not a new one. (The user later
asked, same day, for compounding position sizing and a layered exit — see
below — but the ENTRY signal itself has never changed.)

Position sizing — exposure percentage against LIVE wallet equity, split
across concurrent slots (2026-09-23: percentage-of-equity formula, per the
user's own instruction "expose 10% of total capital so each trade is
3.33%"; 2026-09-24: equity itself became the live on-chain wallet balance
instead of a fixed config stake, per the user's instruction "$7 deposited
-> trade with $7, $15 deposited -> trade with $15 ... size trades with
available wallet capital" — see engine/live_equity.py, the shared module
this and api/app.py both call so the formula can't drift between them)
---------------------------------------------------------------------
See engine/live_equity.py's module docstring and compute_position_usd():
never more than CONFLUENCE_LIVE_EXPOSURE_PCT of CURRENT live equity is at
risk across every open position combined, split evenly across
CONFLUENCE_LIVE_MAX_CONCURRENT slots. Compounds automatically since
equity IS the live wallet balance — a deposit, a withdrawal, and realized
P&L all show up in it for free, no separate bookkeeping needed. Always
capped at CONFLUENCE_LIVE_MAX_POSITION_USD.

Safety layers, all independent, all checked before every entry
--------------------------------------------------------------------
  1. settings.CONFLUENCE_LIVE_ENABLED must be True (master switch).
  2. No more than settings.CONFLUENCE_LIVE_MAX_CONCURRENT trades already
     OPEN (raised from 1 to 3 on 2026-09-23, then 3 to 5 on 2026-09-24 —
     see config/settings.py's comment and analysis/sl_tp_and_concurrency_
     sweep.py for the replay analysis that motivated the original change:
     one-at-a-time trading only captured 15 of 196 real signals and
     concentrated all risk into a single bet at a time, which is what
     produced the near-total wipeout found in an earlier retroactive
     replay).
  3. Live wallet equity must be above CONFLUENCE_LIVE_MIN_TRADEABLE_USD
     (a dust floor, default $1) — below it, this halts PERMANENTLY (not
     just for the day) until the wallet is topped back up. Since equity
     IS the live balance now, this is a direct, always-current read of
     "is there anything meaningful left to trade with", not a fixed
     lifetime-loss figure that could go stale.
  4. A daily circuit breaker (CONFLUENCE_LIVE_DAILY_LOSS_LIMIT_PCT of
     *today's starting* live equity, realized today) pauses new entries
     until the next UTC day — a softer, earlier warning than #3.

Bad-tick guard — deliberately duplicated, not imported, from
workers/confluence_shadow_worker.py
------------------------------------------------------------------------
Same logic (an implausible single-tick move is held until a second,
similar tick confirms it — see that file's docstring for the real
2026-09-22 incident this defends against), copied rather than shared on
purpose: a future change to the paper-shadow experiment must never be
able to silently change real-money behavior, and vice versa.

Exit priority — layered stop (2026-09-23, replacing the old fixed
-6%/+30% pair): velocity breaker -> HARD_FLOOR -> engine/trailing_stop.py's
staircase (starts at the same -6% distance below entry, ratchets up in
10% steps as price makes new highs, never force-sells a winner just for
reaching the old +30% mark) -> TIME_EXIT. Identical logic in
confluence_shadow_worker.py, duplicated on purpose per the isolation
note above.

On a sell failure: execution.py's own sell() already retries 3x with
delay and fires a Telegram alert on total failure (execution.sell_
failed_critical). This worker does not add a second retry loop on top —
a trade that fails to sell simply stays 'open' and gets re-evaluated
(and re-attempted) on the next normal ~1s cycle, same as any other open
position. No infinite tight-loop retries; the existing 1s poll interval
is the backoff.

Logging contract
-----------------
info:
  confluence_live.started / stopped
  confluence_live.entry_attempted / entry_filled / entry_failed
  confluence_live.exit_attempted / exit_filled / exit_failed
  confluence_live.cycle_complete
warning:
  confluence_live.daily_loss_limit_hit
  confluence_live.total_capital_exhausted
  confluence_live.implausible_tick_held / implausible_tick_confirmed
error:
  confluence_live.fetch_error
  confluence_live.cycle_error
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx
from sqlalchemy import select, func

from config.logging import get_logger
from config.settings import settings
from database.engine import get_session
from engine.execution import ExecutionEngine
from engine.sell_coordination import finish_sell, try_start_sell
from engine.live_equity import (
    compute_equity_usd, compute_position_usd, is_daily_halted, is_permanently_halted,
)
from engine.trailing_stop import initial_floor, update_trailing_stop
from models.orm import (
    ConfluenceLiveObservation, ConfluenceLiveTrade, ConfluenceNotification, MomentumSignalEvent, Token,
)
from workers.entry_filters import is_buy_pressure_too_low, is_liquidity_too_high, is_wash_trading_rejected
from workers.http_queue import RateLimitedQueue
from workers.dexscreener_client import _DEXSCREENER_BASE, _best_pair

log = get_logger(__name__)

SOURCE_EXPERIMENT_VERSION = "momentum_confluence_v1"
MIN_RULES_COFIRING = 2
VELOCITY_BREAKER_PCT = Decimal("-0.25")
_EQUITY_CACHE_TTL_S = 10.0

_TIMEOUT = httpx.Timeout(timeout=10.0, connect=5.0)
_MAX_MINTS_PER_REQUEST = 30

IMPLAUSIBLE_TICK_RATIO = 3.0
CONFIRMATION_TOLERANCE = 0.5

# Liquidity guard (2026-09-24) — see ExecutionEngine.get_sell_quote()'s
# docstring for the real incident: BLK's DexScreener-sourced price showed
# +19.5% unrealized while a real Jupiter quote for the exact held size,
# routed through the pool actually trading now, showed 100% price impact
# and a real -76.5% loss. The snapshot price a position is monitored
# against can be arbitrarily stale in a way the normal HARD_FLOOR/
# trailing-stop logic — which only ever looks at that same snapshot —
# cannot detect. Checked at most once per _LIQUIDITY_CHECK_INTERVAL_S per
# trade.
#
# 2026-09-24, same day — found live within minutes of deploying at 20s:
# lite-api.jup.ag (the free, unauthenticated Jupiter tier — the same one
# execution.py uses for real buy/sell quotes) 429'd EVERY single guard
# check, continuously, even with zero other traffic. 3 concurrent
# positions x one check per 20s (~9 req/min sustained) is apparently
# already over its real per-IP budget once added on top of normal trading
# activity — real buy/sell quotes are infrequent (once per signal), so
# they weren't the problem; the guard's own steady background polling
# was. A silently-429ing guard provides zero protection (fails safe,
# falls through to the normal snapshot logic, exactly as if the guard
# didn't exist) while looking deployed and active. Widened to 60s — still
# catches a liquidity crisis well within the timeframe it matters, at a
# request rate this guard alone never 429s at.
#
# CORRECTION, 2026-09-25: "real trading has run without a single 429"
# stopped being true the same day a position (SEND) went unsellable — its
# own endless sell-retry loop, not this guard, was the thing that started
# saturating Jupiter's shared rate limit and blocking real new buys. See
# _SELL_BACKOFF_AFTER_S below, which fixes that.
_LIQUIDITY_CHECK_INTERVAL_S = 60.0
_LIQUIDITY_CRISIS_IMPACT_PCT = Decimal("0.35")     # >=35% impact on a full-size sell = pool has dried up
_LIQUIDITY_CRISIS_PNL_FLOOR_PCT = Decimal("-0.30")  # real executable P&L already worse than any normal stop

# Adaptive faster check for a position in real profit (2026-09-25) — see
# _check_liquidity_guard()'s docstring for the Gavel incident this closes:
# a snapshot showing +40% while the real price had already collapsed to
# -33%, only caught at the very end of a 60s window. 30s halves that
# worst-case gap for the (usually few) positions actually worth protecting,
# without repeating the ~20s-for-everything rate-limit incident this
# module's history already has.
_LIQUIDITY_CHECK_INTERVAL_FAST_S = 30.0
_LIQUIDITY_CHECK_FAST_THRESHOLD_PCT = Decimal("0.15")  # snapshot-based unrealized gain that earns the faster check

# Unsellable-position handling (2026-09-24) — real incident: SEND's exit
# (LIQUIDITY_GUARD, fired 1s after entry into an already-drained pool)
# failed every retry for 4+ minutes straight with the same on-chain error
# (Meteora DAMM v2 custom error 6024) — confirmed NOT a slippage issue by
# manually retrying at 90% slippage tolerance and getting the identical
# failure, meaning no retry-with-wider-tolerance would ever succeed here.
# Left as a normal 'open' position, it would occupy one of
# CONFLUENCE_LIVE_MAX_CONCURRENT's slots FOREVER — at 5 slots, losing even
# one or two this way permanently cripples real trading capacity, which
# matters far more than the (usually tiny) position itself. After
# _UNSELLABLE_AFTER_S of continuous real sell failures, the position is
# downgraded to status='unsellable': _open_trade_count() only ever counts
# status=='open', so this immediately frees its concurrency slot for a new
# trade — while _load_open_trades() still includes it, so it keeps getting
# priced and keeps getting real sell attempts every cycle, exactly as
# before. The ONLY way out of 'unsellable' is a REAL successful sell
# transitioning it straight to 'closed' — never fabricated, per this
# codebase's standing rule that a close always means a real transaction
# happened. If the pool never recovers, the position simply stays
# 'unsellable' indefinitely, visible and honest rather than either eating
# a slot forever or being silently marked closed with a made-up number.
_UNSELLABLE_AFTER_S = 600.0

# Periodic rent sweep (2026-09-25) — real incident: SEND's already-empty
# token account sat unreclaimed for a full day (see the balance-zero
# reconciliation section) because the only existing reclaim path
# (execution.py's close_token_account(), called right after a sell this
# engine itself just executed) structurally cannot recover an account
# that went to zero any other way. This calls ExecutionEngine's general
# sweep_dead_token_accounts() on this interval — housekeeping, not
# time-sensitive, so a wide interval is fine and keeps this off the
# critical path.
_RENT_SWEEP_INTERVAL_S = 1800.0  # 30 minutes

# Sell-retry backoff for a persistently-failing exit (2026-09-25) — real,
# live incident: SEND's exit reason (TIME_EXIT, permanently true forever
# once past max_hold_seconds) re-triggered a full real sell attempt (a
# quote + swap-build + submit, itself internally retried up to
# _MAX_SELL_RETRIES times 2s apart) on every single ~1s poll cycle, all
# day, long after it was already known to be stuck. This wasn't just
# wasted effort — confirmed live, 2026-09-25 05:45:51: a genuinely
# fillable new buy (Luckin) failed with swap_build_failed because
# Jupiter's shared /swap endpoint 429'd at the exact same instant SEND's
# own nonstop retry loop was hitting it. The bot's own stuck position was
# starving its real trade execution of API quota. A confirmed-drained
# pool does not get less drained by retrying every few seconds — once a
# sell has been failing longer than _SELL_BACKOFF_AFTER_S, space real
# attempts out to at most one per _SELL_BACKOFF_INTERVAL_S. Still catches
# a real recovery within half a minute; costs nothing real to slow down.
_SELL_BACKOFF_AFTER_S = 30.0
_SELL_BACKOFF_INTERVAL_S = 30.0


class ConfluenceLiveWorker:
    """REAL on-chain execution of the confluence_entry_v1 rule. See module
    docstring for isolation guarantees and safety layers — this class does
    not enforce them alone, it's the combination of settings gating,
    dedicated wallet, and a separate table that makes this safe to run
    alongside everything else."""

    def __init__(self, shutdown_event: asyncio.Event, poll_interval: float = 1.0) -> None:
        self._shutdown = shutdown_event
        self._poll_interval = poll_interval
        self._http = RateLimitedQueue()
        self._started_at = datetime.now(timezone.utc)
        self._last_accepted_price: dict = {}
        self._pending_tick: dict = {}
        self._equity_cache: tuple[float, Decimal | None] | None = None
        # Edge-trigger state for halt notifications — None means "not observed
        # yet". _safe_to_enter() re-checks these every ~1s and must NOT notify
        # on every check, only when the value actually changes.
        #
        # Real bug found 2026-09-24, from a code-reading audit (not a live
        # incident this time): because these start as None, and `bool != None`
        # is always True, the very FIRST _refresh_halt_notifications() call
        # after ANY restart looked like a transition even when nothing
        # changed, firing "Wallet funded"/"Daily loss limit no longer in
        # effect" as if something had just recovered, purely because the
        # process restarted. With how many restarts happen during active
        # development, this had been spamming misleading recovery
        # notifications into the feed all session. Fixed via
        # self._halt_state_initialized below: the "recovered" notifications
        # (the announcement that something is fine) are suppressed on the
        # first check after a restart — only a genuinely bad state (halted)
        # still announces immediately on first check, since that IS worth
        # knowing right away; "everything is normal" is not news just
        # because the process restarted.
        self._last_permanently_halted: bool | None = None
        self._last_daily_halted: bool | None = None
        self._halt_state_initialized = False
        # A stuck-exit (execution.py's sell() already exhausts all retries
        # internally before ever returning failure, so every result.success
        # False IS the critical case) gets re-attempted every ~1s cycle by
        # design — notify once per trade, not once per cycle, or a token
        # with dried-up liquidity would flood the feed for as long as it
        # stays stuck.
        self._notified_stuck_trades: set = set()
        # Unsellable-position tracking (2026-09-24) — trade id -> time.monotonic()
        # of the FIRST failed sell attempt for that trade (reset to absent
        # once a sell succeeds or the trade is otherwise no longer open).
        # See _UNSELLABLE_AFTER_S's comment for why this exists.
        self._sell_failing_since: dict = {}
        # Sell-retry backoff (2026-09-25) — trade id -> time.monotonic() of
        # the last real sell attempt, once that trade has been failing
        # longer than _SELL_BACKOFF_AFTER_S. See that constant's comment.
        self._last_sell_attempt: dict = {}
        # Liquidity guard state (2026-09-24) — trade id -> time.monotonic()
        # of the last real Jupiter quote check, so it's throttled per-trade
        # rather than per-cycle.
        self._last_liquidity_check: dict = {}
        # Periodic rent sweep (2026-09-25) — see _RENT_SWEEP_INTERVAL_S's
        # comment. None means "never run yet," so the very first cycle
        # doesn't wait a full interval before the first sweep.
        self._last_rent_sweep: float | None = None
        self._execution = ExecutionEngine(
            paper_override=False,
            wallet_private_key_override=settings.CONFLUENCE_LIVE_WALLET_PRIVATE_KEY,
            rpc_url_override=settings.HELIUS_RPC_URL,
        )

    async def run(self) -> None:
        log.info("confluence_live.started", poll_interval=self._poll_interval,
                 started_at=self._started_at.isoformat(),
                 min_tradeable_usd=settings.CONFLUENCE_LIVE_MIN_TRADEABLE_USD,
                 max_position_usd=settings.CONFLUENCE_LIVE_MAX_POSITION_USD,
                 max_concurrent=settings.CONFLUENCE_LIVE_MAX_CONCURRENT)
        self._http.start()
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                while not self._shutdown.is_set():
                    try:
                        await self._cycle(client)
                    except Exception as exc:
                        log.error("confluence_live.cycle_error", error=str(exc), exc_info=True)
                    try:
                        await asyncio.wait_for(self._shutdown.wait(), timeout=self._poll_interval)
                    except asyncio.TimeoutError:
                        pass
        finally:
            self._http.stop()
            log.info("confluence_live.stopped")

    async def _cycle(self, client: httpx.AsyncClient) -> None:
        t0 = time.monotonic()

        # Runs regardless of CONFLUENCE_LIVE_ENABLED — see its own docstring
        # for why: halt-state notifications are informational, not an entry
        # decision, and a paused user still needs to know the moment a
        # deposit clears the dust floor.
        equity = await self._get_equity_usd()
        today_pnl = await self._load_today_realized_pnl() if equity is not None else Decimal("0")
        all_time_pnl = await self._load_all_time_realized_pnl() if equity is not None else Decimal("0")
        await self._refresh_halt_notifications(equity, today_pnl, all_time_pnl)
        await self._maybe_sweep_rent()

        if settings.CONFLUENCE_LIVE_ENABLED:
            await self._maybe_enter()

        open_trades = await self._load_open_trades()
        if not open_trades:
            return

        mints = list(open_trades.keys())
        snapshots: dict[str, dict] = {}
        for i in range(0, len(mints), _MAX_MINTS_PER_REQUEST):
            chunk = mints[i:i + _MAX_MINTS_PER_REQUEST]
            try:
                snapshots.update(await self._fetch(client, chunk))
            except Exception as exc:
                log.error("confluence_live.fetch_error", error=str(exc),
                          exc_type=type(exc).__name__, chunk_size=len(chunk))

        now = datetime.now(timezone.utc)
        for mint, trade in open_trades.items():
            data = snapshots.get(mint)
            if data is None:
                continue
            price = self._accept_price(trade, data["price_usd"])
            if price is None:
                continue
            await self._record_observation(trade["id"], price, now)
            await self._maybe_exit(trade, price, now)

        elapsed_ms = round((time.monotonic() - t0) * 1000)
        log.info("confluence_live.cycle_complete", open_trades=len(mints), elapsed_ms=elapsed_ms)

    # ── in-app notifications (2026-09-24) ───────────────────────────────

    async def _notify(self, level: str, event: str, message: str, trade_id=None) -> None:
        """Persist an in-app notification — see models.orm.ConfluenceNotification
        for the retention/severity contract. Never raises: a notification
        failure must not be allowed to break the trading cycle that
        triggered it."""
        try:
            async with get_session() as session:
                session.add(ConfluenceNotification(level=level, event=event, message=message, trade_id=trade_id))
        except Exception as exc:
            log.error("confluence_live.notify_failed", event=event, error=str(exc))

    # ── safety gates ─────────────────────────────────────────────────────

    async def _load_all_time_realized_pnl(self) -> Decimal:
        """Re-added 2026-09-24 for the absolute-dollar lifetime max-loss cap
        (CONFLUENCE_LIVE_MAX_LOSS_USD) — independent of equity, which no
        longer tracks this on its own now that equity is the live wallet
        balance rather than (stake + all-time pnl)."""
        async with get_session() as session:
            result = await session.execute(
                select(func.coalesce(func.sum(ConfluenceLiveTrade.pnl_usd), 0))
                .where(ConfluenceLiveTrade.status == "closed")
            )
            return Decimal(str(result.scalar_one()))

    async def _load_today_realized_pnl(self) -> Decimal:
        start_of_day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        async with get_session() as session:
            result = await session.execute(
                select(func.coalesce(func.sum(ConfluenceLiveTrade.pnl_usd), 0))
                .where(ConfluenceLiveTrade.status == "closed", ConfluenceLiveTrade.exit_time >= start_of_day)
            )
            return Decimal(str(result.scalar_one()))

    async def _open_trade_count(self) -> int:
        async with get_session() as session:
            result = await session.execute(
                select(func.count()).select_from(ConfluenceLiveTrade).where(ConfluenceLiveTrade.status == "open")
            )
            return result.scalar_one()

    async def _get_equity_usd(self) -> Decimal | None:
        """
        Live wallet equity in USD (2026-09-24) — see engine/live_equity.py
        for the full model. Cached briefly: balance only changes on our own
        fills or an external deposit/withdrawal, so there's no need to hit
        the RPC on every ~1s poll cycle. None means "unknown right now"
        (RPC error, or SOL_PRICE_USD not yet populated) — callers must
        treat that as "can't safely size or check gates", never as zero.

        2026-09-24 — found live, right after deploying an unrelated fix and
        restarting: the very first read after startup failed (SOL_PRICE_USD
        genuinely hadn't been populated by the heartbeat yet), and that
        None got cached for the full TTL — turning one transient,
        one-second startup hiccup into ~10 seconds of the bot reporting
        "equity unknown" and refusing to enter anything, long after the
        real data was already available (confirmed live: /confluence/status,
        which fetches fresh every request, was already returning a correct
        equity the whole time this worker sat on the stale cached None).
        Only a SUCCESSFUL read is worth caching — a failure should be
        retried on the very next ~1s cycle, not stuck for the full TTL.
        """
        now = time.monotonic()
        if self._equity_cache is not None:
            cached_at, cached_value = self._equity_cache
            if now - cached_at < _EQUITY_CACHE_TTL_S:
                return cached_value
        sol_balance = await self._execution.get_wallet_balance_sol()
        equity = compute_equity_usd(sol_balance, Decimal(str(settings.SOL_PRICE_USD)))
        if equity is not None:
            self._equity_cache = (now, equity)
        return equity

    async def _refresh_halt_notifications(
        self, equity: Decimal | None, today_pnl: Decimal, all_time_pnl: Decimal = Decimal("0"),
    ) -> None:
        """
        Edge-triggered halt-state notifications (2026-09-24) — deliberately
        called every cycle from _cycle() UNCONDITIONALLY, not just when
        CONFLUENCE_LIVE_ENABLED / from inside _safe_to_enter(). A user who
        has manually paused trading (the dashboard's PAUSE button) still
        needs to know the moment a deposit clears the dust floor — that's
        informational, not an entry decision, and must not depend on
        whether entries happen to be gated off right now. Found the hard
        way: a deposit landed while paused and produced zero notification,
        because this used to live entirely inside _safe_to_enter(), which
        _cycle() only calls when CONFLUENCE_LIVE_ENABLED is True.
        """
        if equity is None:
            return

        first_check = not self._halt_state_initialized
        self._halt_state_initialized = True

        perm_halted = is_permanently_halted(equity, all_time_pnl)
        if perm_halted != self._last_permanently_halted:
            self._last_permanently_halted = perm_halted
            if perm_halted:
                max_loss_hit = all_time_pnl <= -Decimal(str(settings.CONFLUENCE_LIVE_MAX_LOSS_USD))
                reason = (
                    f"realized losses reached the ${settings.CONFLUENCE_LIVE_MAX_LOSS_USD:.2f} test-phase limit "
                    f"(all-time: ${all_time_pnl:.2f})" if max_loss_hit else
                    f"wallet balance ${equity:.2f} is below the ${settings.CONFLUENCE_LIVE_MIN_TRADEABLE_USD:.2f} minimum"
                )
                await self._notify("critical", "permanently_halted",
                                    f"Trading halted — {reason}. A human needs to re-enable it to resume.")
            elif not first_check:
                # Only a REAL recovery observed during this process's own
                # runtime is worth announcing — not the first read after a
                # restart merely rediscovering the same already-fine state.
                armed_note = "trading is ARMED — may enter a position on the next signal" if settings.CONFLUENCE_LIVE_ENABLED \
                    else "trading is currently PAUSED — resume it from the dashboard when you're ready"
                await self._notify("info", "wallet_funded",
                                    f"Wallet funded — ${equity:.2f} available ({armed_note}).")
        if perm_halted:
            log.warning("confluence_live.total_capital_exhausted", equity_usd=str(equity), all_time_pnl=str(all_time_pnl))
            return

        day_halted = is_daily_halted(equity, today_pnl)
        if day_halted != self._last_daily_halted:
            self._last_daily_halted = day_halted
            if day_halted:
                await self._notify("warning", "daily_loss_limit_hit",
                                    f"Daily loss limit reached (today: ${today_pnl:.2f}) — "
                                    "paused until the next UTC day.")
            elif not first_check:
                await self._notify("info", "daily_loss_limit_cleared",
                                    "Daily loss limit no longer in effect.")
        if day_halted:
            log.warning("confluence_live.daily_loss_limit_hit", today_pnl=str(today_pnl), equity_usd=str(equity))

    async def _maybe_sweep_rent(self) -> None:
        """
        See _RENT_SWEEP_INTERVAL_S's comment — periodic housekeeping, runs
        regardless of CONFLUENCE_LIVE_ENABLED (same reasoning as halt
        notifications: reclaiming real stranded money isn't an entry
        decision, a paused user still benefits from it). Never lets a
        sweep failure break the trading cycle that triggered it.
        """
        now_mono = time.monotonic()
        if self._last_rent_sweep is not None and (now_mono - self._last_rent_sweep) < _RENT_SWEEP_INTERVAL_S:
            return
        self._last_rent_sweep = now_mono
        try:
            reclaimed = await self._execution.sweep_dead_token_accounts()
        except Exception as exc:
            log.warning("confluence_live.rent_sweep_failed", error_type=type(exc).__name__, error=str(exc))
            return
        if reclaimed:
            log.info("confluence_live.rent_swept", count=len(reclaimed), tx_signatures=reclaimed)
            await self._notify(
                "info", "rent_swept",
                f"Reclaimed rent from {len(reclaimed)} dead token account"
                f"{'s' if len(reclaimed) != 1 else ''} during a routine sweep.",
            )

    async def _safe_to_enter(self) -> bool:
        equity = await self._get_equity_usd()
        if equity is None:
            log.warning("confluence_live.equity_unknown", reason="wallet balance or SOL price unavailable")
            return False

        all_time_pnl = await self._load_all_time_realized_pnl()
        if is_permanently_halted(equity, all_time_pnl):
            return False

        today_pnl = await self._load_today_realized_pnl()
        if is_daily_halted(equity, today_pnl):
            return False

        if await self._open_trade_count() >= settings.CONFLUENCE_LIVE_MAX_CONCURRENT:
            return False

        return True

    async def _compute_position_usd(self) -> Decimal:
        """
        Exposure-percentage sizing against LIVE wallet equity (2026-09-24) —
        see engine/live_equity.py's compute_position_usd() for the actual
        formula (equity * CONFLUENCE_LIVE_EXPOSURE_PCT / CONFLUENCE_LIVE_MAX_CONCURRENT,
        capped at CONFLUENCE_LIVE_MAX_POSITION_USD). Deliberately not
        restating the current percentages here — they've already changed
        twice today (config/settings.py is the source of truth); read the
        settings file directly rather than trusting a number in this comment.
        Returns 0 if equity is currently unknown (RPC hiccup) rather than
        raising — _maybe_enter() already treats position_usd <= 0 as
        "nothing to do this cycle".
        """
        equity = await self._get_equity_usd()
        if equity is None:
            return Decimal("0")
        return compute_position_usd(equity)

    # ── entry ────────────────────────────────────────────────────────────

    async def _maybe_enter(self) -> None:
        if not await self._safe_to_enter():
            return

        async with get_session() as session:
            result = await session.execute(
                select(
                    MomentumSignalEvent.token_id, MomentumSignalEvent.n_rules_cofiring,
                    MomentumSignalEvent.triggered_at, MomentumSignalEvent.buy_pressure,
                )
                .where(
                    MomentumSignalEvent.experiment_version == SOURCE_EXPERIMENT_VERSION,
                    MomentumSignalEvent.n_rules_cofiring >= MIN_RULES_COFIRING,
                    MomentumSignalEvent.triggered_at >= self._started_at,
                )
                .order_by(MomentumSignalEvent.triggered_at.asc())
            )
            candidates = result.all()
            if not candidates:
                return

            existing = await session.execute(select(ConfluenceLiveTrade.token_id))
            already_traded = set(existing.scalars().all())

            chosen = None
            for token_id, n_cofiring, triggered_at, buy_pressure in candidates:
                if token_id in already_traded:
                    continue

                # Entry-quality filter (2026-09-24) — see workers/entry_filters.py
                # for the full data: WASH_TRADING-rejected tokens are 56% of
                # all confluence_entry_v1 trades and the single worst-
                # performing population (31.3% rug rate, net-negative even
                # capped). Recorded as a permanent 'wash_skipped' row
                # so this candidate is never re-evaluated on a later cycle —
                # already_traded reflects any row that exists, regardless of
                # status.
                if await is_wash_trading_rejected(session, token_id, triggered_at):
                    session.add(ConfluenceLiveTrade(
                        token_id=token_id, n_rules_cofiring=n_cofiring, status="wash_skipped",
                        entry_time=datetime.now(timezone.utc), entry_price=Decimal("0"),
                        position_usd=Decimal("0"),
                        error_detail="Entry-quality filter: token's most recent TIER1 evaluation before this signal was a WASH_TRADING rejection.",
                    ))
                    already_traded.add(token_id)
                    log.info("confluence_live.entry_wash_skipped", token_id=str(token_id))
                    continue

                # Liquidity-ceiling filter (2026-09-24) — see
                # workers/entry_filters.py for the full data: liquidity <
                # $30k at signal time is a much stronger, cleaner
                # predictor than WASH_TRADING alone (6.5% vs 36.7% rug
                # rate on an evenly-split n=262 dataset), and stays
                # predictive even inside the WASH_TRADING population, so
                # it's checked independently on top of it.
                if await is_liquidity_too_high(session, token_id, triggered_at):
                    session.add(ConfluenceLiveTrade(
                        token_id=token_id, n_rules_cofiring=n_cofiring, status="high_liq_skip",
                        entry_time=datetime.now(timezone.utc), entry_price=Decimal("0"),
                        position_usd=Decimal("0"),
                        error_detail="Entry-quality filter: token's most recent TIER1 evaluation before this signal reported liquidity >= the $30k ceiling.",
                    ))
                    already_traded.add(token_id)
                    log.info("confluence_live.entry_high_liq_skipped", token_id=str(token_id))
                    continue

                # Buy-pressure floor (2026-09-24) — see workers/entry_filters.py
                # for the full data: within the liquidity+wash-trading "good"
                # bucket, buy_pressure >= 0.97 cut the HARD_FLOOR rate from
                # ~34-44% to 11.7% and lifted win rate to 86.4% — the
                # cleanest, most monotonic discriminator found in the whole
                # analysis. Pure/synchronous check, no extra query needed.
                if is_buy_pressure_too_low(buy_pressure):
                    session.add(ConfluenceLiveTrade(
                        token_id=token_id, n_rules_cofiring=n_cofiring, status="low_bp_skip",
                        entry_time=datetime.now(timezone.utc), entry_price=Decimal("0"),
                        position_usd=Decimal("0"),
                        error_detail="Entry-quality filter: buy_pressure at signal time was below the 0.97 floor.",
                    ))
                    already_traded.add(token_id)
                    log.info("confluence_live.entry_low_bp_skipped", token_id=str(token_id))
                    continue

                chosen = (token_id, n_cofiring)
                break
            if chosen is None:
                return

            token_id, n_cofiring = chosen
            token_result = await session.execute(select(Token).where(Token.id == token_id))
            token = token_result.scalar_one_or_none()
            if token is None:
                return

        position_usd = await self._compute_position_usd()
        if position_usd <= 0:
            log.warning("confluence_live.entry_failed", token_id=str(token_id), reason="computed position_usd <= 0")
            return
        sol_price = Decimal(str(settings.SOL_PRICE_USD))
        if sol_price <= 0:
            log.warning("confluence_live.entry_failed", token_id=str(token_id), reason="no SOL price available")
            return

        log.info("confluence_live.entry_attempted", token_id=str(token_id), symbol=token.symbol,
                  position_usd=str(position_usd))
        result = await self._execution.buy(token.mint_address, position_usd, sol_price)

        async with get_session() as session:
            if not result.success:
                session.add(ConfluenceLiveTrade(
                    token_id=token_id, n_rules_cofiring=n_cofiring, status="buy_failed",
                    entry_time=datetime.now(timezone.utc), entry_price=Decimal("0"),
                    position_usd=position_usd, error_detail=f"{result.error_type}: {result.error_detail}",
                ))
                log.error("confluence_live.entry_failed", token_id=str(token_id), symbol=token.symbol,
                          error_type=result.error_type, error_detail=result.error_detail)
                await self._notify("warning", "entry_failed",
                                    f"Buy failed for {token.symbol}: {result.error_type} — {result.error_detail}")
                return

            # engine/execution.py's buy() now always sets actual_price on
            # success (real post-trade balance, or a documented fallback) —
            # never fall back to position_usd/sol_price here again: that's
            # a plain SOL amount, not a per-token price, and was the same
            # class of unit bug this session found and fixed live. Treat a
            # still-missing actual_price as the execution layer's own bug,
            # not something to paper over with more wrong math.
            entry_price = result.actual_price
            entry_price_unknown = entry_price is None
            if entry_price_unknown:
                log.error("confluence_live.entry_price_missing", token_id=str(token_id), symbol=token.symbol,
                          reason="execution.buy() reported success with no actual_price — this should be unreachable")
                # 0, not a guess — _check_exit() already treats entry_price<=0
                # as "can't evaluate an exit", which is the safe behavior
                # here: the position still gets recorded and monitored, it
                # just won't get an automated stop/target until someone
                # looks at it, rather than risking a wrong exit computed
                # from a made-up price.
                entry_price = Decimal("0")
            row = ConfluenceLiveTrade(
                token_id=token_id, n_rules_cofiring=n_cofiring, status="open",
                entry_time=datetime.now(timezone.utc), entry_price=entry_price,
                entry_sol_lamports=int(float(position_usd / sol_price) * 1e9),
                entry_token_lamports=int(result.actual_amount) if result.actual_amount else None,
                entry_tx_signature=result.tx_signature, position_usd=position_usd,
            )
            session.add(row)
            await session.flush()  # populate row.id for the notification below
            trade_id = row.id
        log.info("confluence_live.entry_filled", token_id=str(token_id), symbol=token.symbol,
                  tx_signature=result.tx_signature, entry_price=str(entry_price))
        if entry_price_unknown:
            await self._notify("critical", "entry_price_missing",
                                f"Bought {token.symbol} (${position_usd:.2f}) but couldn't determine a real entry "
                                "price — it won't get an automated exit until this is looked at manually.",
                                trade_id=trade_id)
        else:
            await self._notify("info", "entry_filled",
                                f"Bought {token.symbol} — ${position_usd:.2f} at ${entry_price:.8f}",
                                trade_id=trade_id)

    # ── exit — layered stop (2026-09-23) ────────────────────────────────

    def _check_exit(
        self, entry_price: Decimal, entry_time: datetime, current_price: Decimal, now: datetime,
        current_floor: Decimal, current_hwm: Decimal,
    ) -> tuple[str | None, Decimal, Decimal]:
        """
        Returns (exit_reason_or_None, new_floor, new_hwm).

        A velocity breaker and settings.HARD_FLOOR_PCT still catch a
        violent single-tick crash or an emergency below the normal stop.
        The everyday stop/take-profit split is now
        engine/trailing_stop.py's staircase instead of a fixed
        settings.STOP_LOSS_PCT / settings.TAKE_PROFIT_PCT pair: it starts
        at the same -6% distance below entry, ratchets up in 10% steps as
        price makes new highs (breakeven once up 10%, more locked each
        further 10%), and never force-sells a winner just for reaching
        the old +30% mark — it keeps trailing instead.
        """
        if entry_price is None or entry_price <= Decimal("0"):
            return None, current_floor, current_hwm

        pnl_pct = (current_price - entry_price) / entry_price
        if pnl_pct <= VELOCITY_BREAKER_PCT:
            return "HARD_FLOOR", current_floor, max(current_hwm, current_price)
        hard_floor = Decimal(str(settings.HARD_FLOOR_PCT))
        if pnl_pct <= hard_floor:
            return "HARD_FLOOR", current_floor, max(current_hwm, current_price)

        new_floor, new_hwm, should_close = update_trailing_stop(
            entry_price, current_price, current_floor, current_hwm,
        )
        if should_close:
            reason = "STOP_LOSS" if new_floor <= initial_floor(entry_price) else "TRAILING_STOP"
            return reason, new_floor, new_hwm

        hold_seconds = (now - entry_time).total_seconds()
        if hold_seconds >= settings.max_hold_seconds:
            return "TIME_EXIT", new_floor, new_hwm
        return None, new_floor, new_hwm

    async def _check_liquidity_guard(
        self, trade: dict, current_price: Decimal | None = None,
    ) -> tuple[str | None, Decimal | None]:
        """
        Real-executable-liquidity backstop — see this module's constants
        section and ExecutionEngine.get_sell_quote()'s docstring for the
        real incident that motivated this (BLK: +19.5% on a stale
        snapshot, -76.5% on a real quote for the same size). The normal
        exit logic in _check_exit() only ever looks at the same
        DexScreener snapshot price the position is being monitored
        against, so it structurally cannot see this failure mode.

        Returns ("LIQUIDITY_GUARD", real_price) if a real Jupiter quote
        for the full position size shows either >=35% price impact or an
        already-worse-than-any-normal-stop real P&L. Returns (None, None)
        if the check isn't due yet for this trade, the quote failed (never
        treat a failed check as "safe"), or neither threshold is crossed.

        Side effect (2026-09-25): whenever a real check actually runs
        (i.e. isn't skipped by the throttle above), persists the implied
        real_price/real_pnl_pct/real_price_checked_at onto the trade row
        regardless of the crisis outcome — this is what lets the
        dashboard show a verified, actually-tradeable price instead of the
        raw DexScreener snapshot, which can sit still ("frozen") on thin
        liquidity even when nothing is wrong. See models.orm's
        ConfluenceLiveTrade.real_price docstring.

        Adaptive interval (2026-09-25) — real incident: Gavel's DexScreener
        snapshot climbed steadily to +40% while its REAL price had already
        collapsed to -33% underneath, and the 60s-throttled guard only
        caught it once, at the very end of that window — the snapshot
        never showed anything wrong even once. A position showing a real
        unrealized gain is exactly the one worth protecting fastest (there
        is real profit that a sudden dump can take back), so this checks
        every _LIQUIDITY_CHECK_INTERVAL_FAST_S instead of
        _LIQUIDITY_CHECK_INTERVAL_S once the snapshot-based unrealized gain
        crosses _LIQUIDITY_CHECK_FAST_THRESHOLD_PCT. Deliberately NOT
        applied to every position — that was tried once already (~20s for
        everything) and it saturated Jupiter's shared rate limit; this
        only tightens the check for the (usually few) positions actually
        worth protecting.
        """
        trade_id = trade["id"]
        now_mono = time.monotonic()

        interval = _LIQUIDITY_CHECK_INTERVAL_S
        if current_price is not None and trade.get("entry_price"):
            unrealized_pct = (current_price - trade["entry_price"]) / trade["entry_price"]
            if unrealized_pct >= _LIQUIDITY_CHECK_FAST_THRESHOLD_PCT:
                interval = _LIQUIDITY_CHECK_INTERVAL_FAST_S

        last_check = self._last_liquidity_check.get(trade_id)
        if last_check is not None and (now_mono - last_check) < interval:
            return None, None
        self._last_liquidity_check[trade_id] = now_mono

        if not trade.get("entry_token_lamports"):
            return None, None

        quote = await self._execution.get_sell_quote(trade["mint"], trade["entry_token_lamports"])
        if quote is None:
            return None, None

        sol_price = Decimal(str(settings.SOL_PRICE_USD))
        position_usd = trade.get("position_usd")
        if sol_price <= 0 or not position_usd:
            return None, None

        real_proceeds_usd = (Decimal(quote["out_lamports"]) / Decimal("1e9")) * sol_price
        real_pnl_pct = (real_proceeds_usd / position_usd) - 1
        # Decimals unknown here without a balance lookup — nearly all
        # pump.fun-originated SPL tokens use 6, same documented
        # approximation as execution.py's own post-trade-balance fallback.
        implied_price = real_proceeds_usd / (Decimal(trade["entry_token_lamports"]) / Decimal(10 ** 6))

        async with get_session() as session:
            row_result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade_id))
            row = row_result.scalar_one_or_none()
            if row is not None and row.status in ("open", "unsellable"):
                row.real_price = implied_price
                row.real_pnl_pct = real_pnl_pct * 100
                row.real_price_checked_at = datetime.now(timezone.utc)

        if quote["price_impact_pct"] >= _LIQUIDITY_CRISIS_IMPACT_PCT or real_pnl_pct <= _LIQUIDITY_CRISIS_PNL_FLOOR_PCT:
            # Informational only beyond this point: the real sell that
            # follows sizes itself from entry_token_lamports directly,
            # never from implied_price.
            log.warning("confluence_live.liquidity_crisis_detected", trade_id=str(trade_id),
                        mint=trade["mint"], price_impact_pct=str(quote["price_impact_pct"]),
                        real_pnl_pct=str(real_pnl_pct))
            return "LIQUIDITY_GUARD", implied_price

        return None, None

    async def _maybe_exit(self, trade: dict, current_price: Decimal, now: datetime) -> None:
        current_floor = trade.get("trailing_stop_floor") or initial_floor(trade["entry_price"])
        current_hwm = trade.get("high_watermark_price") or trade["entry_price"]

        guard_reason, guard_price = await self._check_liquidity_guard(trade, current_price)
        if guard_reason is not None:
            # Overrides the snapshot-based decision entirely — the whole
            # point is that the snapshot price is the thing that's wrong
            # here, so nothing downstream should keep using it. Floor/hwm
            # pass through unchanged; the position is about to close.
            reason, current_price, new_floor, new_hwm = guard_reason, guard_price, current_floor, current_hwm
        else:
            reason, new_floor, new_hwm = self._check_exit(
                trade["entry_price"], trade["entry_time"], current_price, now, current_floor, current_hwm,
            )

        if reason is None:
            async with get_session() as session:
                row_result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade["id"]))
                row = row_result.scalar_one_or_none()
                if row is not None and row.status in ("open", "unsellable"):
                    row.trailing_stop_floor = new_floor
                    row.high_watermark_price = new_hwm
            return

        if not trade.get("entry_token_lamports"):
            log.error("confluence_live.exit_failed", trade_id=str(trade["id"]),
                      reason="no entry_token_lamports recorded — cannot size sell")
            if trade["id"] not in self._notified_stuck_trades:
                self._notified_stuck_trades.add(trade["id"])
                await self._notify("critical", "exit_failed_unsizeable",
                                    f"{trade.get('symbol') or trade['mint'][:8]} cannot be sold — no recorded "
                                    "token amount. MANUAL INTERVENTION NEEDED.", trade_id=trade["id"])
            return

        trade_id_str = str(trade["id"])

        # See _SELL_BACKOFF_AFTER_S's comment — a sell that's been failing
        # for a while gets retried at most once every _SELL_BACKOFF_INTERVAL_S
        # instead of every cycle, so a permanently stuck position can't
        # starve the shared Jupiter rate limit that real, fillable trades
        # also depend on. `.get()` (never `.setdefault()`) — this must not
        # be the thing that starts the failure-duration clock; that's
        # _sell_failing_since's job, set only after an actual failed attempt.
        # Keyed by trade["id"] (the raw UUID), matching _sell_failing_since's
        # own key — NOT trade_id_str, which is a separate string form used
        # only for sell_coordination/logging below.
        failing_since = self._sell_failing_since.get(trade["id"])
        if failing_since is not None and (time.monotonic() - failing_since) >= _SELL_BACKOFF_AFTER_S:
            last_attempt = self._last_sell_attempt.get(trade["id"])
            if last_attempt is not None and (time.monotonic() - last_attempt) < _SELL_BACKOFF_INTERVAL_S:
                return

        # Real incident, 2026-09-25 (SEND): a sell can succeed on-chain but
        # never get recorded if this process restarts mid-confirmation-poll
        # — the recovered process then keeps retrying a swap for tokens
        # that are ALREADY GONE, forever, producing an on-chain rejection
        # indistinguishable from a genuinely drained pool (confirmed by
        # forensics days later: the real sell landed 5 seconds after entry,
        # the service happened to restart mid-poll, and every retry since
        # then failed only because there was nothing left to sell). Once a
        # sell has already failed at least once for this trade, verify the
        # REAL on-chain balance before burning another attempt — a
        # CONFIRMED zero (never a lookup failure, which get_token_balance_raw
        # deliberately returns as None, not zero) means the position is
        # already resolved on-chain and no further attempt can ever
        # succeed. Flags it for reconciliation rather than fabricating a
        # pnl_usd this process cannot know without the kind of forensic
        # on-chain lookup that resolved the real SEND incident.
        if failing_since is not None:
            real_balance = await self._execution.get_token_balance_raw(trade["mint"])
            if real_balance is not None and real_balance[0] == 0:
                log.error("confluence_live.balance_already_zero", trade_id=trade_id_str,
                          symbol=trade.get("symbol"), mint=trade["mint"])
                async with get_session() as session:
                    row_result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade["id"]))
                    row = row_result.scalar_one_or_none()
                    if row is not None and row.status in ("open", "unsellable"):
                        row.status = "balance_zero"
                        row.error_detail = (
                            "Real on-chain balance confirmed zero — this position was already sold "
                            "for real at some point (most likely a sell that succeeded on-chain right "
                            "as this service restarted mid-confirmation-poll), but the exact proceeds "
                            "are unknown without a manual on-chain lookup of the wallet's transaction "
                            "history around entry_time. No further automatic sell will be attempted."
                        )
                await self._notify(
                    "critical", "balance_already_zero",
                    f"{trade.get('symbol') or trade['mint'][:8]} already shows a real on-chain balance "
                    "of zero — it was likely already sold successfully but never recorded (see the "
                    "SEND incident, 2026-09-25). No further automatic sell will be attempted; the real "
                    "proceeds need a manual on-chain lookup to close out the ledger accurately.",
                    trade_id=trade["id"],
                )
                self._sell_failing_since.pop(trade["id"], None)
                self._last_sell_attempt.pop(trade["id"], None)
                return

        # See engine/sell_coordination.py — a manual dashboard close can be
        # mid-flight for this exact trade right now. Skip this cycle's sell
        # entirely rather than racing it; the next cycle (~1s later) will
        # either see the position already closed (manual close won) or
        # retry normally (manual close failed/wasn't for this trade).
        if not try_start_sell(trade_id_str):
            log.info("confluence_live.exit_skipped_concurrent_sell", trade_id=trade_id_str)
            return
        self._last_sell_attempt[trade["id"]] = time.monotonic()
        try:
            log.info("confluence_live.exit_attempted", trade_id=trade_id_str, reason=reason)
            result = await self._execution.sell(
                trade["mint"], Decimal(str(trade["entry_token_lamports"])),
                trade_id=trade_id_str, symbol=trade.get("symbol"),
            )
        finally:
            finish_sell(trade_id_str)

        if not result.success:
            log.error("confluence_live.exit_failed", trade_id=str(trade["id"]),
                      error_type=result.error_type, error_detail=result.error_detail)
            # execution.py's sell() already exhausts _MAX_SELL_RETRIES
            # internally before ever returning failure — this branch IS the
            # critical, all-retries-exhausted case every time, not a partial
            # failure. Notify once per trade (see _notified_stuck_trades).
            if trade["id"] not in self._notified_stuck_trades:
                self._notified_stuck_trades.add(trade["id"])
                await self._notify("critical", "exit_failed_critical",
                                    f"{trade.get('symbol') or trade['mint'][:8]} sell failed after all retries "
                                    f"({result.error_type}: {result.error_detail}). Position stays open and will "
                                    "keep retrying — MANUAL INTERVENTION may be needed.", trade_id=trade["id"])

            first_failed_at = self._sell_failing_since.setdefault(trade["id"], time.monotonic())
            failing_for_s = time.monotonic() - first_failed_at

            async with get_session() as session:
                row_result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade["id"]))
                row = row_result.scalar_one_or_none()
                if row is None or row.status not in ("open", "unsellable"):
                    return
                row.trailing_stop_floor = new_floor
                row.high_watermark_price = new_hwm
                if row.status == "open" and failing_for_s >= _UNSELLABLE_AFTER_S:
                    # See _UNSELLABLE_AFTER_S's comment — this frees the
                    # concurrency slot (_open_trade_count() only counts
                    # status=='open') without ever fabricating a close; the
                    # position keeps being priced and keeps getting real
                    # sell attempts every cycle via _load_open_trades().
                    row.status = "unsellable"
                    log.error("confluence_live.marked_unsellable", trade_id=str(trade["id"]),
                              symbol=trade.get("symbol"), failing_for_s=round(failing_for_s))
                    await self._notify(
                        "critical", "marked_unsellable",
                        f"{trade.get('symbol') or trade['mint'][:8]} could not be sold for "
                        f"{round(failing_for_s / 60)}+ minutes — freed its trading slot for a new "
                        "position, but the real position stays open and will keep being retried. "
                        "Real money is stuck until either a sell succeeds or you close it manually.",
                        trade_id=trade["id"],
                    )
            return  # stays 'open' or 'unsellable' — retried on the next normal cycle either way

        sol_price = Decimal(str(settings.SOL_PRICE_USD))
        exit_proceeds_usd = (Decimal(str(result.actual_amount)) / Decimal("1e9")) * sol_price if result.actual_amount else None
        pnl_usd = (exit_proceeds_usd - trade["position_usd"]) if exit_proceeds_usd is not None else None

        async with get_session() as session:
            row_result = await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade["id"]))
            row = row_result.scalar_one_or_none()
            # 'unsellable' included (2026-09-24) — a position downgraded
            # there after a long run of failed sells must still be able to
            # close for real the moment a sell actually succeeds (the pool
            # recovering, or a human intervening some other way); this is
            # the ONLY path 'unsellable' ever exits through.
            if row is None or row.status not in ("open", "unsellable"):
                return
            was_unsellable = row.status == "unsellable"
            row.status = "closed"
            row.exit_time = now
            row.exit_price = current_price
            row.exit_reason = reason
            row.exit_sol_lamports = int(result.actual_amount) if result.actual_amount else None
            row.exit_tx_signature = result.tx_signature
            row.pnl_usd = pnl_usd
            row.trailing_stop_floor = new_floor
            row.high_watermark_price = new_hwm
        self._last_accepted_price.pop(trade["id"], None)
        self._pending_tick.pop(trade["id"], None)
        self._notified_stuck_trades.discard(trade["id"])
        self._sell_failing_since.pop(trade["id"], None)
        self._last_sell_attempt.pop(trade["id"], None)
        if was_unsellable:
            log.info("confluence_live.unsellable_recovered", trade_id=str(trade["id"]), symbol=trade.get("symbol"))
        pnl_pct = (pnl_usd / trade["position_usd"] * 100) if pnl_usd is not None and trade["position_usd"] else None
        # LIQUIDITY_GUARD is always critical regardless of pnl sign — it
        # means the snapshot price this position was being monitored
        # against had already diverged from reality, which is worth
        # flagging distinctly from a routine, expected stop.
        notify_level = "critical" if reason == "LIQUIDITY_GUARD" else ("info" if (pnl_usd or 0) >= 0 else "warning")
        await self._notify(
            notify_level, "exit_filled",
            f"{trade.get('symbol') or trade['mint'][:8]} closed ({reason}): "
            f"{f'{pnl_pct:+.1f}%' if pnl_pct is not None else 'pnl unknown'}"
            f"{f', ${pnl_usd:+.2f}' if pnl_usd is not None else ''}",
            trade_id=trade["id"],
        )
        log.info("confluence_live.exit_filled", trade_id=str(trade["id"]), exit_reason=reason,
                  tx_signature=result.tx_signature, pnl_usd=str(pnl_usd) if pnl_usd is not None else None)

    # ── loading / observation ────────────────────────────────────────────

    async def _load_open_trades(self) -> dict[str, dict]:
        """
        'unsellable' included alongside 'open' (2026-09-24) — a position
        downgraded to 'unsellable' after a long run of failed real sells
        (see _UNSELLABLE_AFTER_S) must keep being priced and keep getting
        real sell attempts every cycle exactly as before; only
        _open_trade_count()'s concurrency gate treats the two statuses
        differently (that one still filters status=='open' only, which is
        the entire point — it's what actually frees the slot).
        """
        async with get_session() as session:
            result = await session.execute(
                select(
                    Token.mint_address, Token.symbol, ConfluenceLiveTrade.id,
                    ConfluenceLiveTrade.entry_price, ConfluenceLiveTrade.entry_time,
                    ConfluenceLiveTrade.entry_token_lamports, ConfluenceLiveTrade.position_usd,
                    ConfluenceLiveTrade.trailing_stop_floor, ConfluenceLiveTrade.high_watermark_price,
                )
                .join(Token, Token.id == ConfluenceLiveTrade.token_id)
                .where(ConfluenceLiveTrade.status.in_(["open", "unsellable"]))
            )
            return {
                mint: dict(id=tid, mint=mint, symbol=symbol, entry_price=ep, entry_time=et,
                           entry_token_lamports=etl, position_usd=pu,
                           trailing_stop_floor=floor, high_watermark_price=hwm)
                for mint, symbol, tid, ep, et, etl, pu, floor, hwm in result.all()
            }

    async def _record_observation(self, trade_id, price: Decimal, now: datetime) -> None:
        async with get_session() as session:
            session.add(ConfluenceLiveObservation(trade_id=trade_id, observed_at=now, price_usd=price))

    # ── bad-tick guard — deliberately duplicated, see module docstring ────

    def _accept_price(self, trade: dict, new_price: Decimal) -> Decimal | None:
        trade_id = trade["id"]
        last_price = self._last_accepted_price.get(trade_id, trade["entry_price"])
        if last_price is None or last_price <= 0:
            self._last_accepted_price[trade_id] = new_price
            return new_price

        ratio = new_price / last_price
        plausible = (Decimal("1") / Decimal(str(IMPLAUSIBLE_TICK_RATIO))) <= ratio <= Decimal(str(IMPLAUSIBLE_TICK_RATIO))
        pending = self._pending_tick.get(trade_id)

        if plausible:
            self._pending_tick.pop(trade_id, None)
            self._last_accepted_price[trade_id] = new_price
            return new_price

        if pending is not None:
            confirm_ratio = new_price / pending if pending > 0 else None
            confirmed = confirm_ratio is not None and (
                (1 - CONFIRMATION_TOLERANCE) <= confirm_ratio <= (1 + CONFIRMATION_TOLERANCE)
            )
            if confirmed:
                log.warning("confluence_live.implausible_tick_confirmed", trade_id=str(trade_id), price=str(new_price))
                self._pending_tick.pop(trade_id, None)
                self._last_accepted_price[trade_id] = new_price
                return new_price
            self._pending_tick.pop(trade_id, None)

        log.warning("confluence_live.implausible_tick_held", trade_id=str(trade_id),
                    last_price=str(last_price), new_price=str(new_price))
        self._pending_tick[trade_id] = new_price
        return None

    # ── DexScreener fetch — same source/parsing as TradeMonitorWorker ─────

    async def _fetch(self, client: httpx.AsyncClient, mints: list[str]) -> dict[str, dict]:
        resp = await self._http.submit(client.get, f"{_DEXSCREENER_BASE}/{','.join(mints)}")
        if resp.status_code == 429:
            log.warning("confluence_live.rate_limited")
            return {}
        resp.raise_for_status()
        data = resp.json()
        pairs_by_mint: dict[str, list[dict]] = {}
        for pair in data.get("pairs") or []:
            mint = (pair.get("baseToken") or {}).get("address")
            if mint in mints:
                pairs_by_mint.setdefault(mint, []).append(pair)
        result: dict[str, dict] = {}
        for mint, pairs in pairs_by_mint.items():
            pair = _best_pair(pairs)
            if not pair:
                continue
            price = pair.get("priceUsd")
            if price is None:
                continue
            result[mint] = {"price_usd": Decimal(str(price))}
        return result
