"""
workers/sampling_worker.py
==========================
Sampling worker — SolanaTracker POST /tokens/multi

Fires every SAMPLE_INTERVAL_SECONDS (default 60s). For each WATCHING/
OBSERVING token, fetches a fresh snapshot from SolanaTracker via a single
batch call, writes a TokenSnapshot row, and hands the snapshot to
workers/momentum_signal.py — THE real entry trigger for both live and
shadow trading (2026-09-29: this used to describe "the scoring worker," a
system removed 2026-09-23; see workers/shared_snapshot.py's
write_snapshot_and_notify() docstring for the current, accurate chain from
here to a real trade).

Request budget: 1 call per sampling cycle regardless of token count (batch),
minus whatever DiscoveryWorker already covered — see below.

Discovery/sampling sync (2026-09-21)
-------------------------------------
DiscoveryWorker's own GET /tokens/multi/graduated poll (every 60s) already
carries full snapshot-quality data for any WATCHING/OBSERVING token still
inside its 30-minute lookback window. Rather than sampling spending its own
API call 30s later for a mint discovery just refreshed, this worker skips
any mint workers.shared_snapshot.is_fresh() says was covered in the last
FRESHNESS_WINDOW_SECONDS (~55s). Age-out logic still runs unconditionally
for every WATCHING/OBSERVING token regardless of coverage source — only the
fetch+write is skipped. See workers/shared_snapshot.py for the shared
freshness state and the write path both workers now call.

Logging contract
----------------
info:
  sampling_worker.started / stopped
  sampling_worker.cycle_complete — watched, sampled, covered_by_discovery,
                                   errors, elapsed_ms
  sampling_worker.snapshot       — per-token: symbol, price, liquidity,
                                   buy_pressure, price_change_1m/5m
  sampling_worker.token_gone     — mint no longer in ST response
  sampling_worker.aged_out       — exceeded TIER1_MAX_AGE_MINUTES
debug:
  sampling_worker.no_watching_tokens
errors:
  sampling_worker.fetch_error
  sampling_worker.cycle_error
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal

import httpx
from sqlalchemy import select

from config.logging import get_logger
from config.settings import settings
from database.engine import get_session
from engine.notify import notify as notify_shared
from models.orm import MomentumSignalEvent, Token, TokenStatus, ShadowTrade
from workers import shared_snapshot
from workers.http_queue import get_http_queue
from workers.key_health import ExhaustionWatcher

log = get_logger(__name__)

_ST_BASE      = "https://data.solanatracker.io"
_MULTI_PATH   = "/tokens/multi"
_HTTP_TIMEOUT = httpx.Timeout(timeout=15.0, connect=5.0)
# Confirmed live: POST /tokens/multi rejects anything over this with
# {"error":"Maximum 20 tokens per request"} — a hard API limit, not tunable.
_MAX_TOKENS_PER_REQUEST = 20


def sample_interval_s(age_min: float, signaled: bool, observing: bool) -> int:
    """Minimum seconds between API fetches of one token (0 = every cycle).

    Why: 97% of entry signals fire within 15 min of a token starting to be
    watched and 99.9% within 30 min (2026-09-24..10-02, n=3,800), and a token
    can signal only once (unique token_id+experiment). Sampling a 6-hour-old
    or already-signaled token every cycle spent most of a lifetime-capped
    free key on tokens that almost never produce a trade.
    """
    if not settings.SAMPLE_TIERING_ENABLED or observing:
        return 0
    if signaled:
        return 600
    if age_min < 30:
        return 0
    if age_min < 120:
        return 300
    return 900


def _best_pool(token_data: dict) -> dict | None:
    return next(
        (p for p in token_data.get("pools", []) if p.get("market") == "pumpfun-amm"),
        None,
    )


class SamplingWorker:

    def __init__(
        self,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._shutdown      = shutdown_event
        self._interval      = settings.SAMPLE_INTERVAL_SECONDS
        # "Last covered at" timestamps live in workers.shared_snapshot,
        # shared with DiscoveryWorker — see that module's docstring for why
        # a shared dict is needed once two pollers can both cover the same
        # mint.
        self._key_watcher = ExhaustionWatcher()
        self._last_fetched: dict[str, datetime] = {}

    async def run(self) -> None:
        log.info("sampling_worker.started", interval=self._interval)

        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            while not self._shutdown.is_set():
                try:
                    await self._sample_cycle(client)
                except Exception as exc:
                    log.error("sampling_worker.cycle_error",
                              error=str(exc), exc_info=True)

                try:
                    await asyncio.wait_for(
                        self._shutdown.wait(),
                        timeout=float(self._interval),
                    )
                except asyncio.TimeoutError:
                    pass

        log.info("sampling_worker.stopped")

    async def _sample_cycle(self, client: httpx.AsyncClient) -> None:
        watching = await self._load_watching_tokens()
        if not watching:
            log.debug("sampling_worker.no_watching_tokens")
            return

        t0  = time.monotonic()
        now = datetime.now(timezone.utc)
        mints = [t.mint_address for t in watching]

        # Discovery/sampling sync: any mint discovery's own graduated-feed
        # poll already wrote a snapshot for in the last
        # shared_snapshot.FRESHNESS_WINDOW_SECONDS gets skipped here — no
        # point spending a second API call 30s after discovery already
        # covered it for free. See workers/shared_snapshot.py.
        covered_mints = {m for m in mints if shared_snapshot.is_fresh(m, now)}

        signaled_ids = await self._load_signaled_token_ids()
        deferred: set[str] = set()
        for t in watching:
            if t.mint_address in covered_mints:
                continue
            anchor = t.watch_started_at or t.discovered_at
            age_min = (now - anchor).total_seconds() / 60 if anchor else 0.0
            gap = sample_interval_s(age_min, t.id in signaled_ids, t.status == TokenStatus.OBSERVING)
            last = self._last_fetched.get(t.mint_address)
            if gap and last is not None and (now - last).total_seconds() < gap:
                deferred.add(t.mint_address)
        to_fetch = [m for m in mints if m not in covered_mints and m not in deferred]
        for m in to_fetch:
            self._last_fetched[m] = now
        for m in [m for m in self._last_fetched if m not in set(mints)]:
            del self._last_fetched[m]

        # SolanaTracker's /tokens/multi hard-caps at 20 tokens per request
        # ("Maximum 20 tokens per request") — confirmed live after this
        # limit was silently exceeded for hours. Widening
        # OBSERVE_WINDOW_SECONDS from 90s to 1800s let WATCHING+OBSERVING
        # grow past 20 concurrently, every request 400'd from then on, and
        # — see below — that meant NOTHING could age out either, since the
        # per-token loop (including age-out) only used to run after a
        # successful fetch. Population could only grow, never shrink: a
        # total lockup. Chunking below fixes the request-size cause; a
        # fetch failure on one chunk is isolated to that chunk's tokens and
        # never blocks sampling or age-out for the rest.
        snapshots: dict[str, dict] = {}
        fetch_failed_mints: set[str] = set()
        auth_error_this_cycle = False
        for i in range(0, len(to_fetch), _MAX_TOKENS_PER_REQUEST):
            chunk = to_fetch[i:i + _MAX_TOKENS_PER_REQUEST]
            try:
                snapshots.update(await self._fetch_batch(client, chunk))
            except Exception as exc:
                log.error("sampling_worker.fetch_error", error=str(exc),
                          exc_type=type(exc).__name__, chunk_size=len(chunk))
                fetch_failed_mints.update(chunk)
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (401, 403):
                    auth_error_this_cycle = True

        sampled = errors = covered = 0

        # Shadow-experiment-flagged tokens (see workers/scoring_worker.py's
        # SHADOW_EXPERIMENT_*) get a longer OBSERVING window so a 1-hour
        # outcome horizon is actually reachable — scoped to just this
        # subset rather than raising the default for everyone, per the
        # SolanaTracker 20-tokens-per-request limit noted above. This is a
        # read-only lookup against an analysis-only table; it has no effect
        # on any production decision.
        shadow_token_ids = await self._load_shadow_token_ids()

        for token in watching:
            is_observing = token.status == TokenStatus.OBSERVING

            # Age-out is time-based and deliberately independent of fetch
            # outcome — this must run for every token every cycle no matter
            # what happened above, or a run of fetch failures reproduces
            # the exact lockup this fix closes. OBSERVING tokens get a
            # short, bounded window (OBSERVE_WINDOW_SECONDS) from
            # observation_started_at; WATCHING tokens keep the existing
            # long TIER1_MAX_AGE_MINUTES cutoff from when they started
            # being watched. Different clocks, different meanings: one is
            # "gave up on being tradeable a while ago", the other is
            # "control-group sample window closed". observation_started_at
            # (not discovered_at) anchors the OBSERVING clock — a
            # Scorer-discarded token can have been discovered long before
            # the moment it was discarded, so discovered_at would badly
            # understate how much window is left.
            if is_observing:
                anchor = token.observation_started_at or token.discovered_at
                age_sec = (now - anchor).total_seconds()
                window = (settings.SHADOW_OBSERVE_WINDOW_SECONDS
                          if token.id in shadow_token_ids else settings.OBSERVE_WINDOW_SECONDS)
                if age_sec > window:
                    shared_snapshot.forget(token.mint_address)
                    await self._age_out(token.mint_address, "OBSERVE_WINDOW_ELAPSED")
                    continue
            elif token.watch_started_at:
                age_min = (now - token.watch_started_at).total_seconds() / 60
                if age_min > settings.TIER1_MAX_AGE_MINUTES:
                    shared_snapshot.forget(token.mint_address)
                    await self._age_out(token.mint_address, "WATCHING_TIMEOUT")
                    continue

            if token.mint_address in covered_mints:
                # Discovery's own poll already wrote a fresh snapshot for
                # this mint this cycle — age-out above still ran, but there
                # is nothing left for sampling to fetch or write.
                covered += 1
                continue

            if token.mint_address in deferred:
                continue

            if token.mint_address in fetch_failed_mints:
                # This token's own chunk request errored — distinct from a
                # successful chunk that simply didn't list this mint
                # (handled below as TOKEN_GONE). Retry next cycle; still
                # fully subject to the age-out check above regardless of
                # how many cycles this repeats for.
                errors += 1
                continue

            snap_data = snapshots.get(token.mint_address)
            if snap_data is None:
                # "Gone from the API" means something different depending on
                # which group the token was in — a WATCHING token vanishing
                # is a strong rug signal; an OBSERVING token vanishing is
                # just confirmation the reject was correct. Keep them apart
                # so the derived-outcome analysis isn't looking at a blended
                # bucket of two different events.
                reason = "TOKEN_GONE_DURING_OBSERVATION" if is_observing else "TOKEN_GONE_LIKELY_RUG"
                log.info("sampling_worker.token_gone",
                         mint=token.mint_address, symbol=token.symbol,
                         status=token.status.value, reason=reason)
                shared_snapshot.forget(token.mint_address)
                await self._age_out(token.mint_address, reason)
                errors += 1
                continue

            try:
                await shared_snapshot.write_snapshot_and_notify(
                    token, snap_data, now,
                )
                sampled += 1

                log.info(
                    "sampling_worker.snapshot",
                    mint=token.mint_address,
                    symbol=token.symbol,
                    price_usd=str(snap_data["price_usd"]),
                    liquidity_usd=str(snap_data["liquidity_usd"]),
                    buy_pressure=str(snap_data["buy_pressure"]),
                    price_change_1m=snap_data["price_change_1m"],
                    price_change_5m=snap_data["price_change_5m"],
                )
            except Exception as exc:
                log.error("sampling_worker.snapshot_error",
                          mint=token.mint_address, error=str(exc))
                errors += 1

        elapsed_ms = round((time.monotonic() - t0) * 1000)
        log.info(
            "sampling_worker.cycle_complete",
            watched=len(watching),
            sampled=sampled,
            covered_by_discovery=covered,
            deferred=len(deferred),
            requests=-(-len(to_fetch) // _MAX_TOKENS_PER_REQUEST),
            errors=errors,
            elapsed_ms=elapsed_ms,
        )

        # Key exhaustion alert (2026-10-01) — "bad" means an auth error
        # actually occurred AND nothing got through at all this cycle; a
        # single chunk 401/403 while the rest of the cycle succeeds is not
        # the key being dead. See workers/key_health.py.
        bad_cycle = auth_error_this_cycle and sampled == 0
        transition = self._key_watcher.observe(bad_cycle)
        if transition is not None:
            await self._notify_key_health(transition)

    async def _notify_key_health(self, transition: str) -> None:
        """Never let a notification failure break the sampling cycle that
        triggered it — same defensive convention as every other _notify in
        this codebase."""
        try:
            if transition == "tripped":
                level, event, message = (
                    "critical", "sampling_key_exhausted",
                    f"SOLANA_TRACKER_API_KEY (sampling) has returned 401/403 with zero successful "
                    f"samples for {self._key_watcher.threshold}+ consecutive cycles — this looks like "
                    "the key running out, not a network blip. Replace it via this account's Settings "
                    "page (SolanaTracker keys) with a fresh one.",
                )
            else:
                level, event, message = (
                    "info", "sampling_key_recovered",
                    "SOLANA_TRACKER_API_KEY (sampling) is successfully fetching data again.",
                )
            async with get_session() as session:
                await notify_shared(session, level, event, message)
        except Exception:
            log.error("sampling_worker.key_health_notify_failed", transition=transition, exc_info=True)

    async def _fetch_batch(
        self,
        client: httpx.AsyncClient,
        mints: list[str],
    ) -> dict[str, dict]:
        """POST /tokens/multi → parse each into a normalised snapshot dict.
        Routed through the central rate-limited queue to prevent 429s when
        discovery and sampling workers fire simultaneously.
        """
        resp = await get_http_queue().submit(
            client.post,
            f"{_ST_BASE}{_MULTI_PATH}",
            json={"tokens": mints},
            headers={
                "x-api-key":    settings.SOLANA_TRACKER_API_KEY,
                "Content-Type": "application/json",
            },
        )
        if resp.status_code == 429:
            log.warning("sampling_worker.rate_limited")
            return {}
        resp.raise_for_status()
        data = resp.json()

        result: dict[str, dict] = {}
        for mint, token_data in data.get("tokens", {}).items():
            pool = _best_pool(token_data)
            if not pool:
                continue
            events = token_data.get("events", {})
            txns   = pool.get("txns", {})
            buys   = txns.get("buys", 0) or 0
            sells  = txns.get("sells", 0) or 0
            total  = buys + sells

            # price_change_5m drives vm_trend in the scorer
            price_5m = events.get("5m", {}).get("priceChangePercentage", 0.0) or 0.0

            result[mint] = {
                "price_usd":       Decimal(str(pool["price"]["usd"] or 0)),
                "liquidity_usd":   Decimal(str(pool["liquidity"]["usd"] or 0)),
                "market_cap_usd":  Decimal(str(pool["marketCap"]["usd"] or 0)),
                "volume_usd":      Decimal(str(txns.get("volume") or 0)),
                "buy_pressure":    Decimal(str(buys / max(total, 1))).quantize(Decimal("0.0001")),
                # price_change_5m normalised to 0–2 multiplier range for SnapshotWindow vm_p*
                # +100% → vm = 2.0, 0% → vm = 1.0, -50% → vm = 0.5
                "volume_mult":     Decimal(str(max(0.0, 1.0 + price_5m / 100))).quantize(Decimal("0.0001")),
                "price_change_1m": events.get("1m",  {}).get("priceChangePercentage", 0.0) or 0.0,
                "price_change_5m": price_5m,
                "price_change_15m":events.get("15m", {}).get("priceChangePercentage", 0.0) or 0.0,
            }
        return result

    async def _load_shadow_token_ids(self) -> set:
        """
        Analysis-only lookup: which tokens have a shadow-experiment row.
        Read-only against shadow_trades, never written here, never affects
        anything but which OBSERVE window length a token gets.
        """
        async with get_session() as session:
            result = await session.execute(select(ShadowTrade.token_id))
            return set(result.scalars().all())

    async def _load_watching_tokens(self) -> list[Token]:
        # WATCHING (tradeable candidates) + OBSERVING (Tier1-rejected, still
        # sampled for a bounded window as the control group) both ride the
        # same batch call — this is the whole point of the design: the
        # extra observation doesn't cost a second API call. ENTERED tokens
        # are deliberately excluded: they're monitored by price_tick_worker
        # via WebSocket, and including them here caused zombie loops when a
        # token was stuck in ENTERED with no open trade (fired aged_out
        # 2,060×/session before this exclusion existed).
        async with get_session() as session:
            result = await session.execute(
                select(Token).where(
                    Token.status.in_((TokenStatus.WATCHING, TokenStatus.OBSERVING))
                )
            )
            return list(result.scalars().all())

    async def _load_signaled_token_ids(self) -> set:
        async with get_session() as session:
            rows = await session.execute(select(MomentumSignalEvent.token_id).distinct())
            return {r[0] for r in rows}

    async def _age_out(self, mint: str, reason: str) -> None:
        async with get_session() as session:
            result = await session.execute(
                select(Token).where(Token.mint_address == mint).with_for_update()
            )
            token = result.scalar_one_or_none()
            # Only age out WATCHING/OBSERVING, not ENTERED — an ENTERED
            # token has an open trade and must never be silently rejected.
            if token and token.status in (TokenStatus.WATCHING, TokenStatus.OBSERVING):
                was_observing = token.status == TokenStatus.OBSERVING
                token.status = TokenStatus.REJECTED
                if was_observing:
                    # rejection_reason already holds the ORIGINAL gate
                    # verdict (why Tier1 or the Scorer said no) — record
                    # how the subsequent observation window ended
                    # separately instead of overwriting it. Before this
                    # fix, every OBSERVING token's real rejection reason
                    # (e.g. "WASH_TRADING") was silently replaced with the
                    # generic "OBSERVE_WINDOW_ELAPSED" the moment its
                    # window closed, which threw away exactly the
                    # information the derived-outcome analysis needs.
                    token.observation_exit_reason = reason
                    if token.rejection_reason is None:
                        token.rejection_reason = reason  # defensive fallback
                else:
                    token.rejection_reason = reason
        log.info("sampling_worker.aged_out", mint=mint, reason=reason)
