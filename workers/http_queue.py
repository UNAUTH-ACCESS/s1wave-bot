"""
workers/http_queue.py
=====================
Central rate-limited HTTP queue for all SolanaTracker requests.

All workers submit coroutines here instead of calling httpx directly.
The queue drains at a fixed 1 req/s ceiling regardless of how many
workers are submitting. Eliminates the burst-and-collide pattern that
was producing 85 rate-limit hits per session.

Usage
-----
    from workers.http_queue import get_http_queue

    queue = get_http_queue()
    result = await queue.submit(client.get, url, headers=headers)

Design
------
- FIFO — no priority lanes needed at current token volumes
  (discovery ~1/min, sampling ~6/min, WS fallback ~2/min → ~51/min headroom)
- submit() returns the coroutine result or re-raises its exception
- start() / stop() wired into main.py startup/shutdown
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Coroutine

from config.logging import get_logger

log = get_logger(__name__)

# Hard ceiling: 1 request per second
_MIN_INTERVAL = 1.0


class RateLimitedQueue:
    """
    Serialises async HTTP callables through a 1 req/s gate.

    Each submit() enqueues (fn, args, kwargs, future). The drain loop
    pops one entry per _MIN_INTERVAL, awaits fn(*args, **kwargs), and
    resolves the future so the caller unblocks with the result.
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue = asyncio.Queue()
        self._last_sent: float = 0.0
        self._drain_task: asyncio.Task | None = None

    def start(self) -> None:
        """Start the background drain loop. Call once at bot startup."""
        if self._drain_task is None or self._drain_task.done():
            self._drain_task = asyncio.create_task(self._drain(), name="http_queue.drain")
            log.info("http_queue.started", rate="1 req/s")

    def stop(self) -> None:
        """Cancel the drain loop. Call in shutdown handler."""
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
            log.info("http_queue.stopped")

    async def submit(
        self,
        fn: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Enqueue fn(*args, **kwargs) for rate-limited execution.
        Awaits until the request completes and returns the result.
        Re-raises any exception from fn.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        await self._queue.put((fn, args, kwargs, future))
        return await future

    async def _drain(self) -> None:
        """Background loop: one request per _MIN_INTERVAL."""
        while True:
            try:
                fn, args, kwargs, future = await self._queue.get()
                try:
                    # Enforce minimum gap since last outbound request
                    now = time.monotonic()
                    gap = _MIN_INTERVAL - (now - self._last_sent)
                    if gap > 0:
                        await asyncio.sleep(gap)

                    result = await fn(*args, **kwargs)
                    self._last_sent = time.monotonic()

                    if not future.done():
                        future.set_result(result)

                except Exception as exc:
                    if not future.done():
                        future.set_exception(exc)
                finally:
                    self._queue.task_done()

            except asyncio.CancelledError:
                break


# Module-level singleton — import and call get_http_queue() everywhere
_queue: RateLimitedQueue | None = None


def get_http_queue() -> RateLimitedQueue:
    global _queue
    if _queue is None:
        _queue = RateLimitedQueue()
    return _queue
