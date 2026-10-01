"""
workers/key_health.py
=======================
Detects a SolanaTracker API key going dead (2026-10-01 — real incident:
the sampling key ran out mid-conversation, repeatedly, and was only ever
noticed because a human happened to ask "check the bot" and someone read
the logs). Both sampling_worker.py and discovery_worker.py use their own
SEPARATE key (see discovery_worker.py's module docstring for why) and can
fail independently — one dead key must never be confused for the other.

Edge-triggered, same philosophy as the halt notifications in
confluence_live_worker.py and the circuit breaker: alert ONCE when the
key goes bad, alert ONCE when it recovers, never spam one notification
per cycle for as long as the outage lasts.

These are free-tier keys with a LIFETIME cap, not a daily one (confirmed
live, 2026-09-29/30) — a transient network blip or a single bad request
is not the same as the key actually being spent, which is why this
requires several consecutive bad cycles before concluding the key (not
the network) is the problem, rather than firing on the first error.
"""

from __future__ import annotations

DEFAULT_THRESHOLD = 5  # consecutive bad cycles before concluding it's the key, not a blip


class ExhaustionWatcher:
    """Call observe(bad) once per cycle/poll. Returns 'tripped' the one
    cycle the threshold is first crossed, 'recovered' the one cycle a
    previously-tripped watcher sees a good cycle again, otherwise None."""

    def __init__(self, threshold: int = DEFAULT_THRESHOLD) -> None:
        self.threshold = threshold
        self.consecutive_bad = 0
        self.is_exhausted = False

    def observe(self, bad: bool) -> str | None:
        if bad:
            self.consecutive_bad += 1
            if self.consecutive_bad >= self.threshold and not self.is_exhausted:
                self.is_exhausted = True
                return "tripped"
            return None

        self.consecutive_bad = 0
        if self.is_exhausted:
            self.is_exhausted = False
            return "recovered"
        return None
