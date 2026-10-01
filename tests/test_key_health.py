"""
tests/test_key_health.py
==========================
workers/key_health.py's ExhaustionWatcher — the edge-triggered consecutive-
bad-cycle detector behind the SolanaTracker key exhaustion alert
(2026-10-01). Pure logic, no I/O, no DB.
"""

from __future__ import annotations

from workers.key_health import ExhaustionWatcher


def test_does_not_trip_before_threshold():
    w = ExhaustionWatcher(threshold=3)
    assert w.observe(True) is None
    assert w.observe(True) is None


def test_trips_exactly_on_the_threshold_cycle():
    w = ExhaustionWatcher(threshold=3)
    assert w.observe(True) is None
    assert w.observe(True) is None
    assert w.observe(True) == "tripped"


def test_does_not_re_trip_every_cycle_while_still_bad():
    w = ExhaustionWatcher(threshold=2)
    assert w.observe(True) is None
    assert w.observe(True) == "tripped"
    assert w.observe(True) is None  # still bad, but already alerted — no spam
    assert w.observe(True) is None


def test_a_single_good_cycle_resets_the_streak():
    w = ExhaustionWatcher(threshold=3)
    w.observe(True)
    w.observe(True)
    assert w.observe(False) is None  # recovered before ever tripping — no false "recovered" either
    assert w.observe(True) is None
    assert w.observe(True) is None
    assert w.observe(True) == "tripped"  # needs a fresh run of 3, the earlier 2 don't carry over


def test_recovers_exactly_once_after_tripping():
    w = ExhaustionWatcher(threshold=2)
    w.observe(True)
    assert w.observe(True) == "tripped"
    assert w.observe(False) == "recovered"
    assert w.observe(False) is None  # still good — no repeated "recovered" spam


def test_can_trip_again_after_a_real_recovery():
    """A key can die, get replaced, work for a while, then die again (or a
    fresh replacement also turns out bad) — must be detectable each time,
    not just once per process lifetime."""
    w = ExhaustionWatcher(threshold=2)
    w.observe(True)
    assert w.observe(True) == "tripped"
    assert w.observe(False) == "recovered"
    w.observe(True)
    assert w.observe(True) == "tripped"
