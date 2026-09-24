"""
tests/test_sell_coordination.py
================================
engine/sell_coordination.py — the in-process guard added 2026-09-24 after
a real race was found live: confluence_live_worker.py's own automatic
exit loop and a manual dashboard close both submitted a real on-chain
sell for the same trade at the same moment, and whichever landed second
was rejected on-chain (real custom program errors, e.g. 0x1788/6024).
"""

from __future__ import annotations

import uuid

from engine.sell_coordination import _IN_FLIGHT_SELLS, finish_sell, try_start_sell


def test_first_claim_succeeds():
    trade_id = str(uuid.uuid4())
    try:
        assert try_start_sell(trade_id) is True
    finally:
        finish_sell(trade_id)


def test_second_concurrent_claim_is_refused():
    trade_id = str(uuid.uuid4())
    try:
        assert try_start_sell(trade_id) is True
        assert try_start_sell(trade_id) is False  # already in flight
    finally:
        finish_sell(trade_id)


def test_finish_sell_releases_the_claim_for_reuse():
    trade_id = str(uuid.uuid4())
    assert try_start_sell(trade_id) is True
    finish_sell(trade_id)
    assert try_start_sell(trade_id) is True
    finish_sell(trade_id)


def test_finish_sell_on_an_unclaimed_id_is_a_safe_noop():
    trade_id = str(uuid.uuid4())
    finish_sell(trade_id)  # must not raise
    assert trade_id not in _IN_FLIGHT_SELLS


def test_different_trade_ids_do_not_interfere():
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    try:
        assert try_start_sell(a) is True
        assert try_start_sell(b) is True  # unrelated trade, unaffected
    finally:
        finish_sell(a)
        finish_sell(b)
