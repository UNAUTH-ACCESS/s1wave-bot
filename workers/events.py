"""
workers/events.py
=================
In-memory message contracts for inter-worker communication.

These are transport types — pure data, no persistence, no vendor
assumptions, no DB imports. They define what workers say to each other.

Replacing a data source (e.g. SolanaTracker → another feed) means
changing the producer only. Consumers and decision logic are untouched
as long as the contract is preserved.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class MarketEvent:
    """
    A snapshot of what the market looks like for a token right now.

    Produced by: TradeMonitorWorker
    Consumed by: TradeRiskWorker

    This represents market state — not trade state. There is no
    trade_id, no entry_price, no pnl. Those are the consumer's concern.
    """
    mint:            str
    price_usd:       Decimal
    liquidity_usd:   Decimal
    buy_pressure:    Decimal
    price_change_1m: float
    price_change_5m: float
    sampled_at:      datetime


@dataclass(frozen=True)
class SnapshotEvent:
    """
    Signals that a snapshot has been written for a token.

    Produced by: SamplingWorker (after every snapshot write)
    Consumed by: S1WaveWorker

    snapshot_number is 1-indexed. S1WaveWorker only acts on snapshot_number == 1.
    All subsequent snapshots are passed through but ignored by S1WaveWorker.
    """
    mint:             str
    symbol:           str | None
    snapshot_number:  int        # 1 = first snapshot ever for this token
    price_usd:        Decimal
    buy_pressure:     Decimal
    price_change_1m:  float
    price_change_5m:  float
    liquidity_usd:    Decimal
    sampled_at:       datetime
