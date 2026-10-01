"""
Modeled executable fills for shadow positions — no network calls.

Constant-product approximation of the pool using the liquidity DexScreener
already reports: one side holds R = liquidity_usd / 2. Trading V dollars of
value moves the price by V / (R + V) (that is the "impact"), and the pool fee
is taken on top. This reproduces the lesson from live trading — a thin pool
pays far less than its quoted price — without touching Jupiter's per-IP
budget that live trading depends on.
"""
from __future__ import annotations

from decimal import Decimal

ZERO = Decimal("0")
ONE = Decimal("1")


def side_reserve(liquidity_usd: Decimal | None) -> Decimal | None:
    if liquidity_usd is None or liquidity_usd <= 0:
        return None
    return liquidity_usd / 2


def buy_fraction(notional_usd: Decimal, reserve: Decimal, fee: Decimal) -> Decimal:
    """Fraction of `notional_usd` (at mid price) actually received as token value."""
    return reserve / (reserve + notional_usd) * (ONE - fee)


def sell_proceeds(value_usd: Decimal, reserve: Decimal, fee: Decimal) -> tuple[Decimal, Decimal]:
    """(proceeds_usd, impact) for selling tokens worth `value_usd` at mid price."""
    impact = value_usd / (reserve + value_usd)
    return value_usd * (ONE - impact) * (ONE - fee), impact


def position_value(notional_usd: Decimal, entry_reserve: Decimal, entry_price: Decimal,
                   price: Decimal, exit_reserve: Decimal, fee: Decimal) -> tuple[Decimal, Decimal]:
    """(proceeds_usd, impact) for exiting a modeled position at `price`."""
    tokens = notional_usd / entry_price * buy_fraction(notional_usd, entry_reserve, fee)
    return sell_proceeds(tokens * price, exit_reserve, fee)
