"""
engine/exit_variants.py
=======================
Parameterised version of the live exit stack, so shadow can run several exit
rules side by side on the same entries and the same price ticks.

Pure logic, no I/O. `BASE` reproduces the live rule exactly (see
tests/test_exit_variants.py): velocity breaker, HARD_FLOOR, staircase
trailing stop, TIME_EXIT. Variants change one knob each.

Exit-rule variants only. They share the primary position's entry, so they
cannot test entry filters (skipped candidates are not price-tracked).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal

from config.settings import settings

_Q = Decimal("0.000000000001")


@dataclass(frozen=True)
class VariantSpec:
    name: str
    initial_stop: Decimal      # distance below entry of the first stop floor
    step: Decimal | None       # staircase step; None = floor never ratchets
    hard_floor: Decimal        # negative pnl fraction, immediate exit
    velocity: Decimal          # negative pnl fraction, immediate exit
    max_hold_s: int
    take_profit: Decimal | None = None  # pnl fraction at which to sell


def base_spec() -> VariantSpec:
    return VariantSpec(
        name="base",
        initial_stop=Decimal("0.12"),
        step=Decimal("0.10"),
        hard_floor=Decimal(str(settings.HARD_FLOOR_PCT)),
        velocity=Decimal("-0.25"),
        max_hold_s=settings.max_hold_seconds,
    )


def variant_specs() -> list[VariantSpec]:
    """Base first, then one-knob variants."""
    b = base_spec()
    return [
        b,
        replace(b, name="wide_stop", initial_stop=Decimal("0.20"),
                hard_floor=Decimal("-0.30"), velocity=Decimal("-0.40")),
        replace(b, name="tight_stop", initial_stop=Decimal("0.08"), hard_floor=Decimal("-0.10")),
        replace(b, name="fine_step", step=Decimal("0.05")),
        replace(b, name="coarse_step", step=Decimal("0.20")),
        replace(b, name="no_trail", step=None),
        replace(b, name="hold_1h", max_hold_s=3600),
        replace(b, name="hold_20m", max_hold_s=1200),
        replace(b, name="tp_50", take_profit=Decimal("0.50")),
    ]


def initial_floor(spec: VariantSpec, entry: Decimal) -> Decimal:
    return (entry * (1 - spec.initial_stop)).quantize(_Q)


def compute_floor(spec: VariantSpec, entry: Decimal, hwm: Decimal) -> Decimal:
    if spec.step is None:
        return initial_floor(spec, entry)
    gain = (hwm - entry) / entry
    if gain < spec.step:
        return initial_floor(spec, entry)
    locked = int(gain / spec.step) - 1
    if locked <= 0:
        return entry.quantize(_Q)
    return (entry * (1 + spec.step * locked)).quantize(_Q)


def step_exit(
    spec: VariantSpec, entry: Decimal, entry_time: datetime, price: Decimal, now: datetime,
    floor: Decimal | None, hwm: Decimal | None,
) -> tuple[str | None, Decimal, Decimal]:
    """One tick. Returns (exit_reason | None, new_floor, new_hwm)."""
    floor = floor or initial_floor(spec, entry)
    hwm = hwm or entry
    if entry is None or entry <= 0:
        return None, floor, hwm
    pnl = (price - entry) / entry
    if pnl <= spec.velocity or pnl <= spec.hard_floor:
        return "HARD_FLOOR", floor, max(hwm, price)

    new_hwm = max(hwm, price)
    new_floor = max(floor, compute_floor(spec, entry, new_hwm))
    if price <= new_floor:
        reason = "STOP_LOSS" if new_floor <= initial_floor(spec, entry) else "TRAILING_STOP"
        return reason, new_floor, new_hwm

    if spec.take_profit is not None and pnl >= spec.take_profit:
        return "TAKE_PROFIT", new_floor, new_hwm
    if (now - entry_time).total_seconds() >= spec.max_hold_s:
        return "TIME_EXIT", new_floor, new_hwm
    return None, new_floor, new_hwm
