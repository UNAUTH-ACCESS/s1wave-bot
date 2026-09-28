"""
engine/halt_override.py
=========================
Resume-after-halt override (2026-09-28) — shared by
workers/confluence_live_worker.py (real enforcement) and api/app.py (the
dashboard's RESUME button and status display), so the two can never
compute this differently.

See models.orm.ConfluenceLiveHaltOverride's docstring for the full design.
In short: a permanent halt is not something a button should just switch
off — that would mean a further loss down to zero goes completely
unnoticed. Instead, acknowledging a halt records the wallet's current
equity and all-time realized P&L as new baselines; the exact same
CONFLUENCE_LIVE_MAX_LOSS_PCT cap keeps protecting every dollar from that
point forward, just measured from a new zero rather than the original
deposit.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from database.engine import get_session
from models.orm import ConfluenceLiveHaltOverride


async def get_halt_override() -> ConfluenceLiveHaltOverride | None:
    """None means no one has ever acknowledged a halt — callers should
    fall back to the original deposit/lifetime-pnl baselines."""
    async with get_session() as session:
        return (await session.execute(
            select(ConfluenceLiveHaltOverride).where(ConfluenceLiveHaltOverride.id == 1)
        )).scalar_one_or_none()


async def acknowledge_halt(equity_usd: Decimal, all_time_pnl_usd: Decimal) -> None:
    """Records (or replaces) the singleton override row. Only the most
    recent acknowledgment matters — each one is a fresh "I accept the
    situation as of right now" decision, not additive."""
    async with get_session() as session:
        row = (await session.execute(
            select(ConfluenceLiveHaltOverride).where(ConfluenceLiveHaltOverride.id == 1)
        )).scalar_one_or_none()
        now = datetime.now(timezone.utc)
        if row is None:
            session.add(ConfluenceLiveHaltOverride(
                id=1, acknowledged_at=now, equity_baseline_usd=equity_usd, pnl_baseline_usd=all_time_pnl_usd,
            ))
        else:
            row.acknowledged_at = now
            row.equity_baseline_usd = equity_usd
            row.pnl_baseline_usd = all_time_pnl_usd


def apply_override(
    override: ConfluenceLiveHaltOverride | None, deposit_usd: Decimal, all_time_pnl_usd: Decimal,
) -> tuple[Decimal, Decimal]:
    """
    Pure function (2026-09-28) — kept separate from the DB read above so
    it's trivially testable without a database. Returns
    (effective_deposit_usd, effective_all_time_pnl_usd) for
    is_permanently_halted() to check against: unchanged if there's no
    override, or reset to the acknowledgment-time baselines if there is.
    """
    if override is None:
        return deposit_usd, all_time_pnl_usd
    return override.equity_baseline_usd, (all_time_pnl_usd - override.pnl_baseline_usd)
