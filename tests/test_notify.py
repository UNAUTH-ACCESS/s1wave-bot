"""
tests/test_notify.py
======================
engine/notify.py's notify() — the single shared funnel deciding which
ConfluenceNotification events also push to Telegram (2026-09-28). See
that module's docstring for the curated allowlist and why it's
deliberately narrow.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from engine.notify import TELEGRAM_PUSH_EVENTS, notify
from models.orm import ConfluenceNotification


@pytest.mark.asyncio
async def test_always_persists_the_in_app_notification(session, monkeypatch):
    monkeypatch.setattr("engine.notify.send_alert", AsyncMock())
    await notify(session, "info", "entry_filled", "Bought TEST — $1.00")
    rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
    assert len(rows) == 1
    assert rows[0].event == "entry_filled"
    assert rows[0].message == "Bought TEST — $1.00"


@pytest.mark.asyncio
async def test_pushes_to_telegram_for_an_allowlisted_event(session, monkeypatch):
    mock_send = AsyncMock()
    monkeypatch.setattr("engine.notify.send_alert", mock_send)
    await notify(session, "critical", "permanently_halted", "Trading halted — real loss reached the cap.")
    mock_send.assert_awaited_once()
    sent_text = mock_send.await_args.args[0]
    assert "PERMANENTLY HALTED" in sent_text
    assert "real loss reached the cap" in sent_text


@pytest.mark.asyncio
async def test_does_not_push_for_a_non_allowlisted_event(session, monkeypatch):
    """exit_filled is deliberately excluded — see the module docstring:
    it's now the routine exit path (LIQUIDITY_GUARD fires on most trades)
    and would be spam at this frequency, even though it's marked
    'critical' in-app for dashboard visual prominence."""
    mock_send = AsyncMock()
    monkeypatch.setattr("engine.notify.send_alert", mock_send)
    await notify(session, "critical", "exit_filled", "TEST closed (LIQUIDITY_GUARD): -50.0%, $-1.00")
    mock_send.assert_not_awaited()


@pytest.mark.asyncio
async def test_every_documented_push_event_actually_pushes(session, monkeypatch):
    """Guards against the allowlist and the actual push check silently
    drifting apart."""
    mock_send = AsyncMock()
    monkeypatch.setattr("engine.notify.send_alert", mock_send)
    for event in TELEGRAM_PUSH_EVENTS:
        mock_send.reset_mock()
        await notify(session, "warning", event, "test message")
        mock_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_telegram_failure_never_breaks_the_in_app_write(session, monkeypatch):
    """send_alert() is expected to never raise on its own, but notify()
    has its own second try/except specifically so a Telegram problem can
    never roll back the whole transaction (session.add() + send_alert()
    happen inside the same `async with get_session()` block at every real
    call site) and silently lose even the in-app notification."""
    monkeypatch.setattr("engine.notify.send_alert", AsyncMock(side_effect=RuntimeError("Telegram is down")))
    await notify(session, "critical", "permanently_halted", "Trading halted.")  # must not raise
    rows = (await session.execute(select(ConfluenceNotification))).scalars().all()
    assert len(rows) == 1
    assert rows[0].event == "permanently_halted"
