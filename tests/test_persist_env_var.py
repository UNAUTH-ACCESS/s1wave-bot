"""
tests/test_persist_env_var.py
==============================
api/app.py's _persist_env_var() (2026-09-24, backing POST /confluence/toggle
— the dashboard's arm/disarm button) rewrites one line of the real .env file
on disk, which also holds CONFLUENCE_LIVE_WALLET_PRIVATE_KEY. The one thing
that must never happen is that rewrite corrupting or dropping any other
line — this is tested directly against a realistic multi-line fixture
rather than trusting the implementation by inspection alone.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def env_file(tmp_path, monkeypatch):
    f = tmp_path / ".env"
    f.write_text(
        "DATABASE_URL=postgresql+asyncpg://s1wave:pw@127.0.0.1:5433/s1wave\n"
        "CONFLUENCE_LIVE_WALLET_PRIVATE_KEY=SomeBase58LookingKeyValueHere123\n"
        "CONFLUENCE_LIVE_ENABLED=True\n"
        "SOL_PRICE_USD=0.0\n"
    )
    import api.app as app_module
    monkeypatch.setattr(app_module, "_ENV_PATH", f)
    return f


def test_rewrites_only_the_target_line(env_file):
    import api.app as app_module
    app_module._persist_env_var("CONFLUENCE_LIVE_ENABLED", "False")
    lines = env_file.read_text().splitlines()
    assert "CONFLUENCE_LIVE_ENABLED=False" in lines
    assert "CONFLUENCE_LIVE_WALLET_PRIVATE_KEY=SomeBase58LookingKeyValueHere123" in lines
    assert "DATABASE_URL=postgresql+asyncpg://s1wave:pw@127.0.0.1:5433/s1wave" in lines
    assert "SOL_PRICE_USD=0.0" in lines
    assert len(lines) == 4  # no line dropped, none duplicated


def test_toggle_back_and_forth_is_idempotent_in_shape(env_file):
    import api.app as app_module
    app_module._persist_env_var("CONFLUENCE_LIVE_ENABLED", "False")
    app_module._persist_env_var("CONFLUENCE_LIVE_ENABLED", "True")
    app_module._persist_env_var("CONFLUENCE_LIVE_ENABLED", "False")
    lines = env_file.read_text().splitlines()
    assert lines.count("CONFLUENCE_LIVE_ENABLED=False") == 1
    assert len(lines) == 4  # still 4 lines, not growing with each toggle


def test_appends_if_key_not_present(env_file):
    import api.app as app_module
    app_module._persist_env_var("TELEGRAM_BOT_TOKEN", "abc123")
    lines = env_file.read_text().splitlines()
    assert "TELEGRAM_BOT_TOKEN=abc123" in lines
    assert len(lines) == 5
    # everything else still intact
    assert "CONFLUENCE_LIVE_WALLET_PRIVATE_KEY=SomeBase58LookingKeyValueHere123" in lines
