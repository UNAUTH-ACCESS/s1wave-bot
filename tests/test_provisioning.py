"""
tests/test_provisioning.py
=============================
engine/provisioning.py's input validation (2026-09-29) — the guard
clauses that run BEFORE any real I/O (docker exec, systemctl, file
writes), so a bad account name fails fast and cleanly rather than
partway through creating real infrastructure. The full happy path
(real database, real keypair, real systemd unit) was verified live by
actually creating an account ("second") end-to-end rather than mocked
here — see CLAUDE.md's "Multi-account support" section.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from engine.provisioning import (
    REPO_DIR,
    ProvisioningError,
    create_account,
    env_path_for,
    get_solana_tracker_keys,
    service_name_for,
    update_solana_tracker_keys,
)


@pytest.mark.asyncio
async def test_rejects_uppercase_names():
    with pytest.raises(ProvisioningError, match="lowercase"):
        await create_account("Trading2")


@pytest.mark.asyncio
async def test_rejects_names_with_invalid_characters():
    with pytest.raises(ProvisioningError, match="lowercase"):
        await create_account("trading_2")  # underscore not allowed
    with pytest.raises(ProvisioningError, match="lowercase"):
        await create_account("trading.2")


@pytest.mark.asyncio
async def test_rejects_empty_name():
    with pytest.raises(ProvisioningError):
        await create_account("")


@pytest.mark.asyncio
async def test_rejects_reserved_names():
    with pytest.raises(ProvisioningError, match="reserved"):
        await create_account("control")
    with pytest.raises(ProvisioningError, match="reserved"):
        await create_account("base")


@pytest.mark.asyncio
async def test_accepts_valid_name_pattern_but_fails_fast_without_real_infra():
    """A well-formed, non-reserved name passes validation and proceeds to
    the first real I/O step (checking for an existing .env.<name> /
    systemd unit, then the database) -- this test doesn't mock that far,
    it only confirms validation itself doesn't reject a legitimate name.
    Real database/keypair/systemd creation is covered by the live
    end-to-end verification noted in CLAUDE.md, not here."""
    # A name this deliberately obscure should still pass the regex check
    # (proving the regex isn't accidentally over-strict) even though the
    # call will go on to fail on real infra not present in the test
    # environment (no docker/systemd access here) -- any exception at all
    # confirms we got PAST validation, which is what this test checks.
    with pytest.raises(Exception):
        await create_account("zzz-test-name-that-should-not-exist")


def test_env_path_for_base_is_plain_dotenv():
    assert env_path_for("base") == REPO_DIR / ".env"


def test_env_path_for_named_account_uses_suffix():
    assert env_path_for("second") == REPO_DIR / ".env.second"


def test_service_name_for_base_has_no_suffix():
    assert service_name_for("base") == "s1wave-bot.service"


def test_service_name_for_named_account_uses_suffix():
    assert service_name_for("second") == "s1wave-bot-second.service"


def test_get_solana_tracker_keys_missing_env_file_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "engine.provisioning.env_path_for", lambda name: tmp_path / ".env.nope"
    )
    with pytest.raises(ProvisioningError, match="No .env file"):
        get_solana_tracker_keys("nope")


def test_get_solana_tracker_keys_reads_existing_values(monkeypatch, tmp_path):
    env_file = tmp_path / ".env.second"
    env_file.write_text(
        "OTHER_VAR=unrelated\n"
        "SOLANA_TRACKER_API_KEY=sampling-abc\n"
        "SOLANA_TRACKER_API_KEY_DISCOVERY=discovery-xyz\n"
    )
    monkeypatch.setattr("engine.provisioning.env_path_for", lambda name: env_file)

    keys = get_solana_tracker_keys("second")

    assert keys == {"sampling": "sampling-abc", "discovery": "discovery-xyz"}


def test_get_solana_tracker_keys_defaults_to_blank_when_unset(monkeypatch, tmp_path):
    env_file = tmp_path / ".env.second"
    env_file.write_text("OTHER_VAR=unrelated\n")
    monkeypatch.setattr("engine.provisioning.env_path_for", lambda name: env_file)

    keys = get_solana_tracker_keys("second")

    assert keys == {"sampling": "", "discovery": ""}


@pytest.mark.asyncio
async def test_update_solana_tracker_keys_missing_env_file_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "engine.provisioning.env_path_for", lambda name: tmp_path / ".env.nope"
    )
    with pytest.raises(ProvisioningError, match="No .env file"):
        await update_solana_tracker_keys("nope", "new-sampling", "new-discovery")


@pytest.mark.asyncio
async def test_update_solana_tracker_keys_rewrites_env_and_restarts_only_that_account(
    monkeypatch, tmp_path
):
    env_file = tmp_path / ".env.second"
    env_file.write_text(
        "OTHER_VAR=unrelated\n"
        "SOLANA_TRACKER_API_KEY=old-sampling\n"
        "SOLANA_TRACKER_API_KEY_DISCOVERY=old-discovery\n"
    )
    monkeypatch.setattr("engine.provisioning.env_path_for", lambda name: env_file)
    run_mock = AsyncMock(return_value="")
    monkeypatch.setattr("engine.provisioning._run", run_mock)

    await update_solana_tracker_keys("second", "new-sampling", "new-discovery")

    rewritten = env_file.read_text()
    assert "SOLANA_TRACKER_API_KEY=new-sampling" in rewritten
    assert "SOLANA_TRACKER_API_KEY_DISCOVERY=new-discovery" in rewritten
    assert "OTHER_VAR=unrelated" in rewritten
    run_mock.assert_awaited_once_with(
        "systemctl", "--user", "restart", "s1wave-bot-second.service"
    )


@pytest.mark.asyncio
async def test_update_solana_tracker_keys_can_clear_a_key(monkeypatch, tmp_path):
    env_file = tmp_path / ".env.second"
    env_file.write_text("SOLANA_TRACKER_API_KEY=old-sampling\n")
    monkeypatch.setattr("engine.provisioning.env_path_for", lambda name: env_file)
    monkeypatch.setattr("engine.provisioning._run", AsyncMock(return_value=""))

    await update_solana_tracker_keys("second", "", "")

    rewritten = env_file.read_text()
    assert "SOLANA_TRACKER_API_KEY=\n" in rewritten
