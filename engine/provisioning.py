"""
engine/provisioning.py
========================
Creates a new, fully isolated S1Wave trading account (2026-09-29) — the
shared logic behind both control_panel/app.py's "+ New Account" button
and control_panel/cli.py's command-line entry point, so there is exactly
ONE implementation of "what a new account needs" rather than the web app
and the CLI drifting apart.

Everything this touches is additive and isolated per account: a new
Postgres database, a freshly generated wallet keypair (private key never
returned or logged — only the public address), a new .env.<name> file,
and a new systemd --user service. Registers the result in the control
panel's own `control_accounts` table so the homepage can list it.

A new account comes up CONFLUENCE_LIVE_ENABLED=False and with blank
SolanaTracker keys on purpose — see the module docstring history in
CLAUDE.md's "Multi-account support" section for why (never auto-arm an
unfunded account; never silently double the load on the base account's
already-strained SolanaTracker credits).

Must be called from a process whose own settings.DATABASE_URL points at
the control panel's database (s1wave_control) — get_session() below
writes the new ControlAccount row into WHATEVER database the calling
process is configured for, so running this from the wrong process would
write the registry entry into the wrong place.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path

from solders.keypair import Keypair
from sqlalchemy import select

from config.settings import settings
from control_panel.models import ControlAccount
from database.engine import get_session

REPO_DIR = Path(__file__).resolve().parent.parent
_NAME_RE = re.compile(r"^[a-z0-9-]+$")


class ProvisioningError(RuntimeError):
    """A real, user-facing provisioning failure (bad name, name taken,
    a subprocess step failed) — always safe to show the message directly,
    never contains a secret."""


@dataclass
class ProvisionedAccount:
    name: str
    port: int
    wallet_pubkey: str
    env_file: str


async def _run(*args: str, cwd: Path | None = None) -> str:
    """Run a subprocess, raising ProvisioningError with its real stderr on
    a non-zero exit — never swallow a provisioning step's failure."""
    proc = await asyncio.create_subprocess_exec(
        *args, cwd=str(cwd or REPO_DIR),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise ProvisioningError(f"{args[0]} failed: {stderr.decode().strip()[:500]}")
    return stdout.decode()


async def _next_port(session) -> int:
    rows = (await session.execute(select(ControlAccount.port))).scalars().all()
    used = set(rows) | {settings.API_PORT if settings.API_PORT != 9000 else 8000}
    port = 8001
    while port in used:
        port += 1
    return port


async def create_account(name: str) -> ProvisionedAccount:
    if not _NAME_RE.match(name):
        raise ProvisioningError("Account name must be lowercase letters, digits, hyphens only.")
    if name in ("control", "base"):
        raise ProvisioningError(f"'{name}' is a reserved name.")

    env_path = REPO_DIR / f".env.{name}"
    unit_path = Path.home() / ".config" / "systemd" / "user" / f"s1wave-bot-{name}.service"
    if env_path.exists() or unit_path.exists():
        raise ProvisioningError(f"An account named '{name}' already exists.")

    async with get_session() as session:
        existing = (await session.execute(
            select(ControlAccount).where(ControlAccount.name == name)
        )).scalar_one_or_none()
        if existing is not None:
            raise ProvisioningError(f"An account named '{name}' already exists.")
        port = await _next_port(session)

    db_name = f"s1wave_{name.replace('-', '_')}"
    await _run("docker", "exec", "s1wave_postgres", "psql", "-U", "s1wave", "-d", "postgres",
                "-c", f"CREATE DATABASE {db_name};")

    keypair = Keypair()
    wallet_secret = str(keypair)
    wallet_pubkey = str(keypair.pubkey())

    base_env = (REPO_DIR / ".env").read_text()
    lines = []
    for line in base_env.splitlines():
        if line.startswith("DATABASE_URL="):
            prefix = line.rsplit("/", 1)[0]
            lines.append(f"{prefix}/{db_name}")
        elif line.startswith("API_PORT="):
            lines.append(f"API_PORT={port}")
        elif line.startswith("CONFLUENCE_LIVE_WALLET_PRIVATE_KEY="):
            lines.append(f"CONFLUENCE_LIVE_WALLET_PRIVATE_KEY={wallet_secret}")
        elif line.startswith("CONFLUENCE_LIVE_ENABLED="):
            lines.append("CONFLUENCE_LIVE_ENABLED=False")
        elif line.startswith("SOLANA_TRACKER_API_KEY="):
            lines.append("SOLANA_TRACKER_API_KEY=")
        elif line.startswith("SOLANA_TRACKER_API_KEY_DISCOVERY="):
            lines.append("SOLANA_TRACKER_API_KEY_DISCOVERY=")
        else:
            lines.append(line)
    lines.append("")
    lines.append("# ── Account identity (added by engine/provisioning.py) ────────────────────")
    lines.append(f"S1WAVE_ACCOUNT_NAME=S1Wave-{name}")
    lines.append(f"# Public wallet address: {wallet_pubkey} — fund this address to activate.")
    env_path.write_text("\n".join(lines) + "\n")
    env_path.chmod(0o600)

    dashboard_user = next((l.split("=", 1)[1] for l in lines if l.startswith("DASHBOARD_AUTH_USER=")), "")
    dashboard_password = next((l.split("=", 1)[1] for l in lines if l.startswith("DASHBOARD_AUTH_PASSWORD=")), "")

    unit_path.parent.mkdir(parents=True, exist_ok=True)
    unit_path.write_text(f"""[Unit]
Description=S1Wave account '{name}' (discovery/sampling/shadow/live workers + API, port {port})
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={REPO_DIR}
Environment=S1WAVE_ENV_FILE=.env.{name}
ExecStart={REPO_DIR}/.venv/bin/python main.py
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=default.target
""")

    try:
        await _run("systemctl", "--user", "daemon-reload")
        await _run("systemctl", "--user", "enable", "--now", f"s1wave-bot-{name}.service")
    except ProvisioningError:
        env_path.unlink(missing_ok=True)
        unit_path.unlink(missing_ok=True)
        raise

    async with get_session() as session:
        session.add(ControlAccount(
            name=name, port=port, wallet_pubkey=wallet_pubkey,
            dashboard_auth_user=dashboard_user, dashboard_auth_password=dashboard_password,
        ))

    return ProvisionedAccount(name=name, port=port, wallet_pubkey=wallet_pubkey, env_file=str(env_path))
