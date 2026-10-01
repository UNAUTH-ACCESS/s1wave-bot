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
import secrets
import tempfile
from dataclasses import dataclass
from pathlib import Path

from solders.keypair import Keypair
from sqlalchemy import select

from config.logging import get_logger
from config.settings import settings
from control_panel.models import ControlAccount
from database.engine import get_session

log = get_logger(__name__)

REPO_DIR = Path(__file__).resolve().parent.parent
_NAME_RE = re.compile(r"^[a-z0-9-]+$")

# The shared nginx container fronts SEVERAL unrelated projects (s1wave,
# contentpipe, anchorledger) and lives in a DIFFERENT repo/checkout on this
# same host — see that repo's own CLAUDE.md. Cross-repo on purpose: there is
# exactly one nginx config for this whole box, and S1Wave account creation
# needs to extend it, not fork a second one.
_QUANTEDGE_DIR = Path("/home/solana/quantedge")
_QUANTEDGE_NGINX_CONF = _QUANTEDGE_DIR / "nginx" / "active.conf"
_NGINX_MARKER_END = "    # S1WAVE-ACCOUNTS-END"
PUBLIC_DOMAIN = "s1wave-solana.duckdns.org"


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


def env_path_for(name: str) -> Path:
    """"base" is the original, pre-multi-account deployment — plain .env,
    no suffix. Every other account is .env.<name> (see create_account())."""
    return REPO_DIR / (".env" if name == "base" else f".env.{name}")


def service_name_for(name: str) -> str:
    return "s1wave-bot.service" if name == "base" else f"s1wave-bot-{name}.service"


def public_url_for(name: str) -> str:
    """Every account's public URL follows the exact same pattern now
    (2026-09-29) — no more per-account hardcoding in the control panel.
    Only meaningful once ControlAccount.nginx_configured is True for that
    account; callers are responsible for checking that, this function
    just knows the URL SHAPE, not whether the route actually exists yet."""
    return f"https://{PUBLIC_DOMAIN}/{name}/"


def _nginx_block_for(name: str, port: int) -> str:
    return f"""    location = /{name} {{
        return 301 /{name}/;
    }}

    location /{name}/ {{
        proxy_pass         http://host.docker.internal:{port}/;
        proxy_http_version 1.1;
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
        proxy_set_header   Connection        '';
        proxy_buffering    off;
        proxy_cache        off;
        proxy_read_timeout 3600s;
    }}
"""


async def add_nginx_route(name: str, port: int) -> None:
    """
    Gives a newly created account its own public URL under the shared
    S1Wave domain (2026-09-29 — "let every be one app," fully automated:
    no more manual nginx edits per account). Inserts a new location block
    into the quantedge repo's nginx/active.conf between the
    S1WAVE-ACCOUNTS-START/END markers, VALIDATES the resulting config in
    an isolated container before touching the live file, and only then
    deploys it (docker compose up -d --force-recreate — the same
    container fronts other unrelated projects, so this briefly touches
    their traffic too, same as every manual nginx change this session).

    Idempotent: if a route for this name already exists, this is a no-op.

    Best-effort by design — create_account() must not roll back a real,
    already-provisioned account just because this last step failed (a
    bad config, docker being briefly unavailable). Callers should catch
    ProvisioningError here and record that the account still has no
    public route yet, rather than letting the whole creation fail.
    """
    if not _QUANTEDGE_NGINX_CONF.exists():
        raise ProvisioningError(
            f"{_QUANTEDGE_NGINX_CONF} not found — is the quantedge repo checked out at the expected path?"
        )

    original = _QUANTEDGE_NGINX_CONF.read_text()
    if f"location /{name}/ {{" in original:
        return  # already has a route

    marker_count = original.count(_NGINX_MARKER_END)
    if marker_count != 1:
        # Fail loud, not silent (2026-09-29, real incident): str.replace's
        # count=1 takes the FIRST match, no questions asked — a stray
        # second occurrence of this exact text (e.g. a comment mentioning
        # the marker by name) makes the block land in the wrong place
        # while nginx -t still happily validates it, since it's still
        # syntactically a fine location block, just not where anyone
        # meant it. Caught exactly this way once already; never again
        # guess which occurrence was the real one.
        raise ProvisioningError(
            f"nginx config has {marker_count} occurrences of the S1WAVE-ACCOUNTS-END "
            "marker, expected exactly 1 — refusing to guess which one to insert before."
        )

    updated = original.replace(_NGINX_MARKER_END, _nginx_block_for(name, port) + _NGINX_MARKER_END, 1)

    tmp_path = Path(tempfile.mkstemp(suffix=".conf")[1])
    try:
        tmp_path.write_text(updated)
        await _run(
            "docker", "run", "--rm", "--add-host=host.docker.internal:host-gateway",
            "-v", f"{tmp_path}:/etc/nginx/conf.d/default.conf:ro",
            "-v", f"{_QUANTEDGE_DIR}/nginx/certbot/conf:/etc/letsencrypt:ro",
            "-v", f"{_QUANTEDGE_DIR}/nginx/certbot/www:/var/www/certbot:ro",
            "nginx:alpine", "nginx", "-t",
        )
    except ProvisioningError as exc:
        raise ProvisioningError(f"Generated nginx config failed validation — NOT deployed. {exc}") from exc
    finally:
        tmp_path.unlink(missing_ok=True)

    _QUANTEDGE_NGINX_CONF.write_text(updated)
    await _run("docker", "compose", "up", "-d", "--force-recreate", "nginx", cwd=_QUANTEDGE_DIR)


# Tunable live-trading risk parameters (2026-09-30) — every one of these is
# a real config/settings.py field, so editing it here + restarting the
# account's service is the exact same effect as hand-editing .env and
# restarting, just through the web app instead of a terminal. Deliberately
# scoped to risk/exposure controls, NOT the research-backed entry-filter
# thresholds in workers/entry_filters.py (buy_pressure floor, liquidity
# ceiling) — those came from real backtested data and shouldn't be casually
# nudged from a form; these are the knobs a human is expected to turn
# themselves as circumstances change (capital size, risk tolerance, an
# ongoing bad stretch). "default" below must be kept in sync with
# config/settings.py by hand — there is no single source of truth shared
# between a Pydantic field default and a plain dict literal.
TRADING_PARAMS: dict[str, dict] = {
    "CONFLUENCE_LIVE_ENABLED": {
        "label": "Live trading enabled (kill switch)",
        "type": "bool", "default": "True",
        "help": "OFF stops all NEW entries immediately. Existing open positions are still "
                "monitored and can still exit normally — this never abandons a position.",
    },
    "CONFLUENCE_LIVE_EXPOSURE_PCT": {
        "label": "Exposure — % of equity at risk across ALL open positions",
        "type": "float", "min": 0.01, "max": 1.0, "default": "0.5",
        "help": "0.20 means at most 20% of your wallet balance can be tied up in open positions "
                "at once, split across your concurrent slots below. Lower = smaller bets per trade, "
                "survives a losing stretch longer. Raise this as capital grows if you want to.",
    },
    "CONFLUENCE_LIVE_MAX_POSITION_USD": {
        "label": "Max position size — hard USD cap per trade",
        "type": "float", "min": 0.01, "default": "50.0",
        "help": "No single trade will ever exceed this dollar amount, no matter how big the "
                "wallet gets. This is the ceiling; exposure % above is usually the binding limit "
                "while capital is small.",
    },
    "CONFLUENCE_LIVE_MAX_CONCURRENT": {
        "label": "Max concurrent open positions",
        "type": "int", "min": 1, "default": "5",
        "help": "How many trades can be open at the same time. Lower also limits how many "
                "trades a single correlated burst of bad signals could open before anything "
                "else (the circuit breaker, a human) can react.",
    },
    "CONFLUENCE_LIVE_DAILY_LOSS_LIMIT_PCT": {
        "label": "Daily loss limit — % of today's starting equity",
        "type": "float", "min": 0.01, "max": 1.0, "default": "0.5",
        "help": "If today's realized losses reach this fraction of what the wallet started the "
                "day with, live entries halt until UTC midnight.",
    },
    "CB_LOSS_COUNT": {
        "label": "Circuit breaker — losses in a row before pausing",
        "type": "int", "min": 1, "default": "3",
        "help": "After this many LOSSES in a row (any win resets the count to 0), live entries "
                "pause automatically. See CLAUDE.md's 2026-09-30 entry for the incident that made this real.",
    },
    "CB_PAUSE_MINUTES": {
        "label": "Circuit breaker — pause duration (minutes)",
        "type": "int", "min": 1, "default": "60",
        "help": "How long entries stay paused after the circuit breaker trips, before "
                "automatically resuming on their own.",
    },
}


def get_trading_params(name: str) -> dict[str, str]:
    """Current value of every TRADING_PARAMS key for one account — straight
    from its .env where set, falling back to the documented default (NOT
    to this process's own settings object, which reflects the CONTROL
    PANEL's own .env, not necessarily this account's)."""
    env_path = env_path_for(name)
    if not env_path.exists():
        raise ProvisioningError(f"No .env file found for account '{name}'.")
    current = {key: spec["default"] for key, spec in TRADING_PARAMS.items()}
    for line in env_path.read_text().splitlines():
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key in TRADING_PARAMS:
            current[key] = value
    return current


def validate_trading_param(key: str, raw_value: str) -> str:
    """Coerce+bounds-check one raw form value against TRADING_PARAMS,
    returning the normalized string to actually write to .env. Raises
    ProvisioningError with a message safe to show directly in the form on
    anything invalid — never writes a value that would fail Pydantic
    validation on the next restart and crash the account's process."""
    spec = TRADING_PARAMS.get(key)
    if spec is None:
        raise ProvisioningError(f"Unknown trading parameter: {key}")

    if spec["type"] == "bool":
        normalized = raw_value.strip().lower()
        if normalized not in ("true", "false"):
            raise ProvisioningError(f"{spec['label']}: must be True or False.")
        return "True" if normalized == "true" else "False"

    try:
        num = float(raw_value) if spec["type"] == "float" else int(raw_value)
    except (TypeError, ValueError):
        raise ProvisioningError(f"{spec['label']}: '{raw_value}' is not a valid number.")

    if "min" in spec and num < spec["min"]:
        raise ProvisioningError(f"{spec['label']}: must be at least {spec['min']}.")
    if "max" in spec and num > spec["max"]:
        raise ProvisioningError(f"{spec['label']}: must be at most {spec['max']}.")

    return str(num)


async def update_trading_params(name: str, updates: dict[str, str]) -> None:
    """
    Rewrites one or more TRADING_PARAMS keys in an account's .env and
    restarts its service — same per-account, never-touches-another-account
    pattern as update_solana_tracker_keys()/update_dashboard_credentials().
    Unlike those, these keys may not already be PRESENT in an existing
    .env (they've always been pure code defaults until now), so this is an
    upsert: replace the line if found, otherwise append a new one under a
    clearly labeled block.
    """
    unknown = set(updates) - set(TRADING_PARAMS)
    if unknown:
        raise ProvisioningError(f"Unknown trading parameter(s): {', '.join(sorted(unknown))}")

    env_path = env_path_for(name)
    if not env_path.exists():
        raise ProvisioningError(f"No .env file found for account '{name}'.")

    remaining = dict(updates)
    lines = env_path.read_text().splitlines()
    for i, line in enumerate(lines):
        key = line.partition("=")[0]
        if key in remaining:
            lines[i] = f"{key}={remaining.pop(key)}"
    if remaining:
        lines.append("")
        lines.append("# ── Trading parameters (added by the control panel) ────────────────────────")
        for key, value in remaining.items():
            lines.append(f"{key}={value}")

    env_path.write_text("\n".join(lines) + "\n")
    env_path.chmod(0o600)
    await _run("systemctl", "--user", "restart", service_name_for(name))


def get_solana_tracker_keys(name: str) -> dict[str, str]:
    """Current SOLANA_TRACKER_API_KEY / _DISCOVERY values for an account,
    straight from its .env — shown prefilled in the control panel's edit
    form so "add or replace" always starts from what's actually set, not
    a guess. Returns empty strings for a key that isn't set."""
    env_path = env_path_for(name)
    if not env_path.exists():
        raise ProvisioningError(f"No .env file found for account '{name}'.")
    keys = {"sampling": "", "discovery": ""}
    for line in env_path.read_text().splitlines():
        if line.startswith("SOLANA_TRACKER_API_KEY="):
            keys["sampling"] = line.split("=", 1)[1]
        elif line.startswith("SOLANA_TRACKER_API_KEY_DISCOVERY="):
            keys["discovery"] = line.split("=", 1)[1]
    return keys


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_CHAT_RE = re.compile(r"^(-?\d{3,20}|@[A-Za-z0-9_]{4,32})$")


def get_notification_contacts(name: str) -> dict[str, str]:
    """This account's email and Telegram chat id from its .env (never the bot token)."""
    env_path = env_path_for(name)
    if not env_path.exists():
        raise ProvisioningError(f"No .env file found for account '{name}'.")
    out = {"email": "", "telegram_chat_id": ""}
    for line in env_path.read_text().splitlines():
        if line.startswith("NOTIFY_EMAIL="):
            out["email"] = line.split("=", 1)[1].strip()
        elif line.startswith("TELEGRAM_CHAT_ID="):
            out["telegram_chat_id"] = line.split("=", 1)[1].strip()
    return out


def validate_notification_contacts(email: str, chat_id: str) -> tuple[str, str]:
    email, chat_id = email.strip(), chat_id.strip()
    if email and not _EMAIL_RE.match(email):
        raise ProvisioningError("That doesn't look like an email address.")
    if chat_id and not _CHAT_RE.match(chat_id):
        raise ProvisioningError("Telegram chat id must be a number (or @channelname).")
    return email, chat_id


def _upsert_env(env_path: Path, updates: dict[str, str]) -> None:
    remaining = dict(updates)
    lines = env_path.read_text().splitlines()
    for i, line in enumerate(lines):
        key = line.partition("=")[0]
        if key in remaining:
            lines[i] = f"{key}={remaining.pop(key)}"
    for key, value in remaining.items():
        lines.append(f"{key}={value}")
    env_path.write_text("\n".join(lines) + "\n")
    env_path.chmod(0o600)


async def update_notification_contacts(name: str, email: str, chat_id: str) -> None:
    """Sets NOTIFY_EMAIL / TELEGRAM_CHAT_ID in the account's .env and restarts only that account."""
    email, chat_id = validate_notification_contacts(email, chat_id)
    env_path = env_path_for(name)
    if not env_path.exists():
        raise ProvisioningError(f"No .env file found for account '{name}'.")
    _upsert_env(env_path, {"NOTIFY_EMAIL": email, "TELEGRAM_CHAT_ID": chat_id})
    await _run("systemctl", "--user", "restart", service_name_for(name))


def generate_dashboard_credentials(name: str) -> tuple[str, str]:
    """A fresh, distinct Basic Auth username/password for one account's own
    dashboard (2026-09-29) — every account used to inherit base's exact
    creds verbatim (copied straight from its .env template), so one leaked
    password opened every account's dashboard, not just one. Username is
    just the account's own name (easy to recognize in the control panel;
    it's shown right next to it, not a secret); the password is random."""
    return name, secrets.token_urlsafe(16)


async def update_dashboard_credentials(name: str, user: str, password: str) -> None:
    """Rewrites an account's own DASHBOARD_AUTH_USER/PASSWORD in its .env
    and restarts its service — the same per-account, never-touches-another-
    account pattern as update_solana_tracker_keys(). The control panel is
    responsible for also updating its own ControlAccount row so what it
    displays matches what the account's .env actually enforces."""
    env_path = env_path_for(name)
    if not env_path.exists():
        raise ProvisioningError(f"No .env file found for account '{name}'.")

    lines = []
    for line in env_path.read_text().splitlines():
        if line.startswith("DASHBOARD_AUTH_USER="):
            lines.append(f"DASHBOARD_AUTH_USER={user}")
        elif line.startswith("DASHBOARD_AUTH_PASSWORD="):
            lines.append(f"DASHBOARD_AUTH_PASSWORD={password}")
        else:
            lines.append(line)
    env_path.write_text("\n".join(lines) + "\n")
    env_path.chmod(0o600)

    await _run("systemctl", "--user", "restart", service_name_for(name))


async def update_solana_tracker_keys(name: str, sampling_key: str, discovery_key: str) -> None:
    """
    Rewrites an account's SOLANA_TRACKER_API_KEY / _DISCOVERY in its own
    .env and restarts its service so the new keys take effect immediately
    — added 2026-09-29 so a new (or credit-exhausted) account's keys can
    be set/replaced through the control panel instead of editing a file
    on the server by hand. Deliberately per-account, never touches any
    OTHER account's keys or .env — the whole point of these being split
    per account in the first place (see CLAUDE.md's SolanaTracker-outage
    history) is that one account's credit exhaustion can't starve another's.
    """
    env_path = env_path_for(name)
    if not env_path.exists():
        raise ProvisioningError(f"No .env file found for account '{name}'.")

    lines = []
    for line in env_path.read_text().splitlines():
        if line.startswith("SOLANA_TRACKER_API_KEY="):
            lines.append(f"SOLANA_TRACKER_API_KEY={sampling_key}")
        elif line.startswith("SOLANA_TRACKER_API_KEY_DISCOVERY="):
            lines.append(f"SOLANA_TRACKER_API_KEY_DISCOVERY={discovery_key}")
        else:
            lines.append(line)
    env_path.write_text("\n".join(lines) + "\n")
    env_path.chmod(0o600)

    await _run("systemctl", "--user", "restart", service_name_for(name))


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
    dashboard_user, dashboard_password = generate_dashboard_credentials(name)

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
        elif line.startswith("DASHBOARD_AUTH_USER="):
            # Distinct per account (2026-09-29) — NOT copied from base, so
            # one account's password leaking doesn't open every account's
            # dashboard. See generate_dashboard_credentials().
            lines.append(f"DASHBOARD_AUTH_USER={dashboard_user}")
        elif line.startswith("DASHBOARD_AUTH_PASSWORD="):
            lines.append(f"DASHBOARD_AUTH_PASSWORD={dashboard_password}")
        else:
            lines.append(line)
    lines.append("")
    lines.append("# ── Account identity (added by engine/provisioning.py) ────────────────────")
    lines.append(f"S1WAVE_ACCOUNT_NAME=S1Wave-{name}")
    lines.append(f"# Public wallet address: {wallet_pubkey} — fund this address to activate.")
    env_path.write_text("\n".join(lines) + "\n")
    env_path.chmod(0o600)

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

    # Best-effort (2026-09-29): the account itself is real and running at
    # this point regardless of what happens next. A failure here (docker
    # briefly unavailable, an nginx config surprise) must not undo it —
    # it just stays reachable only on its own port until this is retried
    # (re-running create_account with the same name would fail on "already
    # exists" before reaching here, so retrying THIS step specifically is
    # a job for whatever calls this, not for create_account itself yet).
    nginx_configured = False
    try:
        await add_nginx_route(name, port)
        nginx_configured = True
    except ProvisioningError as exc:
        log.warning("provisioning.nginx_route_failed", account=name, error=str(exc))

    async with get_session() as session:
        session.add(ControlAccount(
            name=name, port=port, wallet_pubkey=wallet_pubkey,
            dashboard_auth_user=dashboard_user, dashboard_auth_password=dashboard_password,
            nginx_configured=nginx_configured,
        ))

    return ProvisionedAccount(name=name, port=port, wallet_pubkey=wallet_pubkey, env_file=str(env_path))
