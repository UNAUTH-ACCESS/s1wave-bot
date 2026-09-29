"""
control_panel/cli.py
======================
Command-line entry point for engine/provisioning.py — the terminal
equivalent of the control panel's "+ New Account" button, for whenever
that's faster than the web UI. Must run with S1WAVE_ENV_FILE=.env.control
(see scripts/create-account.sh) so it writes into the SAME registry the
web app reads from.

Usage:
    PYTHONPATH=. S1WAVE_ENV_FILE=.env.control .venv/bin/python -m control_panel.cli <name>
"""

from __future__ import annotations

import asyncio
import sys

from control_panel.models import Base
from database.engine import get_engine
from engine.provisioning import ProvisioningError, create_account


async def main(name: str) -> int:
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    try:
        result = await create_account(name)
    except ProvisioningError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("======================================================================")
    print(f"Account '{result.name}' created and running.")
    print(f"  Port:            {result.port} (http://127.0.0.1:{result.port} — not yet public)")
    print(f"  Wallet address:  {result.wallet_pubkey}")
    print(f"  Env file:        {result.env_file}")
    print()
    print("STILL NEEDED before this account can actually trade:")
    print(f"  1. Fund {result.wallet_pubkey} with SOL.")
    print(f"  2. Add real SOLANA_TRACKER_API_KEY / SOLANA_TRACKER_API_KEY_DISCOVERY")
    print(f"     values to {result.env_file} (left blank on purpose).")
    print(f"  3. Set CONFLUENCE_LIVE_ENABLED=True in {result.env_file} once ready, then:")
    print(f"     systemctl --user restart s1wave-bot-{result.name}.service")
    print("======================================================================")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <account-name>", file=sys.stderr)
        sys.exit(1)
    sys.exit(asyncio.run(main(sys.argv[1])))
