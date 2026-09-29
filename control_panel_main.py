"""
control_panel_main.py
=======================
Process entry point for the S1Wave control panel (2026-09-29) — a
separate, much simpler process from main.py's full trading stack. No
workers, no wallet, no token discovery: just the login/homepage/account-
creation web app in control_panel/app.py, serving its own database
(s1wave_control, via .env.control).

Run via S1WAVE_ENV_FILE=.env.control (see the s1wave-control-panel
systemd unit) so config/settings.py loads the control panel's own .env
instead of a trading account's.
"""

from __future__ import annotations

import os

import uvicorn

from config.logging import configure_logging, get_logger
from config.settings import settings

log = get_logger(__name__)


def main() -> None:
    configure_logging(level=os.getenv("LOG_LEVEL", "INFO"))
    log.info("control_panel.starting", host=settings.API_HOST, port=settings.API_PORT)
    uvicorn.run(
        "control_panel.app:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
