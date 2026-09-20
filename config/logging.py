"""
config/logging.py
=================
Structured JSON logging via structlog.

Writes to two destinations simultaneously:
  1. stdout       — JSON lines (production) or pretty-printed (dev)
  2. logs/bot.log — JSON lines, rotating at 10MB, keeping 5 files

The log file is always JSON regardless of ENV, so it can be downloaded
and parsed programmatically.  Rotation keeps total disk usage under ~50MB.

Usage
-----
    from config.logging import get_logger

    log = get_logger(__name__)
    log.info("token.discovered", mint=mint_address, symbol=symbol)
    log.error("enrichment.failed", mint=mint_address, error=str(e))
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from pathlib import Path

import structlog


# Log directory — created next to main.py
_LOG_DIR = Path("logs")
_MAX_BYTES = 10 * 1024 * 1024   # 10 MB per file
_BACKUP_COUNT = 5                # keep 5 rotated files → max ~50 MB

# Session log file — new file per process start, named by UTC timestamp
# Format: logs/session_20260506_143022.log
# This lets you compare sessions side by side via the API
import datetime as _dt
_SESSION_START = _dt.datetime.now(_dt.timezone.utc)
_SESSION_FILENAME = f"session_{_SESSION_START.strftime('%Y%m%d_%H%M%S')}.log"
_LOG_FILE = _LOG_DIR / _SESSION_FILENAME
# Symlink bot.log → current session for backward compatibility
_BOT_LOG_SYMLINK = _LOG_DIR / "bot.log"


def configure_logging(level: str = "INFO") -> None:
    """
    Configure structlog with dual output: stdout + rotating file.

    Call once at process startup before any workers start.
    """
    is_dev = os.getenv("ENV", "production").lower() in ("dev", "development", "local")

    # Ensure log directory exists
    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Create/update bot.log symlink → current session file
    try:
        if _BOT_LOG_SYMLINK.is_symlink() or _BOT_LOG_SYMLINK.exists():
            _BOT_LOG_SYMLINK.unlink()
        _BOT_LOG_SYMLINK.symlink_to(_SESSION_FILENAME)
    except Exception:
        pass  # non-fatal — symlink is convenience only

    # ── Shared structlog processors ───────────────────────────────────────
    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # ── File handler — always JSON, rotating ─────────────────────────────
    file_handler = logging.handlers.RotatingFileHandler(
        filename=str(_LOG_FILE),
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.getLevelName(level.upper()))

    # ── Stdout handler ────────────────────────────────────────────────────
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.getLevelName(level.upper()))

    # ── Root stdlib logger — both handlers ───────────────────────────────
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.getLevelName(level.upper()))
    root_logger.handlers.clear()
    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(file_handler)

    # ── Structlog renderer ────────────────────────────────────────────────
    # stdout: pretty in dev, JSON in production
    # file: always JSON (written via stdlib handler)
    if is_dev:
        stdout_renderer = structlog.dev.ConsoleRenderer(colors=False)
    else:
        stdout_renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=shared_processors + [
            # Render to JSON string then emit via stdlib so both handlers
            # receive every log line
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(level.upper())
        ),
        cache_logger_on_first_use=True,
    )

    # Formatter used by both handlers
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=shared_processors,
    )

    # Dev stdout gets a human-readable formatter instead
    if is_dev:
        dev_formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(colors=False),
            foreign_pre_chain=shared_processors,
        )
        stdout_handler.setFormatter(dev_formatter)
    else:
        stdout_handler.setFormatter(formatter)

    file_handler.setFormatter(formatter)

    # Suppress noisy third-party loggers
    for noisy in ("httpx", "httpcore", "asyncio", "sqlalchemy.engine"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


def log_file_path() -> Path:
    """Return the path to the current log file. Used by the API download endpoint."""
    return _LOG_FILE

