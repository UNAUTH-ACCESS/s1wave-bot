"""
Dataset downloads (behind the dashboard's Basic Auth, which wraps every route).

  GET /export/index              table list with row counts (drives the dashboard card)
  GET /export/{table}.csv.gz     one table, streamed as gzip CSV (never loaded into memory)
  GET /export/handoff.zip        the "AI handoff pack": docs, a fresh calibration report,
                                 schema, small data samples, key code. Small enough to
                                 attach to a chat with a fresh AI session.

Only whitelisted tables are exposed, and handoff.zip only contains whitelisted
repo files — never env files, keys, or the notebook.
"""
from __future__ import annotations

import csv
import io
import json
import zipfile
import zlib
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import func, select

from database.engine import get_session
from models.orm import (
    ConfluenceLiveObservation, ConfluenceLiveTrade, ConfluenceShadowExecCheck,
    ConfluenceShadowObservation, ConfluenceShadowPosition, MomentumSignalEvent,
    Token, TokenEvaluation, TokenSnapshot,
)

router = APIRouter(tags=["export"])
ROOT = Path(__file__).resolve().parent.parent

TABLES = {
    "tokens": (Token, "Every token the bot has discovered"),
    "snapshots": (TokenSnapshot, "Price/volume/liquidity time series per token"),
    "evaluations": (TokenEvaluation, "Gate verdicts with their raw inputs"),
    "signals": (MomentumSignalEvent, "Momentum signal events (entry candidates)"),
    "shadow_positions": (ConfluenceShadowPosition, "Paper trades (incl. skipped candidates)"),
    "shadow_observations": (ConfluenceShadowObservation, "1-second price series of paper trades"),
    "shadow_exec_checks": (ConfluenceShadowExecCheck, "Modeled executable-exit checks"),
    "live_trades": (ConfluenceLiveTrade, "Real-money trades"),
    "live_observations": (ConfluenceLiveObservation, "Price checks on open real trades"),
}

HANDOFF_FILES = [
    "CLAUDE.md", "CALIBRATION.md", "README.md", "models/orm.py",
    "workers/entry_filters.py", "workers/momentum_signal.py", "engine/trailing_stop.py",
    "engine/shadow_exec_model.py", "engine/filter_calibration.py",
]
SAMPLE_ROWS = 300


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"))
    return str(v)


def _columns(model):
    return list(model.__table__.columns)


async def _csv_chunks(model, limit: int | None = None):
    cols = _columns(model)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([c.name for c in cols])
    yield buf.getvalue()
    q = select(*cols)
    pk = list(model.__table__.primary_key.columns)
    if pk:
        q = q.order_by(*pk)
    if limit:
        q = q.limit(limit)
    async with get_session() as session:
        stream = await session.stream(q.execution_options(yield_per=2000))
        batch = 0
        buf = io.StringIO()
        w = csv.writer(buf)
        async for row in stream:
            w.writerow([_fmt(v) for v in row])
            batch += 1
            if batch >= 2000:
                yield buf.getvalue()
                buf.seek(0)
                buf.truncate()
                batch = 0
        if batch:
            yield buf.getvalue()


async def _gzip_stream(model):
    comp = zlib.compressobj(6, zlib.DEFLATED, 31)
    async for chunk in _csv_chunks(model):
        data = comp.compress(chunk.encode("utf-8"))
        if data:
            yield data
    yield comp.flush()


async def _csv_text(model, limit: int) -> str:
    return "".join([c async for c in _csv_chunks(model, limit)])


@router.get("/export/index")
async def export_index() -> dict:
    out = []
    async with get_session() as session:
        for name, (model, desc) in TABLES.items():
            n = (await session.execute(select(func.count()).select_from(model))).scalar_one()
            out.append({"name": name, "description": desc, "rows": n})
    return {"tables": out}


@router.get("/export/handoff.zip")
async def export_handoff() -> Response:
    from analysis.calibration_report import build_report

    now = datetime.now(timezone.utc)
    counts = (await export_index())["tables"]
    manifest = f"""# S1Wave AI handoff pack — {now:%Y-%m-%d %H:%M}Z

Give this whole zip to a fresh AI session and say: "Read MANIFEST.md, CALIBRATION.md and CLAUDE.md,
then continue calibrating this trading bot from calibration_report.md."

## Contents
- `calibration_report.md` — fresh standard battery (data health, live results, shadow, paired gap, filter buckets)
- `CALIBRATION.md` — method, source-of-truth hierarchy, data catalog, open questions
- `CLAUDE.md` / `README.md` — operating rules, architecture, history, runbook
- `schema.json` — every exported table and its columns
- `samples/*.csv` — the first {SAMPLE_ROWS} rows of each table (full tables: dashboard, Data export, or `/export/<table>.csv.gz`)
- `code/` — ORM models, entry filters, signal rules, exit logic, shadow model, and every analysis script

## Table sizes at export time
""" + "\n".join(f"- {t['name']}: {t['rows']:,} rows — {t['description']}" for t in counts) + """

## Rules for the assistant
No secrets in output; never change live trading logic without the operator's explicit go-ahead;
analysis is read-only. This pack deliberately contains no keys, env files, or wallet data.
"""
    try:
        report = await build_report()
    except Exception as exc:  # a report failure must not block the pack
        report = f"calibration report failed: {exc}"

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("MANIFEST.md", manifest)
        z.writestr("calibration_report.md", report)
        z.writestr("schema.json", json.dumps({
            name: [{"column": c.name, "type": str(c.type)} for c in _columns(model)]
            for name, (model, _) in TABLES.items()
        }, indent=2))
        for name, (model, _) in TABLES.items():
            z.writestr(f"samples/{name}.csv", await _csv_text(model, SAMPLE_ROWS))
        for rel in HANDOFF_FILES:
            f = ROOT / rel
            if f.exists():
                z.write(f, rel if "/" not in rel and rel.endswith(".md") else f"code/{rel}")
        for f in sorted((ROOT / "analysis").glob("*.py")):
            z.write(f, f"code/analysis/{f.name}")
    return Response(
        mem.getvalue(), media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="s1wave_ai_handoff_{now:%Y%m%d}.zip"'},
    )


@router.get("/export/{table}.csv.gz")
async def export_table(table: str) -> StreamingResponse:
    if table not in TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown table. Options: {', '.join(TABLES)}")
    return StreamingResponse(
        _gzip_stream(TABLES[table][0]), media_type="application/gzip",
        headers={"Content-Disposition": f'attachment; filename="s1wave_{table}_{datetime.now(timezone.utc):%Y%m%d}.csv.gz"'},
    )
