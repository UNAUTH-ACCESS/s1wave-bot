-- Phase 21 migration
-- Resume-after-halt override (2026-09-28) — a human's explicit "resume"
-- after a permanent halt. See models/orm.py's ConfluenceLiveHaltOverride
-- docstring for the full design: this does NOT disable the safety check,
-- it resets its baselines to the moment of acknowledgment so the same
-- CONFLUENCE_LIVE_MAX_LOSS_USD cap still protects every dollar from here
-- forward.

CREATE TABLE IF NOT EXISTS confluence_live_halt_override (
    id                   INTEGER PRIMARY KEY DEFAULT 1,
    acknowledged_at      TIMESTAMPTZ NOT NULL,
    equity_baseline_usd  NUMERIC(20,6) NOT NULL,
    pnl_baseline_usd     NUMERIC(20,6) NOT NULL,
    CONSTRAINT confluence_live_halt_override_singleton CHECK (id = 1)
);

COMMENT ON TABLE confluence_live_halt_override IS
    'Singleton row (id=1) — resets the permanent-halt safety check''s baselines to the moment a human acknowledges and resumes trading after a halt, rather than disabling the check.';
