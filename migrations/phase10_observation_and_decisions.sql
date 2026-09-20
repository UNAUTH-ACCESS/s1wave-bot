-- Phase 10 migration
-- Widens observation to every discovered token (not just Tier1 survivors)
-- and adds the decision trail that didn't exist before.
-- Run once before deploying Phase 10. Safe on a live DB — additive only.

-- New lifecycle state: a token that failed a gate (Tier1, so far) but is
-- still sampled for a short, bounded window before moving to REJECTED —
-- see OBSERVE_WINDOW_SECONDS in sampling_worker.py. This is what lets
-- token_snapshots capture early behavior for tokens the bot never traded,
-- not just the ones that passed the hard gate. Bounded specifically so
-- this can never grow the sampling batch without limit — a rejected token
-- costs a few extra rows in an already-scheduled batch call, then stops.
ALTER TYPE token_status ADD VALUE IF NOT EXISTS 'OBSERVING';

-- Decision trail: one row per gate evaluation (Tier1 so far; S1 Wave is
-- wired in the same pass), whether or not it led to a trade. Before this,
-- a rejection was only ever a structlog line — nothing queryable, and the
-- one column that DID exist (tokens.rejection_reason) gets overwritten by
-- whichever gate touched it last, so a token evaluated by two different
-- gates only ever remembers the most recent verdict.
--
-- inputs_json is deliberately schema-less: Tier1, S1 Wave, and (later) the
-- Scorer each look at a different set of fields, and this table shouldn't
-- need a migration every time a gate's feature set changes.
--
-- Outcome labels (rug / faded / pumped, peak multiple, time-to-death, etc.)
-- are NOT stored here or anywhere — they're derived on demand from
-- token_snapshots by the analysis pass, so a label is always a
-- reproducible formula over raw data, never a value someone set by hand.
CREATE TABLE IF NOT EXISTS token_evaluations (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_id      UUID NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    evaluated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    gate          VARCHAR(16) NOT NULL,      -- TIER1 | S1_WAVE (SCORER later)
    passed        BOOLEAN NOT NULL,
    reason_code   VARCHAR(64),               -- e.g. LOW_LIQUIDITY, skip_low_wash
    inputs_json   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_token_evaluations_token_id
    ON token_evaluations(token_id, evaluated_at);
CREATE INDEX IF NOT EXISTS ix_token_evaluations_gate
    ON token_evaluations(gate, passed);

COMMENT ON TABLE token_evaluations IS
    'Decision trail: one row per gate look at a token, pass or fail, with the exact inputs seen at that moment. token_snapshots holds what the market did; trades holds what we did about it; this holds why a gate said yes or no. Outcome labels are always derived later from token_snapshots, never stored here.';
