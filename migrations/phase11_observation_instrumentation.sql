-- Phase 11 migration
-- Instrumentation only: no scoring, threshold, gate, or trading behavior
-- changes ride along with this migration. Purely additive, safe on a live
-- DB — no existing column is altered or dropped, no historical row is
-- rewritten.

-- Anchor for the bounded post-rejection observation window. Distinct from
-- discovered_at because a Scorer-discarded token may have been discovered
-- much earlier than the moment it was discarded — using discovered_at as
-- the window clock for that case would be wrong (the window could already
-- be expired the instant the token enters OBSERVING).
ALTER TABLE tokens ADD COLUMN IF NOT EXISTS observation_started_at TIMESTAMPTZ;

-- How the observation window ended (OBSERVE_WINDOW_ELAPSED, TOKEN_GONE_*,
-- etc.) recorded SEPARATELY from rejection_reason, which always holds the
-- original gate verdict (why Tier1 or the Scorer rejected the token in the
-- first place). Before this migration, sampling_worker's age-out path
-- overwrote rejection_reason with the generic elapsed/gone reason, losing
-- the original verdict. That is fixed as part of this same change (see
-- workers/sampling_worker.py) — flagging it here since it's a real
-- behavior change, just not a threshold/scoring one.
ALTER TABLE tokens ADD COLUMN IF NOT EXISTS observation_exit_reason VARCHAR(64);

COMMENT ON COLUMN tokens.observation_started_at IS
    'When this token entered OBSERVING (Tier1 reject or Scorer discard). Anchor for the bounded post-rejection observation window — NOT the same as discovered_at.';
COMMENT ON COLUMN tokens.observation_exit_reason IS
    'How the observation window ended (elapsed / token vanished). Kept separate from rejection_reason, which always holds the original gate verdict.';
