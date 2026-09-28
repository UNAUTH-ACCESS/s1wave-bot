-- Phase 20 migration
-- Itemized fee breakdown (2026-09-28, second pass on the real-audit-trail
-- work) — "all fees and capital paid" needed the pieces visible
-- separately: the real Solana network fee per transaction, distinct from
-- the non-reclaimable Pump.fun protocol-fee cost already captured in
-- entry_real_sol_lamports/exit_real_sol_lamports (phase19).

ALTER TABLE confluence_live_trades
    ADD COLUMN IF NOT EXISTS entry_network_fee_lamports  BIGINT,
    ADD COLUMN IF NOT EXISTS exit_network_fee_lamports    BIGINT;

COMMENT ON COLUMN confluence_live_trades.entry_network_fee_lamports IS
    'Real Solana network fee charged on the entry transaction, from its own meta.fee.';
COMMENT ON COLUMN confluence_live_trades.exit_network_fee_lamports IS
    'Real Solana network fee charged on the exit (sell) transaction, from its own meta.fee.';
