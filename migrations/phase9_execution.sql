-- Phase 9 migration
-- Live execution fields on trades table
-- Run once before deploying Phase 9. Safe on a live DB — additive only.

-- Exact token amount received from buy swap.
-- Used to sell the precise amount held, not an estimate.
-- NULL for simulation trades.
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS token_amount_bought NUMERIC(30, 12);

-- Buy transaction signature for auditability and reconciliation.
-- NULL for simulation trades.
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS tx_signature_entry VARCHAR(128);

-- Sell transaction signature.
-- NULL for simulation trades or if sell not yet executed.
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS tx_signature_exit VARCHAR(128);

COMMENT ON COLUMN trades.token_amount_bought IS
    'Exact token lamports received from Jupiter buy swap. Used for precise sell sizing. NULL in paper trading mode.';

COMMENT ON COLUMN trades.tx_signature_entry IS
    'Solana transaction signature for the entry swap. NULL in paper trading mode.';

COMMENT ON COLUMN trades.tx_signature_exit IS
    'Solana transaction signature for the exit swap. NULL in paper trading mode.';
