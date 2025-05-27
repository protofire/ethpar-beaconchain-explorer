-- +goose NO TRANSACTION
-- +goose Up

SELECT 'creating idx_blocks_withdrawals_validatorindex_slot';
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_blocks_withdrawals_validatorindex_slot ON blocks_withdrawals (validatorindex, block_slot DESC);

-- +goose Down

SELECT 'dropping idx_blocks_withdrawals_validatorindex_slot';
DROP INDEX CONCURRENTLY IF EXISTS idx_blocks_withdrawals_validatorindex_slot;
