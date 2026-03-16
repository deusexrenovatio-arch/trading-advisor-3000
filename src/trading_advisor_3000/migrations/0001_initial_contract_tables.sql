-- Phase 1 migration skeleton
-- Runtime state tables for signals and execution intents.

CREATE TABLE IF NOT EXISTS app_signal_candidates (
    signal_id TEXT PRIMARY KEY,
    contract_id TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    side TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    ts_decision TIMESTAMPTZ NOT NULL,
    dataset_version TEXT NOT NULL,
    snapshot_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS app_order_intents (
    intent_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    limit_price DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL
);
