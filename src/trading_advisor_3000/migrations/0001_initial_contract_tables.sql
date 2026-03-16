-- Phase 1 migration skeleton
-- Runtime state tables for signals and execution intents.

CREATE TABLE IF NOT EXISTS app_signal_candidates (
    signal_id TEXT PRIMARY KEY,
    contract_id TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL,
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

CREATE TABLE IF NOT EXISTS app_decision_publications (
    signal_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    message_id TEXT NOT NULL,
    status TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS app_position_snapshots (
    account_id TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    broker_ts TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, contract_id, mode, broker_ts)
);
