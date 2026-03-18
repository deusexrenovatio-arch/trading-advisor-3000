-- Phase 2C durable runtime state baseline
-- PostgreSQL schema for active signals, signal events, and publication history.

CREATE SCHEMA IF NOT EXISTS signal;

CREATE TABLE IF NOT EXISTS signal.active_signals (
    signal_id TEXT PRIMARY KEY,
    strategy_version_id TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price DOUBLE PRECISION NOT NULL,
    stop_price DOUBLE PRECISION NOT NULL,
    target_price DOUBLE PRECISION NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    state TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NULL,
    publication_message_id TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_active_signals_contract_state
    ON signal.active_signals (contract_id, state, updated_at);

CREATE TABLE IF NOT EXISTS signal.signal_events (
    event_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL REFERENCES signal.active_signals (signal_id) ON DELETE CASCADE,
    event_ts TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    dedup_key TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_signal_events_signal_ts
    ON signal.signal_events (signal_id, event_ts);

CREATE TABLE IF NOT EXISTS signal.publications (
    publication_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL REFERENCES signal.active_signals (signal_id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    message_id TEXT NOT NULL,
    publication_type TEXT NOT NULL,
    status TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    dedup_key TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_publications_signal_ts
    ON signal.publications (signal_id, published_at DESC);
