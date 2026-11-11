CREATE SCHEMA IF NOT EXISTS journal;
CREATE TABLE journal.trades (
    trade_id TEXT PRIMARY KEY,
    current_revision INTEGER NOT NULL CHECK (current_revision > 0),
    status TEXT NOT NULL CHECK (status IN ('draft','planned','active','closed','cancelled','invalidated')),
    symbol TEXT NOT NULL,
    title TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('long','short')),
    contract_code TEXT,
    open_contracts BIGINT CHECK (open_contracts >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE journal.trade_revisions (
    trade_id TEXT NOT NULL REFERENCES journal.trades(trade_id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    snapshot JSONB NOT NULL CHECK (snapshot->>'schema_version' = '1'),
    PRIMARY KEY (trade_id, revision)
);
CREATE TABLE journal.entries (
    entry_id TEXT PRIMARY KEY,
    trade_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    kind TEXT NOT NULL CHECK (kind IN ('note','execution')),
    event_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    contract_code TEXT,
    quantity BIGINT CHECK (quantity > 0),
    price NUMERIC(30,10),
    fee NUMERIC(30,10),
    currency TEXT,
    supersedes_entry_id TEXT UNIQUE REFERENCES journal.entries(entry_id),
    payload JSONB NOT NULL,
    UNIQUE (trade_id, sequence),
    FOREIGN KEY (trade_id, revision) REFERENCES journal.trade_revisions(trade_id,revision),
    CHECK ((kind = 'execution') = (quantity IS NOT NULL AND contract_code IS NOT NULL))
);
CREATE TABLE journal.command_receipts (
    command_id TEXT PRIMARY KEY,
    payload_hash TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    receipt JSONB NOT NULL CHECK (receipt->>'schema_version' = '1'),
    FOREIGN KEY (trade_id,revision) REFERENCES journal.trade_revisions(trade_id,revision)
);
CREATE INDEX journal_trade_list ON journal.trades (created_at,trade_id);
CREATE INDEX journal_trade_status ON journal.trades (status,created_at,trade_id);
CREATE INDEX journal_entry_timeline ON journal.entries (trade_id,sequence);
CREATE FUNCTION journal.reject_history_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Journal history is immutable';
END;
$$;
CREATE TRIGGER immutable_revisions BEFORE UPDATE OR DELETE ON journal.trade_revisions
FOR EACH ROW EXECUTE FUNCTION journal.reject_history_mutation();
CREATE TRIGGER immutable_entries BEFORE UPDATE OR DELETE ON journal.entries
FOR EACH ROW EXECUTE FUNCTION journal.reject_history_mutation();
CREATE TRIGGER immutable_receipts BEFORE UPDATE OR DELETE ON journal.command_receipts
FOR EACH ROW EXECUTE FUNCTION journal.reject_history_mutation();
