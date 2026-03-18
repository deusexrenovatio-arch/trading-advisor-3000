# App Migrations

This directory stores ordered PostgreSQL migrations for the Trading Advisor 3000 product plane.

## Layout
- `0001_initial_contract_tables.sql` keeps the original contract-table baseline.
- `0002_signal_runtime_state.sql` adds durable runtime state for signals, events, and publications.
- `versions/` remains reserved for future split/archived migration chains.

## Canonical Apply Command
Use the Python runner from the repository root:

```bash
python scripts/apply_app_migrations.py --dsn postgresql://postgres:postgres@127.0.0.1:5432/ta3000
```

You can also set `TA3000_APP_DSN` and omit `--dsn`.

## Rules
1. Migration files are immutable after they land.
2. New migrations must be idempotent where practical.
3. Schema changes must be accompanied by tests and contract/docs updates.
