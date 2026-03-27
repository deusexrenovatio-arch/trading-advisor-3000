# Evidence map

This map records the key repository surfaces used for validation.

## Target stack and target architecture

- `docs/architecture/app/product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md`
  - target product state includes Delta Lake, Spark, Dagster, PostgreSQL, vectorbt, FastAPI, aiogram, Alembic, OpenTelemetry, Docker Compose, .NET 8 sidecar, StockSharp.
- `docs/architecture/app/product-plane-spec-v2/01_Architecture_Overview.md`
  - data plane technologies: Delta Lake, Spark, Polars/DuckDB, Dagster
  - research plane technologies: Python, vectorbt, Delta, Dagster
  - runtime plane technologies: Python, PostgreSQL, FastAPI, aiogram
  - execution plane technologies: Python contracts, .NET 8 StockSharp sidecar
- `docs/architecture/app/product-plane-spec-v2/07_Tech_Stack_and_Open_Source.md`
  - selected stack inventory.

## Phase docs proving lighter acceptance model

- `docs/architecture/app/phase2a-data-plane-mvp.md`
  - full Delta/Spark runtime wiring out of scope
  - executable skeletons without external runtime dependency
- `docs/architecture/app/phase2b-research-plane-mvp.md`
  - Delta-compatible JSONL sample outputs
  - distributed Delta/Spark runtime orchestration out of scope
- `docs/architecture/app/phase2c-runtime-mvp.md`
  - in-memory signal store
  - external Telegram API network calls and persistent PostgreSQL out of scope
- `docs/architecture/app/phase2d-execution-mvp.md`
  - sidecar stub baseline
  - live broker transport and real StockSharp process out of scope
- `docs/architecture/app/phase4-live-execution-integration.md`
  - direct network transport to real broker endpoints still out of scope

## Checklists overclaiming closure

- `docs/checklists/app/phase2a-acceptance-checklist.md`
  - marked as MVP and full module DoD despite Phase 2A out-of-scope runtime wiring
- `docs/checklists/app/phase2b-acceptance-checklist.md`
  - marked as MVP and full module DoD despite JSONL-only research artifacts
- `docs/checklists/app/phase4-acceptance-checklist.md`
  - controlled-live phase accepted while sidecar README still says no real .NET sidecar exists

## Tests proving proxy evidence

- `tests/app/unit/test_phase2a_manifests.py`
  - manifest, asset specs, and SQL plan checks
- `tests/app/integration/test_phase2a_data_plane.py`
  - JSONL sample outputs + manifest keys
- `tests/app/integration/test_phase2b_research_plane.py`
  - JSONL sample outputs + manifest checks

## Code surfaces proving current implementation shape

- `src/trading_advisor_3000/app/data_plane/pipeline.py`
  - writes `*.sample.jsonl` outputs
- `src/trading_advisor_3000/app/research/backtest/engine.py`
  - writes `*.sample.jsonl` outputs and returns a manifest
- `src/trading_advisor_3000/spark_jobs/canonical_bars_job.py`
  - SQL plan builders
- `src/trading_advisor_3000/dagster_defs/phase2a_assets.py`
  - asset specs only
- `src/trading_advisor_3000/app/runtime/pipeline.py`
  - default fallback to `InMemorySignalStore`
- `src/trading_advisor_3000/app/interfaces/api/runtime_api.py`
  - plain Python `RuntimeAPI` class, not FastAPI
- `src/trading_advisor_3000/app/runtime/publishing/telegram.py`
  - custom in-memory publication engine
- `src/trading_advisor_3000/app/execution/adapters/stocksharp_http_transport.py`
  - real Python HTTP transport exists
- `src/trading_advisor_3000/app/runtime/signal_store/postgres.py`
  - durable Postgres signal store exists
- `scripts/apply_app_migrations.py`
  - custom checksum-based migration runner
- `src/trading_advisor_3000/migrations/*`
  - ordered SQL migrations, not Alembic tree

## Truth-source docs correcting narrative

- PR #2 description
  - rebaselines acceptance claims to implemented reality
- `docs/architecture/app/STATUS.md`
  - live/prod readiness not accepted; durable runtime partial; real .NET sidecar not landed
- `docs/architecture/app/phase0-3-acceptance-verdict-2026-03-17.md`
  - scaffold and partial integration accepted, not full target closure
- `deployment/stocksharp-sidecar/README.md`
  - explicitly states no real .NET sidecar implementation in repo
- `docs/architecture/app/README.md`
  - current app docs index; phases listed through Phase 8 + MCP rollout

## CI/gate evidence

- `docs/agent/checks.md`
  - process/gate matrix, no stack-runtime proof
- `.github/workflows/ci.yml`
  - installs only `pytest` and `pyyaml` before running tests/gates

## Observability reality

- `docs/architecture/app/phase5-review-analytics-observability.md`
  - file-contract based metrics/log exports for reproducible acceptance
- `deployment/docker/observability/docker-compose.observability.yml`
  - local compose profile for Prometheus / Loki / Grafana file bridge

## Non-verifiable exact references from external note

The following exact filenames were **not** found in current `main` or in the uploaded product spec package reviewed here:

- `current-repo-baseline-2026-03-18.md`
- `phase9-battle-runs-and-real-signal-rollout.md`
- `phase9-phase9a-battle-run-integration.md`

Equivalent current evidence exists, but these specific files were not directly validated.
