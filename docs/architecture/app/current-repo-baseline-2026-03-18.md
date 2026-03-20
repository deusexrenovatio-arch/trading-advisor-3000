# Current Repo Baseline - 2026-03-18

## Purpose

This document records the engineering baseline that Phase 9 builds on top of.
It does not replace [STATUS.md](docs/architecture/app/STATUS.md); it explains what is already real, what remains partial,
and which external systems are already part of the accepted boundary map.

## Canonical truth sources

Read these first when Phase 9 docs disagree with older wording:

- `docs/architecture/app/STATUS.md`
- `docs/architecture/app/CONTRACT_SURFACES.md`
- `docs/runbooks/app/bootstrap.md`
- `docs/architecture/app/phase4-live-execution-integration.md`
- `docs/architecture/app/phase6-operational-hardening.md`
- `docs/architecture/app/phase8-ci-pilot-operational-proving.md`

## Implemented repo reality

| Surface | Current state | What is already real |
| --- | --- | --- |
| Product-plane landing | implemented | App code, tests, docs, and deployment bundles already live inside the shell repo. |
| Contracts baseline | implemented / partial | Versioned schemas, fixtures, and compatibility tests exist for market, signal, runtime, publication, and execution payloads. |
| Data plane scaffolding | implemented | Provider seams, ingestion seams, canonical builder, and acceptance slices already exist. |
| Research plane scaffolding | implemented | Features, backtest, replay, and strategy seams already exist. |
| Runtime signal lifecycle | implemented | Create, update, close, cancel, expire, and publication traceability are already modeled. |
| Durable runtime state | partial | Postgres migrations, store, and restart/idempotency coverage exist, but battle-run mode is not yet forced to Postgres by default. |
| Paper execution | implemented | Paper intent, sync, and reconciliation baseline already exist. |
| Live execution transport | partial | Python bridge, HTTP transport, staging gateway stub, and rollout runbook are landed. |
| Real sidecar process | not implemented | No real `.NET` StockSharp sidecar project or pinned external build contract is landed yet. |
| Real broker execution | not implemented | `StockSharp -> QUIK -> Finam` exists as route contract and rollout envelope only. |
| Operational hardening | implemented | Retry, fail-closed startup, secret policy, production-like profile, and recovery flow exist. |
| Operational proving | implemented | Phase 8 proving script and evidence flow are already landed. |

## External integration baseline

Phase 9 starts from these explicit outside-the-repo systems and boundaries:

| External system | Baseline role now | Current status | Reading rule |
| --- | --- | --- | --- |
| `MOEX` | historical data and backfill source for pilot research closure | planned for Phase 9 | Treat as the fixed historical source for the Phase 9 docs package. |
| `QUIK` | live market feed for battle runs and broker route hop for canary path | partial / route-only | Keep live-feed semantics separate from broker execution semantics. |
| `Telegram` | signal publication and lifecycle surface | implemented at contract/runtime level | Phase 9 closes real shadow/advisory operating contour, not just stub behavior. |
| `PostgreSQL` | durable runtime state, migrations, and battle-run state source | partial | Existing store is real, but battle-run default path still needs explicit closure. |
| `HTTP sidecar gateway` | network boundary from Python runtime into execution sidecar | partial | Current gateway is a staging-first contract surface, not proof of real execution readiness. |
| `StockSharp` | execution sidecar runtime | partial / contract-only | No real in-repo sidecar process is landed yet. |
| `Finam` | broker route behind sidecar | planned | Keep out of Phase 9A acceptance; it opens only in Phase 9B. |
| `Prometheus / Loki / Grafana` | observability, metrics, logs, and evidence snapshots | implemented | Reuse current observability bundle, do not invent a second stack. |
| `MCP` | dev/ops support and read-only inspection | implemented with restrictions | Not a runtime dependency and must not carry live broker credentials. |

## Existing repo packages that Phase 9 must reuse

- `src/trading_advisor_3000/app/data_plane/*`
- `src/trading_advisor_3000/app/research/*`
- `src/trading_advisor_3000/app/runtime/*`
- `src/trading_advisor_3000/app/execution/*`
- `deployment/docker/staging-gateway/*`
- `deployment/docker/observability/*`
- `deployment/mcp/*`
- `deployment/stocksharp-sidecar/*`

## What Phase 9 must not do

1. Do not rewrite accepted Phase 0-8 artifacts instead of adding a Phase 9 layer.
2. Do not describe the staging gateway stub as a closed real broker path.
3. Do not treat green `tests/app` or green Phase 8 proving as proof of production readiness.
4. Do not collapse the two `QUIK` roles into one undifferentiated integration.
5. Do not place live secrets in tracked files or repo-managed MCP config.
6. Do not build battle runs on in-memory runtime state.

## Baseline commands that must remain valid

```bash
python -m pip install -e .[dev]
python -m pytest tests/app -q
python scripts/run_loop_gate.py --from-git --git-ref HEAD
python scripts/run_pr_gate.py --from-git --git-ref HEAD
python scripts/apply_app_migrations.py --dsn <POSTGRES_DSN>
python scripts/run_phase8_operational_proving.py --from-git --git-ref HEAD --output artifacts/phase8-operational-proving.json
```
