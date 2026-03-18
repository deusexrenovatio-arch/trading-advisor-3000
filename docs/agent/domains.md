# Execution Domains And Change Surfaces

## Purpose
Keep changes predictable by assigning each patch to explicit ownership surfaces.

## Shell Surfaces

| Surface | Owns | Guarded paths | Minimum check |
| --- | --- | --- | --- |
| `GOV-POLICY` | root governance rules | `AGENTS.md`, `CODEOWNERS`, `.githooks/*` | policy validators |
| `GOV-DOCS` | hot/warm docs | `docs/agent/*`, `docs/checklists/*`, `docs/workflows/*` | docs + contract checks |
| `GOV-RUNTIME` | lifecycle/gates/validators | `scripts/*`, `configs/*` | loop gate |
| `PROCESS-STATE` | plans/memory/task ledgers | `plans/*`, `memory/*`, `docs/tasks/*` | state validators |
| `CTX-CONTRACTS` (high-risk) | policy contracts and durable schemas | `configs/*`, `plans/*`, `memory/*`, contract validators | contract validators + loop gate |
| `ARCH-DOCS` | architecture package | `docs/architecture/*`, `tests/architecture/*` | architecture policy + tests |
| `CTX-DATA` | data ingestion/canonical flows | `src/trading_advisor_3000/app/data_plane/*` | data-plane tests |
| `CTX-RESEARCH` | research/analysis surfaces | `src/trading_advisor_3000/app/research/*` | research tests |
| `CTX-ORCHESTRATION` | runtime/execution wiring | `src/trading_advisor_3000/app/runtime/*`, `app/execution/*` | orchestration tests |
| `CTX-API-UI` | interface and API surfaces | `src/trading_advisor_3000/app/interfaces/*` | interface tests |
| `CTX-DOMAIN` | residual domain internals | `src/trading_advisor_3000/app/domain/*` | domain tests |

## High-Risk Policy
Changes touching policy + runtime + state simultaneously are high-risk.

Required split order:
1. contracts/policy
2. runtime code
3. docs/reports

## Context Card Freeze
- `CTX-STRATEGY` migrated to `CTX-DOMAIN`.
- `CTX-NEWS` migrated to `CTX-EXTERNAL-SOURCES`.
- App-space ownership is not coarse single-context: use `CTX-DATA`, `CTX-RESEARCH`, `CTX-ORCHESTRATION`, `CTX-API-UI`.
