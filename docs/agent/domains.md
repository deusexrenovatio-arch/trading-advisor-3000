# Execution Domains And Change Surfaces

## Purpose
Keep changes predictable by assigning each patch to explicit ownership surfaces.

## Shell surfaces

| Surface | Owns | Guarded paths | Minimum check |
| --- | --- | --- | --- |
| `GOV-POLICY` | root governance rules | `AGENTS.md`, `CODEOWNERS`, `.githooks/*` | policy validators |
| `GOV-DOCS` | hot/warm docs | `docs/agent/*`, `docs/checklists/*`, `docs/workflows/*` | docs link + contract checks |
| `GOV-RUNTIME` | lifecycle/gates/validators | `scripts/*`, `configs/*` | loop gate |
| `PROCESS-STATE` | plans/memory/task ledgers | `plans/*`, `memory/*`, `docs/tasks/*` | state validators |
| `CTX-CONTRACTS` (high-risk) | policy contracts and durable schemas | `configs/*`, `plans/*`, `memory/*`, contract validators | contract validators + loop gate |
| `ARCH-DOCS` | architecture package | `docs/architecture/*`, `tests/architecture/*` | architecture policy + tests |
| `APP-PLACEHOLDER` | neutral app package | `src/trading_advisor_3000/*`, `tests/app/*` | app smoke tests |

## High-risk policy
Changes touching policy + runtime + state simultaneously are high-risk.

Required split order:
1. contracts/policy
2. runtime code
3. docs/reports

## Context card freeze
- `CTX-STRATEGY` migrated to `CTX-DOMAIN`.
- `CTX-NEWS` migrated to `CTX-EXTERNAL-SOURCES`.
