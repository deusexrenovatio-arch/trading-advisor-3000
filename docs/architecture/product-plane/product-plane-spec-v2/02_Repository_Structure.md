# 02. Repository Structure

## 1. Принцип

Репозиторий уже имеет shell-структуру. Product development должен **достраивать** её, а не ломать.

## 2. Существующие top-level зоны

| Путь | Назначение | Владелец | Риск |
| --- | --- | --- | --- |
| `.codex/skills/` | TA3000-specific repo-local Codex skills | shell/product-plane bridge | medium |
| `.githooks/` | PR-only and push protection | shell | high |
| `configs/` | shell contracts / validators / routing configs | shell | high |
| `docs/agent/` | hot docs | shell | high |
| `docs/architecture/` | shell architecture package | shell/shared | medium |
| `memory/` | durable memory | shell | high |
| `plans/` | durable planning | shell | high |
| `scripts/` | lifecycle, validators, gates | shell | high |
| `src/trading_advisor_3000/` | application plane | product | low/medium |
| `tests/process/` | shell process suite | shell | medium |
| `tests/architecture/` | shell architecture suite | shell | medium |
| `tests/product-plane/` | application tests | product | low |

## 3. Рекомендуемая product structure

```text
src/trading_advisor_3000/
  AGENTS.md
  app/
    common/
    contracts/
    domain/
    config/
    data_plane/
      ingestion/
      canonical/
      schemas/
    research/
      features/
      backtest/
      forward/
      strategies/
    runtime/
      decision/
      signal_store/
      publishing/
      analytics/
    execution/
      intents/
      broker_sync/
      reconciliation/
      adapters/
    interfaces/
      api/
      telegram/
  dagster_defs/
  spark_jobs/
  migrations/

tests/product-plane/
  unit/
  contracts/
  integration/
  e2e/
  fixtures/

docs/
  architecture/
    shell/
    app/
  runbooks/
    app/
  workflows/
    app/
  checklists/
    app/

deployment/
  docker/
  stocksharp-sidecar/
  mcp/
```

## 4. Размещение AGENTS

### Root `AGENTS.md`
Остаётся shell-first и описывает process/governance.

### `src/trading_advisor_3000/AGENTS.md`
Рекомендуется добавить как product-specific overlay.
Он должен:
- уточнять правила для application plane,
- не дублировать shell policy без необходимости,
- усиливать product constraints.

## 5. Где НЕ размещать product concerns

### Не default location
- root `configs/` — shell high-risk path;
- `plans/` — это о процессе, а не о бизнес-данных;
- `memory/` — это durable dev memory, а не product runtime;
- `scripts/` — shell runtime entrypoints; app utilities можно держать внутри `src/` или `tools/`, если специально выделены.

## 6. Ownership boundaries

### Shell ownership
- `docs/agent/*`
- `scripts/*`
- `plans/*`
- `memory/*`
- `.githooks/*`
- `.github/workflows/*`

### Product ownership
- `src/trading_advisor_3000/*`
- `tests/product-plane/*`
- `docs/architecture/product-plane/*`
- `docs/runbooks/app/*`
- `deployment/*`

### Shared governance paths
- `docs/architecture/*`
- `.codex/skills/*`
- `CODEOWNERS`
- CI definitions

Такие пути меняются отдельными patch set'ами.

## 7. Рекомендации по параллелизации

### Поток A — data/research
- `src/trading_advisor_3000/product_plane/data_plane/*`
- `src/trading_advisor_3000/product_plane/research/*`

### Поток B — runtime/publishing
- `src/trading_advisor_3000/product_plane/runtime/*`
- `src/trading_advisor_3000/product_plane/interfaces/telegram/*`

### Поток C — execution
- `src/trading_advisor_3000/product_plane/execution/*`
- `deployment/stocksharp-sidecar/*`

### Поток D — docs/runbooks
- `docs/architecture/product-plane/*`
- `docs/runbooks/app/*`
- `tests/product-plane/e2e/*`

Эти потоки можно вести параллельно после contracts-and-scaffolding freeze.
