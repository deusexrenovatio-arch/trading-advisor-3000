# 04. ADRs

## ADR-001 — Monorepo split: control plane + application plane

**Статус:** accepted

### Контекст
Репозиторий уже является AI delivery shell.

### Решение
Не строить второй независимый repo.
Развивать product plane внутри текущего monorepo, но с чётким разделением по путям и ownership.

### Последствия
- shell governance остаётся единым;
- продукт получает встроенный control plane;
- требуется дисциплина по change surfaces.

---

## ADR-002 — Delta Lake как долгоживущий storage contract

**Статус:** accepted

### Контекст
Нужен формат данных, который не придётся быстро мигрировать.

### Решение
Использовать Delta Lake для исторических и аналитических таблиц.

### Последствия
- проще масштабировать data plane;
- можно использовать Spark для тяжёлых джобов и Polars/DuckDB для локального анализа;
- advanced Delta features включать только по мере необходимости.

---

## ADR-003 — Spark scoped, runtime stays Python

**Статус:** accepted

### Контекст
Нужен масштабируемый compute, но runtime должен быть управляемым и низко-фрикционным.

### Решение
Spark используется для ingestion rebuild / heavy transforms / batch recompute.
Runtime decisioning, signal lifecycle и publishing остаются Python services.

### Последствия
- нет hard dependency runtime на Spark;
- проще дебажить live flows;
- меньше operational coupling.

---

## ADR-004 — Dagster как orchestration layer

**Статус:** accepted

### Контекст
Нужен оркестратор для data/research контуров.

### Решение
Использовать Dagster как основной orchestrator для assets, checks и schedules.

### Последствия
- хорошо ложится на D1–D5;
- есть естественная интеграция с data quality;
- необходимо держать runtime plane отдельно.

---

## ADR-005 — vectorbt только для research/backtest

**Статус:** accepted

### Контекст
Нужен быстрый исследовательский движок, но не live OMS.

### Решение
vectorbt использовать для P4 и связанных research сценариев.
Не использовать его как центр live runtime.

### Последствия
- сильный research контур;
- stateful signal lifecycle реализуется отдельно.

---

## ADR-006 — StockSharp sidecar вместо прямого встраивания broker logic в Python core

**Статус:** accepted

### Контекст
Нужна интеграция с QUIK/Finam без переписывания execution stack с нуля.

### Решение
Использовать StockSharp как отдельный execution sidecar.
Python core работает через transport-neutral execution contracts.

### Последствия
- меньше риск ошибок на низкоуровневом execution path;
- sidecar можно заменить позже;
- strategy logic остаётся в Python.

---

## ADR-007 — Execution contracts transport-neutral

**Статус:** accepted

### Решение
Базовые сущности:
- `OrderIntent`
- `BrokerOrder`
- `BrokerFill`
- `PositionSnapshot`
- `RiskSnapshot`
- `BrokerEvent`

не должны зависеть от конкретного брокера.

### Последствия
- можно поддержать paper/live и несколько adapters;
- легче тестировать forward и reconciliation.

---

## ADR-008 — Research/live split on contracts

**Статус:** accepted

### Решение
Research может использовать continuous series.
Live runtime и execution работают только на реальных торгуемых контрактах.

### Последствия
- меньше риск ошибочного live поведения;
- требуется `roll_map`.

---

## ADR-009 — Product config outside shell high-risk root configs

**Статус:** accepted

### Контекст
Root `configs/*` уже являются shell contracts.

### Решение
Product runtime/config files по умолчанию размещать не в root `configs/`, а в:
- `src/trading_advisor_3000/config/*`,
- `deployment/*`,
- environment/secrets manager.

### Последствия
- меньше shell cross-contamination;
- ниже риск ложных governance конфликтов.

---

## ADR-010 — Phase-first delivery with shell gates

**Статус:** accepted

### Решение
Каждая продуктовая фаза закрывается через:
- task session,
- loop gate,
- product-specific tests,
- PR gate.

Nightly/dashboard lanes используются как hygiene/reporting layer, а не как замена фазной приёмке.

### Последствия
- Codex получает чёткие stop/go критерии;
- проще параллелить работу по фазам.

---

## ADR-011 - Remove aiogram from current stack baseline

**Status:** accepted

### Context
The runtime Telegram publication path in this repository is implemented by the deterministic in-repo adapter (`src/trading_advisor_3000/app/runtime/publishing/telegram.py`).
There is no real aiogram runtime adapter, dependency-backed execution path, or mocked Telegram API proof in the current baseline.

### Decision
Remove aiogram from the chosen stack baseline for the current governed phase state.
Keep Telegram publication on the deterministic in-repo adapter until a real external Telegram integration is implemented.

### Consequences
- Stack registry marks aiogram as `removed` and enforces ADR linkage.
- Stack/spec docs must not claim aiogram as chosen in the current baseline.
- Reintroducing aiogram requires a new ADR and runtime proof (dependency evidence, adapter implementation, and mocked Telegram API tests).

---

## ADR-012 - Remove replaceable stack ghost claims from current baseline

**Status:** accepted

### Context
The governed stack-conformance remediation identified five replaceable technologies that were still described as chosen without executable proof in the current repository baseline:
- vectorbt
- Alembic
- OpenTelemetry
- Polars
- DuckDB

Current implemented slices use deterministic in-repo alternatives:
- research/backtest path uses the in-repo deterministic backtest engine and fixtures;
- database schema evolution uses ordered SQL migrations with checksum verification;
- observability proof path is file-contract based with Prometheus/Grafana/Loki overlays;
- local dataframe/SQL acceleration toolchains are not part of the active product runtime baseline.

### Decision
Remove vectorbt, Alembic, OpenTelemetry, Polars, and DuckDB from the chosen stack baseline for the current governed phase state.
For baseline conformance, this decision supersedes earlier assumptions that these five technologies are part of the active chosen runtime stack.

For this baseline:
- vectorbt is removed from active chosen stack claims;
- alembic is removed from active chosen stack claims;
- opentelemetry is removed from active chosen stack claims;
- polars is removed from active chosen stack claims;
- duckdb is removed from active chosen stack claims.

### Consequences
- Stack registry must mark each of these technologies as `removed` and link back to ADR-012.
- Stack/spec truth documents must not keep `chosen` markers for these technologies.
- Reintroducing any removed technology requires a dedicated ADR update plus executable evidence:
  dependency presence, runtime entrypoint/path evidence, and focused tests.
