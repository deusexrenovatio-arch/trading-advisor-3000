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
