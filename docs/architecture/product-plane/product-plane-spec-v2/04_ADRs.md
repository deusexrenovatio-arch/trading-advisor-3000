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
- можно использовать Spark для тяжёлых джобов и PyArrow для локального dataframe/query анализа (Polars/DuckDB removed by ADR-011; this historical wording is superseded);
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

## ADR-005 — vectorbt only for research/backtest (superseded by ADR-011)

**Status:** superseded by ADR-011

### Historical superseded context
An earlier superseded design step considered vectorbt as the default research/backtest engine.

### Historical superseded decision (no longer active target stack)
vectorbt was historically allowed for P4-oriented research scenarios, but was never accepted as live-runtime core and is now removed by ADR-011.

### Supersession note
F1-B terminal-stack closure replaces this decision:
- vectorbt removed by ADR and replaced by the internal backtest engine in `src/trading_advisor_3000/app/research/backtest/engine.py`;
- the active stack truth-source is ADR-011 plus `docs/architecture/product-plane/product-plane-spec-v2/07_Tech_Stack_and_Open_Source.md` and `registry/stack_conformance.yaml`;
- any future vectorbt reintroduction requires a new ADR and aligned runtime/test evidence while vectorbt stays removed under ADR-011.

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

## ADR-011 — F1-B replaceable stack terminal decisions

**Статус:** accepted

### Context
The F1-B closure phase requires one terminal state per replaceable technology with no `chosen but planned` ghosts.
Runtime reality already ships a custom Telegram publication engine, SQL-file migrations, internal backtest execution, and Prometheus/Loki observability artifacts.

### Decision
- aiogram removed by ADR and replaced by custom Bot API engine (`TelegramPublicationEngine`).
- polars removed by ADR and replaced by pyarrow local dataframe path.
- duckdb removed by ADR and replaced by delta lake plus pyarrow local query path.
- vectorbt removed by ADR and replaced by internal backtest engine.
- alembic removed by ADR and replaced by sql migration runner.
- opentelemetry removed by ADR and replaced by prometheus loki observability path.

### Evidence anchors
- Telegram runtime path: `src/trading_advisor_3000/app/runtime/publishing/telegram.py`, `tests/app/unit/test_phase2c_runtime_components.py`.
- SQL migration runner: `scripts/apply_app_migrations.py`, `src/trading_advisor_3000/migrations/*.sql`, `tests/app/integration/test_phase2c_runtime_postgres_store.py`.
- Internal backtest runtime: `src/trading_advisor_3000/app/research/backtest/engine.py`, `tests/app/integration/test_phase2b_research_plane.py`.
- Observability runtime: `src/trading_advisor_3000/app/runtime/analytics/review.py`, `tests/app/integration/test_phase5_review_observability.py`.

### Consequences
- Replaceable stack claims stay honest and terminal for release blocking checks.
- Future reintroduction of any removed technology must land as a new ADR plus runtime and test proof.
