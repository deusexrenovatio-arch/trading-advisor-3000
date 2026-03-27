# Техническое задание на разработку Trading Advisor 3000 product plane

## 1. Назначение документа

Этот документ фиксирует ТЗ на разработку **прикладной части** платформы сигналов по MOEX futures
внутри уже существующего репозитория с **AI delivery shell**.

Документ должен использоваться Codex'ом как основа для:
- планирования фаз,
- определения change surface,
- реализации модулей,
- проверки DoD,
- прохождения phase gates,
- согласованной работы с существующим AI control plane.

## 2. Цель проекта

Разработать модульную платформу для:
- поиска и генерации rule-based сигналов по фьючерсам МосБиржи,
- исторической и forward-валидации,
- публикации и сопровождения сигналов в Telegram,
- controlled paper/live execution через StockSharp sidecar -> QUIK -> Finam,
- дальнейшего масштабирования на новые активы, новые data providers, fundamentals/news и portfolio orchestration.

## 3. Репозиторный контекст

В репозитории уже существует **AI delivery shell**:
- governance policy,
- hot/warm/cold docs,
- task lifecycle,
- context routing,
- gate stack,
- durable plans and memory.

Следовательно:

1. **Shell не переписывается** ради продуктовой логики.
2. Product plane должен быть встроен как **новый слой приложения**, а не как новый независимый фреймворк.
3. Любые изменения в `scripts/*`, root `configs/*`, `plans/*`, `memory/*`, `docs/agent/*` считаются shell-sensitive.
4. Product code по умолчанию развивается в `src/trading_advisor_3000/*`, `tests/app/*`, `docs/architecture/app/*`, `docs/runbooks/app/*`, `deployment/*`.

## 4. Целевое состояние продукта

### 4.1 MVP

- Активы: фьючерсы MOEX на товары и индексы по whitelist.
- Таймфреймы: `5m`, `15m`, `1h`.
- Стратегии: 2 active + 1 shadow.
- Режимы:
  - advisory,
  - shadow-forward,
  - paper execution,
  - live execution под feature flag.
- Канал доставки: Telegram.
- Execution path: StockSharp sidecar -> QUIK -> Finam.
- Исторические и аналитические таблицы: Delta Lake.
- Runtime state: PostgreSQL.
- Оркестрация: Dagster.
- Тяжёлые пересчёты: Spark.
- Быстрый локальный анализ: pandas + SQL on PostgreSQL snapshots (Polars/DuckDB removed from current chosen baseline by ADR-012).
- Research/backtest: in-repo deterministic backtest engine (vectorbt removed from current chosen baseline by ADR-012).

### 4.2 Full / scale-up

- Новые классы активов: акции, облигации, FX, ETF.
- Новые data feeds: fundamentals, news/event feeds.
- Graph/relatedness context между активами.
- Portfolio/risk orchestration.
- Multi-timeframe orchestration.
- Дополнительные execution adapters.
- Возможность перехода к direct Finam API или отдельному MOEX direct path.

## 5. Жёсткие архитектурные инварианты

1. Верхнеуровневую DFD не менять.
2. Data plane, research plane, runtime plane и execution plane держать раздельно.
3. Research может использовать continuous series, live execution — только `contract_id`.
4. StockSharp sidecar не содержит strategy logic.
5. Broker/execution adapter — источник истины для live order/fill/position state.
6. Runtime не должен зависеть от Spark.
7. Strategy layer не должен зависеть от Telegram и StockSharp.
8. Raw layer не читается напрямую strategy/runtime кодом.
9. Все side effects идемпотентны.
10. Для сигналов и исполнения хранить и snapshot, и event history.
11. Shell governance остаётся главным процессным контуром.
12. Product configs не добавлять в root `configs/`, пока не требуется изменение shell.

## 6. Целевая архитектура

### 6.1 Control plane (уже существует)

- `AGENTS.md`
- `docs/agent/*`
- `scripts/task_session.py`
- `scripts/run_loop_gate.py`
- `scripts/run_pr_gate.py`
- `scripts/run_nightly_gate.py`
- `plans/*`
- `memory/*`
- `tests/process/*`
- `tests/architecture/*`

### 6.2 Application plane (разрабатывается по этому ТЗ)

#### Data plane
- `P1 Data Ingestion`
- `P2 Canonical Data Builder/Orchestrator`
- `D1 Raw Data Lake`
- `D2 Canonical Market Data`

#### Research plane
- `P3 Feature Engine`
- `P4 Feature-based Backtest`
- `P9 Event-Based Forward`
- `D3 Feature / Context Store`
- `D4 Signal Candidates`
- `D5 Outcome / Analytics Store`

#### Runtime plane
- `P5 Signal Decision Engine`
- `P6 Publishing Engine`
- `P7 Review / Analytics`
- `D7 Active Signals`
- `D8 Strategy Registry`

#### Execution plane
- thin Python execution contracts
- StockSharp sidecar (.NET)
- QUIK connector
- Finam account/execution transport
- reconciliation / sync / broker event log

## 7. Выбранный технологический стек

- Python 3.12+
- Delta Lake
- Apache Spark
- Dagster
- Polars (removed in current baseline; ADR-012)
- DuckDB (removed in current baseline; ADR-012)
- PostgreSQL
- vectorbt (removed in current baseline; ADR-012)
- FastAPI
- deterministic in-repo Telegram publication adapter
- Alembic (removed in current baseline; ADR-012)
- OpenTelemetry (removed in current baseline; ADR-012)
- Prometheus + Grafana + Loki
- Docker Compose
- .NET 8 sidecar
- StockSharp

## 8. Общие требования к реализации

### 8.1 Кодовая организация
- Product code живёт в `src/trading_advisor_3000/`.
- Product tests живут в `tests/app/`.
- Product docs живут в `docs/architecture/app/`, `docs/runbooks/app/`.
- Sidecar deployment artifacts живут в `deployment/stocksharp-sidecar/`.

### 8.2 Контракты
- Все ключевые DTO и события должны быть типизированы.
- Версионируемые схемы и публичные интерфейсы оформляются как contracts.
- Любое изменение contract surface требует:
  - contract tests,
  - docs update,
  - при необходимости ADR update.

### 8.3 Данные
- Delta используется для исторических и аналитических таблиц.
- PostgreSQL используется для живого состояния.
- Runtime работает на нормализованных canonical bars.
- `mode` обязателен для forward/paper/live сущностей.

### 8.4 Процесс
- Каждая фаза изолирована и имеет свой gate.
- Нельзя переходить к следующей фазе без закрытия acceptance текущей.
- Любое изменение shell-sensitive paths выделяется в отдельный patch set.

## 9. Целевые KPI MVP

### Продуктовые
- 2–15 сигналов в неделю на активном whitelist.
- Полный lifecycle Telegram: create/edit/close/cancel.
- Shadow-forward и paper execution используют те же strategy contracts, что и runtime.

### Качественные
- Полная трассировка: bar -> feature snapshot -> signal -> publication -> outcome.
- Повторяемость backtest run на одном dataset version.
- Отсутствие look-ahead и несогласованных live fills.

### Архитектурные
- Добавление новой стратегии без переписывания runtime ядра.
- Добавление нового data provider без переписывания canonical contracts.
- Добавление нового execution adapter без переписывания decision logic.
- Масштабирование data plane без миграции runtime state layer.

## 10. Основные deliverables

- product-plane monorepo structure внутри существующего repo,
- contracts и migrations,
- DFD/ERD и ADR package,
- Dagster definitions,
- Spark jobs,
- runtime services,
- Telegram bot,
- forward/paper/live execution contracts,
- StockSharp sidecar contract,
- tests/app для unit/integration/e2e,
- runbooks,
- Codex/MCP integration docs,
- фазные acceptance gates.

## 11. Документы пакета

- `docs/architecture/app/product-plane-spec-v2/00_AI_Shell_Alignment.md`
- `docs/architecture/app/product-plane-spec-v2/01_Architecture_Overview.md`
- `docs/architecture/app/product-plane-spec-v2/02_Repository_Structure.md`
- `docs/architecture/app/product-plane-spec-v2/03_Data_Model_and_Flows.md`
- `docs/architecture/app/product-plane-spec-v2/04_ADRs.md`
- `docs/architecture/app/product-plane-spec-v2/05_Modules_DoD_and_Parallelization.md`
- `docs/architecture/app/product-plane-spec-v2/06_Phases_and_Acceptance_Gates.md`
- `docs/architecture/app/product-plane-spec-v2/07_Tech_Stack_and_Open_Source.md`
- `docs/architecture/app/product-plane-spec-v2/08_Codex_AI_Shell_Integration.md`
- `docs/architecture/app/product-plane-spec-v2/09_Constraints_Risks_and_NFR.md`
- `docs/architecture/app/product-plane-spec-v2/10_MCP_Deployment_Request.md`

