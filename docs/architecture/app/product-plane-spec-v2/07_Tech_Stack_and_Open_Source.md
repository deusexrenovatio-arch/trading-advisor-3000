# 07. Tech Stack and Open Source

## 1. Выбранный стек

| Компонент | Роль | Статус | OSS / Commercial | Комментарий |
| --- | --- | --- | --- | --- |
| Python 3.12+ | основной язык product plane | chosen | OSS | data/research/runtime |
| Delta Lake | durable data tables | chosen | OSS | основной storage contract |
| Apache Spark | heavy compute / rebuilds | chosen (scoped) | OSS | не входит в live runtime |
| Dagster | orchestration | chosen | OSS | data/research assets and checks |
| Polars | fast local dataframe compute | removed | OSS | baseline uses pandas + SQL; reintroduction requires ADR-backed proof (ADR-012) |
| DuckDB | local SQL and Delta/Parquet access | removed | OSS | kept only as optional MCP/local tooling; not in product runtime baseline (ADR-012) |
| PostgreSQL | runtime state | chosen | OSS | signal/config/execution |
| vectorbt | backtest/research | removed | OSS core | baseline research uses in-repo backtest engine; reintroduction requires ADR-backed proof (ADR-012) |
| FastAPI | service/API layer | chosen | OSS | runtime/admin APIs |
| aiogram | Telegram integration | removed | OSS | replaced by deterministic in-repo publication adapter (ADR-011) |
| Alembic | DB migrations | removed | OSS | baseline uses ordered SQL migrations with checksum runner (ADR-012) |
| OpenTelemetry | tracing/metrics/log correlation | removed | OSS | baseline observability is file-contract based; OTel requires dedicated rollout ADR (ADR-012) |
| Prometheus | metrics | chosen | OSS | monitoring |
| Grafana | dashboards | chosen | OSS | metrics and logs |
| Loki | logs | chosen | OSS | structured log aggregation |
| Docker Compose | local/dev orchestration | chosen | OSS | reproducible local env |
| .NET 8 | execution sidecar runtime | chosen | OSS | for StockSharp bridge |
| StockSharp | execution/QUIK bridge | chosen | mixed | core platform + connector path |

## 2. Почему именно так

## 2.1 Delta Lake
Выбран как долгоживущий storage contract.
Причина:
- не хочется ранней миграции data format;
- поддерживает batch-heavy future;
- совместим с Spark и локальным Python tooling.

### Альтернативы
- Iceberg — сильная альтернатива при приоритете vendor-neutrality.
- Parquet-only — хорошо для простого MVP, но слабее как long-lived contract.

## 2.2 Spark
Нужен как **scoped heavy compute layer**, а не как центр архитектуры.

### Использовать для
- large rebuilds,
- recompute features,
- batch normalization,
- bigger historical pipelines.

### Не использовать для
- live signal runtime,
- Telegram publication,
- execution loop.

## 2.3 Dagster
Подходит для:
- assets,
- data lineage,
- checks,
- schedules,
- integration with Spark jobs.

## 2.4 vectorbt (current baseline decision)
vectorbt removed from the chosen baseline for the current governed phase state.
Research/backtest currently runs on the in-repo deterministic engine and fixtures.
Reintroduction path: new ADR + dependency/runtime proof + dedicated research tests.

## 2.5 StockSharp
Подходит как:
- sidecar bridge к QUIK/Finam,
- transport for orders/fills/portfolio sync.

Не использовать как носитель strategy logic и не делать из него центр всего продукта.

## 2.6 Replaceable stack decisions (S1)
Removed from current chosen baseline (ADR-012):
- Polars
- DuckDB
- vectorbt
- Alembic
- OpenTelemetry

Removal means these technologies are not claimed as active product runtime stack today.
Any comeback requires explicit ADR update plus executable proof and tests.

## 3. Open-source решения по слоям

### Data/research
- Delta Lake
- Apache Spark
- Dagster

### Runtime
- FastAPI
- Pydantic
- deterministic in-repo Telegram publication adapter
- PostgreSQL

### Observability
- Prometheus
- Grafana
- Loki

### Dev process
- GitHub Actions
- local `.cursor/skills`
- shell validators and gates

## 4. Что оставить вне core

- direct MOEX DMA;
- прямой Finam API как единственный путь;
- custom OMS/EMS “с нуля”;
- перевод всей логики в .NET;
- размещение app configs в root `configs/`.

## 5. MCP servers

## Обязательные
- GitHub official MCP
- OpenAI Developer Docs MCP
- Dagster MCP
- Docker MCP Toolkit / Gateway

## Рекомендуемые
- PostgreSQL read-only MCP (dev/stage only)
- MotherDuck / DuckDB MCP
- Context7 / docs search MCP, если нужен

## 6. Skills strategy

### Wave 1 — уже существующие generic shell skills
- process/governance/architecture/testing

### Wave 2 — CI/governance hardening
- skill governance automation
- doc sync
- architecture map sync

### Wave 3 — stack-specific product skills
Вводить только когда соответствующий стек реально появился в repo:
- `delta-contracts`
- `dagster-assets`
- `spark-job-layout`
- `stocksharp-sidecar`
- `telegram-lifecycle`
- `execution-reconciliation`

### Wave 4 — domain-specialized skills
Только после стабилизации бизнес-логики и strategy packages.

