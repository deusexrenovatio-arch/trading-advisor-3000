# 07. Tech Stack and Open Source

## 1. Р’С‹Р±СЂР°РЅРЅС‹Р№ СЃС‚РµРє

| РљРѕРјРїРѕРЅРµРЅС‚ | Р РѕР»СЊ | РЎС‚Р°С‚СѓСЃ | OSS / Commercial | РљРѕРјРјРµРЅС‚Р°СЂРёР№ |
| --- | --- | --- | --- | --- |
| Python 3.12+ | РѕСЃРЅРѕРІРЅРѕР№ СЏР·С‹Рє product plane | chosen | OSS | data/research/runtime |
| Delta Lake | durable data tables | chosen | OSS | РѕСЃРЅРѕРІРЅРѕР№ storage contract |
| Apache Spark | heavy compute / rebuilds | chosen (scoped) | OSS | РЅРµ РІС…РѕРґРёС‚ РІ live runtime |
| Dagster | orchestration | chosen | OSS | data/research assets and checks |
| Polars | fast local dataframe compute | removed (ADR-011) | OSS | replaced by PyArrow-based local dataframe path |
| DuckDB | local SQL and Delta/Parquet access | removed (ADR-011) | OSS | replaced by Delta Lake + PyArrow query/read path |
| PostgreSQL | runtime state | chosen | OSS | signal/config/execution |
| vectorbt | backtest/research | chosen (governed profile, ADR-012) | fair-code (Apache-2.0 + Commons Clause) | research-only contour; runtime/execution stays on internal engine |
| pandas-ta-classic | governed feature engineering | chosen (governed profile, ADR-012) | OSS | bounded indicator/materialization contour paired with vectorbt research profile |
| FastAPI | service/API layer | chosen | OSS | runtime/admin APIs |
| aiogram | Telegram integration | removed (ADR-011) | OSS | replaced by custom Bot API publication engine |
| Alembic | DB migrations | removed (ADR-011) | OSS | replaced by SQL migration runner (`scripts/apply_app_migrations.py`) |
| OpenTelemetry | tracing/metrics/log correlation | removed (ADR-011) | OSS | replaced by Prometheus + Loki evidence path |
| Prometheus | metrics | chosen | OSS | monitoring |
| Grafana | dashboards | chosen | OSS | metrics and logs |
| Loki | logs | chosen | OSS | structured log aggregation |
| Docker Compose | local/dev orchestration | chosen | OSS | reproducible local env |
| .NET 8 | execution sidecar runtime | chosen | OSS | for StockSharp bridge |
| StockSharp | execution/QUIK bridge | chosen | mixed | core platform + connector path |

## 2. РџРѕС‡РµРјСѓ РёРјРµРЅРЅРѕ С‚Р°Рє

## 2.1 Delta Lake
Р’С‹Р±СЂР°РЅ РєР°Рє РґРѕР»РіРѕР¶РёРІСѓС‰РёР№ storage contract.
РџСЂРёС‡РёРЅР°:
- РЅРµ С…РѕС‡РµС‚СЃСЏ СЂР°РЅРЅРµР№ РјРёРіСЂР°С†РёРё data format;
- РїРѕРґРґРµСЂР¶РёРІР°РµС‚ batch-heavy future;
- СЃРѕРІРјРµСЃС‚РёРј СЃ Spark Рё Р»РѕРєР°Р»СЊРЅС‹Рј Python tooling.

### РђР»СЊС‚РµСЂРЅР°С‚РёРІС‹
- Iceberg вЂ” СЃРёР»СЊРЅР°СЏ Р°Р»СЊС‚РµСЂРЅР°С‚РёРІР° РїСЂРё РїСЂРёРѕСЂРёС‚РµС‚Рµ vendor-neutrality.
- Parquet-only вЂ” С…РѕСЂРѕС€Рѕ РґР»СЏ РїСЂРѕСЃС‚РѕРіРѕ MVP, РЅРѕ СЃР»Р°Р±РµРµ РєР°Рє long-lived contract.

## 2.2 Spark
РќСѓР¶РµРЅ РєР°Рє **scoped heavy compute layer**, Р° РЅРµ РєР°Рє С†РµРЅС‚СЂ Р°СЂС…РёС‚РµРєС‚СѓСЂС‹.

### РСЃРїРѕР»СЊР·РѕРІР°С‚СЊ РґР»СЏ
- large rebuilds,
- recompute features,
- batch normalization,
- bigger historical pipelines.

### РќРµ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ РґР»СЏ
- live signal runtime,
- Telegram publication,
- execution loop.

## 2.3 Dagster
РџРѕРґС…РѕРґРёС‚ РґР»СЏ:
- assets,
- data lineage,
- checks,
- schedules,
- integration with Spark jobs.

## 2.4 vectorbt
Governed amendment (`ADR-012`): vectorbt is reintroduced only for bounded research/backtest workloads.
Internal runtime/execution contracts remain decoupled and continue using the in-repo execution contour.

Allowed:
- research materialization and batch backtests;
- strategy ranking/robustness analytics;
- reproducible evidence generation for promotion gates.

Forbidden without a separate legal/product decision:
- embedding vectorbt internals into runtime decisioning and live execution;
- redistribution of vectorbt as part of an external paid service envelope.

## 2.5 StockSharp
РџРѕРґС…РѕРґРёС‚ РєР°Рє:
- sidecar bridge Рє QUIK/Finam,
- transport for orders/fills/portfolio sync.

РќРµ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ РєР°Рє РЅРѕСЃРёС‚РµР»СЊ strategy logic Рё РЅРµ РґРµР»Р°С‚СЊ РёР· РЅРµРіРѕ С†РµРЅС‚СЂ РІСЃРµРіРѕ РїСЂРѕРґСѓРєС‚Р°.

## 3. Open-source СЂРµС€РµРЅРёСЏ РїРѕ СЃР»РѕСЏРј

### Data/research
- Delta Lake
- Apache Spark
- Dagster

### Runtime
- FastAPI
- Pydantic
- PostgreSQL

### Observability
- Prometheus
- Grafana
- Loki

### Dev process
- GitHub Actions
- local `.cursor/skills`
- shell validators and gates

## 4. Р§С‚Рѕ РѕСЃС‚Р°РІРёС‚СЊ РІРЅРµ core

- direct MOEX DMA;
- РїСЂСЏРјРѕР№ Finam API РєР°Рє РµРґРёРЅСЃС‚РІРµРЅРЅС‹Р№ РїСѓС‚СЊ;
- custom OMS/EMS вЂњСЃ РЅСѓР»СЏвЂќ;
- РїРµСЂРµРІРѕРґ РІСЃРµР№ Р»РѕРіРёРєРё РІ .NET;
- СЂР°Р·РјРµС‰РµРЅРёРµ app configs РІ root `configs/`.

## 5. MCP servers

## РћР±СЏР·Р°С‚РµР»СЊРЅС‹Рµ
- GitHub official MCP
- OpenAI Developer Docs MCP
- Dagster MCP
- Docker MCP Toolkit / Gateway

## Р РµРєРѕРјРµРЅРґСѓРµРјС‹Рµ
- PostgreSQL read-only MCP (dev/stage only)
- MotherDuck / DuckDB MCP
- Context7 / docs search MCP, РµСЃР»Рё РЅСѓР¶РµРЅ

## 6. Skills strategy

### Wave 1 вЂ” СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓСЋС‰РёРµ generic shell skills
- process/governance/architecture/testing

### Wave 2 вЂ” CI/governance hardening
- skill governance automation
- doc sync
- architecture map sync

### Wave 3 вЂ” stack-specific product skills
Р’РІРѕРґРёС‚СЊ С‚РѕР»СЊРєРѕ РєРѕРіРґР° СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓСЋС‰РёР№ СЃС‚РµРє СЂРµР°Р»СЊРЅРѕ РїРѕСЏРІРёР»СЃСЏ РІ repo:
- `delta-contracts`
- `dagster-assets`
- `spark-job-layout`
- `stocksharp-sidecar`
- `telegram-lifecycle`
- `execution-reconciliation`

### Wave 4 вЂ” domain-specialized skills
РўРѕР»СЊРєРѕ РїРѕСЃР»Рµ СЃС‚Р°Р±РёР»РёР·Р°С†РёРё Р±РёР·РЅРµСЃ-Р»РѕРіРёРєРё Рё strategy packages.
