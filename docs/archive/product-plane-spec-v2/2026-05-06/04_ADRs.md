# 04. ADRs

## ADR-001 вЂ” Monorepo split: control plane + application plane

**РЎС‚Р°С‚СѓСЃ:** accepted

### РљРѕРЅС‚РµРєСЃС‚
Р РµРїРѕР·РёС‚РѕСЂРёР№ СѓР¶Рµ СЏРІР»СЏРµС‚СЃСЏ AI delivery shell.

### Р РµС€РµРЅРёРµ
РќРµ СЃС‚СЂРѕРёС‚СЊ РІС‚РѕСЂРѕР№ РЅРµР·Р°РІРёСЃРёРјС‹Р№ repo.
Р Р°Р·РІРёРІР°С‚СЊ product plane РІРЅСѓС‚СЂРё С‚РµРєСѓС‰РµРіРѕ monorepo, РЅРѕ СЃ С‡С‘С‚РєРёРј СЂР°Р·РґРµР»РµРЅРёРµРј РїРѕ РїСѓС‚СЏРј Рё ownership.

### РџРѕСЃР»РµРґСЃС‚РІРёСЏ
- shell governance РѕСЃС‚Р°С‘С‚СЃСЏ РµРґРёРЅС‹Рј;
- РїСЂРѕРґСѓРєС‚ РїРѕР»СѓС‡Р°РµС‚ РІСЃС‚СЂРѕРµРЅРЅС‹Р№ control plane;
- С‚СЂРµР±СѓРµС‚СЃСЏ РґРёСЃС†РёРїР»РёРЅР° РїРѕ change surfaces.

---

## ADR-002 вЂ” Delta Lake РєР°Рє РґРѕР»РіРѕР¶РёРІСѓС‰РёР№ storage contract

**РЎС‚Р°С‚СѓСЃ:** accepted

### РљРѕРЅС‚РµРєСЃС‚
РќСѓР¶РµРЅ С„РѕСЂРјР°С‚ РґР°РЅРЅС‹С…, РєРѕС‚РѕСЂС‹Р№ РЅРµ РїСЂРёРґС‘С‚СЃСЏ Р±С‹СЃС‚СЂРѕ РјРёРіСЂРёСЂРѕРІР°С‚СЊ.

### Р РµС€РµРЅРёРµ
РСЃРїРѕР»СЊР·РѕРІР°С‚СЊ Delta Lake РґР»СЏ РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… Рё Р°РЅР°Р»РёС‚РёС‡РµСЃРєРёС… С‚Р°Р±Р»РёС†.

### РџРѕСЃР»РµРґСЃС‚РІРёСЏ
- РїСЂРѕС‰Рµ РјР°СЃС€С‚Р°Р±РёСЂРѕРІР°С‚СЊ data plane;
- РјРѕР¶РЅРѕ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ Spark РґР»СЏ С‚СЏР¶С‘Р»С‹С… РґР¶РѕР±РѕРІ Рё PyArrow РґР»СЏ Р»РѕРєР°Р»СЊРЅРѕРіРѕ dataframe/query Р°РЅР°Р»РёР·Р° (Polars/DuckDB removed by ADR-011; this historical wording is superseded);
- advanced Delta capabilities РІРєР»СЋС‡Р°С‚СЊ С‚РѕР»СЊРєРѕ РїРѕ РјРµСЂРµ РЅРµРѕР±С…РѕРґРёРјРѕСЃС‚Рё.

---

## ADR-003 вЂ” Spark scoped, runtime stays Python

**РЎС‚Р°С‚СѓСЃ:** accepted

### РљРѕРЅС‚РµРєСЃС‚
РќСѓР¶РµРЅ РјР°СЃС€С‚Р°Р±РёСЂСѓРµРјС‹Р№ compute, РЅРѕ runtime РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ СѓРїСЂР°РІР»СЏРµРјС‹Рј Рё РЅРёР·РєРѕ-С„СЂРёРєС†РёРѕРЅРЅС‹Рј.

### Р РµС€РµРЅРёРµ
Spark РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґР»СЏ ingestion rebuild / heavy transforms / batch recompute.
Runtime decisioning, signal lifecycle Рё publishing РѕСЃС‚Р°СЋС‚СЃСЏ Python services.

### РџРѕСЃР»РµРґСЃС‚РІРёСЏ
- РЅРµС‚ hard dependency runtime РЅР° Spark;
- РїСЂРѕС‰Рµ РґРµР±Р°Р¶РёС‚СЊ live flows;
- РјРµРЅСЊС€Рµ operational coupling.

---

## ADR-004 вЂ” Dagster РєР°Рє orchestration layer

**РЎС‚Р°С‚СѓСЃ:** accepted

### РљРѕРЅС‚РµРєСЃС‚
РќСѓР¶РµРЅ РѕСЂРєРµСЃС‚СЂР°С‚РѕСЂ РґР»СЏ data/research РєРѕРЅС‚СѓСЂРѕРІ.

### Р РµС€РµРЅРёРµ
РСЃРїРѕР»СЊР·РѕРІР°С‚СЊ Dagster РєР°Рє РѕСЃРЅРѕРІРЅРѕР№ orchestrator РґР»СЏ assets, checks Рё schedules.

### РџРѕСЃР»РµРґСЃС‚РІРёСЏ
- С…РѕСЂРѕС€Рѕ Р»РѕР¶РёС‚СЃСЏ РЅР° D1вЂ“D5;
- РµСЃС‚СЊ РµСЃС‚РµСЃС‚РІРµРЅРЅР°СЏ РёРЅС‚РµРіСЂР°С†РёСЏ СЃ data quality;
- РЅРµРѕР±С…РѕРґРёРјРѕ РґРµСЂР¶Р°С‚СЊ runtime plane РѕС‚РґРµР»СЊРЅРѕ.

---

## ADR-005 - vectorbt only for research/backtest (historical)

**Status:** superseded (historically by ADR-011, currently governed by ADR-012 scope amendment)

### Historical superseded context
An earlier superseded design step considered vectorbt as the default research/backtest engine.

### Historical superseded decision
vectorbt was historically allowed for P4-oriented research scenarios, but was never accepted as live-runtime core.

### Supersession note
- ADR-011 moved vectorbt to a removed terminal state for the F1-B cycle.
- ADR-012 later reintroduced vectorbt in a bounded governed scope for research-only workloads.
- Runtime/live execution remains decoupled from vectorbt internals and continues to use internal execution contours.

---
## ADR-006 вЂ” StockSharp sidecar РІРјРµСЃС‚Рѕ РїСЂСЏРјРѕРіРѕ РІСЃС‚СЂР°РёРІР°РЅРёСЏ broker logic РІ Python core

**РЎС‚Р°С‚СѓСЃ:** accepted

### РљРѕРЅС‚РµРєСЃС‚
РќСѓР¶РЅР° РёРЅС‚РµРіСЂР°С†РёСЏ СЃ QUIK/Finam Р±РµР· РїРµСЂРµРїРёСЃС‹РІР°РЅРёСЏ execution stack СЃ РЅСѓР»СЏ.

### Р РµС€РµРЅРёРµ
РСЃРїРѕР»СЊР·РѕРІР°С‚СЊ StockSharp РєР°Рє РѕС‚РґРµР»СЊРЅС‹Р№ execution sidecar.
Python core СЂР°Р±РѕС‚Р°РµС‚ С‡РµСЂРµР· transport-neutral execution contracts.

### РџРѕСЃР»РµРґСЃС‚РІРёСЏ
- РјРµРЅСЊС€Рµ СЂРёСЃРє РѕС€РёР±РѕРє РЅР° РЅРёР·РєРѕСѓСЂРѕРІРЅРµРІРѕРј execution path;
- sidecar РјРѕР¶РЅРѕ Р·Р°РјРµРЅРёС‚СЊ РїРѕР·Р¶Рµ;
- strategy logic РѕСЃС‚Р°С‘С‚СЃСЏ РІ Python.

---

## ADR-007 вЂ” Execution contracts transport-neutral

**РЎС‚Р°С‚СѓСЃ:** accepted

### Р РµС€РµРЅРёРµ
Р‘Р°Р·РѕРІС‹Рµ СЃСѓС‰РЅРѕСЃС‚Рё:
- `OrderIntent`
- `BrokerOrder`
- `BrokerFill`
- `PositionSnapshot`
- `RiskSnapshot`
- `BrokerEvent`

РЅРµ РґРѕР»Р¶РЅС‹ Р·Р°РІРёСЃРµС‚СЊ РѕС‚ РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ Р±СЂРѕРєРµСЂР°.

### РџРѕСЃР»РµРґСЃС‚РІРёСЏ
- РјРѕР¶РЅРѕ РїРѕРґРґРµСЂР¶Р°С‚СЊ paper/live Рё РЅРµСЃРєРѕР»СЊРєРѕ adapters;
- Р»РµРіС‡Рµ С‚РµСЃС‚РёСЂРѕРІР°С‚СЊ forward Рё reconciliation.

---

## ADR-008 вЂ” Research/live split on contracts

**РЎС‚Р°С‚СѓСЃ:** accepted

### Р РµС€РµРЅРёРµ
Research РјРѕР¶РµС‚ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ continuous series.
Live runtime Рё execution СЂР°Р±РѕС‚Р°СЋС‚ С‚РѕР»СЊРєРѕ РЅР° СЂРµР°Р»СЊРЅС‹С… С‚РѕСЂРіСѓРµРјС‹С… РєРѕРЅС‚СЂР°РєС‚Р°С….

### РџРѕСЃР»РµРґСЃС‚РІРёСЏ
- РјРµРЅСЊС€Рµ СЂРёСЃРє РѕС€РёР±РѕС‡РЅРѕРіРѕ live РїРѕРІРµРґРµРЅРёСЏ;
- С‚СЂРµР±СѓРµС‚СЃСЏ `roll_map`.

---

## ADR-009 вЂ” Product config outside shell high-risk root configs

**РЎС‚Р°С‚СѓСЃ:** accepted

### РљРѕРЅС‚РµРєСЃС‚
Root `configs/*` СѓР¶Рµ СЏРІР»СЏСЋС‚СЃСЏ shell contracts.

### Р РµС€РµРЅРёРµ
Product runtime/config files РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ СЂР°Р·РјРµС‰Р°С‚СЊ РЅРµ РІ root `configs/`, Р° РІ:
- `src/trading_advisor_3000/config/*`,
- `deployment/*`,
- environment/secrets manager.

### РџРѕСЃР»РµРґСЃС‚РІРёСЏ
- РјРµРЅСЊС€Рµ shell cross-contamination;
- РЅРёР¶Рµ СЂРёСЃРє Р»РѕР¶РЅС‹С… governance РєРѕРЅС„Р»РёРєС‚РѕРІ.

---

## ADR-010 вЂ” Phase-first delivery with shell gates

**РЎС‚Р°С‚СѓСЃ:** accepted

### Р РµС€РµРЅРёРµ
РљР°Р¶РґР°СЏ РїСЂРѕРґСѓРєС‚РѕРІР°СЏ С„Р°Р·Р° Р·Р°РєСЂС‹РІР°РµС‚СЃСЏ С‡РµСЂРµР·:
- task session,
- loop gate,
- product-specific tests,
- PR gate.

Nightly/dashboard lanes РёСЃРїРѕР»СЊР·СѓСЋС‚СЃСЏ РєР°Рє hygiene/reporting layer, Р° РЅРµ РєР°Рє Р·Р°РјРµРЅР° С„Р°Р·РЅРѕР№ РїСЂРёС‘РјРєРµ.

### РџРѕСЃР»РµРґСЃС‚РІРёСЏ
- Codex РїРѕР»СѓС‡Р°РµС‚ С‡С‘С‚РєРёРµ stop/go РєСЂРёС‚РµСЂРёРё;
- РїСЂРѕС‰Рµ РїР°СЂР°Р»Р»РµР»РёС‚СЊ СЂР°Р±РѕС‚Сѓ РїРѕ С„Р°Р·Р°Рј.

---

## ADR-011 вЂ” F1-B replaceable stack terminal decisions

**РЎС‚Р°С‚СѓСЃ:** accepted

### Context
The F1-B closure phase requires one terminal state per replaceable technology with no `chosen but planned` ghosts.
Runtime reality already ships a custom Telegram publication engine, SQL-file migrations, internal backtest execution, and Prometheus/Loki observability artifacts.

### Decision
- aiogram removed by ADR and replaced by custom Bot API engine (`TelegramPublicationEngine`).
- polars removed by ADR and replaced by pyarrow local dataframe path.
- duckdb removed by ADR and replaced by delta lake plus pyarrow local query path.
- vectorbt removed by ADR and replaced by internal backtest engine (later superseded in bounded scope by ADR-012).
- alembic removed by ADR and replaced by sql migration runner.
- opentelemetry removed by ADR and replaced by prometheus loki observability path.

### Evidence anchors
- Telegram runtime path: `src/trading_advisor_3000/product_plane/runtime/publishing/telegram.py`, `tests/product-plane/unit/test_phase2c_runtime_components.py`.
- SQL migration runner: `scripts/apply_app_migrations.py`, `src/trading_advisor_3000/migrations/*.sql`, `tests/product-plane/integration/test_phase2c_runtime_postgres_store.py`.
- Internal backtest runtime: `src/trading_advisor_3000/product_plane/research/backtests/engine.py`, `tests/product-plane/integration/test_materialized_research_plane.py`.
- Observability runtime: `src/trading_advisor_3000/product_plane/runtime/analytics/review.py`, `tests/product-plane/integration/test_phase5_review_observability.py`.

### Consequences
- Replaceable stack claims stay honest and terminal for release blocking checks.
- Future reintroduction of any removed technology must land as a new ADR plus runtime and test proof.

---

## ADR-012 - Governed vectorbt reintroduction (research-only envelope)

**Status:** accepted

### Date
2026-04-10

### Context
Package intake for the vectorbt/pandas-ta-classic migration requires vectorbt for medium-term research throughput, while ADR-011 had previously removed vectorbt from the active stack.
A strict legal/commercial envelope is required because vectorbt is distributed under a fair-code model (Apache-2.0 + Commons Clause).

### Decision
- Reintroduce `vectorbt` only for governed research-plane workloads (materialization, backtests, ranking evidence).
- Keep runtime decisioning, publication, and live execution independent from vectorbt internals.
- Treat reintroduction scope as bounded and non-default for commercial redistribution scenarios.
- Pin governed compatibility envelope for this contour: Python 3.12.10, numpy 2.4.4, pandas 2.3.3, pyarrow 21.0.0, numba 0.65.0, vectorbt 0.28.5, pandas-ta-classic 0.4.47.

### Legal/commercial envelope
- Allowed: internal research, backtesting, and decision-support evidence generation inside the governed repo/process.
- Not allowed by default: packaging vectorbt-driven engine internals as an external paid service or redistributable product component.
- Escalation rule: any external commercialization path must pass a separate legal/product decision before release-readiness claims.

### Consequences
- ADR-011 remains valid for terminal removals except vectorbt, which is now governed by ADR-012 scope restrictions.
- Stack-conformance truth sources must reflect `vectorbt` as a bounded governed contour (not runtime/live core).
- Promotion evidence must remain reproducible and decoupled via runtime-compatible candidate projections.
