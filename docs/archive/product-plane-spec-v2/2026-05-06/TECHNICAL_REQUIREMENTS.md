# РўРµС…РЅРёС‡РµСЃРєРѕРµ Р·Р°РґР°РЅРёРµ РЅР° СЂР°Р·СЂР°Р±РѕС‚РєСѓ Trading Advisor 3000 product plane

## 1. РќР°Р·РЅР°С‡РµРЅРёРµ РґРѕРєСѓРјРµРЅС‚Р°

Р­С‚РѕС‚ РґРѕРєСѓРјРµРЅС‚ С„РёРєСЃРёСЂСѓРµС‚ РўР— РЅР° СЂР°Р·СЂР°Р±РѕС‚РєСѓ **РїСЂРёРєР»Р°РґРЅРѕР№ С‡Р°СЃС‚Рё** РїР»Р°С‚С„РѕСЂРјС‹ СЃРёРіРЅР°Р»РѕРІ РїРѕ MOEX futures
РІРЅСѓС‚СЂРё СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓСЋС‰РµРіРѕ СЂРµРїРѕР·РёС‚РѕСЂРёСЏ СЃ **AI delivery shell**.

Р”РѕРєСѓРјРµРЅС‚ РґРѕР»Р¶РµРЅ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊСЃСЏ Codex'РѕРј РєР°Рє РѕСЃРЅРѕРІР° РґР»СЏ:
- РїР»Р°РЅРёСЂРѕРІР°РЅРёСЏ С„Р°Р·,
- РѕРїСЂРµРґРµР»РµРЅРёСЏ change surface,
- СЂРµР°Р»РёР·Р°С†РёРё РјРѕРґСѓР»РµР№,
- РїСЂРѕРІРµСЂРєРё DoD,
- РїСЂРѕС…РѕР¶РґРµРЅРёСЏ phase gates,
- СЃРѕРіР»Р°СЃРѕРІР°РЅРЅРѕР№ СЂР°Р±РѕС‚С‹ СЃ СЃСѓС‰РµСЃС‚РІСѓСЋС‰РёРј AI control plane.

## 2. Р¦РµР»СЊ РїСЂРѕРµРєС‚Р°

Р Р°Р·СЂР°Р±РѕС‚Р°С‚СЊ РјРѕРґСѓР»СЊРЅСѓСЋ РїР»Р°С‚С„РѕСЂРјСѓ РґР»СЏ:
- РїРѕРёСЃРєР° Рё РіРµРЅРµСЂР°С†РёРё rule-based СЃРёРіРЅР°Р»РѕРІ РїРѕ С„СЊСЋС‡РµСЂСЃР°Рј РњРѕСЃР‘РёСЂР¶Рё,
- РёСЃС‚РѕСЂРёС‡РµСЃРєРѕР№ Рё forward-РІР°Р»РёРґР°С†РёРё,
- РїСѓР±Р»РёРєР°С†РёРё Рё СЃРѕРїСЂРѕРІРѕР¶РґРµРЅРёСЏ СЃРёРіРЅР°Р»РѕРІ РІ Telegram,
- controlled paper/live execution С‡РµСЂРµР· StockSharp sidecar -> QUIK -> Finam,
- РґР°Р»СЊРЅРµР№С€РµРіРѕ РјР°СЃС€С‚Р°Р±РёСЂРѕРІР°РЅРёСЏ РЅР° РЅРѕРІС‹Рµ Р°РєС‚РёРІС‹, РЅРѕРІС‹Рµ data providers, fundamentals/news Рё portfolio orchestration.

## 3. Р РµРїРѕР·РёС‚РѕСЂРЅС‹Р№ РєРѕРЅС‚РµРєСЃС‚

Р’ СЂРµРїРѕР·РёС‚РѕСЂРёРё СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚ **AI delivery shell**:
- governance policy,
- hot/warm/cold docs,
- task lifecycle,
- context routing,
- gate stack,
- durable plans and memory.

РЎР»РµРґРѕРІР°С‚РµР»СЊРЅРѕ:

1. **Shell РЅРµ РїРµСЂРµРїРёСЃС‹РІР°РµС‚СЃСЏ** СЂР°РґРё РїСЂРѕРґСѓРєС‚РѕРІРѕР№ Р»РѕРіРёРєРё.
2. Product plane РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ РІСЃС‚СЂРѕРµРЅ РєР°Рє **РЅРѕРІС‹Р№ СЃР»РѕР№ РїСЂРёР»РѕР¶РµРЅРёСЏ**, Р° РЅРµ РєР°Рє РЅРѕРІС‹Р№ РЅРµР·Р°РІРёСЃРёРјС‹Р№ С„СЂРµР№РјРІРѕСЂРє.
3. Р›СЋР±С‹Рµ РёР·РјРµРЅРµРЅРёСЏ РІ `scripts/*`, root `configs/*`, `plans/*`, `memory/*`, `docs/agent/*` СЃС‡РёС‚Р°СЋС‚СЃСЏ shell-sensitive.
4. Product code РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ СЂР°Р·РІРёРІР°РµС‚СЃСЏ РІ `src/trading_advisor_3000/*`, `tests/product-plane/*`, `docs/architecture/product-plane/*`, `docs/runbooks/app/*`, `deployment/*`.

## 4. Р¦РµР»РµРІРѕРµ СЃРѕСЃС‚РѕСЏРЅРёРµ РїСЂРѕРґСѓРєС‚Р°

### 4.1 MVP

- РђРєС‚РёРІС‹: С„СЊСЋС‡РµСЂСЃС‹ MOEX РЅР° С‚РѕРІР°СЂС‹ Рё РёРЅРґРµРєСЃС‹ РїРѕ whitelist.
- РўР°Р№РјС„СЂРµР№РјС‹: `5m`, `15m`, `1h`.
- РЎС‚СЂР°С‚РµРіРёРё: 2 active + 1 shadow.
- Р РµР¶РёРјС‹:
  - advisory,
  - shadow-forward,
  - paper execution,
  - live execution РїРѕРґ feature flag.
- РљР°РЅР°Р» РґРѕСЃС‚Р°РІРєРё: Telegram.
- Execution path: StockSharp sidecar -> QUIK -> Finam.
- РСЃС‚РѕСЂРёС‡РµСЃРєРёРµ Рё Р°РЅР°Р»РёС‚РёС‡РµСЃРєРёРµ С‚Р°Р±Р»РёС†С‹: Delta Lake.
- Runtime state: PostgreSQL.
- РћСЂРєРµСЃС‚СЂР°С†РёСЏ: Dagster.
- РўСЏР¶С‘Р»С‹Рµ РїРµСЂРµСЃС‡С‘С‚С‹: Spark.
- Р‘С‹СЃС‚СЂС‹Р№ Р»РѕРєР°Р»СЊРЅС‹Р№ Р°РЅР°Р»РёР·: PyArrow + Delta Lake table reads.
- Research/backtest: governed profile combines vectorbt (ADR-012, research-only) with the internal research/backtest engine; runtime/live execution remains independent from vectorbt internals.

### 4.2 Full / scale-up

- РќРѕРІС‹Рµ РєР»Р°СЃСЃС‹ Р°РєС‚РёРІРѕРІ: Р°РєС†РёРё, РѕР±Р»РёРіР°С†РёРё, FX, ETF.
- РќРѕРІС‹Рµ data feeds: fundamentals, news/event feeds.
- Graph/relatedness context РјРµР¶РґСѓ Р°РєС‚РёРІР°РјРё.
- Portfolio/risk orchestration.
- Multi-timeframe orchestration.
- Р”РѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Рµ execution adapters.
- Р’РѕР·РјРѕР¶РЅРѕСЃС‚СЊ РїРµСЂРµС…РѕРґР° Рє direct Finam API РёР»Рё РѕС‚РґРµР»СЊРЅРѕРјСѓ MOEX direct path.

## 5. Р–С‘СЃС‚РєРёРµ Р°СЂС…РёС‚РµРєС‚СѓСЂРЅС‹Рµ РёРЅРІР°СЂРёР°РЅС‚С‹

1. Р’РµСЂС…РЅРµСѓСЂРѕРІРЅРµРІСѓСЋ DFD РЅРµ РјРµРЅСЏС‚СЊ.
2. Data plane, research plane, runtime plane Рё execution plane РґРµСЂР¶Р°С‚СЊ СЂР°Р·РґРµР»СЊРЅРѕ.
3. Research РјРѕР¶РµС‚ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ continuous series, live execution вЂ” С‚РѕР»СЊРєРѕ `contract_id`.
4. StockSharp sidecar РЅРµ СЃРѕРґРµСЂР¶РёС‚ strategy logic.
5. Broker/execution adapter вЂ” РёСЃС‚РѕС‡РЅРёРє РёСЃС‚РёРЅС‹ РґР»СЏ live order/fill/position state.
6. Runtime РЅРµ РґРѕР»Р¶РµРЅ Р·Р°РІРёСЃРµС‚СЊ РѕС‚ Spark.
7. Strategy layer РЅРµ РґРѕР»Р¶РµРЅ Р·Р°РІРёСЃРµС‚СЊ РѕС‚ Telegram Рё StockSharp.
8. Raw layer РЅРµ С‡РёС‚Р°РµС‚СЃСЏ РЅР°РїСЂСЏРјСѓСЋ strategy/runtime РєРѕРґРѕРј.
9. Р’СЃРµ side effects РёРґРµРјРїРѕС‚РµРЅС‚РЅС‹.
10. Р”Р»СЏ СЃРёРіРЅР°Р»РѕРІ Рё РёСЃРїРѕР»РЅРµРЅРёСЏ С…СЂР°РЅРёС‚СЊ Рё snapshot, Рё event history.
11. Shell governance РѕСЃС‚Р°С‘С‚СЃСЏ РіР»Р°РІРЅС‹Рј РїСЂРѕС†РµСЃСЃРЅС‹Рј РєРѕРЅС‚СѓСЂРѕРј.
12. Product configs РЅРµ РґРѕР±Р°РІР»СЏС‚СЊ РІ root `configs/`, РїРѕРєР° РЅРµ С‚СЂРµР±СѓРµС‚СЃСЏ РёР·РјРµРЅРµРЅРёРµ shell.

## 6. Р¦РµР»РµРІР°СЏ Р°СЂС…РёС‚РµРєС‚СѓСЂР°

### 6.1 Control plane (СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚)

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

### 6.2 Application plane (СЂР°Р·СЂР°Р±Р°С‚С‹РІР°РµС‚СЃСЏ РїРѕ СЌС‚РѕРјСѓ РўР—)

#### Data plane
- `P1 Data Ingestion`
- `P2 Canonical Data Builder/Orchestrator`
- `D1 Raw Data Lake`
- `D2 Canonical Market Data`

#### Research plane
- `P3 Indicator Engine`
- `P4 Indicator-based Backtest`
- `P9 Event-Based Forward`
- `D3 Indicator / Derived Indicator Store`
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

## 7. Р’С‹Р±СЂР°РЅРЅС‹Р№ С‚РµС…РЅРѕР»РѕРіРёС‡РµСЃРєРёР№ СЃС‚РµРє

- Python 3.12+
- Delta Lake
- Apache Spark
- Dagster
- vectorbt (governed research-only profile, ADR-012)
- pandas-ta-classic (import namespace `pandas_ta_classic`, no fallback)
- PostgreSQL
- FastAPI
- custom Bot API publication engine (aiogram removed by ADR-011)
- SQL migration runner (Alembic removed by ADR-011)
- Prometheus + Loki observability path (OpenTelemetry removed by ADR-011)
- Prometheus + Grafana + Loki
- Docker Compose
- .NET 8 sidecar
- StockSharp

## 8. РћР±С‰РёРµ С‚СЂРµР±РѕРІР°РЅРёСЏ Рє СЂРµР°Р»РёР·Р°С†РёРё

### 8.1 РљРѕРґРѕРІР°СЏ РѕСЂРіР°РЅРёР·Р°С†РёСЏ
- Product code Р¶РёРІС‘С‚ РІ `src/trading_advisor_3000/`.
- Product tests Р¶РёРІСѓС‚ РІ `tests/product-plane/`.
- Product docs Р¶РёРІСѓС‚ РІ `docs/architecture/product-plane/`, `docs/runbooks/app/`.
- Sidecar deployment artifacts Р¶РёРІСѓС‚ РІ `deployment/stocksharp-sidecar/`.

### 8.2 РљРѕРЅС‚СЂР°РєС‚С‹
- Р’СЃРµ РєР»СЋС‡РµРІС‹Рµ DTO Рё СЃРѕР±С‹С‚РёСЏ РґРѕР»Р¶РЅС‹ Р±С‹С‚СЊ С‚РёРїРёР·РёСЂРѕРІР°РЅС‹.
- Р’РµСЂСЃРёРѕРЅРёСЂСѓРµРјС‹Рµ СЃС…РµРјС‹ Рё РїСѓР±Р»РёС‡РЅС‹Рµ РёРЅС‚РµСЂС„РµР№СЃС‹ РѕС„РѕСЂРјР»СЏСЋС‚СЃСЏ РєР°Рє contracts.
- Р›СЋР±РѕРµ РёР·РјРµРЅРµРЅРёРµ contract surface С‚СЂРµР±СѓРµС‚:
  - contract tests,
  - docs update,
  - РїСЂРё РЅРµРѕР±С…РѕРґРёРјРѕСЃС‚Рё ADR update.

### 8.3 Р”Р°РЅРЅС‹Рµ
- Delta РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґР»СЏ РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… Рё Р°РЅР°Р»РёС‚РёС‡РµСЃРєРёС… С‚Р°Р±Р»РёС†.
- PostgreSQL РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґР»СЏ Р¶РёРІРѕРіРѕ СЃРѕСЃС‚РѕСЏРЅРёСЏ.
- Runtime СЂР°Р±РѕС‚Р°РµС‚ РЅР° РЅРѕСЂРјР°Р»РёР·РѕРІР°РЅРЅС‹С… canonical bars.
- `mode` РѕР±СЏР·Р°С‚РµР»РµРЅ РґР»СЏ forward/paper/live СЃСѓС‰РЅРѕСЃС‚РµР№.

### 8.4 РџСЂРѕС†РµСЃСЃ
- РљР°Р¶РґР°СЏ С„Р°Р·Р° РёР·РѕР»РёСЂРѕРІР°РЅР° Рё РёРјРµРµС‚ СЃРІРѕР№ gate.
- РќРµР»СЊР·СЏ РїРµСЂРµС…РѕРґРёС‚СЊ Рє СЃР»РµРґСѓСЋС‰РµР№ С„Р°Р·Рµ Р±РµР· Р·Р°РєСЂС‹С‚РёСЏ acceptance С‚РµРєСѓС‰РµР№.
- Р›СЋР±РѕРµ РёР·РјРµРЅРµРЅРёРµ shell-sensitive paths РІС‹РґРµР»СЏРµС‚СЃСЏ РІ РѕС‚РґРµР»СЊРЅС‹Р№ patch set.

## 9. Р¦РµР»РµРІС‹Рµ KPI MVP

### РџСЂРѕРґСѓРєС‚РѕРІС‹Рµ
- 2вЂ“15 СЃРёРіРЅР°Р»РѕРІ РІ РЅРµРґРµР»СЋ РЅР° Р°РєС‚РёРІРЅРѕРј whitelist.
- РџРѕР»РЅС‹Р№ lifecycle Telegram: create/edit/close/cancel.
- Shadow-forward Рё paper execution РёСЃРїРѕР»СЊР·СѓСЋС‚ С‚Рµ Р¶Рµ strategy contracts, С‡С‚Рѕ Рё runtime.

### РљР°С‡РµСЃС‚РІРµРЅРЅС‹Рµ
- РџРѕР»РЅР°СЏ С‚СЂР°СЃСЃРёСЂРѕРІРєР°: bar -> indicator frame -> derived indicator frame -> signal -> publication -> outcome.
- РџРѕРІС‚РѕСЂСЏРµРјРѕСЃС‚СЊ backtest run РЅР° РѕРґРЅРѕРј dataset version.
- РћС‚СЃСѓС‚СЃС‚РІРёРµ look-ahead Рё РЅРµСЃРѕРіР»Р°СЃРѕРІР°РЅРЅС‹С… live fills.

### РђСЂС…РёС‚РµРєС‚СѓСЂРЅС‹Рµ
- Р”РѕР±Р°РІР»РµРЅРёРµ РЅРѕРІРѕР№ СЃС‚СЂР°С‚РµРіРёРё Р±РµР· РїРµСЂРµРїРёСЃС‹РІР°РЅРёСЏ runtime СЏРґСЂР°.
- Р”РѕР±Р°РІР»РµРЅРёРµ РЅРѕРІРѕРіРѕ data provider Р±РµР· РїРµСЂРµРїРёСЃС‹РІР°РЅРёСЏ canonical contracts.
- Р”РѕР±Р°РІР»РµРЅРёРµ РЅРѕРІРѕРіРѕ execution adapter Р±РµР· РїРµСЂРµРїРёСЃС‹РІР°РЅРёСЏ decision logic.
- РњР°СЃС€С‚Р°Р±РёСЂРѕРІР°РЅРёРµ data plane Р±РµР· РјРёРіСЂР°С†РёРё runtime state layer.

## 10. РћСЃРЅРѕРІРЅС‹Рµ deliverables

- product-plane monorepo structure РІРЅСѓС‚СЂРё СЃСѓС‰РµСЃС‚РІСѓСЋС‰РµРіРѕ repo,
- contracts Рё migrations,
- DFD/ERD Рё ADR package,
- Dagster definitions,
- Spark jobs,
- runtime services,
- Telegram bot,
- forward/paper/live execution contracts,
- StockSharp sidecar contract,
- tests/product-plane РґР»СЏ unit/integration/e2e,
- runbooks,
- Codex/MCP integration docs,
- С„Р°Р·РЅС‹Рµ acceptance gates.

## 11. Р”РѕРєСѓРјРµРЅС‚С‹ РїР°РєРµС‚Р°

- `docs/archive/product-plane-spec-v2/2026-05-06/00_AI_Shell_Alignment.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/01_Architecture_Overview.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/02_Repository_Structure.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/03_Data_Model_and_Flows.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/04_ADRs.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/05_Modules_DoD_and_Parallelization.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/06_Capability_Slices_and_Acceptance_Gates.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/07_Tech_Stack_and_Open_Source.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/08_Codex_AI_Shell_Integration.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/09_Constraints_Risks_and_NFR.md`
- `docs/archive/product-plane-spec-v2/2026-05-06/10_MCP_Deployment_Request.md`
