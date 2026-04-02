# 01. Architecture Overview

## 1. Общая идея

Архитектура состоит из двух больших контуров:

1. **Control plane** — уже существующий AI delivery shell.
2. **Application plane** — платформа сигналов по MOEX futures, описанная этим ТЗ.

Control plane управляет:
- процессом,
- контекстом Codex,
- validation и gates,
- durable state разработки,
- governance reporting.

Application plane управляет:
- market data,
- features/backtests,
- runtime signals,
- Telegram publishing,
- paper/live execution,
- analytics.

## 2. Combined architecture map

```mermaid
flowchart TB
    subgraph CP["Control Plane (existing AI shell)"]
      CP1["AGENTS.md + docs/agent/*"]
      CP2["task_session.py + run_loop_gate.py + run_pr_gate.py + run_nightly_gate.py"]
      CP3["plans/* + memory/*"]
      CP4["tests/process/* + tests/architecture/* + governance dashboard"]
      CP1 --> CP2 --> CP3 --> CP4
    end

    subgraph AP["Application Plane (to implement)"]
      AP1["contracts + domain model"]
      AP2["data plane"]
      AP3["research plane"]
      AP4["runtime plane"]
      AP5["execution plane"]
      AP6["tests/product-plane/* + app runbooks"]
      AP1 --> AP2 --> AP3 --> AP4 --> AP5 --> AP6
    end

    CP2 -. governs patch flow .-> AP1
    CP2 -. governs patch flow .-> AP2
    CP2 -. governs patch flow .-> AP3
    CP2 -. governs patch flow .-> AP4
    CP2 -. governs patch flow .-> AP5
```

## 3. Верхнеуровневая DFD продукта

```mermaid
flowchart LR
    E1[/"E1 Market Data Provider"/]
    E3[/"E3 User / Trader"/]

    P1(("P1 Data Ingestion"))
    P2(("P2 Canonical Data Builder/Orchestrator"))
    P3(("P3 Feature Engine"))
    P4(("P4 Feature-based Backtest"))
    P5(("P5 Signal Decision Engine"))
    P6(("P6 Publishing Engine"))
    P7(("P7 Review / Analytics"))
    P9(("P9 Event-Based Forward"))

    D1[["D1 Raw Data Lake"]]
    D2[["D2 Canonical Market Data"]]
    D3[["D3 Feature / Context Store"]]
    D4[["D4 Signal Candidates"]]
    D5[["D5 Outcome / Analytics Store"]]
    D7[["D7 Active Signals"]]
    D8[["D8 Strategy"]]

    E1 --> P1
    P1 --> D1
    D1 --> P2
    P2 --> D2

    D2 --> P5
    D2 --> P3
    D2 --> P4
    D2 --> P7

    P3 --> D3
    D3 --> P4

    P4 --> D4
    D4 --> P9

    P5 --> D7
    D7 --> P6
    P6 --> E3

    D7 --> P7
    P7 --> D5

    D8 --> P5
    P9 --> D8
```

## 4. Plane decomposition

### 4.1 Data plane
Ответственность:
- ingestion,
- normalization,
- canonical data,
- session/roll correctness.

Технологии:
- Delta Lake,
- Spark (scoped heavy compute),
- PyArrow local dataframe/query path (ADR-011 replacement for Polars/DuckDB),
- Dagster.

### 4.2 Research plane
Ответственность:
- features,
- backtest,
- walk-forward,
- candidate scoring,
- shadow-forward.

Технологии:
- Python,
- internal backtest engine (`src/trading_advisor_3000/product_plane/research/backtest/engine.py`),
- Delta,
- Dagster.

### 4.3 Runtime plane
Ответственность:
- signal decision,
- signal lifecycle,
- Telegram publishing,
- review/analytics.

Технологии:
- Python,
- PostgreSQL,
- FastAPI,
- custom Bot API publication engine (`TelegramPublicationEngine`).

### 4.4 Execution plane
Ответственность:
- order intents,
- broker order/fill sync,
- positions,
- reconciliation,
- live/paper separation.

Технологии:
- Python contracts,
- .NET 8 StockSharp sidecar,
- QUIK connector,
- Finam execution path.

## 5. Ключевая граница: shell vs product

### Shell owns
- task lifecycle,
- context routing,
- validation and governance,
- plans/memory/state of work,
- CI lane semantics,
- skills governance.

### Product owns
- business/domain logic,
- data contracts,
- strategies,
- signal lifecycle,
- runtime services,
- execution integration,
- product analytics.

## 6. Основные architectural decisions

1. **Data lake contract first** — Delta как долгоживущий storage contract.
2. **Heavy compute separate from runtime** — Spark не входит в live decision path.
3. **Execution sidecar** — StockSharp интегрируется как внешний execution bridge.
4. **Forward and live share strategy contracts**.
5. **Control plane and app plane remain loosely coupled** — через процесс, а не через runtime imports.

6. **F1-B terminal replaceable stack policy** — aiogram/Polars/DuckDB/vectorbt/Alembic/OpenTelemetry are resolved through ADR-011 terminal decisions and must not reappear as active chosen stack items.

## 7. Что не входит в MVP

- direct MOEX DMA,
- full portfolio optimizer,
- fundamentals/news ingestion,
- low-latency tick-driven engine,
- automated strategy self-promotion without human governance.
