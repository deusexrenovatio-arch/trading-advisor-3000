# Product-Plane Native Runtime Ownership

## Purpose
This document defines which runtime owns each product-plane data, research,
compute, optimization, and orchestration responsibility.

It is an architecture rule, not a delivery shortcut. Python can coordinate the
route, adapt contracts, and report evidence, but it must not silently replace a
library that already owns the problem shape.

## Core Rule
If requested behavior sits inside a library's documented strength, the native
library primitive owns the core operation.

Custom Python is acceptable for:
- configuration, contracts, and small adapters;
- orchestration between established runtime surfaces;
- validation, reporting, and test fixtures;
- TA3000-specific derived relationships that are not standard library indicators;
- explicit fallback when the native primitive does not fit, with the reason recorded.

Custom Python is not the default owner for durable table materialization,
standard technical indicators, portfolio simulation, optimizer search, or
repeatable orchestration.

## Strong Zones

| Runtime | Owns | Prefer | Avoid by default |
| --- | --- | --- | --- |
| Spark SQL/DataFrames | Large structured transforms, joins, aggregations, windows, batch/stream DataFrame processing, and work that should stay inside Spark's optimized execution engine. | Spark SQL or DataFrame APIs over collections of records. Keep computation distributed until the contract requires collection. | Driver-side loops, local pandas rewrites, or collecting broad tables to Python for transforms Spark can express. |
| Delta Lake | Authoritative durable tables, ACID table updates, schema enforcement/evolution, time travel, batch/stream unification, merge/update/delete/upsert, and reproducible storage proof. | Delta read/write/merge APIs through Spark or the approved Delta connector. Prove writes with `_delta_log`, schema, row counts, and target path. | Raw Parquet folder writes, manual directory mutation, one-off CSV/JSON snapshots, or bypassing Delta for research tables. |
| Dagster | Asset graph ownership, jobs, schedules, sensors, run metadata, freshness, duplicate prevention, and repeatable operational launch. | Assets for materialized data products, jobs for executable selections, schedules/sensors for automation, run keys/cursors for event routes. | Manual chat/script-only continuation for recurring or governed data routes. |
| pandas-ta-classic | Standard technical indicators: candles, momentum, overlap/trend, volatility, volume, returns, statistics, and named bundles of indicators. | `df.ta` methods, standard functions, category/custom `ta.Strategy`, catalog lookup, and `ta.tsignals(..., asbool=True)` as a bridge into vectorbt. | Reimplementing SMA/EMA/RSI/MACD/ATR/VWAP/candle patterns or indicator bundles in custom pandas when the catalog covers them. |
| vectorbt | Signal/order/portfolio simulation, parameterized matrices, broadcasting, stops, long/short direction, grouped metrics, trades, drawdowns, and portfolio statistics. | `Portfolio.from_signals` when the strategy is entries/exits; `from_orders` for explicit order arrays; `from_order_func` only for stateful callbacks or multiple orders per symbol/bar; `SignalFactory` and broadcasting for signal surfaces. | Python trade-ledger loops, per-instrument simulation loops, or hand-built portfolio accounting when vectorbt can own the simulation. |
| Optuna | Adaptive parameter search, sampling, pruning, ask/tell trial orchestration, and optimizer provenance. | Studies, samplers, pruners, `trial.suggest_*`, `study.ask()`, `study.tell()`, and persisted trial/study outputs in the TA3000 research contract. | Custom random/adaptive search loops that do not preserve Optuna study/trial semantics. |
| DuckDB | Local analytical SQL, focused profiling, query-plan inspection, and fast validation against local frames or files. | SQL over pandas/Arrow/Parquet, `EXPLAIN`, `EXPLAIN ANALYZE`, and profiling output for local evidence. | Treating DuckDB as the authoritative product-plane store when the contract requires Delta/Dagster materialization. |

## Product-Plane Split

- Storage truth belongs to Delta-backed product-plane paths under the approved data root.
- Dataflow ownership belongs to Spark/Dagster when the work is durable, repeatable, scheduled, or broad enough to require the product data route.
- Base technical indicators belong to pandas-ta-classic unless the indicator is unavailable or deliberately TA3000-specific.
- Derived indicators belong to TA3000 code only when they represent causal relationships, session logic, multi-timeframe overlays, or strategy-facing relationships that the base indicator library does not own.
- Strategy simulation belongs to vectorbt, with Python assembling aligned matrices and contracts.
- Strategy-family search belongs to Optuna when the search is adaptive, with grid kept as the explicit deterministic fallback.
- DuckDB is a validation and profiling helper, not a replacement for the durable route.

## Required Design Trace
Every non-trivial product-plane runtime change must record:

1. Task domain: data materialization, indicator compute, derived compute, signal generation, backtest, optimizer search, orchestration, or local validation.
2. Runtime owner: Spark, Delta Lake, Dagster, pandas-ta-classic, vectorbt, Optuna, DuckDB, or explicit custom code.
3. Native primitive: for example Spark SQL/DataFrame transform, Delta merge/write, Dagster asset/job/sensor, `df.ta.strategy`, `Portfolio.from_signals`, `SignalFactory`, Optuna `study.ask()` / `study.tell()`, or DuckDB SQL/profiling.
4. Python boundary: what Python is allowed to do in this patch.
5. Proof surface: the evidence that the native path ran, such as Dagster asset/job wiring, Delta `_delta_log`, row counts, vectorbt portfolio outputs, Optuna trial rows, or DuckDB query/profile output.
6. Fallback reason: required only when custom Python owns logic that a library might otherwise cover.

## Red Flags
Stop and record a fallback reason when a design includes:

- loops over bars, instruments, parameter rows, or table rows where Spark, vectorbt, pandas-ta-classic, or Optuna has a documented primitive;
- writing research outputs without Delta transaction evidence;
- a recurring materialization route without Dagster asset/job/sensor wiring;
- custom indicator formulas that match the pandas-ta-classic catalog;
- optimizer state that cannot be traced to Optuna trials or a declared deterministic grid;
- local smoke output presented as production proof.

## Source Docs

- Apache Spark SQL, DataFrames, and Datasets: `https://spark.apache.org/docs/latest/sql-programming-guide.html`
- Delta Lake overview and table operations: `https://docs.delta.io/index.html`, `https://docs.delta.io/quick-start/`, `https://docs.delta.io/delta-update/`
- Dagster assets, jobs, schedules, and sensors: `https://docs.dagster.io/guides/build/assets/defining-assets`, `https://master.dagster.dagster-docs.io/concepts/assets/asset-jobs`, `https://docs.dagster.io/guides/automate/sensors`
- pandas-ta-classic usage, strategies, indicators, and vectorbt bridge: `https://xgboosted.github.io/pandas-ta-classic/usage.html`, `https://xgboosted.github.io/pandas-ta-classic/strategies.html`, `https://xgboosted.github.io/pandas-ta-classic/indicators.html`, `https://xgboosted.github.io/pandas-ta-classic/performance.html`
- vectorbt portfolio, SignalFactory, IndicatorFactory, and broadcasting: `https://vectorbt.dev/api/portfolio/base/`, `https://vectorbt.dev/api/signals/factory/`, `https://vectorbt.dev/api/indicators/factory/`, `https://vectorbt.dev/api/base/reshape_fns/`
- Optuna ask-and-tell and optimization algorithms: `https://optuna.readthedocs.io/en/stable/tutorial/20_recipes/009_ask_and_tell.html`, `https://optuna.readthedocs.io/en/stable/tutorial/10_key_features/003_efficient_optimization_algorithms.html`
- DuckDB SQL on pandas, query plans, and profiling: `https://duckdb.org/docs/current/guides/python/sql_on_pandas`, `https://duckdb.org/docs/current/guides/meta/explain`, `https://duckdb.org/docs/current/dev/profiling`
