# Native Runtime Selection

## Purpose
This is the agent routing shim for the architecture rule in
`docs/architecture/product-plane/native-runtime-ownership.md`.

Use this file during ordinary chat and implementation planning so the agent
does not treat custom Python as the default owner for product-plane data,
research, compute, optimization, or orchestration work.

## Agent Route
Before non-trivial product-plane runtime work:

1. Read `docs/architecture/product-plane/native-runtime-ownership.md`.
2. Choose the runtime owner from the architecture rule.
3. Record the Native Runtime Choice:
   - task domain;
   - runtime owner;
   - native primitive;
   - Python boundary;
   - proof surface;
   - fallback reason, if custom Python owns logic in a documented native zone.
4. Use repo-local `ta3000-quant-compute-methodology` for vectorbt, pandas-ta-classic, Optuna, indicator, signal-matrix, and backtest work.
5. Use global `data-engineer` for Spark, Delta Lake, Dagster, DuckDB, durable materialization, and data-pipeline ownership.

## Routing Bias
- Spark/Delta/Dagster own durable dataflow and storage routes.
- pandas-ta-classic owns standard technical indicators and indicator bundles.
- vectorbt owns strategy simulation and portfolio metrics.
- Optuna owns adaptive strategy-family search.
- DuckDB owns local SQL validation and profiling only.
- Python coordinates, adapts contracts, and reports evidence unless the architecture rule records a fallback.
