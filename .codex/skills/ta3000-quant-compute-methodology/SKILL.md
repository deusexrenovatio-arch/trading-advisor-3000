---
name: ta3000-quant-compute-methodology
description: Use for TA3000 research compute work that touches vectorbt, pandas-ta-classic, Optuna, Spark/Delta/Dagster research materialization, indicators, derived indicators, signal matrices, strategy execution, optimizer search, or backtest integration after strategy intent and measurable TA states are clear, especially when library-native methodology should guide design before local TA3000 patterns.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-COMPUTE
scope: TA3000 library-native compute, optimizer, signal-matrix, runtime-ownership, and materialization integration boundary
routing_triggers:
  - vectorbt
  - pandas-ta-classic
  - pandas_ta_classic
  - Optuna
  - optimizer search
  - native runtime ownership
  - signal matrix
  - from_signals
  - from_order_func
  - technical indicators
  - derived indicators
  - research backtest
  - strategy execution
  - native runtime choice
  - research materialization
  - Spark Delta Dagster
---

# TA3000 Quant Compute Methodology

## When To Use
- A task touches `vectorbt`, `pandas-ta-classic`, `Optuna`, technical indicators, derived indicators, strategy families, signal matrices, backtest execution, optimizer search, or research campaign compute.
- A strategy or indicator implementation is being designed and the strongest library-native pattern is not yet clear.
- A previous implementation risked hand-rolling logic that the libraries already support.

Do not use this for pure storage proof; pair with `ta3000-data-plane-proof` when the claim depends on real `D:/TA3000-data` outputs.

## Boundary With Neighbor Skills
- This skill owns compute methodology: library-native indicators, derived matrices, signal matrices, vectorbt execution, Optuna search, and native runtime choice.
- `ta3000-strategy-research-methodology` owns why the strategy should exist and what evidence would reject it.
- `ta3000-technical-analysis-system-design` owns the measurable TA state model before implementation.
- `ta3000-data-plane-proof` owns proof against authoritative storage roots, Delta logs, row counts, and production materialization evidence.
- `ta3000-backtest-validation-and-overfit-control` owns robustness, walk-forward, OOS, overfit, cost, and slippage acceptance.

## First Move: Library Recon
Before designing or editing compute logic, inspect the relevant library entry points and write down the chosen pattern in the work notes or final summary:

- TA3000 native runtime ownership: `docs/architecture/product-plane/native-runtime-ownership.md`
- TA3000 native runtime selection route: `docs/agent/native-runtime-selection.md`
- VectorBT overview: `https://vectorbt.dev/`
- VectorBT portfolio modes: `https://vectorbt.dev/api/portfolio/base/`
- VectorBT signals accessor: `https://vectorbt.dev/api/signals/accessors/`
- VectorBT signal factory: `https://vectorbt.dev/api/signals/factory/`
- VectorBT indicator factory: `https://vectorbt.dev/api/indicators/factory/`
- Pandas TA Classic usage styles: `https://xgboosted.github.io/pandas-ta-classic/usage.html`
- Pandas TA Classic strategy system: `https://xgboosted.github.io/pandas-ta-classic/strategies.html`
- Pandas TA Classic indicators catalog: `https://xgboosted.github.io/pandas-ta-classic/indicators.html`
- Pandas TA Classic vectorbt bridge: `https://xgboosted.github.io/pandas-ta-classic/performance.html`
- Optuna ask-and-tell: `https://optuna.readthedocs.io/en/stable/tutorial/20_recipes/009_ask_and_tell.html`
- Optuna optimization algorithms: `https://optuna.readthedocs.io/en/stable/tutorial/10_key_features/003_efficient_optimization_algorithms.html`

If API shape, feature availability, or semantics matter, verify the current docs or installed package instead of relying on memory.

## Library-Native Questions
Answer these before choosing an implementation:

0. What is the Native Runtime Choice: pandas-ta-classic, vectorbt, Optuna, Spark, Delta Lake, Dagster, DuckDB, or explicit custom code?
1. Can a standard indicator be expressed with `pandas-ta-classic` rather than custom pandas?
2. Can a bundle of base indicators be expressed as a `df.ta.strategy(...)` or category strategy?
3. Can the trade logic be expressed as boolean `entries` / `exits` and run through `vbt.Portfolio.from_signals(...)`?
4. Is `from_orders(...)` more direct because the strategy already produces order arrays?
5. Is `from_order_func(...)` truly needed because the logic depends on simulation state, complex callbacks, or multiple orders per symbol and bar?
6. Can `vbt.signals` or `SignalFactory` handle signal cleanup, chaining, stops, or analysis instead of a manual loop?
7. Can `IndicatorFactory`, `from_pandas_ta`, parameter grids, or broadcasting keep the implementation vectorized?
8. If the task is adaptive search, can Optuna own the study/trial loop instead of a custom optimizer loop?
9. If the task materializes or refreshes research tables, should Spark, Delta Lake, and Dagster own the route instead of local pandas or one-off scripts?

## Role Split
- Use `pandas-ta-classic` for base market measurements: moving averages, momentum, trend, volatility, volume, candle patterns, returns, and other standard technical indicators.
- Use custom pandas for TA3000 derived relationships: distance, slope, cross state, divergence, session/opening-range logic, VWAP relationships, and multi-timeframe overlays.
- Use `vectorbt` for trade-level semantics: entry/exit matrices, long/short direction, stops, portfolio simulation, grouped metrics, parameter sweeps, and signal analysis.

Practical rule: `pandas-ta-classic` answers "what is the market state?"; `vectorbt` answers "what happens if we trade this state?".

## Native Runtime Boundary
- Keep Python thin when a documented runtime primitive owns the operation: assemble inputs, validate contracts, pass matrices/configs, persist provenance, and report evidence.
- Do not hand-roll portfolio accounting, parameter search, standard indicators, durable table writes, or recurring materialization routes unless the native primitive is explicitly unsuitable.
- For adaptive search, prefer Optuna studies, samplers, pruners, and `study.ask()` / `study.tell()` with TA3000 Delta-backed provenance.
- For durable research outputs, keep Spark/Delta/Dagster as the first-choice dataflow and storage route; a local pandas smoke is not production proof.
- Use DuckDB for focused local SQL validation or profiling, not as a substitute for the authoritative Delta/Dagster path.

## VectorBT Design Bias
- Prefer matrix-shaped inputs over per-instrument loops.
- Preserve index/column meaning so parameter, symbol, timeframe, and split levels remain analyzable after simulation.
- Prefer `Portfolio.from_signals(...)` for signal strategies.
- Escalate to `from_orders(...)` only when order arrays are the natural output.
- Escalate to `from_order_func(...)` only when callbacks or simulation state are required.
- Treat broadcasting as a feature to use deliberately, not as an incidental side effect.
- Keep closed-bar causality explicit; shift signals when execution happens on the next bar.

## Pandas TA Classic Design Bias
- Check the indicator catalog before adding a formula.
- Prefer standard usage when explicit input/output control matters.
- Prefer `df.ta` when OHLCV conventions and append behavior fit the pipeline.
- Prefer `ta.Strategy` when computing a named bundle of base indicators.
- Control column names, prefixes, suffixes, warmup, and NaN handling deliberately.
- Treat `ta.tsignals(..., asbool=True)` as a bridge to `vectorbt.from_signals`, not as a replacement for strategy execution.

## TA3000 Adaptation
- Keep product-plane compute inside the existing materialized indicator and derived-indicator route unless the task explicitly changes the architecture.
- Do not recreate an active feature layer when the requested layer is indicators or derived indicators.
- Keep `research_derived_indicator_frames` as the causal relationship layer between base indicators and strategy/backtest inputs.
- Keep launch strategy intent explicit: intent, regimes, required base/derived columns, entry/exit logic, and verification questions.
- For launch strategies, prefer `1h` or `4h` as primary signal timeframes unless the user reopens the decision; treat `15m` as an execution/trigger timeframe.
- Keep `5m` out of active production scope unless the user explicitly reopens it.
- Add ephemeral cross-sectional ranks in loader/backtest input space when the rank is strategy input, not a durable research table.
- Fail closed on missing required columns and preserve rejected rows with `failure_code`.

## Verification
- Add or update targeted tests for indicator shape, derived causality, signal timing, and required-column failure behavior.
- Verify that generated signals are boolean, index-aligned, and causally shifted for the intended execution bar.
- For vectorbt work, inspect portfolio outputs, rejected rows, and at least one metric path that proves the intended simulation mode ran.
- Run the relevant focused tests, then the repo loop gate for the changed files when the change is more than a docs-only skill edit:
  `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
