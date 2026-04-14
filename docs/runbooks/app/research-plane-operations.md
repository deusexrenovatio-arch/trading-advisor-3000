# Research Plane Operations

## Purpose
This runbook describes how to operate the current research plane without relying on historical task notes.

## When To Use Which Command

Use bootstrap when canonical data changed or indicator and feature definitions changed:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.bootstrap `
  --canonical-output-dir <canonical-dir> `
  --research-output-dir <research-dir> `
  --dataset-version dataset_core_v1 `
  --timeframes 15m 1h `
  --report-json artifacts/benchmarks/bootstrap-report.json
```

Use backtest when the materialized layer is ready and you want strategy execution artifacts:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.backtest `
  --canonical-output-dir <canonical-dir> `
  --research-output-dir <research-dir> `
  --dataset-version dataset_core_v1 `
  --timeframes 15m `
  --strategy-versions ma-cross-v1 breakout-v1 `
  --combination-count 25 `
  --backtest-timeframe 15m `
  --report-json artifacts/benchmarks/backtest-report.json
```

Use projection when ranked research results should become runtime candidates:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.project_candidates `
  --canonical-output-dir <canonical-dir> `
  --research-output-dir <research-dir> `
  --dataset-version dataset_core_v1 `
  --timeframes 15m `
  --strategy-versions ma-cross-v1 `
  --combination-count 10 `
  --backtest-timeframe 15m `
  --selection-policy all_policy_pass `
  --min-robust-score 0.0 `
  --decision-lag-bars-max 4 `
  --report-json artifacts/benchmarks/projection-report.json
```

Use benchmark when you need cold vs hot proof and param-scalability artifacts:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.benchmark `
  --output-dir artifacts/benchmarks/research-phase2b-benchmark-2026-04-14 `
  --dataset-version benchmark_small_v1 `
  --instruments 6 `
  --bars-per-instrument 96 `
  --combination-sizes 10 50 100 250
```

## What To Inspect

For bootstrap:
- `rows_by_table`
- `duration_seconds`
- `contract_validation.status`

For backtest:
- `rows_by_table` for runs, stats, trades, orders, and drawdowns
- `duration_seconds`
- selected and materialized assets

For projection:
- candidate table row count
- selection policy and lag settings
- output path for `research_signal_candidates`

For benchmark:
- cold bootstrap time
- cold backtest time
- hot backtest time
- cache markers
- threshold verdicts

## Expected Success Pattern

A healthy run should show:
- `success=true`
- contract validation passed
- expected Delta tables present with `_delta_log`
- no recompute of indicator and feature layers during hot benchmark rerun

## Failure Interpretation

If bootstrap fails:
- treat the materialized layer as invalid;
- do not proceed to backtest or projection.

If backtest fails:
- inspect the JSON report first;
- check strategy and timeframe selection before debugging engine details.

If projection fails:
- confirm rankings were produced;
- confirm selection policy is not filtering everything out.

If benchmark fails threshold checks:
- inspect `cache-markers.log`;
- compare cold bootstrap weight vs hot backtest duration;
- confirm the benchmark still includes a meaningful scalability sweep.
