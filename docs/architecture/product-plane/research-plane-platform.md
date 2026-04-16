# Research Plane Platform

## Purpose
This page is the stable map of the current research platform.

It replaces the old mental model where research was mostly a snapshot-centric path with a candidate table at the end.
The primary route is now a materialized, versioned pipeline with explicit bootstrap, backtest, ranking, and projection stages.

## Primary Path

```text
canonical data
  -> research_datasets / research_bar_views
  -> research_indicator_frames
  -> research_feature_frames
  -> research_backtest_batches / runs / stats / trades / orders / drawdowns
  -> research_strategy_rankings
  -> research_signal_candidates
  -> runtime
```

What this means in practice:
- expensive indicator and feature work is moved out of the hot backtest loop;
- repeated research runs reuse the materialized layer and in-process cache;
- runtime consumes projected candidates instead of knowing research internals.

## Stable Entry Points

### Supported Operational Route
- `python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config <campaign.yaml>`

This is the only supported user-facing route.
It is a thin Product Plane launcher that validates a machine-readable campaign config, writes immutable run artifacts, and dispatches only into the Dagster `phase2b` contour.

Campaign contracts:
- `research_campaign.v1.json`
- `research_run_summary.v1.json`

### Programmatic Compatibility Route
- `trading_advisor_3000.product_plane.research.run_research_from_bars(...)`

This API remains available for programmatic compatibility, but it is not the supported operational route for research campaigns.

### Internal / Debug Only
- `python -m trading_advisor_3000.product_plane.research.jobs.bootstrap`
- `python -m trading_advisor_3000.product_plane.research.jobs.backtest`
- `python -m trading_advisor_3000.product_plane.research.jobs.project_candidates`
- `python -m trading_advisor_3000.product_plane.research.jobs.benchmark`
- `phase2b_bootstrap_job`
- `phase2b_backtest_job`
- `phase2b_projection_job`

These remain execution/debug surfaces, not the canonical front door.

## What Is Materialized

Bootstrap layer:
- `research_datasets`
- `research_bar_views`
- `research_indicator_frames`
- `research_feature_frames`

Backtest layer:
- `research_backtest_batches`
- `research_backtest_runs`
- `research_strategy_stats`
- `research_trade_records`
- `research_order_records`
- `research_drawdown_records`
- `research_strategy_rankings`

Projection layer:
- `research_signal_candidates`

## Storage Model

Research storage is split into two layers:
- reusable materialized outputs under `<materialized_root>/<materialization_key>/`
- immutable run artifacts under `<runs_root>/<campaign_name>/<run_id>/`

Committed example configs should prefer external-first roots under `D:/TA3000-data`.
Worktree-local roots are allowed only when a config explicitly asks for them.

## Derived Feature Helper Semantics

The feature layer now includes explicit helper outputs that downstream strategy and projection code can read directly:
- `breakout_ready_flag`
- `reversion_ready_flag`
- `atr_stop_ref_1x`
- `atr_target_ref_2x`

These are not decorative metadata.
They are materialized together with the rest of the feature frame and travel as part of the stable contract.

## Benchmark Evidence

Benchmark evidence is now a first-class artifact, not a side comment in tests.

The benchmark job produces:
- machine-readable JSON report;
- markdown summary;
- cache hit and miss markers;
- cold vs hot timings;
- param-scalability timings and completion evidence.

Package-level acceptance now also reads the committed benchmark artifact and checks that the recorded threshold verdicts stay true.

Current committed evidence:
- `artifacts/benchmarks/research-phase2b-benchmark-2026-04-14/phase2b-benchmark-report.json`
- `artifacts/benchmarks/research-phase2b-benchmark-2026-04-14/phase2b-benchmark-report.md`
- `artifacts/benchmarks/research-phase2b-benchmark-2026-04-14/cache-markers.log`

## Legacy Status

The following paths remain in the repository, but they are no longer the accepted primary route:
- `trading_advisor_3000.product_plane.research.compat.legacy_pipeline`
- `trading_advisor_3000.spark_jobs.research_candidates_job`

Their status is:
- compatibility only;
- historical bridge only;
- not used as acceptance truth for the current research platform;
- on a removal path, not a hidden second center of gravity.

## Operational Reading Rule

If there is a disagreement between old phase notes and current behavior:
- trust [STATUS.md](docs/architecture/product-plane/STATUS.md) for overall implemented reality;
- trust this page for research-plane primary-path semantics;
- trust the runbook for how to execute and inspect the platform today.
