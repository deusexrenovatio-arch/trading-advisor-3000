# Research Plane Operations

## Purpose
This runbook describes how to operate the current research plane without relying on historical task notes.

## Official Route

Use the campaign runner for every supported research execution:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config product-plane/campaigns/fut_br_base_15m.explore.yaml
```

This is the canonical Product Plane route.
It validates `research_campaign.v1.json`, writes immutable run artifacts, and dispatches only into the Dagster `phase2b` contour.

## How To Choose `target_stage`

Use `bootstrap` when canonical data changed or indicator/feature definitions changed.

Use `backtest` when the reusable materialized layer is ready and you want strategy execution plus ranking outputs.

Use `projection` when ranked research results should become runtime candidates.

## Storage Expectations

Recommended committed configs should point to `D:/TA3000-data`.

Storage is split on purpose:
- reusable materialized layer: `<materialized_root>/<materialization_key>/`
- immutable run artifacts: `<runs_root>/<campaign_name>/<run_id>/`

## What To Inspect

For `bootstrap`:
- `rows_by_table`
- `durations.total_seconds`
- `dagster_materialized_assets`
- `warnings`

For `backtest`:
- `rows_by_table` for runs, stats, trades, orders, and drawdowns
- `durations.total_seconds`
- `dagster_selected_assets`
- `dagster_materialized_assets`
- `result_digest.ranking_top_rows`

For `projection`:
- candidate table row count
- selection policy and lag settings
- output path for `research_signal_candidates`
- `result_digest.candidate_count`
- `result_digest.candidate_rows_by_strategy`

For all stages:
- `campaign.lock.json`
- `status.json`
- `run-summary.json`
- `artifacts-index.json`
- `logs/stdout.log`
- `logs/stderr.log`

## Expected Success Pattern

A healthy run should show:
- `status=success`
- `campaign.lock.json` persisted
- expected Delta tables present with `_delta_log`
- materialized layer reused only when `materialization_key` matches and rematerialization was not forced

Important distinction:
- a green run means the route completed and the declared artifact set exists.
- it does not automatically mean the strategy payload is useful.
- if ranking or candidate outputs are `0`, treat that as a research/data issue, not as a platform success signal.

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
- if rankings exist but candidates are `0`, check `decision_lag_bars_max`, current signal freshness, and whether the selected policy requires `policy_pass=1`.

## Internal / Debug Only

The following entrypoints remain available, but they are internal/debug-only and not the supported route:
- `python -m trading_advisor_3000.product_plane.research.jobs.bootstrap`
- `python -m trading_advisor_3000.product_plane.research.jobs.backtest`
- `python -m trading_advisor_3000.product_plane.research.jobs.project_candidates`
- `python -m trading_advisor_3000.product_plane.research.jobs.benchmark`
