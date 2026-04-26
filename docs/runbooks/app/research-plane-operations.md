# Research Plane Operations

## Purpose
This runbook describes how to operate the current research plane without relying on historical task notes.

## Official Route

Use the campaign runner for every supported research execution:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config product-plane/campaigns/fut_br_base_15m.explore.yaml
```

This is the canonical Product Plane route.
It validates `research_campaign.v1.json`, writes immutable run artifacts, and dispatches only into the Dagster research contour.

## How To Choose `target_stage`

Use `data_prep` when canonical data changed or indicator, derived indicator, or feature definitions changed.
This now means research data prep only: datasets, instrument tree, bar views, base indicators, derived indicators, and curated features.

Use `backtest` when the reusable materialized layer is ready and you want strategy registry refresh, strategy execution, and ranking outputs.

Use `projection` when ranked research results should become runtime candidates.

## Storage Expectations

Recommended committed configs should point to `D:/TA3000-data`.
For the accepted MOEX baseline, use:

- canonical root: `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current`
- research gold root: `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current`
- research registry root: `D:/TA3000-data/trading-advisor-3000-nightly/research/registry/current`
- research runs root: `D:/TA3000-data/trading-advisor-3000-nightly/research/runs`

Research data prep expects the canonical root to contain:
- `canonical_bars.delta`
- `canonical_bar_provenance.delta`
- `canonical_session_calendar.delta`
- `canonical_roll_map.delta`

`canonical_session_calendar.delta` and `canonical_roll_map.delta` are canonical sidecars.
They are refreshed by the MOEX canonical job, not by the research job.

Storage is split on purpose:
- reusable gold layer: `<materialized_root>/`
- immutable run artifacts: `<runs_root>/<campaign_name>/<run_id>/`

`materialization_key` is an identity/fingerprint recorded in `materialization.lock.json` and run artifacts.
It is not used as a physical folder segment.

## What To Inspect

For `data_prep`:
- `rows_by_table`
- `research_instrument_tree` coverage by instrument, contract, timeframe, and lineage hash
- `durations.total_seconds`
- `dagster_materialized_assets`
- `warnings`

For scheduled freshness:
- `research_data_prep_job` should run after `moex_baseline_update_job` succeeds
- `research_data_prep_after_moex_sensor` is the Dagster handoff from canonical MOEX refresh to research data prep
- `strategy_registry_refresh_job` remains separate and should be run when strategy inventory or campaign strategy-space inputs change

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

If data prep fails:
- treat the materialized layer as invalid;
- do not proceed to backtest or projection.

If backtest fails:
- inspect the JSON report first;
- check strategy and timeframe selection before debugging engine details.

If projection fails:
- confirm rankings were produced;
- confirm selection policy is not filtering everything out.
- if rankings exist but candidates are `0`, check `decision_lag_bars_max`, current signal freshness, and whether the selected policy requires `policy_pass=1`.

## Operational Boundary

Use `run_campaign` for operator-driven research runs.
Use `research_data_prep_job` plus `research_data_prep_after_moex_sensor` for scheduled freshness after MOEX canonical refresh.
Do not treat implementation modules or benchmark tooling as separate supported run paths.
