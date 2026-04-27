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

Use `data_prep` when canonical data changed or indicator / derived-indicator definitions changed.
This means research data prep only: datasets, instrument tree, bar views, base indicators, and derived indicators.

Use `backtest` when the reusable materialized layer is ready and you want strategy registry refresh, vectorbt family-search execution, and ranking outputs.

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

The authoritative persisted technical-analysis tables are Delta directories under the reusable gold layer:
- base indicators: `<materialized_root>/research_indicator_frames.delta`
- derived indicators: `<materialized_root>/research_derived_indicator_frames.delta`

For the accepted MOEX baseline these resolve to:
- `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current/research_indicator_frames.delta`
- `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current/research_derived_indicator_frames.delta`

Folders under `research/gold/verification/` are temporary proof or acceptance copies.
They are not the production-current serving location and can be cleaned up after evidence is captured.

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

For materialized-layer freshness:
- partition identity is dataset/profile/instrument/contract/timeframe;
- source fingerprints decide reuse, following the same operating idea as MOEX canonical changed-window refresh;
- base indicators compare `source_bars_hash`;
- both base and derived layers also record `output_columns_hash`, so profile expansion can be distinguished from source-data changes;
- when an indicator or derived-indicator profile is extended with new output columns and source data is unchanged, existing wide-table values are reused and only missing columns are computed before replacing the affected partition;
- derived indicators compare `source_indicators_hash` only over the base indicator columns consumed by derived formulas, so adding an unrelated base indicator does not invalidate derived partitions;
- derived no-op checks reuse stored indicator-source metadata and load base indicator rows only for partitions that actually need a derived refresh;
- unchanged partitions are reused, changed/deleted partitions are replaced, and reports expose refreshed, reused, extended, recomputed, and deleted partition counts.

For Dagster memory behavior:
- research data-prep assets should pass lightweight Delta table summaries between steps, not multi-million-row payloads;
- row counts for materialized tables should be read through Delta metadata/count helpers instead of loading full tables into the Dagster IO manager;
- if a run shows large memory growth on an unchanged data-prep route, inspect whether a new asset started returning rows instead of a summary.

For `backtest`:
- `rows_by_table` for search specs, vectorbt search runs, param results, gate events, runs, stats, trades, orders, and drawdowns
- `durations.total_seconds`
- `dagster_selected_assets`
- `dagster_materialized_assets`
- `result_digest.ranking_top_rows`
Backtest is expected to run from `StrategyFamilySearchSpec` and param chunks. A run that requires pre-materialized `StrategyInstance` rows before vectorbt is using the wrong route.

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
