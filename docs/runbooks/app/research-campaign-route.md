# Research Campaign Route

## Purpose
This runbook defines the only supported user-facing route for Product Plane research runs.

Official command:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config <campaign.yaml>
```

The runner is a thin Product Plane front door.
It does not compute indicators, features, or backtests by itself.
It validates a machine-readable campaign config, writes immutable run artifacts, and dispatches only into the Dagster research contour.

## Contracts

Input contract:
- `src/trading_advisor_3000/product_plane/contracts/schemas/research_campaign.v1.json`

Run output contract:
- `src/trading_advisor_3000/product_plane/contracts/schemas/research_run_summary.v1.json`

## Storage Model

Recommended committed configs should point to external-first storage under `D:/TA3000-data`.

Storage split:
- reusable gold layer: `<materialized_root>/`
- immutable per-run artifacts: `<runs_root>/<campaign_name>/<run_id>/`

The same compatible campaign can reuse a materialized layer.
`materialization_key` is stored in `materialization.lock.json` and run metadata; it is not a physical folder segment.
Each execution still gets a fresh `run_id` and a fresh immutable run folder.

## Stage Selection

The route is selected strictly by `target_stage` in the campaign config:
- `data_prep` -> materialize reusable research data prep only: continuous front, dataset, instrument tree, bar view, base indicator, and derived indicator layers
- `backtest` -> reuse or rebuild research data prep, refresh family/template registry rows, resolve campaign `strategy_space` into `StrategyFamilySearchSpec`, then run vectorbt family-search surfaces and rankings
- `projection` -> run the full route through candidate projection

The scheduled freshness contour is `research_data_prep_job`.
It is triggered after `moex_baseline_update_job` succeeds so `continuous_front_refresh` and materialized research data stay current with the canonical MOEX baseline.
Strategy refresh is separate because strategy inventory changes are not the same decision as data freshness.
Backtest execution must not pre-materialize thousands of `StrategyInstance` rows. The accepted order is family search spec -> optional Delta-first optimizer study/trials -> MTF input resolver -> vectorbt `SignalFactory.from_choice_func` -> vectorbt `Portfolio.from_signals` over selected param chunks -> param_hash metrics/gates/ranking. `StrategyInstance` materialization belongs only to post-ranking promotion.

Backtest input loading is part of the battle contour, not a convenience layer. It must read `research_bar_views.delta`, `research_indicator_frames.delta`, and `research_derived_indicator_frames.delta` through native Delta/Arrow predicates and column projection derived from the selected `StrategyFamilySearchSpec`. Do not use Python row-object reloaders or full-table row-list scans as a fallback for active campaign backtests.

Research data-prep writes follow the same rule. Dataset-version and indicator/derived partitions must be replaced through Delta predicates and append writes. Do not rebuild an existing materialized table by loading all rows into Python, filtering them there, and overwriting the table.

Dagster asset handoff in the research route must also stay small. Assets may pass table manifests, paths, row counts, and run metadata between steps; they must not pass large backtest result row lists such as strategy stats or trade records through the Dagster IO manager. Ranking and projection steps read their required inputs back from the persisted Delta result tables.

Before integration or promotion of a changed data-prep contour, run a forced data-prep proof into a separate verification root, not into `research/gold/current`. The committed proof config is:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config product-plane/campaigns/moex_approved_subset_optuna_15m.trend_mtf_pullback_research.data_prep_proof.yaml
```

The resulting `run-summary.json` must include `result_digest.data_prep_proof.mode=forced_refresh` and `_delta_log` proof for the dataset, bar, indicator, and derived-indicator tables.

When `strategy_space.optimizer.engine=optuna`, Optuna runs in memory and persists official provenance only through Delta tables:
- `research_optimizer_studies.delta`
- `research_optimizer_trials.delta`

The optimizer uses Optuna ask/tell batching and records only Optuna trial rows as optimizer provenance. The optimizer tables explain search decisions. They do not replace `research_vbt_param_results.delta`, rankings, or promotion gates.

Full or battle research results are canonical only when launched through `run_campaign` and
registered in `research_campaign_runs.delta`. Benchmark scripts are allowed only as
noncanonical speed/proof tooling; they must write under:

`D:/TA3000-data/trading-advisor-3000-nightly/research/runs/_benchmarks/<benchmark_id>/`

Treat benchmark JSON as a receipt, not a result store. The campaign tables, vectorbt result
tables, ranking tables, and projection tables remain the queryable research model.

Accepted baseline defaults should resolve to:
- canonical root: `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current`
- materialized root: `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current`
- registry root: `D:/TA3000-data/trading-advisor-3000-nightly/research/registry/current`
- runs root: `D:/TA3000-data/trading-advisor-3000-nightly/research/runs`

The canonical root must include `canonical_bars.delta`, `canonical_bar_provenance.delta`,
`canonical_session_calendar.delta`, and `canonical_roll_map.delta`.
The MOEX canonical job owns all four tables.
Research data prep consumes them, builds `continuous_front_bars.delta`, `continuous_front_roll_events.delta`, `continuous_front_adjustment_ladder.delta`, and `continuous_front_qc_report.delta`, then builds the reusable research layer.
`continuous_front` is historical/batch research truth only and must not be used as a live intraday decision source.

## Run Artifacts

Each run folder contains:
- `campaign.lock.json`
- `status.json`
- `run-summary.json`
- `artifacts-index.json`
- `logs/stdout.log`
- `logs/stderr.log`

For backtest and projection stages, `result_digest` separates:
- `best_overall_rows` / `ranking_top_rows`: highest scoring rows, including rows blocked by policy;
- `projection_eligible_top_rows`: highest scoring rows that actually satisfy projection policy gates.

Status transitions are:
- `queued`
- `running`
- terminal `success | failed | blocked`

## Route Boundary

Operator-facing research execution starts and ends at `run_campaign`.
Scheduled freshness remains Dagster-owned through `research_data_prep_job` after `moex_baseline_update_job`.
Implementation modules and benchmark tooling may exist inside the repo, but they are not part of the committed operator path for research campaigns.
