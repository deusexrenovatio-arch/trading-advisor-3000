# Research Plane Platform

## Purpose
This page is the stable map of the current research platform.

It replaces the old mental model where research was mostly a snapshot-centric path with a candidate table at the end.
The primary route is now a materialized, versioned pipeline with explicit data-prep, strategy-registry, backtest, ranking, and projection stages.

## Primary Path

```text
canonical data
  -> research_datasets / research_bar_views
  -> research_indicator_frames
  -> research_derived_indicator_frames
  -> strategy registry refresh, when strategy inventory changes or a campaign needs family templates
  -> research_strategy_families / research_strategy_templates / research_strategy_template_modules
  -> StrategyFamilySearchSpec
  -> MTF input resolver / vectorbt SignalFactory / vectorbt Portfolio.from_signals
  -> research_strategy_search_specs / research_optimizer_studies / research_optimizer_trials
  -> research_vbt_search_runs / research_vbt_param_results / research_vbt_param_gate_events
  -> compatibility runs / stats / trades / orders / drawdowns
  -> research_strategy_rankings / research_run_findings
  -> research_signal_candidates
  -> promotion events, only for selected winners
  -> research_run_stats_index / research_rankings_index / research_strategy_notes
  -> runtime
```

What this means in practice:
- expensive indicator and derived-indicator work is moved out of the hot backtest loop;
- `research_data_prep_job` is the product data-prep contour and is triggered after the canonical MOEX baseline update succeeds;
- strategy registry refresh is intentionally separate from data prep, so strategy inventory changes do not masquerade as canonical data freshness work;
- strategy templates live in Delta registry tables; concrete `StrategyInstance` rows are created only after promotion from parametric results;
- backtest inputs are read from Delta through native Delta/Arrow predicates and strategy-column projection before Python/vectorbt sees them;
- vectorbt receives family-level MTF-resolved matrix inputs, generates entry/exit indices through `SignalFactory.from_choice_func`, and executes through `Portfolio.from_signals`;
- primary result identity is `param_hash`, while compatibility tables may still expose downstream run/stat/trade rows for ranking and projection consumers;
- adaptive optimizer state is Delta-first run provenance; Optuna proposes trial parameters, while `research_optimizer_studies` and `research_optimizer_trials` explain selected `param_hash` rows and vectorbt result tables remain the execution truth;
- Dagster handoff between research assets passes Delta manifests, paths, row counts, and run metadata; large row sets such as trades, stats, and ranking inputs are read from Delta inside the owning step instead of being pickled through the orchestrator;
- repeated research runs reuse the materialized layer and in-process cache;
- successful campaign runs publish global run-stat/ranking indices and strategy notes from the research registry root;
- runtime consumes projected candidates instead of knowing research internals.

## Strategy Registry Layer

The research strategy registry layer in Delta includes:
- `research_strategy_families`
- `research_strategy_templates`
- `research_strategy_template_modules`
- `research_campaigns`
- `research_campaign_runs`
- `research_run_stats_index`
- `research_rankings_index`
- `research_strategy_notes`

Current adapter inventory for parametric family search:
1. `ma_cross` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.ma_cross.ma_cross_family_adapter`
2. `breakout` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.breakout.breakout_family_adapter`
3. `mean_reversion` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.mean_reversion.mean_reversion_family_adapter`
4. `trend_mtf_pullback_v1` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.mtf_pullback.mtf_pullback_family_adapter`
5. `volatility_squeeze_release_v1` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.squeeze_release.squeeze_release_family_adapter`
6. `trend_movement_cross_v1` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.trend_movement_cross.trend_movement_cross_family_adapter`
7. `channel_breakout_continuation_v1` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.channel_breakout_continuation.channel_breakout_continuation_family_adapter`
8. `range_vwap_band_reversion_v1` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.range_vwap_band_reversion.range_vwap_band_reversion_family_adapter`
9. `failed_breakout_reversal_v1` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.failed_breakout_reversal.failed_breakout_reversal_family_adapter`
10. `divergence_reversal_v1` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.divergence_reversal.divergence_reversal_family_adapter`

Provenance policy for template rows:
1. `author_source` and `source_ref` are mandatory provenance fields in Stage 2 generated template manifests.
2. `python_adapter` rows are compiler outputs only and must include the adapter import-path `source_ref`.
3. `repo_seed` rows are the canonical override source when both sources describe the same template identity (`family_key`, `template_key`, `template_version`); `python_adapter` rows remain traceability evidence and do not silently overwrite seeded canonical rows.
4. `StrategySpec` and `StrategyCatalog` remain execution adapters only and are not a supported canonical storage route.

This policy keeps inventory/provenance semantics stable while campaign routing resolves `strategy_space` into `StrategyFamilySearchSpec` rows.

## Stable Entry Points

### Supported Operational Route
- `python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config <campaign.yaml>`

This is the only supported user-facing route.
It is a thin Product Plane launcher that validates a machine-readable campaign config, writes immutable run artifacts, and dispatches only into the Dagster research contour.

Campaign contracts:
- `research_campaign.v1.json`
- `research_run_summary.v1.json`

### Programmatic API
- `trading_advisor_3000.product_plane.research.run_research_from_bars(...)`

This API remains as a thin programmatic adapter over the materialized research path.
It is not an independent operator route for research campaigns.

### Scheduled Freshness Contour
- `research_data_prep_job`
- `research_data_prep_after_moex_sensor`

Scheduled freshness stays Dagster-owned and follows `moex_baseline_update_job`.
Strategy registry refresh stays campaign-driven instead of exposing a second operator CLI route.

## What Is Materialized

Research data prep layer:
- `research_datasets`
- `research_bar_views`
- `research_indicator_frames`
- `research_derived_indicator_frames`

Strategy registry layer:
- `research_strategy_families`
- `research_strategy_templates`
- `research_strategy_template_modules`
- `research_campaigns`
- `research_campaign_runs`
- `research_run_stats_index`
- `research_rankings_index`
- `research_strategy_notes`

Backtest layer:
- `research_strategy_search_specs`
- `research_optimizer_studies`
- `research_optimizer_trials`
- `research_vbt_search_runs`
- `research_vbt_param_results`
- `research_vbt_param_gate_events`
- `research_vbt_ephemeral_indicator_cache`
- `research_strategy_promotion_events`
- `research_backtest_batches`
- `research_backtest_runs`
- `research_strategy_stats`
- `research_trade_records`
- `research_order_records`
- `research_drawdown_records`
- `research_strategy_rankings`

Projection layer:
- `research_signal_candidates`
- `research_run_findings`

## Storage Model

Research storage is split into two layers:
- reusable gold outputs under `<materialized_root>/`
- immutable run artifacts under `<runs_root>/<campaign_name>/<run_id>/`

Canonical research runs are created only by `run_campaign`. A canonical run must have a
`research_campaign_runs` row that points at its `results_output_dir`, plus the matching
`campaign.lock.json`, `run-summary.json`, `artifacts-index.json`, and Delta result tables.

Benchmark or speed-proof tooling may write Delta-shaped artifacts for inspection, but it must
use `<runs_root>/_benchmarks/<benchmark_id>/` and `artifact_role=benchmark_noncanonical`.
Those folders are not campaign results, must not publish `research_campaign_runs`, and must
not feed the global run-stat/ranking indices. JSON emitted by benchmark tooling is only a
receipt with paths, counts, and timings.

`materialization_key` remains the reproducibility fingerprint for a gold snapshot and is stored in
`materialization.lock.json` plus run metadata; it is not part of the physical folder layout.

Committed example configs should prefer external-first roots under `D:/TA3000-data`.
Worktree-local roots are allowed only when a config explicitly asks for them.

## Delta Input Contract

The battle research/backtest contour must not use Python row-object/list scans as a data-loading fallback.
The accepted sequence is:

```text
Delta table
  -> native Delta predicates for dataset/version/instrument/timeframe/slice
  -> native column projection from StrategyFamilySearchSpec requirements
  -> Arrow/Pandas frame
  -> MTF resolver and vectorbt matrices
```

This keeps Python as orchestration and matrix preparation only.
Materialized research-table replacement follows the same boundary: replace a dataset version or an indicator/derived partition with Delta delete/append semantics, not by reading the full table into Python and writing the preserved rows back.
Historical row-list helpers may remain for compatibility, small metadata, or tests, but they are not a supported route for `research_bar_views`, `research_indicator_frames`, or `research_derived_indicator_frames` in active research, backtest, ranking, or projection execution.

Merge-readiness proof for this contour requires a forced data-prep refresh into a verification root. A reused gold layer proves the backtest path, but it does not prove that the current materialization code can rebuild the full dataset, indicator, and derived-indicator tables. The proof surface is the campaign run summary plus physical `_delta_log` checks for every data-prep table.

## Derived Indicator Semantics

`research_derived_indicator_frames` is the wide causal technical-relationship layer between base indicators and strategies.
It keeps one row per candle and versioning separate from base indicators.

The derived indicator layer includes explicit helper outputs that downstream strategy and projection code can read directly:
- `breakout_ready_flag`
- `reversion_ready_flag`
- `atr_stop_ref_1x`
- `atr_target_ref_2x`

These are not decorative metadata.
They are materialized together with the rest of the derived indicator frame and travel as part of the stable contract.

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
- `artifacts/benchmarks/research-benchmark-2026-04-14/research-benchmark-report.json`
- `artifacts/benchmarks/research-benchmark-2026-04-14/research-benchmark-report.md`
- `artifacts/benchmarks/research-benchmark-2026-04-14/cache-markers.log`

## Legacy Status

The old snapshot-centric compatibility bridge and Spark candidate bridge are retired from the active branch.
They are not part of the accepted research route, not used as acceptance truth, and no longer represent a supported fallback path.

## Operational Reading Rule

If there is a disagreement between old phase notes and current behavior:
- trust [STATUS.md](docs/architecture/product-plane/STATUS.md) for overall implemented reality;
- trust this page for research-plane primary-path semantics;
- trust the runbook for how to execute and inspect the platform today.
