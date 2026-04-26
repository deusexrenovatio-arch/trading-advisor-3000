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
  -> strategy registry refresh, when strategy inventory changes or a campaign needs instances
  -> research_strategy_families / research_strategy_templates / research_strategy_template_modules
  -> research_strategy_instances / research_strategy_instance_modules
  -> research_backtest_batches / runs / stats / trades / orders / drawdowns
  -> research_strategy_rankings / research_run_findings
  -> research_signal_candidates
  -> research_run_stats_index / research_rankings_index / research_strategy_notes
  -> runtime
```

What this means in practice:
- expensive indicator and derived-indicator work is moved out of the hot backtest loop;
- `research_data_prep_job` is the product data-prep contour and is triggered after the canonical MOEX baseline update succeeds;
- strategy registry refresh is intentionally separate from data prep, so strategy inventory changes do not masquerade as canonical data freshness work;
- strategy templates and concrete strategy instances live in Delta registry tables instead of Python-only catalog state;
- repeated research runs reuse the materialized layer and in-process cache;
- successful campaign runs publish global run-stat/ranking indices and strategy notes from the research registry root;
- runtime consumes projected candidates instead of knowing research internals.

## Strategy Registry Layer

The research strategy registry layer in Delta includes:
- `research_strategy_families`
- `research_strategy_templates`
- `research_strategy_template_modules`
- `research_strategy_instances`
- `research_strategy_instance_modules`
- `research_campaigns`
- `research_campaign_runs`
- `research_run_stats_index`
- `research_rankings_index`
- `research_strategy_notes`

Frozen Stage 2 adapter inventory (must stay exactly five until a governed source amendment):
1. `ma_cross` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.ma_cross.ma_cross_family_adapter`
2. `breakout` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.breakout.breakout_family_adapter`
3. `mean_reversion` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.mean_reversion.mean_reversion_family_adapter`
4. `mtf_pullback` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.mtf_pullback.mtf_pullback_family_adapter`
5. `squeeze_release` -> `python_adapter:trading_advisor_3000.product_plane.research.strategies.families.squeeze_release.squeeze_release_family_adapter`

Provenance policy for template rows:
1. `author_source` and `source_ref` are mandatory provenance fields in Stage 2 generated template manifests.
2. `python_adapter` rows are compiler outputs only and must include the adapter import-path `source_ref`.
3. `repo_seed` rows are the canonical override source when both sources describe the same template identity (`family_key`, `template_key`, `template_version`); `python_adapter` rows remain traceability evidence and do not silently overwrite seeded canonical rows.
4. `StrategySpec` and `StrategyCatalog` remain execution adapters only and are not a supported canonical storage route.

This policy keeps inventory/provenance semantics stable while campaign routing runs through `strategy_space`.

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

Strategy registry layer (Stage 2 pre-cutover):
- `research_strategy_families`
- `research_strategy_templates`
- `research_strategy_template_modules`
- `research_strategy_instances`
- `research_strategy_instance_modules`
- `research_campaigns`
- `research_campaign_runs`
- `research_run_stats_index`
- `research_rankings_index`
- `research_strategy_notes`

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
- `research_run_findings`

## Storage Model

Research storage is split into two layers:
- reusable gold outputs under `<materialized_root>/`
- immutable run artifacts under `<runs_root>/<campaign_name>/<run_id>/`

`materialization_key` remains the reproducibility fingerprint for a gold snapshot and is stored in
`materialization.lock.json` plus run metadata; it is not part of the physical folder layout.

Committed example configs should prefer external-first roots under `D:/TA3000-data`.
Worktree-local roots are allowed only when a config explicitly asks for them.

## Derived Indicator And Feature Semantics

`research_derived_indicator_frames` is the wide causal technical-relationship layer between base indicators and curated feature sets.
It keeps one row per candle and versioning separate from both base indicators and features.

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
