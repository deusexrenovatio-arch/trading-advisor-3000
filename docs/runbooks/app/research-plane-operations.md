# Research Plane Operations

## Purpose
This runbook describes how to operate the current research plane without relying on historical task notes.

## Official Route

Use existing Dagster jobs for every supported research execution:

```yaml
campaign_config_path: product-plane/campaigns/fut_br_base_15m.explore.yaml
```

Launch the matching job in Dagster UI/daemon/CLI:
- `research_data_prep_job`
- `strategy_registry_refresh_job`
- `research_backtest_job`
- `research_projection_job`

The config is mapped through `research_campaign_context`. It validates `research_campaign.v1.json`, writes immutable start artifacts, and expands the internal stage asset config. `run_campaign` is removed from the active product surface and must not be used as an operator route.

## How To Choose `target_stage`

Use `data_prep` when canonical data changed or indicator / derived-indicator definitions changed.
This means research data prep only: datasets, instrument tree, bar views, base indicators, and derived indicators.

Use `backtest` when the reusable materialized layer is ready and you want strategy registry refresh, vectorbt family-search execution, and ranking outputs.
For reused materialization, the Dagster backtest surface is `research_backtest_job` with campaign config on `research_campaign_context`; it must validate the existing gold layer and must not materialize data-prep assets.

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

## Reviewed hash transition

The pre/post volume-profile invalidation releases have one finite compatibility
bridge in `research/hash_compatibility.py`. Only the two recorded deployed source
hashes can migrate to its exact reviewed package fingerprint. The fingerprint
covers every Python path and byte, normalizes CRLF, and substitutes only its own
single anchored digest literal. Any other source/dependency/contract/profile
change retains conservative admission. Explicit full admission reaches both L1/L2
numerical owners as `force_recompute`, disabling metadata, partition, extension
and cached-prefix reuse. Freeze the literal after the final source
diff; independent review must bind that same diff. Do not add aliases for an
unreviewed numerical change.

`hash_compatibility` is an admission receipt carrying accepted Research id/hash
and old source, not an operator permission switch. The snapshot owner rechecks
historical Data pins, unchanged current Research outputs, mandatory PASS QC and
successful native owners. Changed Data is admitted only through native bounded
ancestry with exact names/path/ID/schema/layout on every edge. Runtime independently
resolves and compares complete instrument/TF/UTC windows; contract filters and
narrowed ranges reject admission. Canonical migration tails come from immutable
accepted Data/Research pins and native windows. Strict current native CF proof is
imported before compatibility validation; return still requires validation. Only
unchanged Data pins permit accepted CF reuse; bounded changes run native CF. Empty bounded scope explicitly dispatches revalidation;
it does not become a fake window or full refresh. Resume, same-run CF handoff and
publication still compare exact raw runtime provenance.

L1 migrates only an exact complete legacy bars/ladder/minute proof, row membership
and context. It updates row input proofs while preserving every value/NULL and
legacy source provenance. Complete metadata-only partitions use one existing atomic
predicate write with the exact proven row count as its budget (the accepted
maximum is 105307); computed suffixes keep the ordinary 100000-row limit. L2 verifies actual bars/count/MTF/content plus the finite
old compute recipe before advancing its complete metadata carrier. Unknown or
mismatched legacy partitions compute a complete native partition without cached
prefix or VP values, using the existing full-partition writer. This affects only
those partitions, retains the ordinary suffix limit and emits migration full
partition/row counters. Existing full-partition delete/append recovery limitations
remain: a failed partial staging write stays unaccepted; the old pinned manifest
remains public truth until all native QC/publication complete. Compatible fixtures forbid numerical
builders; subsequent same-code runs use the normal metadata noop. Migration reads
inputs and rewrites proof metadata; it is not zero-I/O or a measured speed claim.

## Research preflight and recovery

Change surface: product-plane.

Owning assets validate refresh windows, native profiles, report entry points and
existing Delta writer layouts before loading price history. Single-bar closed
windows remain valid. Layout mismatch requires an isolated migration; preflight
does not mutate tables. Run a small complete native data-prep route when changing
writer projections or contracts; metadata checks do not prove computed schemas.
Physical checks use the activated Spark sidecar storage contract. Legacy CF and
report writers retain supported unpartitioned targets; yearly layouts remain strict.

Resume a failed attempt through native Dagster **Re-execute from failure**, or
inside the webserver container:

```text
python -m trading_advisor_3000.product_plane.research.operations resume --run-id <failed-run-id>
```

The new attempt adopts the parent's checkpoint only with identical calculation
parameters, code/dependencies and accepted Data/Research parents. Each reused QC
stage must have a native successful Dagster step and unchanged Delta pins.
Incomplete stage outputs are rewritten by that stage. The publication manifest
records `quality_owners` and `recovery_lineage`; publication still requires every
mandatory QC stage. Source attempts retain their terminal failure status.
Legacy attempts without an enabled snapshot owner require a fresh full attempt;
their successful steps cannot be reused without verified input pins.

`research_worker_health_sensor` checks every 30 seconds. The existing multiprocess
executor records the run worker plus its current child, process start identities,
container generation and a 15-second heartbeat. A missing/stale heartbeat alone
never marks failure. Another container namespace or a legacy lock remains unknown.
Live children prevent recovery even if their run owner has disappeared. Recovery
of an indicator lock requires a proven dead owner and a kernel recovery lock.

Nightly bootstrap uses `--no-recreate --no-build`, building only a missing image.
It cannot replace an existing running worker. Explicit image/config delivery is
a separate maintenance operation: pause launching schedules/sensors, drain active
and queued runs, then run `operations check-restart` before container recreation.
Re-enable the prior controls after health checks. Direct Docker restarts outside
this route remain an operator responsibility. Worker monitoring requires the
stable `TA3000_RESEARCH_WORKER_OWNER` role configured in the product compose file.

Product-main services use Docker `restart: always`: Engine startup restores the
webserver and daemon, including after Windows shutdown. An explicit container
stop lasts until Engine restarts. For a durable nightly pause, save and stop
`moex_baseline_daily_update_schedule`,
`moex_cf_catch_up_after_moex_baseline_sensor`, and
`research_data_prep_after_moex_cf_catch_up_sensor` through Dagster; preserve their
cursors and restore their prior statuses after maintenance. Inventory queued and
active runs before delivery; leave unrelated controls unchanged. The base
on-demand Compose keeps its existing `unless-stopped` lifecycle.

Policy/resource changes need explicit drained delivery: synchronize the local
launcher, then apply and read back the exact existing containers' restart policy,
RAM and total RAM-plus-swap limits. Preserve mounts and the Dagster home volume.
`--no-recreate` bootstrap does not apply these changes to existing containers.

## Solution Intent

- Solution Class: staged
- Critical Contour: multi-contour
- Forbidden Shortcuts: none
- Closure Evidence: native Dagster failure/re-execution with real Delta pins;
  changed input/code rejection; owner/child/lock tests; nightly syntax contract.
- Shortcut Waiver: none

Spark/Delta/Dagster and existing indicator algorithms keep ownership. This patch
does not claim production activation or close full-history memory limits.

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
- `rows_by_table` for search specs, optimizer studies/trials, vectorbt search runs, param results, gate events, runs, stats, trades, orders, and drawdowns
- `durations.total_seconds`
- `dagster_selected_assets`
- `dagster_materialized_assets`
- `result_digest.ranking_top_rows`
Backtest is expected to run from `StrategyFamilySearchSpec`, MTF-resolved inputs, vectorbt `SignalFactory.from_choice_func`, and vectorbt `Portfolio.from_signals`. A run that requires pre-materialized `StrategyInstance` rows before vectorbt is using the wrong route.
When `reuse_existing_materialization=true`, `dagster_materialized_assets` should contain only backtest/ranking assets, not `research_datasets`, `research_indicator_frames`, or `research_derived_indicator_frames`.
Backtest input reads must be Delta-native: dataset/version/instrument/timeframe/slice predicates and strategy-column projection happen in the Delta/Arrow layer before Python builds vectorbt matrices. If a run loads `research_bar_views`, `research_indicator_frames`, or `research_derived_indicator_frames` through Python row-object reloaders, treat it as the wrong route even if the run finishes.
If `strategy_space.optimizer.engine=optuna`, inspect `research_optimizer_studies.delta` and `research_optimizer_trials.delta` as ask/tell search provenance; the trading result truth remains in the vectorbt param/result/ranking tables.
For full or battle results, require a `research_campaign_runs.delta` row that points to the run folder. Delta-shaped benchmark folders under `research/runs/_benchmarks/` are useful for timing and proof, but they are not canonical campaign results and must not be used as ranking/projection truth.

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
- if the run is slow before vectorbt starts, check that the input loader is using native Delta filters and projected columns, not a full Python row scan.

If projection fails:
- confirm rankings were produced;
- confirm selection policy is not filtering everything out.
- if rankings exist but candidates are `0`, check `decision_lag_bars_max`, current signal freshness, and whether the selected policy requires `policy_pass=1`.

## Operational Boundary

Use the matching Dagster research job for operator-driven research runs.
Use `research_data_prep_job` plus `research_data_prep_after_moex_sensor` for scheduled freshness after MOEX canonical refresh.
Do not treat implementation modules or benchmark tooling as separate supported run paths.
