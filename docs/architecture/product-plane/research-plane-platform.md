# Research Plane Platform

## Unpublished native observation L0

Decision status: proposed; independent design admission required before dependent
implementation. Surface: product-plane; impact: contract-change. Owner: Research
Data Factory; Market Data owns the bounded input read; Contracts owns exact shapes.
Authorization: bounded stages 4–6 after stage 3.1 integration. Production, bulk
history/universe acquisition, current pointers and legacy-writer retirement are
excluded. Implementation/deployment acceptance is separate from this decision.

Solution Class: staged. Critical Contour: data-integration-closure. Forbidden
Shortcuts: scaffold-only. Closure Evidence: real isolated integration test from
the completed native source candidate through Spark/Delta L0 and pinned public
readback; source/NULL/grain oracle, per-operation capabilities and old-reader
compatibility. Shortcut Waiver: none. This is not full target closure.

The [native source observation boundary](docs/architecture/product-plane/market-identity.md#unpublished-native-observation-trial)
preserves exact Decimal buffers and naive source keys. It does not prove UTC,
session completeness, finality, historical availability, canonical Q or PIT.
Official MOEX [September schedule](https://www.moex.com/n103379) and
[index schedule](https://www.moex.com/n103770) establish dated market hours, not
the ISS candle clock/end/finality/availability contract. Never manufacture the
canonical provenance, session, roll or r7 inputs required by the old L0 route.

First patch: observation-bound `native_tradable` L0 bars and instrument tree.
The contour name grants no execution authorization. Keep equities, the reference
index and dated futures, including expired SRU6; do not filter by today's tradable
state or derive identity/kind from ticker prefixes. Retain the complete native
grain, namespace, asset/instrument/context, timeframe, original begin/end, units,
quantity states and exact row lineage. No new canonical `series_id` is written.

Native Runtime Choice: `NativeObservationSnapshot.read_spark` is the narrow Market
Data compute handoff. It verifies the completed source graph and exact table pin,
then uses existing `snapshot_read_scope` and `read_spark_delta(versionAsOf)`.
Research does not read private Market Data paths. Its fixed compute limits are
source rows, active files and physical bytes, separate from interactive Arrow
`row_limit`/IPC `byte_limit`. Reject quarantine, inconsistent source/native counts,
missing/extra declared sequences, duplicate grain or incompatible physical schema.
The first compute policy rejects simulated-correction parents; their existing
interactive read route remains available with its original provenance.
There is no latest-path fallback, new query engine or active Python row-list scan.

Reuse `research_bar_views_job._with_l0_metrics`. Order by strict source key within
the full native instrument/context/timeframe sequence. Its transient `ts` is a
string ordering alias, never a UTC timestamp. Retain all captured history: use the
kernel without tail trimming, then assign warmup/analysis roles by the explicit
source key. History starts at the first pinned observation. Adjacent observations
carry across dates/gaps, without filling, resampling or session reset.

Keep source OHLC/quantities unchanged as Decimal(38,10). Compute features from a
temporary float64 price copy, restore the untouched source fields, and persist
only the declared new features. Returns, logarithms and ranges use the existing
Spark kernel, not a parallel formula implementation. L0 first-row TR is high-low;
lag/return need two observations. Undefined/nonfinite arithmetic becomes NULL.
`execution_eligible=false` and execution OHLC=NULL for this mode, including index.
No economics or money calculation is admitted.

Shared `market_identity.v3` definitions and thin `native_observation_compute.v1`,
`native_research_l0_context.v1`, intent and completion wrappers own metadata,
limits, field/policy declarations and executable assertions. Derive unchanged L0
source fields from the existing native-bars layout; declare only additional L0
fields and the tree layout here. Common snapshot references bind actual physical
schemas, UUIDs, Delta versions and active files. Source and numerical compatibility
are different: the numerical descriptor includes observation basis, actual kernel
AST, float64 conversion, history/gap/seed policy and engine versions. Reuse the
existing semantic fingerprint primitives; do not alter unrelated legacy numerical
identities merely because a new helper exists. Runtime release provenance stays
separate. Metadata contains no JSON price DTOs.

Capabilities are versioned and per operation/sequence. Distinguish ready,
not_applicable, missing_source, insufficient_history, unverified_policy and
undefined_arithmetic, with observed/required/finite counts and reasons. Ready
means ready only in the observed-sequence basis. Daily two-row input supports a
second-row return/change and first-row ranges, but not SMA20. Do not label a
blanket all-NULL daily frame as successful compute. Index quantity N/A and absent
futures OI are different source states. UTC/session/VWAP/MTF-availability/CF/PIT/
execution remain unavailable unless a separate sourced boundary admits them.

The owner must require exactly one capability for every declared feature and every
verified source sequence, with no missing, duplicate or extra pair. Match each
sequence (including asset, namespace, source symbol, engine and row count) to the
verified MD assignments and actual native grouping. Feature history requirements
are fixed by policy; finite counts cannot exceed sequence rows, and ready requires
a positive finite count. Unsupported UTC/session/execution states are always
unverified_policy with zero finite count. These cross-collection checks are
mandatory producer and public-reader validation, not a new generic schema engine.

Canonical hashing reuses the existing sorted UTF-8 JSON bytes, finite numbers,
compact separators and SHA256. Policy hash covers exactly the declared L0 policy.
Layout hash covers the two table-name -> {fields, grain} entries, deriving source
fields from the native layout. Numerical digest covers the complete numerical
descriptor excluding only its own digest; the adapter AST is the numerical frame
adapter, not the operational writer. NCTX is `NCTX:` plus the canonical hash of
the complete context excluding only research_context_id. The intent input hash
covers the entire canonical intent. Quality is separate from numerical identity:
its versioned policy hash covers exact source/NULL/integer equality and per-feature
float64 `abs(actual-reference) <= 1e-10 + 1e-10*abs(reference)`; undefined masks
must match exactly and nonfinite values are forbidden. Freeze this before compute.

Before accepting a receipt, verify its intent pin/hash/command/attempt, the context
pin and NCTX, all policy/layout/quality/numerical hashes, and intent-context equality
for source root/completion, analysis start, policy, numerical identity and code SHA.
The common manifest must reference that same context/intent/input hash and exact
Market Data parent snapshot; its ID/path/hash must equal the completion snapshot
pin. Verify context.source against the MD owner's freshly verified compute
metadata, including source/assignment/metadata pins, limits, limitations and complete
sequences. Both table refs bind the exact declared schemas, counts and unique
grains. No filename, declared capability or hash string substitutes these checks.

Mount the completed source at `/trial` RO, reviewed code RO, and only a new unique
Research child at `/research-trial` RW. Keep runtime scratch there; use the pinned
one-shot image and bounded CPU/memory. Never mount production code/data/Dagster.
One OS-owned lock serializes the generation and releases on process death. Reuse
the existing immutable intent/receipt and common capture/create/save/readback
primitives. Same key/input returns its committed receipt; changed input conflicts.
Fresh attempts use exclusive paths; partial attempts cannot become readable.
Validate an unacknowledged complete graph before reconciling its receipt; reject
ambiguous outcomes. No mutable head, current/accepted/latest or publish call exists.

The acyclic graph is: completed Market Data parent -> L0 tables and context ->
common immutable snapshot -> terminal unpublished receipt. The context does not
contain its future snapshot hash. The receipt is last, after input/namespace,
types/counts, unique keys, source equality, capability and physical-pin checks.
The public Research reader requires an explicit completion pin and verifies the
whole graph; it cannot substitute an accepted-current parent. Failed attempts and
old candidates remain intact. Rollback selects an explicit old completion pin.

Native proof requires exact source buffers/NULLs and full grain, cardinality and
join checks; independent original-byte L0 values/masks; positive finite 1m compute;
truthful daily operation history; source and old-snapshot readback unchanged;
replay/conflict/failure recovery and actual root/mount isolation. Fixtures do not
replace this producer-consumer proof. The independent reference artifact has role
`reference_only_not_research_dataset`, never a materialization acceptance.

Follow-ups are separate patches after the actual L0 example: L1 reuses existing
pandas-ta dispatch; L2 reuses existing derived kernels. Their explicit price-only
capability gate must not treat absent `bar_usage_flags` as permission. Transient
compute returns only new feature columns joined by the complete unique grain to
unchanged source buffers. Pin library/backend, seed/offset/fill/history/gap policy;
the first planned L1 profile rejects TA-Lib/different unadmitted backends. A bounded
daily prefix requires its own source-owner amendment/new generation; the existing
two-day acquisition guard is not relaxed here. Stage 5 public consumer/cache/
shadow graph follows real L0/L1/L2 examples. Positive UTC/as-of MTF and stage 6 CF
remain dependent on the actual sourced clock/availability/specification/expiry
facts; a last-trade cutoff is not a settlement expiry instant.

Rejected alternatives: fake canonical/r7 inputs, copying source price into an
execution frame, broad latest-path reads, source-key-to-UTC coercion, silent
deduplication, Python L0 analytics, a second indicator engine, implicit missing-
flags admission, mutable publication heads and production promotion. Revisit this
observation basis when source-backed UTC/session/availability facts permit a
separately reviewed admission; do not widen this mode silently.

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
  -> research_strategy_search_specs / research_optimizer_studies / research_optimizer_trials / research_optimizer_selections
  -> research_validation_folds, when strict nested validation is configured
  -> research_vbt_search_runs / research_vbt_param_results / research_vbt_param_gate_events
  -> compatibility runs / stats / trades / orders / drawdowns
  -> research_strategy_rankings / research_run_findings
  -> research_signal_candidates
  -> research_strategy_evaluation_profiles
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
- reused backtest runs enter through `research_backtest_job` and validate the existing gold layer through `research_campaign_context` and `research_backtest_batches` run config instead of rematerializing data-prep assets;
- vectorbt receives family-level MTF-resolved matrix inputs, generates entry/exit indices through `SignalFactory.from_choice_func`, and executes through `Portfolio.from_signals`;
- futures contract sizing and monetary aggregates follow the [futures backtest money authority](docs/architecture/product-plane/futures-backtest-money-authority.md): the ledger interprets vectorbt trade events using execution economics, while Sharpe/Sortino/Calmar remain vectorbt metrics;
- primary result identity is `param_hash`, while compatibility tables may still expose downstream run/stat/trade rows for ranking and projection consumers;
- adaptive optimizer state is Delta-first run provenance; Optuna proposes trial parameters, while `research_optimizer_studies`, `research_optimizer_trials`, and `research_optimizer_selections` explain selected `param_hash` rows. Persisted backtest results retain the distinct event-simulation and monetary-metric authorities above;
- `nested_walk_forward_v1` separates optimizer-visible validation folds from blind confirmation folds; confirmation runs use the frozen selected `param_hash` and can only pass or fail the strategy evaluation verdict;
- `policy_pass` is only research ranking success; `research_strategy_evaluation_profiles` records paper-signal, paper-trade, and live-candidate readiness with blocker reasons in the same post-backtest evaluation output;
- Dagster handoff between research assets passes Delta manifests, paths, row counts, and run metadata; large row sets such as trades, stats, and ranking inputs are read from Delta inside the owning step instead of being pickled through the orchestrator;
- repeated research runs reuse the materialized layer and in-process cache;
- [semantic compute contracts](docs/architecture/product-plane/semantic-compute-contract.md) distinguish numerical compatibility from operational source releases, QC/proof changes and physical schema; raw operation/resume/publication provenance remains exact;
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
- Launch the matching existing Dagster job with `campaign_config_path` or inline `campaign_config` mapped through `research_campaign_context`.

This is the only supported user-facing route.
`research_campaign_context` validates the machine-readable campaign config, writes immutable start artifacts, and expands the internal stage asset config inside the Dagster run. The old `run_campaign` Python route is removed from the active product surface.

Campaign contracts:
- `research_campaign.v1.json`
- `research_run_summary.v1.json`

### Removed Legacy API
- `trading_advisor_3000.product_plane.research.run_research_from_bars(...)`

The inline Python L0 route is removed. Research L0 materialization goes through Dagster and the Spark/Delta `research_bar_views` job.

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
- `research_optimizer_selections`
- `research_validation_folds`
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
- `research_strategy_evaluation_profiles`

Projection layer:
- `research_signal_candidates`
- `research_run_findings`

## Storage Model

Research storage is split into two layers:
- reusable gold outputs under `<materialized_root>/`
- immutable run artifacts under `<runs_root>/<campaign_name>/<run_id>/`

Canonical research runs are created only by the matching Dagster research job. A canonical run must have a
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
