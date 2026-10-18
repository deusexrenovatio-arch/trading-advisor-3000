# Continuous Front Indicator Storage

The continuous-front indicator contour is a product-plane research/data-plane surface. It keeps pandas-ta-classic and pandas responsible for indicator formula calculation, while Dagster/Delta own orchestration, storage, QC, lineage, and publish/quarantine evidence.

## Calculation Groups

Every base and derived output is covered by exactly one `indicator_roll_rules` row. The active rule set is `roll_rules_hybrid_v1`.

| Group | Roll class | Adapter | Roll behavior |
|---|---:|---|---|
| `price_level_post_transform` | A | `pandas_ta_with_post_transform_adapter` | compute `Y0` on causal `open0/high0/low0/close0`, publish `Y = Y0 + A_t` |
| `price_range_on_p0` | B | `pandas_ta_on_p0_adapter` | compute ranges on causal `*_0`; no additive post-transform |
| `oscillator_on_p0` | C | `pandas_ta_on_p0_adapter` | compute shift-invariant oscillators on causal `*_0` |
| `anchor_sensitive_roll_aware` | D | `custom_roll_aware_adapter` | compute ratios using `P0 + A_t` for the current bar's known roll epoch |
| `price_volume_roll_aware` | E | `custom_roll_aware_adapter` | price uses causal normalized/current-anchor semantics; volume stays native; cumulative state resets by rule |
| `native_volume_oi_roll_aware` | F | `custom_roll_aware_adapter` | mask or reset native volume/OI windows across roll boundaries |
| `native_state_relationship_roll_aware` | F-DERIVED | `custom_roll_aware_adapter` | derived windows over reset/native volume or OI state are NULL while their dependency window crosses a roll |
| `grid_locked_native` | G | `custom_roll_aware_adapter` | native price-grid levels are not additively shifted |
| `pandas_window_derived_level` | A | `pandas_window_adapter` | rolling/session/week levels use causal `*_0` plus current-bar `A_t` |
| `derived_relationship_roll_aware` | C | `custom_roll_aware_adapter` | validate price-space compatibility for distances, positions, crosses, angle slopes, movement divergence |

## Storage

The governed sidecar tables are:

- `cf_indicator_input_frame`
- `indicator_roll_rules`
- `continuous_front_indicator_frames`
- `continuous_front_derived_indicator_frames`
- `continuous_front_indicator_qc_observations`
- `continuous_front_indicator_run_manifest`
- `continuous_front_indicator_acceptance_report`

The legacy `research_indicator_frames` and `research_derived_indicator_frames` remain the consumer-facing research tables. For `series_mode=continuous_front`, data prep also writes the governed sidecar tables so each output has a rule, adapter, price space, row hash, and acceptance evidence.

## Invariant

The technical identity is:

```text
A_t = sum(additive_gap_s for roll s effective at or before t)
P0_t = P_native_t - A_t
P[i|t] = P0_i + A_t
```

Formula calculation is owned by pandas-ta-classic for base indicators and pandas window logic for derived indicators during research-table materialization. The sidecar proof reads those materialized Delta tables instead of recalculating the full base and derived indicator stack. Delta tables are the durable output surface; Dagster coordinates refresh and QC. Native volume/OI windows are masked or reset across roll boundaries, and derived levels are computed from causal normalized state before being shifted only to the current bar's known roll anchor.

`ts` is the bar open timestamp. `ts_close` and `causality_watermark_ts` are the actual close timestamp derived from the timeframe, so native-timeframe strategy joins cannot consume an unfinished bar.

## Acceptance

Acceptance is anti-bypass: the publish decision requires actual QC observation rows for prefix evidence, materialized formula samples, pandas-ta parity, lineage, schema, rule coverage, input projection, and adapter authorization. Missing required QC groups create a blocker `anti_bypass` observation and quarantine the run.

The formula-sample gate covers every active base output from `default_indicator_profile()` and every active derived output from `current_derived_indicator_profile()` by sampling the already-materialized sidecar rows. It checks that the expected output columns are present and sampleable without recalculating the full formula stack during proof. The TZ minimum classes such as `sma_20`, `ema_20`, `atr_14`, `rsi_14`, `ppo_12_26_9`, `session_vwap`, native volume/OI relationships, levels, positions, cross codes, movement-angle divergence, and angle slopes are a floor, not the gate boundary.

Prefix evidence uses selected cut timestamps around roll boundaries and the tail, plus roll-window coverage on the already-materialized sidecar rows. It checks row presence and cross-window flags without rebuilding the full input, base, and derived indicator stack during proof.

Lineage is fail-closed: acceptance requires non-empty runtime evidence including `spark_app_id`, `spark_event_log_path`, input/output Delta version hashes, rule/adapter/formula/job/code/dependency hashes, and `created_by_pipeline=spark_delta_governed`.

## Retired persisted timeframe projections

Change surface: product-plane. Storage contract change, 2026-10-18.
Strategies may consume several native timeframes and their existing labels.
Research tables do not materialize duplicate `mtf_*` values. The 24 former
projections, their calculation group/rules and cross-timeframe source caches
are retired. Profiles and writers reject retired fields.

`research_derived_indicator_frames.source_inputs_hash` fingerprints only the
current instrument/series/timeframe inputs, profile and causal CF projection.
It replaces `source_mtf_hash`; the production alias `mtf_source_family_hash`
is also retired. Legacy readers discard those aliases and projections.
Unknown local input hashes force validation on the next changed refresh; a
cross-timeframe hash is never relabelled as a local checksum.

Migration owner: `research.derived_indicators.retire_mtf`. Its default command
is a read-only physical Delta plan. Apply requires promotion of the native-only
writer to the actual production checkout, refreshed worker processes, an idle
pipeline and a short operator-controlled pause of recurring writes. A local
code change alone does not meet that prerequisite.

The version-bound migration projects the two derived tables through Arrow and
Delta `overwrite`/`schema_mode=overwrite`, without computing indicators. It
removes retired rows from `indicator_roll_rules`. Recognized old output-profile
hashes become the native profile hash; unknown hashes abort before any commit.
The CF sidecar row checksum is recalculated by its existing hash owner using
retained values. Original adapter provenance, timestamps, QC and run manifests
remain historical evidence. New local input hashes are NULL until validated.

Native values, keys, duplicates, physical types and row counts are checked by
independent readback. Each table commit is atomic; the three-table migration
is not a cross-table transaction. The plan and incrementally written receipt
record versions for partial recovery. Old Delta files are retained; no vacuum.
Rollback streams the pinned old data AND schema into a new Delta commit,
rejecting newer-version drift. `DeltaTable.restore` alone is insufficient for
schema rollback in the installed delta-rs 0.25.5 runtime.

Run inside the updated runtime, with writes paused:

```text
python -m trading_advisor_3000.product_plane.research.derived_indicators.retire_mtf --root <current-root> --output <plan.json>
python -m trading_advisor_3000.product_plane.research.derived_indicators.retire_mtf --root <current-root> --apply-plan <plan.json> --writer-root /workspace --output <receipt.json>
python -m trading_advisor_3000.product_plane.research.derived_indicators.retire_mtf --root <current-root> --rollback-receipt <receipt.json> --writer-root /workspace --output <rollback.json>
```

Resume writes only after native schemas, sidecar parity and normal consumer
reads pass. A verification copy does not constitute production migration.
