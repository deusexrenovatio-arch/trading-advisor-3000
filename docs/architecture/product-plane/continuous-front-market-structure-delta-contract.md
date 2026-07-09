# Continuous Front Market Structure Delta Contract

This document defines the Delta storage contract for the continuous-front
market-structure state layer.

## Phase Contract

Input: Phase 1 state vocabulary and Phase 2 FSM output fields.

Output: Delta table names, grain, partitioning, required columns, QC rows,
manifest rows, and acceptance rows.

Done when: Spark and Dagster implementation can write/read the market-structure
layer without inventing schema details.

## Implementation Criteria

Implementation is acceptable when:

- the layer has a dedicated product-plane store contract or equivalent table
  resolver for all four Delta outputs;
- schema validation fails closed for missing required columns, unknown state
  labels, missing lineage fields, or missing acceptance fields;
- main table writes are scoped by the declared grain and partition keys;
- `market_structure_row_hash` is deterministic for the same input versions,
  config hash, and FSM version;
- manifest rows link input Delta versions, output Delta versions, code
  provenance, runtime evidence, timings, and publication status;
- acceptance rows can be joined to QC observations by `run_id`;
- no existing indicator or derived-indicator table contract is broadened to own
  market-structure tables.

## Tables

The layer owns four Delta tables:

```text
continuous_front_market_structure_frames.delta
continuous_front_market_structure_qc_observations.delta
continuous_front_market_structure_run_manifest.delta
continuous_front_market_structure_acceptance_report.delta
```

These tables should be exposed through a dedicated store contract, for example
`continuous_front_market_structure_store_contract()`. Do not add them to the
continuous-front indicator contract: market structure is not an indicator or a
derived indicator.

## Main Table Grain

`continuous_front_market_structure_frames.delta` has one row per closed
continuous-front bar:

```text
dataset_version + instrument_id + timeframe + ts
```

Partition keys:

```text
dataset_version
roll_policy_version
adjustment_policy_version
indicator_set_version
structure_set_version
instrument_id
timeframe
```

Unique constraint:

```text
unique(
  dataset_version,
  roll_policy_version,
  adjustment_policy_version,
  indicator_set_version,
  structure_set_version,
  instrument_id,
  timeframe,
  ts
)
```

## Main Table Columns

Identity:

```text
dataset_version
roll_policy_version
adjustment_policy_version
indicator_set_version
rule_set_version
structure_set_version
structure_model_version
instrument_id
timeframe
ts
ts_close
session_date
active_contract_id
roll_epoch_id
roll_seq
```

Source and lineage:

```text
source_front_row_hash
source_indicator_row_hash
source_indicator_row_hash_version
market_structure_row_hash
market_structure_row_hash_version
cross_contract_window_any
known_at_ts
created_at_utc
```

Price and threshold snapshot:

```text
high0
low0
close0
atr_14
threshold_price
threshold_atr_multiple
threshold_tick_size
threshold_tick_multiple
```

State machine output:

```text
trend_state_code
trend_state_label
previous_trend_state_code
state_changed_flag
state_age_bars
reason_code
```

Local structure:

```text
current_leg_direction
current_leg_high
current_leg_high_ts
current_leg_low
current_leg_low_ts
last_confirmed_local_high
last_confirmed_local_high_ts
prev_confirmed_local_high
prev_confirmed_local_high_ts
high_relation
last_confirmed_local_low
last_confirmed_local_low_ts
prev_confirmed_local_low
prev_confirmed_local_low_ts
low_relation
```

Break and pullback flags:

```text
break_up
break_down
pullback_flag
compression_flag
expansion_flag
```

## QC Table

`continuous_front_market_structure_qc_observations.delta` records one row per
check per materialization run.

Required fields:

```text
run_id
check_group
check_name
check_status
severity
dataset_version
roll_policy_version
adjustment_policy_version
indicator_set_version
structure_set_version
instrument_id
timeframe
observed_value
expected_value
details_json
created_at_utc
```

Blocking check groups:

```text
schema
row_count
state_enum
tick_size
prefix_invariance
causality
performance
```

## Manifest Table

`continuous_front_market_structure_run_manifest.delta` records one row per
materialization run.

Required fields:

```text
run_id
dataset_version
roll_policy_version
adjustment_policy_version
indicator_set_version
rule_set_version
structure_set_version
structure_model_version
input_delta_versions_json
input_delta_versions_hash
output_delta_versions_json
output_delta_versions_hash
job_config_hash
spark_app_id
spark_event_log_path
code_artifact_hash
dependency_lock_hash
created_by_pipeline
stage_timings_json
performance_metrics_json
publish_status
created_at_utc
```

`input_delta_versions_hash` must include:

- `continuous_front_bars.delta`;
- `continuous_front_indicator_frames.delta`;
- market-structure configuration;
- tick-size mapping.

## Acceptance Table

`continuous_front_market_structure_acceptance_report.delta` records whether the
run is accepted or quarantined.

Required counters:

```text
blocker_fail_count
schema_fail_count
row_count_fail_count
null_state_fail_count
tick_size_fail_count
prefix_invariance_fail_count
causality_fail_count
performance_fail_count
publish_status
```

`publish_status=accepted` is allowed only when all blocker groups pass.

## Ticket Handoff

Implementation tickets may reference this document for storage schema,
partitioning, row hashes, QC rows, manifests, and acceptance publication. Schema
work should land before Spark materialization writes production-shaped output.
