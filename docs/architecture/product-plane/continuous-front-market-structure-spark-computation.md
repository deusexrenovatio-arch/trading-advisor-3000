# Continuous Front Market Structure Spark Computation

This document defines the compute route for continuous-front market-structure
materialization.

## Phase Contract

Input: Phase 3 Delta contract plus existing continuous-front bars and indicator
frames.

Output: Spark-native materialization route that writes
`continuous_front_market_structure_frames.delta`.

Done when: full-scope computation stays in Spark/Delta and returns row counts,
Delta output paths, manifests, and stage timings.

## Implementation Criteria

Implementation is acceptable when:

- the production route reads scoped Delta inputs and writes Delta outputs
  through Spark;
- the job accepts explicit input paths, output paths, scope filters, and
  market-structure config from Dagster or a tested wrapper;
- the job validates source columns and tick-size coverage before publication;
- the job does not collect the full materialization scope to the Python driver;
- the job does not read derived-indicator tables;
- row counts, Delta output versions, and stage timings are returned to the
  caller;
- smoke tests cover a small synthetic Delta scope and prove output schema,
  state enum, row parity, and timing fields.

## Runtime Ownership

Spark and Delta own the durable calculation route:

- Spark reads scoped Delta inputs.
- Spark computes per-series state rows.
- Spark writes Delta outputs with scoped delete and append semantics.
- Dagster coordinates the run and records acceptance evidence.

Python may assemble configuration, call Spark, validate contracts, and write
run metadata. Python must not collect the full materialization scope to the
driver and write the table as the production route.

## Spark Job

Add a Spark job:

```text
src/trading_advisor_3000/spark_jobs/continuous_front_market_structure_job.py
```

Responsibilities:

- validate source Delta tables and required columns;
- validate tick-size coverage for every scoped instrument;
- read continuous-front bars;
- read continuous-front indicator frames;
- join inputs;
- compute state rows;
- write `continuous_front_market_structure_frames.delta`;
- return output paths, row counts, Delta manifest, and stage timings.

## Input Tables

Required tables:

```text
continuous_front_bars.delta
continuous_front_indicator_frames.delta
```

Explicit non-inputs:

```text
continuous_front_derived_indicator_frames.delta
research_derived_indicator_frames.delta
```

Input join key:

```text
bars.dataset_version = indicators.dataset_version
bars.instrument_id = indicators.instrument_id
bars.timeframe = indicators.timeframe
bars.ts = indicators.ts
```

Indicator filters:

```text
indicator_set_version
rule_set_version
```

Scope filters:

```text
dataset_version
roll_policy_version
adjustment_policy_version
timeframes
instrument universe
```

## Price Space

Use continuous normalized signal price fields:

```text
high0
low0
close0
```

If the job reads directly from `continuous_front_bars`, it must derive the
equivalent normalized fields from continuous OHLC and adjustment metadata.
Native OHLC must remain audit/execution context and must not drive the v1 trend
state.

## Threshold

For each bar:

```text
threshold_price = max(
  atr_14 * threshold_atr_multiple,
  tick_size_by_instrument[instrument_id] * threshold_tick_multiple
)
```

Defaults:

```text
structure_set_version = market-structure-v1
structure_model_version = causal-atr-zigzag-fsm-v1
threshold_atr_multiple = 0.5
threshold_tick_multiple = 2
```

Missing tick size is a blocker and must fail before output publication. Null
`atr_14` keeps the row but assigns `insufficient_history`.

## Grouping And Ordering

State is computed independently for:

```text
dataset_version + instrument_id + timeframe
```

Rows must be ordered by:

```text
ts ASC
```

The implementation may use Spark grouped execution for the state machine.
Grouped local pandas is acceptable only inside Spark execution; it is not an
alternative materialization route.

## Stage Timings

The Spark report must include these stages:

```text
validate_sources
start_spark
read_inputs
join_inputs
fsm_compute
write_frames
qc_counts
row_counts
```

These timings feed the manifest and the performance acceptance gate.

## Ticket Handoff

Implementation tickets may reference this document for the Spark job boundary.
The Spark ticket should depend on the FSM-core ticket and the Delta-contract
ticket, then produce a job that Dagster can call without changing semantics.
