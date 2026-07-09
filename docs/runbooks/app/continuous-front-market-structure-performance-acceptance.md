# Continuous Front Market Structure Performance Acceptance

The market-structure layer must be comparable in runtime to continuous-front
indicator-frame materialization.

## Phase Contract

Input: Phase 4 Spark timings and accepted indicator-frame timing baseline.

Output: blocker performance criteria for full/current refresh and non-blocking
smoke-run metric requirements.

Done when: acceptance can prove the market-structure table is not materially
slower than indicator-frame materialization for the same scope.

## Implementation Criteria

Implementation is acceptable when:

- market-structure and indicator timings are compared on the same scope;
- missing indicator baseline timing is a blocker for full/current acceptance;
- elapsed-time and rows-per-second ratios are written to manifest and QC;
- performance failures quarantine the run instead of silently publishing it;
- smoke runs expose the same metric fields without enforcing noisy ratio
  thresholds;
- the performance gate can be evaluated without reading full output rows into
  the Python driver.

## Baseline

Compare market-structure runtime against `continuous_front_indicator_frames`
runtime for the same:

```text
dataset_version
roll_policy_version
adjustment_policy_version
indicator_set_version
timeframes
instrument universe
output root
```

Use existing indicator stage timings and row counts from the accepted indicator
run manifest or sidecar result.

## Required Metrics

The market-structure manifest must include:

```text
market_structure_total_elapsed_seconds
market_structure_rows_per_second
indicator_total_elapsed_seconds
indicator_rows_per_second
market_to_indicator_elapsed_ratio
market_to_indicator_throughput_ratio
```

The Spark report must expose stage timings for:

```text
read_inputs
join_inputs
fsm_compute
write_frames
qc_counts
row_counts
```

## Blocker Thresholds

For full/current refresh:

```text
market_structure_total_elapsed_seconds <= 1.25 * indicator_total_elapsed_seconds
market_structure_rows_per_second >= 0.80 * indicator_rows_per_second
```

If either threshold fails:

```text
check_group = performance
publish_status = quarantined
```

## Smoke Runs

Small smoke tests do not block on elapsed-time ratios because timing noise can
dominate small inputs. Smoke tests still must prove:

- timing fields are present;
- rows-per-second fields are present;
- performance QC rows can be written.

## Ticket Handoff

Implementation tickets may reference this runbook for performance acceptance.
The performance ticket should depend on Spark timings, Dagster acceptance rows,
and at least one accepted indicator-frame baseline for the same scope.
