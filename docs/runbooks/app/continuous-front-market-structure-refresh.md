# Continuous Front Market Structure Refresh

Use this route after continuous-front bars and continuous-front indicator frames
are materialized.

## Phase Contract

Input: Phase 5 Dagster assets and jobs.

Output: operator route for normal refresh, standalone rebuild, proof checks,
and failure handling.

Done when: an operator can refresh the table, identify the accepted output, and
explain whether failures are source, contract, computation, QC, or publication
failures.

## Implementation Criteria

Implementation is acceptable when:

- the normal route is callable through Dagster, not a standalone manual script;
- first proof can run against a verification root without overwriting
  `research/gold/current`;
- proof records exact input table paths, output table paths, row counts,
  manifest location, and acceptance-report location;
- failed checks write QC rows and a quarantined acceptance report;
- promotion to current is allowed only after accepted verification output;
- the runbook names how to distinguish physical Delta files from accepted
  research data.

## Normal Route

The normal refresh is the Dagster research data-prep route. It should include
market-structure assets after `continuous_front_indicator_frames`.

Expected upstream tables:

```text
continuous_front_bars.delta
continuous_front_indicator_frames.delta
```

Expected output tables:

```text
continuous_front_market_structure_frames.delta
continuous_front_market_structure_qc_observations.delta
continuous_front_market_structure_run_manifest.delta
continuous_front_market_structure_acceptance_report.delta
```

The route must not require:

```text
continuous_front_derived_indicator_frames.delta
```

## Verification Root

Use a verification root for first proof and performance acceptance. Do not
overwrite `research/gold/current` until the verification run is accepted.

Required proof:

- every output path exists;
- every output path has `_delta_log`;
- row count of `continuous_front_market_structure_frames` equals scoped
  `continuous_front_bars`;
- all states are in the allowed enum;
- no null `trend_state_label`;
- acceptance report has `publish_status=accepted`.

## Failure Handling

If validation fails, write QC rows and an acceptance report with:

```text
publish_status = quarantined
```

Do not treat a quarantined table as current research data, even when Delta files
exist physically.

## Operator Reading Guide

Read the acceptance report first, then QC rows, then the manifest.

The manifest should show:

- input Delta versions and hash;
- output Delta versions and hash;
- Spark app id and event log path;
- stage timings;
- performance metrics;
- row counts by table.

## Ticket Handoff

Implementation tickets may reference this runbook for real-data proof and
operator acceptance. The proof ticket should run after Dagster wiring and before
performance acceptance is treated as blocking.
