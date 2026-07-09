# Continuous Front Market Structure Dagster Integration

This document defines how the market-structure layer is wired into the research
data-prep route.

## Phase Contract

Input: Phase 4 Spark materialization route.

Output: Dagster assets, job selections, dependency ordering, config surface,
and acceptance publication behavior.

Done when: `research_data_prep_job` can materialize market structure after
indicator frames, and standalone rebuild can run without derived indicators.

## Implementation Criteria

Implementation is acceptable when:

- Dagster assets pass manifests, paths, counts, and acceptance metadata rather
  than full result row lists;
- market-structure assets depend on `continuous_front_indicator_frames` and do
  not depend on derived-indicator assets;
- `research_data_prep_job` materializes market structure after indicator frames;
- `moex_market_structure_rebuild_job` rebuilds only market-structure assets and
  required upstream context;
- campaign config exposes the `market_structure` block and fails closed on
  missing tick-size coverage;
- asset tests prove selection order, dependency shape, config parsing, accepted
  publication, and quarantined publication.

## Asset Ownership

Dagster owns orchestration, dependencies, acceptance, and operator visibility.
Spark owns the calculation.

Do not add a separate `pandas_job` route for this layer. If a Python wrapper is
needed, it is an implementation helper behind a Dagster asset, not an
orchestration owner.

Add assets:

```text
continuous_front_market_structure_acceptance_report
continuous_front_market_structure_frames
continuous_front_market_structure_qc_observations
continuous_front_market_structure_run_manifest
```

Dependencies:

```text
research_datasets
continuous_front_bars
continuous_front_indicator_frames
```

Explicit non-dependency:

```text
continuous_front_derived_indicator_frames
```

## Jobs

Add the new assets to:

```text
RESEARCH_ASSETS
research_data_prep_job
```

The selection order in `research_data_prep_job` should place market structure
after `continuous_front_indicator_frames`.

Add a standalone rebuild job:

```text
moex_market_structure_rebuild_job
```

This job selects only market-structure assets and their required upstream
materialization context. It must not rebuild derived-indicator tables.

## Campaign Config

Add a `market_structure` block:

```yaml
market_structure:
  enabled: true
  structure_set_version: market-structure-v1
  structure_model_version: causal-atr-zigzag-fsm-v1
  threshold_atr_multiple: 0.5
  threshold_tick_multiple: 2
  tick_size_by_instrument:
    FUT_BR: 0.01
    FUT_NG: 0.001
    FUT_GOLD: 0.1
    FUT_SILV: 0.01
    FUT_PLD: 0.01
    FUT_PLT: 0.1
    FUT_WHEAT: 10.0
    FUT_RTS: 10.0
    FUT_MIX: 25.0
    FUT_MXI: 0.05
    FUT_NASD: 1.0
    FUT_SPYF: 0.01
    FUT_RGBI: 1.0
```

## Acceptance Behavior

The acceptance asset calls the Spark materialization route, writes QC rows,
writes a run manifest, and writes the acceptance report.

If any blocker check fails:

```text
publish_status = quarantined
```

Consumers may trust the table only when:

```text
continuous_front_market_structure_acceptance_report.publish_status = accepted
```

## Public Surface

The output table is a research data-prep layer. Backtest and signal loaders may
consume it later through Delta-native predicates and column projection. They
must not recompute market structure in the hot loop.

## Ticket Handoff

Implementation tickets may reference this document for orchestration wiring.
The Dagster ticket should depend on Spark materialization and should not create
a parallel `pandas_job` orchestration path.
