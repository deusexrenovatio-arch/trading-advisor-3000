# TA3000 Performance Baseline

This runbook defines the fixed comparison point for the MOEX no-network downstream
data-prep benchmark.

## Scenario

- Scenario id: `ta3000-moex-no-network-research-data-prep-v1`
- Change surface: `product-plane`
- Canonical input root:
  `D:/TA3000-data/staging/verification/pipeline-speed-current-20260615T125911Z/branch/canonical/moex/baseline-4y-current`
- Raw/canonical reference run:
  `artifacts/benchmarks/pipeline-speed-current-20260615T125911Z/no-network-pipeline-speed-rerun-20260616T042028Z-summary.json`
- Campaign config:
  `product-plane/campaigns/moex_performance_baseline_research_data_prep.yaml`
- Data window:
  `2021-04-01T00:00:00Z` through `2026-06-10T21:00:00Z`
- Timeframes: `15m`, `1h`, `4h`, `1d`
- Universe scope: approved MOEX futures universe, no contract filter, no instrument filter
- Series mode: `continuous_front`
- QC mode: `hot_path`
- Continuous-front indicator sidecar mode: `spark`
- Spark master: `local[4]`
- Spark resource profile for full rebuild: driver memory `12g`, executor memory `12g`,
  driver max result size `4g`, shuffle partitions `8`
- Network rule: no MOEX HTTPS/API fetches during this benchmark

## Required Tables

The benchmark is valid only if these Delta tables are materialized under the
isolated benchmark materialized root and have `_delta_log` directories:

- `continuous_front_bars`
- `continuous_front_roll_events`
- `continuous_front_adjustment_ladder`
- `continuous_front_qc_report`
- `research_datasets`
- `research_instrument_tree`
- `research_bar_views`
- `research_indicator_frames`
- `research_derived_indicator_frames`
- `cf_indicator_input_frame`
- `indicator_roll_rules`
- `continuous_front_indicator_frames`
- `continuous_front_derived_indicator_frames`
- `continuous_front_indicator_qc_observations`
- `continuous_front_indicator_run_manifest`
- `continuous_front_indicator_acceptance_report`

## Comparison Fields

Every baseline result must preserve:

- total wall-clock seconds and minutes
- per-asset/report `stage_timings`
- nested `sidecar_stage_timings` and `sidecar_spark_stage_timings` when present
- rows by table
- Delta table paths and `_delta_log` proof
- run config path and git revision / working-tree status

The primary comparison is against a fresh isolated materialization. Reuse runs are
allowed, but they must be labeled separately because they measure cache/reuse
behavior, not full downstream rebuild cost.

## Command

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign `
  --config product-plane/campaigns/moex_performance_baseline_research_data_prep.yaml
```

Before a full rebuild comparison, use a new empty materialized root or archive the
previous benchmark root. Do not run this against `research/gold/current` unless the
goal is an authoritative refresh rather than a performance baseline.
