# Continuous Front Indicator Refresh

Use this route after continuous-front bars have been materialized.

## Data Prep Route

The normal Dagster data-prep route now writes both the legacy research tables and the governed continuous-front sidecar tables when `series_mode=continuous_front`:

```powershell
py -3.11 -m trading_advisor_3000.dagster_defs.research_assets
```

The callable job used inside the route is:

```python
run_continuous_front_indicator_pandas_job(
    materialized_output_dir=...,
    dataset_version=...,
    indicator_set_version=...,
    derived_set_version=...,
)
```

## Expected Tables

- `cf_indicator_input_frame.delta`
- `indicator_roll_rules.delta`
- `continuous_front_indicator_frames.delta`
- `continuous_front_derived_indicator_frames.delta`
- `continuous_front_indicator_qc_observations.delta`
- `continuous_front_indicator_run_manifest.delta`
- `continuous_front_indicator_acceptance_report.delta`

Acceptance is fail-closed: consumers should trust an accepted run only when blocker checks in `continuous_front_indicator_qc_observations` pass and the acceptance report has `publish_status=accepted`.
