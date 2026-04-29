# Research Campaign Route

## Purpose
This runbook defines the only supported user-facing route for Product Plane research runs.

Official command:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config <campaign.yaml>
```

The runner is a thin Product Plane front door.
It does not compute indicators, features, or backtests by itself.
It validates a machine-readable campaign config, writes immutable run artifacts, and dispatches only into the Dagster research contour.

## Contracts

Input contract:
- `src/trading_advisor_3000/product_plane/contracts/schemas/research_campaign.v1.json`

Run output contract:
- `src/trading_advisor_3000/product_plane/contracts/schemas/research_run_summary.v1.json`

## Storage Model

Recommended committed configs should point to external-first storage under `D:/TA3000-data`.

Storage split:
- reusable gold layer: `<materialized_root>/`
- immutable per-run artifacts: `<runs_root>/<campaign_name>/<run_id>/`

The same compatible campaign can reuse a materialized layer.
`materialization_key` is stored in `materialization.lock.json` and run metadata; it is not a physical folder segment.
Each execution still gets a fresh `run_id` and a fresh immutable run folder.

## Stage Selection

The route is selected strictly by `target_stage` in the campaign config:
- `data_prep` -> materialize reusable research data prep only: continuous front, dataset, instrument tree, bar view, base indicator, and derived indicator layers
- `backtest` -> reuse or rebuild research data prep, refresh the strategy registry needed by the campaign, then run backtests and rankings
- `projection` -> run the full route through candidate projection

The scheduled freshness contour is `research_data_prep_job`.
It is triggered after `moex_baseline_update_job` succeeds so `continuous_front_refresh` and materialized research data stay current with the canonical MOEX baseline.
Strategy refresh is separate because strategy inventory changes are not the same decision as data freshness.

Accepted baseline defaults should resolve to:
- canonical root: `D:/TA3000-data/trading-advisor-3000-nightly/canonical/moex/baseline-4y-current`
- materialized root: `D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current`
- registry root: `D:/TA3000-data/trading-advisor-3000-nightly/research/registry/current`
- runs root: `D:/TA3000-data/trading-advisor-3000-nightly/research/runs`

The canonical root must include `canonical_bars.delta`, `canonical_bar_provenance.delta`,
`canonical_session_calendar.delta`, and `canonical_roll_map.delta`.
The MOEX canonical job owns all four tables.
Research data prep consumes them, builds `continuous_front_bars.delta`, `continuous_front_roll_events.delta`, `continuous_front_adjustment_ladder.delta`, and `continuous_front_qc_report.delta`, then builds the reusable research layer.
`continuous_front` is historical/batch research truth only and must not be used as a live intraday decision source.

## Run Artifacts

Each run folder contains:
- `campaign.lock.json`
- `status.json`
- `run-summary.json`
- `artifacts-index.json`
- `logs/stdout.log`
- `logs/stderr.log`

Status transitions are:
- `queued`
- `running`
- terminal `success | failed | blocked`

## Route Boundary

Operator-facing research execution starts and ends at `run_campaign`.
Scheduled freshness remains Dagster-owned through `research_data_prep_job` after `moex_baseline_update_job`.
Implementation modules and benchmark tooling may exist inside the repo, but they are not part of the committed operator path for research campaigns.
