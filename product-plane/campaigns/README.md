# Research Campaign Configs

This directory holds Product Plane campaign configs for the canonical research route.

Official route:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config product-plane/campaigns/fut_br_base_15m.explore.yaml
```

Approved-universe data prep route:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config product-plane/campaigns/moex_approved_universe_data_prep.yaml
```

Forced verification data-prep proof for the Optuna/vectorbt trend MTF pullback campaign:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config product-plane/campaigns/moex_approved_subset_optuna_15m.trend_mtf_pullback_research.data_prep_proof.yaml
```

Rules:
- committed example configs should prefer external-first storage under `D:/TA3000-data`;
- worktree-local output roots are allowed only when they are explicitly written into a campaign config;
- the runner does not pick alternate storage roots on its own;
- reusable gold outputs live directly under `materialized_root`; `materialization_key` is persisted in lock/run metadata, not used as a folder segment;
- immutable per-run artifacts live under `runs_root/<campaign_name>/<run_id>/`.
- forced proof configs must use a separate `research/gold/verification/...` materialized root and `execution.force_rematerialize: true`.
- scheduled freshness is Dagster-owned: `research_data_prep_after_moex_sensor` starts `research_data_prep_job` after `moex_baseline_update_job` succeeds.
- the data prep materialization includes `research_instrument_tree`, `research_bar_views`, `research_indicator_frames`, and `research_derived_indicator_frames`.
- backtest stages must consume those Delta tables through native Delta/Arrow filters and strategy-column projection; Python row-object reloaders are not a campaign fallback.

Committed configs should describe the full operator path through `run_campaign`.
Implementation modules and benchmark tooling are intentionally outside this config contract.
