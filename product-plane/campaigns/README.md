# Research Campaign Configs

This directory holds Product Plane campaign configs for the canonical research route.

Official route:

```powershell
python -m trading_advisor_3000.product_plane.research.jobs.run_campaign --config product-plane/campaigns/fut_br_base_15m.explore.yaml
```

Rules:
- committed example configs should prefer external-first storage under `D:/TA3000-data`;
- worktree-local output roots are allowed only when they are explicitly written into a campaign config;
- the runner does not pick alternate storage roots on its own;
- reusable materialized outputs live under `materialized_root/<materialization_key>/`;
- immutable per-run artifacts live under `runs_root/<campaign_name>/<run_id>/`.

Low-level bootstrap/backtest/projection jobs remain available for diagnostics, but they are internal/debug-only and not the supported operational entrypoint.
