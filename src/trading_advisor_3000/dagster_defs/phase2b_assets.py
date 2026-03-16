from __future__ import annotations

from .phase2a_assets import AssetSpec


def phase2b_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="feature_snapshots",
            description="Build point-in-time feature snapshots from canonical bars.",
            inputs=("canonical_bars_delta",),
            outputs=("feature_snapshots_delta",),
        ),
        AssetSpec(
            key="research_backtest_runs",
            description="Run deterministic backtests and store run metadata.",
            inputs=("feature_snapshots_delta",),
            outputs=("research_backtest_runs_delta",),
        ),
        AssetSpec(
            key="research_signal_candidates",
            description="Persist contract-safe signal candidates from backtests.",
            inputs=("research_backtest_runs_delta",),
            outputs=("research_signal_candidates_delta",),
        ),
    ]
