from __future__ import annotations

from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.research.pipeline import run_research_from_bars


def test_public_research_pipeline_rejects_invalid_volume_profile_tick_sizes(
    tmp_path: Path,
) -> None:
    bar = CanonicalBar.from_dict(
        {
            "contract_id": "BR-6.26",
            "instrument_id": "FUT_BR",
            "timeframe": "15m",
            "ts": "2026-04-01T10:00:00Z",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 10,
            "open_interest": 100,
        }
    )

    with pytest.raises(ValueError, match="invalid volume profile tick_size for FUT_BR"):
        run_research_from_bars(
            bars=[bar],
            instrument_by_contract={"BR-6.26": "FUT_BR"},
            strategy_version_id="ma-cross-v1",
            dataset_version="dataset-v1",
            output_dir=tmp_path,
            backtest_config={"volume_profile_tick_size_by_instrument": {"FUT_BR": float("nan")}},
        )
