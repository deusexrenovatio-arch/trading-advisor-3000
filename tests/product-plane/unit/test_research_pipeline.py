from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.research.indicators import compute_volume_profile_features


def test_public_volume_profile_features_rejects_invalid_tick_size() -> None:
    with pytest.raises(ValueError, match="tick_size must be finite and positive"):
        compute_volume_profile_features(
            [{"low": 99.0, "high": 101.0, "volume": 10}],
            tick_size=float("nan"),
            target_volume=10.0,
            expected_source_bars=1,
        )
