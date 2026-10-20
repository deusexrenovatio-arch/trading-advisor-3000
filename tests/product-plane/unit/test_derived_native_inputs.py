from dataclasses import replace

import pandas as pd
import pytest
from test_research_derived_indicator_layer import _indicator_row, _view

from trading_advisor_3000.product_plane.research.derived_indicators import materialize as m
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    DerivedIndicatorProfile,
    current_derived_indicator_profile,
)
from trading_advisor_3000.product_plane.research.derived_indicators.store import (
    DerivedIndicatorFrameRow,
    research_derived_indicator_store_contract,
)


def test_default_profile_and_store_have_no_projected_timeframe_fields():
    profile = current_derived_indicator_profile()
    columns = research_derived_indicator_store_contract()["research_derived_indicator_frames"][
        "columns"
    ]
    assert not any(c.startswith("mtf_") for c in profile.output_columns)
    assert "source_mtf_hash" not in columns
    assert "source_inputs_hash" in columns


@pytest.mark.parametrize(
    "column", ["mtf_1h_to_15m_ema_20", "source_mtf_hash", "mtf_source_family_hash"]
)
def test_removed_projection_cannot_be_requested_again(column):
    with pytest.raises(ValueError, match="retired"):
        DerivedIndicatorProfile("custom", "legacy", (column,), 0)


def test_native_build_does_not_depend_on_other_timeframes():
    bars = [_view(ts_index=i, close=100 + i) for i in range(24)]
    indicators = [_indicator_row(ts_index=i, close=100 + i) for i in range(24)]
    kwargs = dict(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
    )
    alone = m.build_derived_indicator_frames(**kwargs, bar_views=bars, indicator_rows=indicators)
    other_bars = [replace(b, timeframe="1h", close=b.close + 1000) for b in bars]
    other_indicators = [replace(r, timeframe="1h", values={"ema_20": 9999.0}) for r in indicators]
    together = m.build_derived_indicator_frames(
        **kwargs, bar_views=bars + other_bars, indicator_rows=indicators + other_indicators
    )
    selected = [r for r in together if r.timeframe == "15m"]
    assert {r.ts: r.values for r in alone} == {r.ts: r.values for r in selected}
    assert not any(c.startswith("mtf_") for r in selected for c in r.values)


def test_local_input_digest_tracks_projection_and_ignores_write_timestamp():
    profile = DerivedIndicatorProfile("test", "native", ("rolling_high_20",), 0)
    frame = pd.DataFrame(
        [
            dict(
                ts="2026-01-01",
                contract_id="A",
                close=10.0,
                instrument_id="BR",
                timeframe="15m",
                source_frame_created_at="old",
            )
        ]
    )
    before = m._source_inputs_hash(profile=profile, base_frame=frame)
    frame["source_frame_created_at"] = "new"
    assert m._source_inputs_hash(profile=profile, base_frame=frame) == before
    frame["_cf_close0"] = 8.0
    assert m._source_inputs_hash(profile=profile, base_frame=frame) != before


def test_legacy_read_discards_retired_fields_and_hash_aliases():
    row = m.build_derived_indicator_frames(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        bar_views=[_view(ts_index=0, close=100)],
        indicator_rows=[_indicator_row(ts_index=0, close=100)],
        profile=DerivedIndicatorProfile("test", "native", ("rolling_high_20",), 0),
    )[0]
    payload = row.to_dict()
    payload.update(
        source_mtf_hash="old", mtf_source_family_hash="production-old", mtf_1h_to_15m_ema_20=100.0
    )
    restored = DerivedIndicatorFrameRow.from_dict(payload)
    assert restored.values == row.values
    assert not any(c.startswith("mtf_") or c == "source_mtf_hash" for c in restored.to_dict())
