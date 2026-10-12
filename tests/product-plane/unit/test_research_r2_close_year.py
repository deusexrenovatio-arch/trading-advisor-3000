from dataclasses import replace

from trading_advisor_3000.product_plane.research.derived_indicators.source_frames import (
    DERIVED_SOURCE_FRAME_TABLE,
    research_derived_source_frame_store_contract,
)
from trading_advisor_3000.product_plane.research.derived_indicators.store import (
    DerivedIndicatorFrameRow,
    research_derived_indicator_store_contract,
)


def test_r2_contracts_separate_close_year_from_business_columns():
    contracts = (
        research_derived_source_frame_store_contract()[DERIVED_SOURCE_FRAME_TABLE],
        research_derived_indicator_store_contract()["research_derived_indicator_frames"],
    )
    for contract in contracts:
        assert contract["partition_by"] == ["ts_close_year"]
        assert contract["physical_columns"] == {"ts_close_year": "int"}
        assert "ts_close_year" not in contract["columns"]
        assert contract["file_policy"]["max_compressed_file_bytes"] == 512 * 1024 * 1024


def test_l2_close_metadata_preserves_business_payload_and_reader_values():
    row = DerivedIndicatorFrameRow(
        dataset_version="fixture",
        indicator_set_version="i",
        derived_indicator_set_version="d",
        profile_version="p",
        contract_id="c",
        instrument_id="x",
        timeframe="1d",
        ts="2025-12-31T00:00:00Z",
        values={"signal_flag": 1},
        source_bars_hash="bars",
        source_indicators_hash="indicators",
        row_count=1,
        warmup_span=0,
        null_warmup_span=0,
        created_at="2026-01-01T00:00:00Z",
    )
    carried = replace(row, bar_end_ts="2026-01-01T00:00:00Z")
    assert carried.to_dict() == row.to_dict()
    loaded = DerivedIndicatorFrameRow.from_dict({**row.to_dict(), "ts_close_year": 2026})
    assert loaded.values == row.values
