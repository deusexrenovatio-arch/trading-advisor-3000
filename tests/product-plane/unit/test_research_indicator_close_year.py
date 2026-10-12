from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.indicators import (
    IndicatorParameter,
    IndicatorProfile,
    IndicatorSpec,
    materialize,
    store,
)


def test_l1_native_writers_use_authoritative_close_and_preserve_existing_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = IndicatorProfile(
        version="close-year-fixture",
        description="Physical writer fixture; numerical computation is mocked",
        indicators=(
            IndicatorSpec(
                indicator_id="sma_2",
                category="trend",
                operation_key="sma",
                parameters=(IndicatorParameter(name="length", value=2),),
                required_input_columns=("close",),
                output_columns=("sma_2",),
                warmup_bars=2,
            ),
        ),
    )
    bar = ResearchBarView(
        dataset_version="close-year-fixture",
        contract_id="BR-3.26",
        instrument_id="FUT_BR",
        timeframe="15m",
        ts="2025-12-31T23:45:00Z",
        bar_end_ts="2026-01-01T00:00:00Z",
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.25,
        volume=10,
        open_interest=20,
        session_date="2025-12-31",
        session_open_ts=None,
        session_close_ts=None,
        active_contract_id="BR-3.26",
        ret_1=None,
        log_ret_1=None,
        true_range=1.5,
        hl_range=1.5,
        oc_range=0.25,
        bar_index=0,
        slice_role="analysis",
        contour_id="pit_active_front",
        series_mode="continuous_front",
        series_id="FUT_BR",
    )
    monkeypatch.setattr(
        materialize,
        "_compute_profile_frame_for_series",
        lambda frame, profile, **kwargs: pd.DataFrame({"sma_2": [1.25]}, index=frame.index),
    )
    monkeypatch.setattr(materialize, "_indicator_compute_contract_hash", lambda profile: "COMPUTE")
    monkeypatch.setattr(materialize, "_refresh_context_hash", lambda *args, **kwargs: "CONTEXT")
    monkeypatch.setattr(
        materialize, "_indicator_input_row_hashes", lambda series, **kwargs: ["INPUT"] * len(series)
    )
    row = materialize._build_partition_rows(
        dataset_version="close-year-fixture",
        indicator_set_version="indicators-fixture",
        profile=profile,
        series=[bar],
        series_mode="continuous_front",
        source_dataset_bars_hash="DATASET",
        precomputed_source_hash="SOURCE",
    )[0]
    business = row.to_dict()
    columns = store.indicator_store_contract(profile)["research_indicator_frames"]["columns"]
    arrow_types = {
        "string": pa.string(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "double": pa.float64(),
        "int": pa.int32(),
        "bigint": pa.int64(),
    }
    normalized_business = {
        name: (
            datetime.fromisoformat(str(business[name]).replace("Z", "+00:00"))
            if kind == "timestamp"
            else business[name]
        )
        for name, kind in columns.items()
    }

    def invoke(entrypoint: str, output_dir: Path, payload):
        partition = payload.partition_key(series_mode="continuous_front")
        if entrypoint == "batches":
            return store.write_indicator_frame_batches(
                output_dir=output_dir,
                row_batches=([payload],),
                replace_partitions=(partition,),
                profile=profile,
            )
        return store.write_indicator_frame_partition_batches(
            output_dir=output_dir,
            partition_row_batches=((partition, [payload]),),
            profile=profile,
        )

    for physical_type in (pa.int32(), pa.int64()):
        for entrypoint in ("batches", "partition_batches"):
            output_dir = tmp_path / str(physical_type) / entrypoint
            path = output_dir / "research_indicator_frames.delta"
            schema = pa.schema(
                [pa.field(name, arrow_types[kind]) for name, kind in columns.items()]
                + [pa.field("ts_close_year", physical_type)]
            )
            seed = {**normalized_business, "sma_2": 1.0, "ts_close_year": 2026}
            write_deltalake(
                str(path),
                pa.Table.from_pylist([seed], schema=schema),
                partition_by=["ts_close_year"],
            )
            original = DeltaTable(str(path), version=0)
            old_rows = original.to_pyarrow_table().to_pylist()
            paths, row_count, batch_count = invoke(entrypoint, output_dir, row)
            current = DeltaTable(str(path))
            actual = current.to_pyarrow_table().to_pylist()
            # This is the consumed native defect: the old writer appends NULL year.
            # New DTO assertions deliberately follow the physical write/readback.
            assert [item["ts_close_year"] for item in actual] == [2026]
            assert current.metadata().id == original.metadata().id
            assert current.metadata().partition_columns == ["ts_close_year"]
            assert current.schema().to_pyarrow().field("ts_close_year").type == physical_type
            assert actual == [{**normalized_business, "ts_close_year": 2026}]
            assert DeltaTable(str(path), version=0).to_pyarrow_table().to_pylist() == old_rows
            assert paths["research_indicator_frames"] == path.as_posix()
            assert (row_count, batch_count) == (1, 1)
            assert row.bar_end_ts == bar.bar_end_ts
            without_close = replace(row, bar_end_ts=None)
            assert without_close == row
            assert without_close.to_dict() == business
            assert "bar_end_ts" not in business and "ts_close_year" not in business
            assert {
                name: actual[0][name]
                for name in (
                    "source_bars_hash",
                    "source_dataset_bars_hash",
                    "compute_contract_hash",
                    "input_row_hash",
                    "refresh_context_hash",
                )
            } == {
                name: business[name]
                for name in (
                    "source_bars_hash",
                    "source_dataset_bars_hash",
                    "compute_contract_hash",
                    "input_row_hash",
                    "refresh_context_hash",
                )
            }
            before_missing = current.version()
            with pytest.raises(ValueError, match="authoritative bar_end_ts"):
                invoke(entrypoint, output_dir, without_close)
            after_missing = DeltaTable(str(path))
            assert after_missing.version() == before_missing
            assert after_missing.metadata().id == original.metadata().id
            assert after_missing.to_pyarrow_table().to_pylist() == actual
