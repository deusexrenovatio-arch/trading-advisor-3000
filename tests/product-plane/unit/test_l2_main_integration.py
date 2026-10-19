"""Main-era contracts retained by the performance adapters."""

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from test_derived_indicator_tails import native_materializer as native_materializer

from trading_advisor_3000.product_plane.research.derived_indicators import materialize as m
from trading_advisor_3000.product_plane.research.derived_indicators import tails
from trading_advisor_3000.product_plane.research.derived_indicators.store import (
    load_derived_indicator_partition_metadata,
)


def test_audited_native_kernel_unchanged():
    from trading_advisor_3000.product_plane.research.derived_indicators import (
        adapt_native_metadata as adaptation,
    )

    assert adaptation.native_kernel_digest() == adaptation.NATIVE_KERNEL


@pytest.mark.parametrize(
    "field",
    ["ts_close_year", "warmup_span", "null_warmup_span", "profile_version", "output_columns_hash"],
)
def test_exact_prefix_includes_main_row_semantics(tmp_path, monkeypatch, field):
    frame = pd.DataFrame(
        {
            "ts": pd.date_range("2026-01-01", periods=4, tz="UTC"),
            "contract_id": ["A"] * 4,
            "ts_close_year": [2026] * 4,
            "warmup_span": [1] * 4,
            "null_warmup_span": [1] * 4,
            "profile_version": ["p"] * 4,
            "output_columns_hash": ["h"] * 4,
            "distance": [None, 2.0, 3.0, 4.0],
        }
    )
    old = pa.Table.from_pandas(frame.copy(deep=True), preserve_index=False)
    monkeypatch.setattr(tails, "has_delta_log", lambda _: True)
    monkeypatch.setattr(tails, "read_delta_table_arrow", lambda *_, **__: old)
    frame.loc[1, field] = "changed" if field in {"profile_version", "output_columns_hash"} else 2027
    partition = m.DerivedIndicatorFramePartitionKey("v", "l1", "l2", "15m", "A", "A")
    assert (
        tails.suffix_start_index(
            output_dir=tmp_path,
            partition=partition,
            computed=frame,
            output_columns=("distance",),
            compatible=True,
        )
        == 1
    )


def test_compute_contract_adoption_uses_exact_tail(native_materializer, monkeypatch):
    _, run, root, _ = native_materializer
    assert run()["refreshed_row_count"] == 48
    monkeypatch.setattr(m, "_derived_compute_contract_hash", lambda _: "new-compute")
    report = run()
    assert report["recomputed_partition_count"] == 2
    assert report["refreshed_row_count"] == 2
    metadata = load_derived_indicator_partition_metadata(
        output_dir=root,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
    )
    assert {row["compute_contract_hash"] for row in metadata} == {"new-compute"}
    assert run()["reused_partition_count"] == 2


def test_snapshot_noop_does_not_scan_wide(native_materializer, monkeypatch):
    _, run, _, _ = native_materializer
    run()
    real_fast_path = m.__dict__["_integration_original_fast_path"]
    monkeypatch.setattr(m, "_derived_metadata_fast_reuse", real_fast_path)
    monkeypatch.setattr(
        m, "_source_frame_arrow_to_inputs", lambda **_: pytest.fail("wide read on snapshot no-op")
    )
    assert run()["reused_partition_count"] == 2


def test_source_drift_does_not_commit_pending_pack(native_materializer, monkeypatch):
    inputs, run, root, _ = native_materializer
    run()
    path = root / "research_derived_indicator_frames.delta"
    before = DeltaTable(str(path)).version()
    for rows in inputs.values():
        rows[-1]["ema_20"] += 10
    original = m._build_prepared_partition_rows

    def drift(**kwargs):
        rows = original(**kwargs)
        write_deltalake(
            str(root / "research_indicator_frames.delta"), pa.table({"probe": [2]}), mode="append"
        )
        return rows

    monkeypatch.setattr(m, "_build_prepared_partition_rows", drift)
    with pytest.raises(ValueError, match="inputs changed"):
        run()
    assert DeltaTable(str(path)).version() == before


def test_pair_scope_keeps_off_diagonal_partitions(native_materializer):
    inputs, run, root, profile = native_materializer
    for asset in ("A", "B"):
        inputs[asset + "-1h"] = [{**row, "timeframe": "1h"} for row in inputs[asset]]
    assert run(timeframes=())["refreshed_row_count"] == 96
    path = root / "research_derived_indicator_frames.delta"
    before = DeltaTable(str(path)).to_pyarrow_table().to_pylist()
    for rows in inputs.values():
        rows[-1]["ema_20"] += 10
    report = run(
        timeframes=(),
        refresh_windows=(
            {"instrument_id": "A", "timeframe": "15m"},
            {"instrument_id": "B", "timeframe": "1h"},
        ),
    )
    assert report["refreshed_partition_count"] == 2
    after = DeltaTable(str(path)).to_pyarrow_table().to_pylist()

    def off_diagonal(rows):
        return sorted(
            [
                row
                for row in rows
                if (row["instrument_id"], row["timeframe"]) in {("A", "1h"), ("B", "15m")}
            ],
            key=lambda row: (row["instrument_id"], row["ts"]),
        )

    assert off_diagonal(after) == off_diagonal(before)


def test_conflicting_carrier_blocks_snapshot_reuse_and_scoped_write(
    native_materializer, monkeypatch
):
    _, run, root, _ = native_materializer
    run()
    path = root / "research_derived_indicator_frames.delta"
    table = DeltaTable(str(path)).to_pyarrow_table()
    latest = table.sort_by([("ts", "descending")]).slice(0, 1)
    latest = latest.set_column(
        latest.schema.get_field_index("source_indicator_columns_hash"),
        "source_indicator_columns_hash",
        pa.array(["conflict"]),
    )
    write_deltalake(str(path), latest, mode="append", partition_by=["ts_close_year"])
    original_fast = m.__dict__["_integration_original_fast_path"]
    monkeypatch.setattr(m, "_derived_metadata_fast_reuse", original_fast)
    monkeypatch.setattr(
        m,
        "run_research_derived_source_frames_spark_job",
        lambda **_: pytest.fail("Spark ran before conflict guard"),
    )
    with pytest.raises(ValueError, match="metadata adaptation"):
        run()
