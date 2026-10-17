from __future__ import annotations

import gc
import weakref
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from trading_advisor_3000.product_plane.research.derived_indicators import materialize as owner


def test_cf_materialization_releases_finished_source_families(monkeypatch, tmp_path: Path) -> None:
    instruments = ("FUT_A", "FUT_B", "FUT_C")
    timeframes = ("15m", "1h")
    metadata = [
        {
            "instrument_id": instrument,
            "contour_id": "pit_active_front",
            "series_mode": "continuous_front",
            "series_id": instrument,
            "timeframe": timeframe,
            "partition_row_count": 1,
        }
        for instrument in instruments
        for timeframe in timeframes
    ]
    indicator_dir = tmp_path / "indicators"
    derived_dir = tmp_path / "derived"
    (indicator_dir / "research_indicator_frames.delta" / "_delta_log").mkdir(parents=True)
    (derived_dir / "research_derived_indicator_frames.delta" / "_delta_log").mkdir(parents=True)
    frames: list[tuple[str, weakref.ReferenceType[pd.DataFrame]]] = []
    observations: list[tuple[str, tuple[str, ...]]] = []
    computed: list[tuple[str, str]] = []
    written: list[tuple[str, str]] = []

    def observe(stage: str) -> None:
        gc.collect()
        live = tuple(sorted({instrument for instrument, ref in frames if ref() is not None}))
        observations.append((stage, live))

    def load_source_rows(*, partition, **kwargs):
        return [{"instrument_id": partition.instrument_id, "timeframe": partition.timeframe}]

    def source_inputs(*, rows, **kwargs):
        row = rows[0]
        frame = pd.DataFrame({"adx_14": [1.25]})
        frame.attrs["family"] = row["instrument_id"]
        frames.append((row["instrument_id"], weakref.ref(frame)))
        observe("load")
        bar = SimpleNamespace(instrument_id=row["instrument_id"], timeframe=row["timeframe"])
        indicator = SimpleNamespace(profile_version="core_v1", output_columns_hash="source-hash")
        return [bar], [indicator], frame

    def build_rows(*, local_series, source_frames, **kwargs):
        bar = local_series[0]
        assert set(source_frames) == set(timeframes)
        assert {frame.attrs["family"] for frame in source_frames.values()} == {bar.instrument_id}
        observe("compute")
        key = (bar.instrument_id, bar.timeframe)
        computed.append(key)
        return [key]

    def write_batches(*, row_batches, replace_partitions, **kwargs):
        # The existing owning writer still receives every scoped replacement,
        # while consuming the public flow lazily instead of retaining its inputs.
        assert {(part.instrument_id, part.timeframe) for part in replace_partitions} == {
            (instrument, timeframe) for instrument in instruments for timeframe in timeframes
        }
        assert all(
            part.dataset_version == "memory-fixture"
            and part.contour_id == "pit_active_front"
            and part.series_mode == "continuous_front"
            and part.contract_id is None
            for part in replace_partitions
        )
        count = 0
        for batch in row_batches:
            observe("write")
            written.extend(batch)
            count += 1
        return {"research_derived_indicator_frames": "mocked.delta"}, len(written), count

    monkeypatch.setattr(
        owner,
        "_load_dataset_manifest",
        lambda **kwargs: {"series_mode": "continuous_front", "bars_hash": "dataset-hash"},
    )
    monkeypatch.setattr(
        owner.cf_input_projection, "load_adjustment_ladder_rows", lambda **kwargs: ()
    )
    monkeypatch.setattr(owner, "_source_indicator_columns_for_profile", lambda profile: ("adx_14",))
    monkeypatch.setattr(owner, "_delta_source_refs", lambda paths: [{"fixture": "stable"}])
    monkeypatch.setattr(owner, "existing_indicator_value_columns", lambda **kwargs: ("adx_14",))
    monkeypatch.setattr(
        owner,
        "run_research_derived_source_frames_spark_job",
        lambda **kwargs: {"rows_by_table": {owner.DERIVED_SOURCE_FRAME_TABLE: len(metadata)}},
    )
    monkeypatch.setattr(owner, "_derived_projection_contract_changed", lambda **kwargs: False)
    monkeypatch.setattr(owner, "_derived_partition_coverage_changed", lambda **kwargs: False)
    monkeypatch.setattr(
        owner, "load_derived_source_frame_partition_metadata", lambda **kwargs: metadata
    )
    monkeypatch.setattr(owner, "load_derived_source_frame_partition_rows", load_source_rows)
    monkeypatch.setattr(owner, "_source_frame_rows_to_inputs", source_inputs)
    monkeypatch.setattr(owner, "_with_continuous_front_projection", lambda frame, **kwargs: frame)
    monkeypatch.setattr(owner, "_latest_delta_commit_timestamp", lambda path: None)
    monkeypatch.setattr(owner, "load_derived_indicator_partition_metadata", lambda **kwargs: [])
    monkeypatch.setattr(owner, "existing_derived_indicator_value_columns", lambda **kwargs: ())
    monkeypatch.setattr(owner, "_bars_hash", lambda rows: "bars-hash")
    monkeypatch.setattr(owner, "_indicator_hash", lambda rows, **kwargs: "indicators-hash")
    monkeypatch.setattr(owner, "_mtf_source_family_hash", lambda **kwargs: "family-hash")
    monkeypatch.setattr(owner, "_build_partition_rows", build_rows)
    monkeypatch.setattr(owner, "write_derived_indicator_frame_batches", write_batches)

    report = owner.materialize_derived_indicator_frames(
        dataset_output_dir=tmp_path / "datasets",
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="memory-fixture",
        indicator_set_version="indicators-fixture",
        contour_id="pit_active_front",
        timeframes=timeframes,
        dataset_instrument_ids=instruments,
    )
    expected = {(instrument, timeframe) for instrument in instruments for timeframe in timeframes}
    assert len(computed) == len(written) == len(expected)
    assert set(computed) == set(written) == expected
    assert report["recomputed_partition_count"] == report["refreshed_partition_count"] == 6
    assert report["refreshed_row_count"] == report["write_batch_count"] == 6
    # Completed families must be released before loading the next family.
    assert max(len(live) for _, live in observations) <= 1, observations
    assert any(stage == "write" for stage, _ in observations)
