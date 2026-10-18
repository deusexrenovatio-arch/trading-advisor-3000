from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.research.derived_indicators.materialize import (
    _source_indicator_columns_for_profile,
)
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    build_derived_indicator_profile_registry,
)
from trading_advisor_3000.product_plane.research.derived_indicators.source_frames import (
    derived_source_indicator_columns_hash,
)
from trading_advisor_3000.spark_jobs import research_derived_source_frames_job as job


def test_native_outputs_have_no_projected_dependencies() -> None:
    profile = build_derived_indicator_profile_registry().get("core_v1")
    required = _source_indicator_columns_for_profile(profile)
    assert not any(column.startswith("mtf_") for column in profile.output_columns)
    assert len(required) == len(set(required))


def _source_metadata(root: Path, hashes: tuple[str | None, ...]) -> Path:
    path = root / "research_derived_source_frames.delta"
    write_delta_table_rows(
        table_path=path,
        columns={
            "dataset_version": "string",
            "contour_id": "string",
            "indicator_set_version": "string",
            "instrument_id": "string",
            "timeframe": "string",
            "source_indicator_columns_hash": "string",
        },
        rows=[
            {
                "dataset_version": "dataset-v1",
                "contour_id": "native_tradable",
                "indicator_set_version": "indicators-v1",
                "instrument_id": "BR",
                "timeframe": "1h",
                "source_indicator_columns_hash": value,
            }
            for value in hashes
        ],
    )
    return path


@pytest.mark.parametrize("hashes", [("old", "new"), ("old", None)])
def test_mixed_source_projection_contract_fails_before_spark(tmp_path: Path, hashes) -> None:
    bars = tmp_path / "research_bar_views.delta"
    indicators = tmp_path / "research_indicator_frames.delta"
    for path in (bars, indicators):
        write_delta_table_rows(table_path=path, rows=[], columns={"adx_14": "double"})
    _source_metadata(tmp_path / "derived", hashes)
    with pytest.raises(ValueError, match="mixed source projection contracts"):
        job.run_research_derived_source_frames_spark_job(
            bar_views_path=bars,
            indicator_frames_path=indicators,
            output_dir=tmp_path / "derived",
            dataset_version="dataset-v1",
            contour_id="native_tradable",
            indicator_set_version="indicators-v1",
            derived_profile_version="core_v1",
            source_indicator_columns=("adx_14",),
            timeframes=("15m",),
            dataset_instrument_ids=("BR",),
            refresh_windows=[
                {
                    "instrument_id": "BR",
                    "timeframe": "15m",
                    "start_ts": "2026-06-10T10:00:00Z",
                    "end_ts": "2026-06-10T10:15:00Z",
                }
            ],
            spark_session_factory=lambda *_: pytest.fail("mixed projection must fail before Spark"),
        )


@pytest.mark.parametrize(
    "kind, changed", [("current", False), ("old", True), ("null", True), ("empty", True)]
)
def test_native_projection_contract_distinct(tmp_path: Path, kind: str, changed: bool) -> None:
    expected = derived_source_indicator_columns_hash(("adx_14",))
    hashes = {"current": (expected,), "old": ("old",), "null": (None,), "empty": ()}[kind]
    path = _source_metadata(tmp_path, hashes)
    assert (
        job._source_projection_contract_changed(
            table_path=path,
            expected_hash=expected,
            dataset_version="dataset-v1",
            contour_id="native_tradable",
            indicator_set_version="indicators-v1",
            dataset_instrument_ids=("BR",),
        )
        is changed
    )


def test_projection_contract_scope_excludes_other_datasets_and_instruments(tmp_path: Path) -> None:
    expected = derived_source_indicator_columns_hash(("adx_14",))
    path = _source_metadata(tmp_path, (expected,))
    from trading_advisor_3000.product_plane.data_plane.delta_runtime import delta_table_columns

    columns = {column: "string" for column in delta_table_columns(path)}
    write_delta_table_rows(
        table_path=path,
        columns=columns,
        mode="append",
        rows=[
            {
                "dataset_version": "other-dataset",
                "contour_id": "native_tradable",
                "indicator_set_version": "indicators-v1",
                "instrument_id": "BR",
                "timeframe": "15m",
                "source_indicator_columns_hash": "old",
            },
            {
                "dataset_version": "dataset-v1",
                "contour_id": "native_tradable",
                "indicator_set_version": "indicators-v1",
                "instrument_id": "Si",
                "timeframe": "1d",
                "source_indicator_columns_hash": None,
            },
        ],
    )
    assert not job._source_projection_contract_changed(
        table_path=path,
        expected_hash=expected,
        dataset_version="dataset-v1",
        contour_id="native_tradable",
        indicator_set_version="indicators-v1",
        dataset_instrument_ids=("BR",),
    )


def test_legacy_projection_without_hash_requires_refresh(tmp_path: Path) -> None:
    path = tmp_path / "research_derived_source_frames.delta"
    write_delta_table_rows(table_path=path, columns={"dataset_version": "string"}, rows=[])
    assert job._source_projection_contract_changed(
        table_path=path,
        expected_hash="expected",
        dataset_version="dataset-v1",
        contour_id="native_tradable",
        indicator_set_version="indicators-v1",
    )


def test_declared_column_missing_fails_with_existing_source_before_spark(tmp_path: Path) -> None:
    bars, indicators = (tmp_path / name for name in ("bars.delta", "indicators.delta"))
    for path in (bars, indicators):
        write_delta_table_rows(table_path=path, columns={"ema_20": "double"}, rows=[])
    _source_metadata(tmp_path / "derived", ("old",))
    with pytest.raises(ValueError, match="requires source indicator columns: adx_14"):
        job.run_research_derived_source_frames_spark_job(
            bar_views_path=bars,
            indicator_frames_path=indicators,
            output_dir=tmp_path / "derived",
            dataset_version="dataset-v1",
            contour_id="native_tradable",
            indicator_set_version="indicators-v1",
            derived_profile_version="core_v1",
            source_indicator_columns=("adx_14",),
            spark_session_factory=lambda *_: pytest.fail("missing ADX must fail before Spark"),
        )


def test_current_hash_with_missing_declared_source_column_fails_closed(tmp_path: Path) -> None:
    expected = derived_source_indicator_columns_hash(("adx_14",))
    path = _source_metadata(tmp_path, (expected,))
    with pytest.raises(
        ValueError, match="current source projection contract is missing declared columns: adx_14"
    ):
        job._source_projection_contract_changed(
            table_path=path,
            expected_hash=expected,
            dataset_version="dataset-v1",
            contour_id="native_tradable",
            indicator_set_version="indicators-v1",
            source_indicator_columns=("adx_14",),
        )


@pytest.mark.parametrize("declared_ips", [("BR", "Si"), ()])
def test_hash_check_includes_declared_instrument_scope_before_windows(
    tmp_path: Path, declared_ips
) -> None:
    expected = derived_source_indicator_columns_hash(("adx_14",))
    bars, indicators = (tmp_path / name for name in ("bars.delta", "indicators.delta"))
    for path in (bars, indicators):
        write_delta_table_rows(table_path=path, rows=[], columns={"adx_14": "double"})
    write_delta_table_rows(
        table_path=tmp_path / "derived" / "research_derived_source_frames.delta",
        columns={
            "dataset_version": "string",
            "contour_id": "string",
            "indicator_set_version": "string",
            "instrument_id": "string",
            "timeframe": "string",
            "source_indicator_columns_hash": "string",
            "adx_14": "double",
        },
        rows=[
            {
                "dataset_version": "dataset-v1",
                "contour_id": "native_tradable",
                "indicator_set_version": "indicators-v1",
                "instrument_id": instrument,
                "timeframe": "1h",
                "source_indicator_columns_hash": hash_value,
                "adx_14": 11.0,
            }
            for instrument, hash_value in (("BR", expected), ("Si", "old"))
        ],
    )
    with pytest.raises(ValueError, match="mixed source projection contracts"):
        job.run_research_derived_source_frames_spark_job(
            bar_views_path=bars,
            indicator_frames_path=indicators,
            output_dir=tmp_path / "derived",
            dataset_version="dataset-v1",
            contour_id="native_tradable",
            indicator_set_version="indicators-v1",
            derived_profile_version="core_v1",
            source_indicator_columns=("adx_14",),
            dataset_instrument_ids=declared_ips,
            timeframes=("15m",),
            refresh_windows=(
                {"instrument_id": "BR", "timeframe": "15m", "start_ts": "2026-03-02T16:30:00Z"},
            ),
            spark_session_factory=lambda *_: pytest.fail(
                "declared mixed scope must fail before Spark"
            ),
        )


@pytest.mark.parametrize(
    "kind", ["missing", "null", "blank", "old", "partial", "missing-family", "mixed-family"]
)
def test_l2_projection_identity_unknown_requires_full_family_repair(
    tmp_path: Path, kind: str
) -> None:
    from trading_advisor_3000.product_plane.research.derived_indicators import materialize as owner

    expected = derived_source_indicator_columns_hash(("adx_14",))
    columns = {
        "dataset_version": "string",
        "contour_id": "string",
        "indicator_set_version": "string",
        "derived_indicator_set_version": "string",
        "instrument_id": "string",
        "timeframe": "string",
        "source_indicator_columns_hash": "string",
        "source_inputs_hash": "string",
    }
    identities = {
        "null": (None,),
        "blank": ("",),
        "old": ("old",),
        "partial": (expected, "old"),
    }.get(kind, (expected,))
    rows = [
        {
            "dataset_version": "dataset-v1",
            "contour_id": "native_tradable",
            "indicator_set_version": "indicators-v1",
            "derived_indicator_set_version": "derived-v1",
            "instrument_id": "Si",
            "timeframe": "1h",
            "source_indicator_columns_hash": value,
            "source_inputs_hash": "known",
        }
        for value in identities
    ]
    if kind == "missing":
        columns.pop("source_indicator_columns_hash")
    if kind == "missing-family":
        columns.pop("source_inputs_hash")
    if kind == "mixed-family":
        rows.append({**rows[0], "source_inputs_hash": "different"})
    rows = [{key: value for key, value in row.items() if key in columns} for row in rows]
    path = tmp_path / "research_derived_indicator_frames.delta"
    write_delta_table_rows(table_path=path, columns=columns, rows=rows)
    changed = owner._derived_projection_contract_changed(
        table_path=path,
        expected_hash=expected,
        dataset_version="dataset-v1",
        contour_id="native_tradable",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        dataset_instrument_ids=("BR", "Si"),
    )

    assert changed is (kind not in {"missing-family", "mixed-family"})


def test_l2_dependency_metadata_is_reserved_and_legacy_nullable(tmp_path: Path) -> None:
    from trading_advisor_3000.product_plane.research.derived_indicators.store import (
        DERIVED_INDICATOR_RESERVED_COLUMNS,
        DerivedIndicatorFrameRow,
        research_derived_indicator_store_contract,
    )

    payload = {
        "dataset_version": "d",
        "indicator_set_version": "l1",
        "derived_indicator_set_version": "l2",
        "profile_version": "core_v1",
        "contract_id": "BR",
        "instrument_id": "BR",
        "timeframe": "15m",
        "ts": "2026-03-02T09:00:00Z",
        "source_bars_hash": "bars",
        "source_indicators_hash": "local",
        "row_count": 1,
        "warmup_span": 0,
        "null_warmup_span": 0,
        "created_at": "2026-03-02T09:00:00Z",
        "rolling_high_20": None,
    }
    for field in (
        "source_indicator_columns_hash",
        "source_inputs_hash",
        "source_snapshot_hash",
        "compute_contract_hash",
    ):
        assert field in DERIVED_INDICATOR_RESERVED_COLUMNS
        assert (
            research_derived_indicator_store_contract()["research_derived_indicator_frames"][
                "columns"
            ][field]
            == "string"
        )
    legacy = DerivedIndicatorFrameRow.from_dict(payload)
    assert legacy.source_indicator_columns_hash == ""
    assert legacy.source_inputs_hash == ""
    current = DerivedIndicatorFrameRow.from_dict(
        {
            **payload,
            "source_indicator_columns_hash": "ABCD",
            "source_inputs_hash": "EFGH",
            "source_snapshot_hash": "snapshot",
            "compute_contract_hash": "compute",
        }
    )
    current = replace(current, bar_end_ts="2026-03-02T09:15:00Z")
    assert current.values == legacy.values
    assert current.to_dict()["source_indicator_columns_hash"] == "ABCD"
    assert current.to_dict()["source_inputs_hash"] == "EFGH"

    from trading_advisor_3000.product_plane.research.derived_indicators.store import (
        existing_derived_indicator_value_columns,
        load_derived_indicator_frames,
        load_derived_indicator_partition_rows,
        write_derived_indicator_frame_batches,
    )

    write_derived_indicator_frame_batches(output_dir=tmp_path, row_batches=[[current]])
    for loaded in (
        load_derived_indicator_frames(
            output_dir=tmp_path,
            dataset_version="d",
            indicator_set_version="l1",
            derived_indicator_set_version="l2",
            value_columns=("rolling_high_20",),
        ),
        load_derived_indicator_partition_rows(
            output_dir=tmp_path,
            partition=current.partition_key(series_mode="contract"),
            value_columns=("rolling_high_20",),
        ),
    ):
        assert len(loaded) == 1
        assert loaded[0].source_indicator_columns_hash == "ABCD"
        assert loaded[0].source_inputs_hash == "EFGH"
        assert loaded[0].source_snapshot_hash == "snapshot"
        assert loaded[0].compute_contract_hash == "compute"
        assert loaded[0].values == current.values
    assert not {"source_indicator_columns_hash", "source_inputs_hash"} & set(
        existing_derived_indicator_value_columns(output_dir=tmp_path)
    )


def test_l2_current_family_identities_are_partition_specific_and_scoped(tmp_path: Path) -> None:
    from trading_advisor_3000.product_plane.research.derived_indicators import materialize as owner

    expected = derived_source_indicator_columns_hash(("adx_14",))
    base = {
        "dataset_version": "d",
        "contour_id": "native_tradable",
        "indicator_set_version": "l1",
        "derived_indicator_set_version": "l2",
        "instrument_id": "BR",
        "timeframe": "15m",
        "source_indicator_columns_hash": expected,
        "source_inputs_hash": "first",
    }
    rows = [
        base,
        {**base, "timeframe": "1h", "source_inputs_hash": "second"},
        {**base, "instrument_id": "Si", "source_indicator_columns_hash": "old"},
        {**base, "derived_indicator_set_version": "other", "source_indicator_columns_hash": "old"},
    ]
    path = tmp_path / "l2.delta"
    write_delta_table_rows(
        table_path=path, columns={column: "string" for column in base}, rows=rows
    )
    assert not owner._derived_projection_contract_changed(
        table_path=path,
        expected_hash=expected,
        dataset_version="d",
        contour_id="native_tradable",
        indicator_set_version="l1",
        derived_indicator_set_version="l2",
        dataset_instrument_ids=("BR",),
    )
    assert owner._derived_projection_contract_changed(
        table_path=path,
        expected_hash=expected,
        dataset_version="d",
        contour_id="native_tradable",
        indicator_set_version="l1",
        derived_indicator_set_version="l2",
        dataset_instrument_ids=(),
    )


def test_changed_local_input_identity_forbids_reuse() -> None:
    from trading_advisor_3000.product_plane.research.derived_indicators import materialize as owner

    head = {
        "source_bars_hash": "same",
        "source_indicators_hash": "same",
        "profile_version": "core_v1",
        "row_count": 32,
        "output_columns_hash": "same",
        "source_inputs_hash": "old-local-inputs",
        "compute_contract_hash": "native-contract",
    }
    assert not owner._existing_partition_matches(
        [head],
        source_bars_hash="same",
        source_indicators_hash="same",
        profile_version="core_v1",
        row_count=32,
        output_columns_hash="same",
        source_inputs_hash="new-local-inputs",
        compute_contract_hash="native-contract",
    )

    assert owner._existing_partition_matches(
        [head],
        source_bars_hash="same",
        source_indicators_hash="same",
        profile_version="core_v1",
        row_count=32,
        output_columns_hash="same",
        source_inputs_hash="old-local-inputs",
        compute_contract_hash="native-contract",
    )


@pytest.mark.parametrize("physical_count, include_higher", [(16, True), (32, False), (32, True)])
def test_l2_coverage_uses_physical_rows_before_narrow_target_scope(
    tmp_path: Path,
    physical_count: int,
    include_higher: bool,
) -> None:
    from trading_advisor_3000.product_plane.research.derived_indicators import materialize as owner
    from trading_advisor_3000.product_plane.research.derived_indicators.store import (
        DerivedIndicatorFramePartitionKey,
    )

    base = {
        "dataset_version": "d",
        "contour_id": "native_tradable",
        "indicator_set_version": "l1",
        "derived_indicator_set_version": "l2",
        "instrument_id": "BR",
        "contract_id": "BR-6.26",
        "series_mode": "contract",
        "series_id": "BR-6.26",
        "timeframe": "15m",
        "row_count": 32,
    }
    rows = [base] * physical_count
    if include_higher:
        rows += [{**base, "timeframe": "1h"}] * 32
    # Another mode is valid coexistence and must not be required or removed.
    rows += [{**base, "series_mode": "continuous_front", "series_id": "BR"}]
    columns = {column: "string" for column in base}
    columns["row_count"] = "int"
    path = tmp_path / "l2.delta"
    write_delta_table_rows(table_path=path, columns=columns, rows=rows)
    expected = {
        DerivedIndicatorFramePartitionKey(
            dataset_version="d",
            indicator_set_version="l1",
            derived_indicator_set_version="l2",
            timeframe=tf,
            instrument_id="BR",
            contract_id="BR-6.26",
            series_id="BR-6.26",
        ): 32
        for tf in ("15m", "1h")
    }
    assert owner._derived_partition_coverage_changed(
        table_path=path,
        expected_counts=expected,
        dataset_version="d",
        contour_id="native_tradable",
        indicator_set_version="l1",
        derived_indicator_set_version="l2",
        dataset_instrument_ids=("BR",),
        series_mode="contract",
    ) is (physical_count != 32 or not include_higher)


def test_l2_physical_coverage_cf_mapping_ignores_contract_coexistence(tmp_path: Path) -> None:
    from trading_advisor_3000.product_plane.research.derived_indicators import materialize as owner
    from trading_advisor_3000.product_plane.research.derived_indicators.store import (
        DerivedIndicatorFramePartitionKey,
    )

    base = {
        "dataset_version": "d",
        "contour_id": "native_tradable",
        "indicator_set_version": "l1",
        "derived_indicator_set_version": "l2",
        "instrument_id": "BR",
        "timeframe": "15m",
        "series_mode": "continuous_front",
        "series_id": None,
        "contract_id": "BR-6.26",
    }
    rows = [base] * 16 + [{**base, "contract_id": "BR-9.26"}] * 16
    rows += [{**base, "series_mode": "contract", "series_id": "BR-6.26"}]
    path = tmp_path / "l2.delta"
    write_delta_table_rows(
        table_path=path, columns={column: "string" for column in base}, rows=rows
    )
    key = DerivedIndicatorFramePartitionKey(
        dataset_version="d",
        indicator_set_version="l1",
        derived_indicator_set_version="l2",
        timeframe="15m",
        instrument_id="BR",
        contract_id=None,
        series_mode="continuous_front",
        series_id="BR",
    )
    assert not owner._derived_partition_coverage_changed(
        table_path=path,
        expected_counts={key: 32},
        dataset_version="d",
        contour_id="native_tradable",
        indicator_set_version="l1",
        derived_indicator_set_version="l2",
        dataset_instrument_ids=("BR",),
        series_mode="continuous_front",
    )
