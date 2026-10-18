from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from deltalake import DeltaTable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    read_delta_table_arrow,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.datasets import (
    ResearchBarView,
    ResearchDatasetManifest,
    research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.derived_indicators import materialize as owner
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    build_derived_indicator_profile_registry,
)
from trading_advisor_3000.product_plane.research.indicators import (
    IndicatorFrameRow,
    indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.indicators.store import (
    indicator_output_columns_hash,
)
from trading_advisor_3000.spark_jobs import research_derived_source_frames_job as source_job


def _stamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _seed_l0_l1(root: Path) -> tuple[Path, Path]:
    dataset_dir, indicator_dir = root / "dataset", root / "indicators"
    profile = build_derived_indicator_profile_registry().get("core_v1")
    columns = owner._source_indicator_columns_for_profile(profile)
    anchor = datetime(2026, 3, 2, 9, tzinfo=UTC)
    bars, indicators = [], []
    for timeframe, step, last in (
        ("15m", timedelta(minutes=15), anchor + timedelta(minutes=15 * 31)),
        ("1h", timedelta(hours=1), anchor + timedelta(hours=1)),
        ("4h", timedelta(hours=4), anchor),
        ("1d", timedelta(days=1), anchor),
    ):
        for index in range(32):
            opened = last - step * (31 - index)
            ts, day = _stamp(opened), _stamp(opened)[:10]
            close = 80 + index * 0.15 + ((index % 7) - 3) * 0.07
            bar = ResearchBarView(
                dataset_version="mtf-fixture-v1",
                contract_id="BR-6.26",
                instrument_id="BR",
                timeframe=timeframe,
                ts=ts,
                open=close - 0.2,
                high=close + 0.4,
                low=close - 0.5,
                close=close,
                volume=1000 + index * 5 + (index % 3) * 13,
                open_interest=20000 + index * 3 + (index % 4) * 7,
                session_date=day,
                session_open_ts=day + "T09:00:00Z",
                session_close_ts=day + "T23:45:00Z",
                active_contract_id="BR-6.26",
                ret_1=None if not index else 0.002,
                log_ret_1=None if not index else 0.001998,
                true_range=0.9,
                hl_range=0.9,
                oc_range=0.2,
                bar_index=index,
                slice_role="analysis",
                series_id="BR-6.26",
                bar_start_ts=ts,
                bar_end_ts=day + "T23:45:00Z" if timeframe == "1d" else _stamp(opened + step),
            )
            values = {column: 10 + index * 0.2 for column in columns}
            for column in columns:
                if column.startswith(("sma_", "ema_", "hma_", "vwma_")):
                    values[column] = close - 0.5
                elif "upper" in column or column.startswith("donchian_high_"):
                    values[column] = close + 1
                elif "lower" in column or column.startswith("donchian_low_"):
                    values[column] = close - 1
            values.update(
                atr_14=1.5, ema_20=close - 0.2, ema_50=close - 0.5, rsi_14=55 + index * 0.1
            )
            if timeframe == "1h" and index >= 29:
                values["adx_14"] = {29: 7.0, 30: 11.0, 31: 99.0}[index]
            if timeframe == "4h" and index >= 30:
                values["adx_14"] = {30: 33.0, 31: 44.0}[index]
            if timeframe == "1d" and index >= 30:
                values["adx_14"] = {30: 55.0, 31: 66.0}[index]
            if index == 0:
                values["adx_14"] = None  # Legitimate source warmup must survive unchanged.
            bars.append(bar.to_dict())
            indicators.append(
                IndicatorFrameRow(
                    dataset_version="mtf-fixture-v1",
                    indicator_set_version="l1-v1",
                    profile_version="core_v1",
                    contract_id=bar.contract_id,
                    instrument_id="BR",
                    timeframe=timeframe,
                    ts=ts,
                    values=values,
                    source_bars_hash="frozen-l1",
                    source_dataset_bars_hash="frozen-dataset",
                    row_count=32,
                    warmup_span=1,
                    null_warmup_span=1,
                    created_at="2026-03-03T00:00:00Z",
                    output_columns_hash=indicator_output_columns_hash(columns),
                    series_id=bar.contract_id,
                ).to_dict()
            )
    # Instrument B is in the declared scope but absent from the narrow A window.
    bars.extend(
        {
            **row,
            "instrument_id": "Si",
            "contract_id": "Si-6.26",
            "series_id": "Si-6.26",
            "active_contract_id": "Si-6.26",
        }
        for row in tuple(bars)
    )
    indicators.extend(
        {**row, "instrument_id": "Si", "contract_id": "Si-6.26", "series_id": "Si-6.26"}
        for row in tuple(indicators)
    )
    contract = research_dataset_store_contract()
    manifest = ResearchDatasetManifest(
        dataset_version="mtf-fixture-v1",
        universe_id="moex-futures",
        timeframes=("15m", "1h", "4h", "1d"),
        base_timeframe="15m",
        series_mode="contract",
        split_method="full",
        warmup_bars=0,
        bars_hash="frozen-dataset",
        code_version="fixture",
    )
    write_delta_table_rows(
        table_path=dataset_dir / "research_datasets.delta",
        rows=[manifest.to_dict()],
        columns=contract["research_datasets"]["columns"],
    )
    write_delta_table_rows(
        table_path=dataset_dir / "research_bar_views.delta",
        rows=bars,
        columns=contract["research_bar_views"]["columns"],
    )
    l1_columns = {
        **indicator_store_contract()["research_indicator_frames"]["columns"],
        **{column: "double" for column in columns},
    }
    write_delta_table_rows(
        table_path=indicator_dir / "research_indicator_frames.delta",
        rows=indicators,
        columns=l1_columns,
    )
    return dataset_dir, indicator_dir


def _input_pin(path: Path) -> tuple[object, ...]:
    table = DeltaTable(str(path))
    files = tuple(
        sorted(
            (name, hashlib.sha256((path / name).read_bytes()).hexdigest()) for name in table.files()
        )
    )
    return table.metadata().id, table.version(), files


def _read_values(path: Path, columns: tuple[str, ...]) -> pd.DataFrame:
    keys = ["instrument_id", "contract_id", "timeframe", "ts"]
    return (
        read_delta_table_arrow(
            path, columns=[*keys, *columns], filters=[("dataset_version", "=", "mtf-fixture-v1")]
        )
        .to_pandas()
        .sort_values(keys)
        .reset_index(drop=True)
    )


@pytest.fixture
def native_source_runtime(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("pyspark")
    jars_dir = os.environ.get("TA3000_TEST_SPARK_JARS", "")
    if not jars_dir:
        yield  # Ordinary native owning factory; no synthetic runner.
        return
    from pyspark.sql import SparkSession

    jars = tuple(Path(jars_dir).glob("*.jar"))
    assert jars, "offline native Spark/Delta proof requires installed jars"

    def factory(app_name: str, master: str):
        return (
            SparkSession.builder.master(master)
            .appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.jars", ",".join(str(path) for path in jars))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.driver.memory", os.environ.get("TA3000_SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.executor.memory", os.environ.get("TA3000_SPARK_EXECUTOR_MEMORY", "4g"))
            .getOrCreate()
        )

    monkeypatch.setattr(source_job, "_create_spark_session", factory)
    yield


def test_real_native_source_contract_repair_preserves_values_before_narrow_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    native_source_runtime,
) -> None:
    dataset_dir, indicator_dir = _seed_l0_l1(tmp_path)
    derived_dir = tmp_path / "derived"
    l1_path = indicator_dir / "research_indicator_frames.delta"
    l1_pin = _input_pin(l1_path)
    profile = build_derived_indicator_profile_registry().get("core_v1")
    current_columns = owner._source_indicator_columns_for_profile(profile)
    old_columns = (*current_columns, "adx_14")
    kwargs = dict(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="mtf-fixture-v1",
        indicator_set_version="l1-v1",
        spark_master="local[1]",
    )
    # Reproduce the historical defect through the real Spark writer and L2 owner.
    with monkeypatch.context() as legacy:
        legacy.setattr(owner, "_source_indicator_columns_for_profile", lambda _: old_columns)
        owner.materialize_derived_indicator_frames(**kwargs)
    l2_path = derived_dir / "research_derived_indicator_frames.delta"
    mtf_columns = tuple(column for column in profile.output_columns if column.startswith("mtf_"))
    other_columns = tuple(column for column in profile.output_columns if column not in mtf_columns)
    assert not mtf_columns and len(other_columns) == len(profile.output_columns)
    before = _read_values(l2_path, profile.output_columns)
    assert not any(column.startswith("mtf_") for column in before.columns)
    assert _input_pin(l1_path) == l1_pin

    narrow = dict(
        timeframes=("15m",),
        dataset_instrument_ids=("BR", "Si"),
        refresh_windows=(
            {
                "instrument_id": "BR",
                "timeframe": "15m",
                "start_ts": "2026-03-02T16:30:00Z",
                "end_ts": "2026-03-02T16:45:00Z",
            },
        ),
    )
    report = owner.materialize_derived_indicator_frames(**kwargs, **narrow)
    source_report = report["source_frame_report"]
    assert source_report["source_projection_contract_changed"]
    assert source_report["requested_refresh_window_count"] == 1
    assert source_report["refresh_window_count"] == 0
    assert source_report["timeframes"] == ()
    assert source_report["dataset_instrument_ids"] == ("BR", "Si")
    assert source_report["row_counts"]["joined_row_count"] == 256
    assert report["recomputed_partition_count"] == 8
    assert report["reused_partition_count"] == 0
    assert report["extended_partition_count"] == 0
    assert _input_pin(l1_path) == l1_pin

    after = _read_values(l2_path, profile.output_columns)
    pd.testing.assert_frame_equal(
        before[["instrument_id", "contract_id", "timeframe", "ts", *other_columns]],
        after[["instrument_id", "contract_id", "timeframe", "ts", *other_columns]],
        check_exact=True,
        check_dtype=False,
    )
    compatible = owner.materialize_derived_indicator_frames(**kwargs, **narrow)
    assert not compatible["source_projection_contract_changed"]
    assert compatible["source_frame_report"]["mode"] == "exact_snapshot_reuse"
    assert compatible["reused_partition_count"] == 2
    pd.testing.assert_frame_equal(
        after, _read_values(l2_path, profile.output_columns), check_exact=True
    )
    assert _input_pin(l1_path) == l1_pin


def _runtime_fixture(root: Path) -> tuple[dict[str, object], Path, Path, object]:
    dataset_dir, indicator_dir = _seed_l0_l1(root)
    derived_dir = root / "derived"
    kwargs = dict(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="mtf-fixture-v1",
        indicator_set_version="l1-v1",
        spark_master="local[1]",
    )
    profile = build_derived_indicator_profile_registry().get("core_v1")
    return kwargs, indicator_dir / "research_indicator_frames.delta", derived_dir, profile


def _narrow_15m() -> dict[str, object]:
    return dict(
        timeframes=("15m",),
        dataset_instrument_ids=("BR", "Si"),
        refresh_windows=(
            {
                "instrument_id": "BR",
                "timeframe": "15m",
                "start_ts": "2026-03-02T16:30:00Z",
                "end_ts": "2026-03-02T16:45:00Z",
            },
        ),
    )


def _revise_fixture_adx(path: Path, *, timeframe: str, ts: str, value: float) -> None:
    # Simulate an upstream input revision; the materialization owner never computes/writes L1.
    rows = read_delta_table_arrow(
        path, filters=[("dataset_version", "=", "mtf-fixture-v1")]
    ).to_pylist()
    matches = [
        row
        for row in rows
        if row["instrument_id"] == "BR"
        and row["timeframe"] == timeframe
        and pd.Timestamp(row["ts"]) == pd.Timestamp(ts)
    ]
    assert len(matches) == 1
    matches[0]["ema_20"] = value
    profile = build_derived_indicator_profile_registry().get("core_v1")
    columns = {
        **indicator_store_contract()["research_indicator_frames"]["columns"],
        **{column: "double" for column in owner._source_indicator_columns_for_profile(profile)},
    }
    write_delta_table_rows(table_path=path, rows=rows, columns=columns)


def test_source_commit_l2_failure_narrow_retry_repairs_full_family(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    native_source_runtime,
) -> None:
    kwargs, l1_path, derived_dir, profile = _runtime_fixture(tmp_path)
    l1_pin = _input_pin(l1_path)
    old_columns = (*owner._source_indicator_columns_for_profile(profile), "adx_14")
    with monkeypatch.context() as legacy:
        legacy.setattr(owner, "_source_indicator_columns_for_profile", lambda _: old_columns)
        legacy.setattr(
            owner,
            "_derived_compute_contract_hash",
            lambda *_args, **_kwargs: "previous-native-contract",
        )
        owner.materialize_derived_indicator_frames(**kwargs)
    l2_path = derived_dir / "research_derived_indicator_frames.delta"
    l2_pin = _input_pin(l2_path)
    other = tuple(column for column in profile.output_columns if not column.startswith("mtf_"))
    before_other = _read_values(l2_path, other)
    from trading_advisor_3000.product_plane.data_plane import delta_runtime

    real_append = delta_runtime.append_delta_table_rows
    real_l2_writer = owner.write_derived_indicator_frame_batches
    with monkeypatch.context() as failure:

        def fail_after_first_committed_append(**write_args):
            real_append(**write_args)
            if write_args["table_path"] == l2_path:
                raise RuntimeError("injected after committed L2 append")

        def bounded_actual_l2_writer(**write_args):
            batches = write_args.pop("row_batches")

            def split_batches():
                for batch in batches:
                    yield batch[:16]
                    yield batch[16:]

            return real_l2_writer(
                **write_args, row_batches=split_batches(), max_rows_per_delta_write=16
            )

        failure.setattr(delta_runtime, "append_delta_table_rows", fail_after_first_committed_append)
        failure.setattr(owner, "write_derived_indicator_frame_batches", bounded_actual_l2_writer)
        with pytest.raises(RuntimeError, match="injected after committed L2 append"):
            owner.materialize_derived_indicator_frames(**kwargs, **_narrow_15m())
    assert _input_pin(l2_path)[1] > l2_pin[1]
    partial = _read_values(l2_path, ("row_count", "source_indicator_columns_hash"))
    assert len(partial) == 256 - 32 + 16
    assert partial["row_count"].eq(32).all()
    assert (
        partial["source_indicator_columns_hash"]
        .eq(
            owner.derived_source_indicator_columns_hash(
                owner._source_indicator_columns_for_profile(profile)
            )
        )
        .sum()
        == 16
    )
    retry = owner.materialize_derived_indicator_frames(**kwargs, **_narrow_15m())
    assert not retry["source_projection_contract_changed"]
    assert retry["derived_projection_contract_changed"]
    assert retry["derived_partition_coverage_changed"]
    assert retry["source_frame_report"]["row_counts"]["joined_row_count"] == 2
    assert retry["recomputed_partition_count"] == 8
    assert retry["reused_partition_count"] == retry["extended_partition_count"] == 0
    pd.testing.assert_frame_equal(before_other, _read_values(l2_path, other), check_exact=True)
    after = _read_values(
        l2_path, ("distance_to_ema_20_atr", "source_indicator_columns_hash", "source_inputs_hash")
    )
    assert after["source_indicator_columns_hash"].notna().all()
    assert after["source_inputs_hash"].str.len().eq(64).all()
    assert _input_pin(l1_path) == l1_pin
    compatible = owner.materialize_derived_indicator_frames(**kwargs, **_narrow_15m())
    assert not compatible["derived_projection_contract_changed"]
    assert compatible["reused_partition_count"] == 2
    assert compatible["refreshed_partition_count"] == 0


def test_changed_native_15m_recalculation_keeps_other_timeframes_unchanged(
    tmp_path: Path,
    native_source_runtime,
) -> None:
    kwargs, l1_path, derived_dir, profile = _runtime_fixture(tmp_path)
    owner.materialize_derived_indicator_frames(**kwargs)
    l2_path = derived_dir / "research_derived_indicator_frames.delta"
    before = _read_values(l2_path, profile.output_columns)
    _revise_fixture_adx(l1_path, timeframe="15m", ts="2026-03-02T16:30:00Z", value=123.0)
    revised_l1_pin = _input_pin(l1_path)
    report = owner.materialize_derived_indicator_frames(**kwargs, **_narrow_15m())
    assert not report["source_projection_contract_changed"]
    assert not report["derived_projection_contract_changed"]
    assert report["recomputed_partition_count"] == 1
    assert report["reused_partition_count"] == 1
    assert report["extended_partition_count"] == 0
    assert report["derived_indicator_row_count"] == 64
    after = _read_values(l2_path, profile.output_columns)
    pd.testing.assert_frame_equal(
        before.query("timeframe != '15m'"), after.query("timeframe != '15m'"), check_exact=True
    )
    assert not before.query("timeframe == '15m'").equals(after.query("timeframe == '15m'"))
    assert _input_pin(l1_path) == revised_l1_pin


def test_higher_tf_change_does_not_invalidate_native_lower_timeframe(
    tmp_path: Path,
    native_source_runtime,
) -> None:
    kwargs, l1_path, derived_dir, profile = _runtime_fixture(tmp_path)
    owner.materialize_derived_indicator_frames(**kwargs)
    l2_path = derived_dir / "research_derived_indicator_frames.delta"
    before = _read_values(l2_path, profile.output_columns)
    lower_l1_before = _read_values(l1_path, ("adx_14",)).query("timeframe == '15m'")
    _revise_fixture_adx(l1_path, timeframe="1h", ts="2026-03-02T09:00:00Z", value=22.0)
    revised_l1_pin = _input_pin(l1_path)
    report = owner.materialize_derived_indicator_frames(
        **kwargs,
        dataset_instrument_ids=("BR", "Si"),
        timeframes=("1h",),
        refresh_windows=(
            {
                "instrument_id": "BR",
                "timeframe": "1h",
                "start_ts": "2026-03-02T09:00:00Z",
                "end_ts": "2026-03-02T09:00:00Z",
            },
        ),
    )
    assert not report["source_projection_contract_changed"]
    assert not report["derived_projection_contract_changed"]
    assert report["source_frame_report"]["row_counts"]["joined_row_count"] == 1
    assert report["recomputed_partition_count"] == 1
    assert report["reused_partition_count"] == 1
    assert report["extended_partition_count"] == 0
    pd.testing.assert_frame_equal(
        lower_l1_before,
        _read_values(l1_path, ("adx_14",)).query("timeframe == '15m'"),
        check_exact=True,
    )
    after = _read_values(l2_path, profile.output_columns)
    pd.testing.assert_frame_equal(
        before.query("timeframe == '15m'"), after.query("timeframe == '15m'"), check_exact=True
    )
    pd.testing.assert_frame_equal(
        before.query("instrument_id == 'Si'"),
        after.query("instrument_id == 'Si'"),
        check_exact=True,
    )
    lower = owner.materialize_derived_indicator_frames(**kwargs, **_narrow_15m())
    assert lower["recomputed_partition_count"] == 0
    assert lower["reused_partition_count"] == 2
    assert _input_pin(l1_path) == revised_l1_pin
