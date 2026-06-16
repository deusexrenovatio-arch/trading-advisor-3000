from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.datasets import (
    research_dataset_store_contract,
)
from trading_advisor_3000.spark_jobs import research_derived_source_frames_job as job
from trading_advisor_3000.spark_jobs.research_derived_source_frames_job import (
    run_research_derived_source_frames_spark_job,
)


def _write_empty_l0_l1_tables(
    root: Path, *, indicator_columns: dict[str, str]
) -> tuple[Path, Path]:
    bar_path = root / "research_bar_views.delta"
    indicator_path = root / "research_indicator_frames.delta"
    write_delta_table_rows(
        table_path=bar_path,
        rows=[],
        columns=research_dataset_store_contract()["research_bar_views"]["columns"],
    )
    write_delta_table_rows(
        table_path=indicator_path,
        rows=[],
        columns={
            "dataset_version": "string",
            "contour_id": "string",
            "series_mode": "string",
            "series_id": "string",
            "indicator_set_version": "string",
            "profile_version": "string",
            "contract_id": "string",
            "instrument_id": "string",
            "timeframe": "string",
            "ts": "timestamp",
            **indicator_columns,
            "source_bars_hash": "string",
            "source_dataset_bars_hash": "string",
            "row_count": "int",
            "warmup_span": "int",
            "null_warmup_span": "int",
            "created_at": "timestamp",
            "output_columns_hash": "string",
        },
    )
    return bar_path, indicator_path


def test_source_frame_job_rejects_missing_required_indicator_columns_before_spark(
    tmp_path: Path,
) -> None:
    bar_path, indicator_path = _write_empty_l0_l1_tables(
        tmp_path, indicator_columns={"atr_14": "double"}
    )

    with pytest.raises(ValueError, match="requires source indicator columns: rsi_14"):
        run_research_derived_source_frames_spark_job(
            bar_views_path=bar_path,
            indicator_frames_path=indicator_path,
            output_dir=tmp_path,
            dataset_version="dataset-v1",
            contour_id="native_tradable",
            indicator_set_version="indicators-v1",
            derived_profile_version="core_v1",
            source_indicator_columns=("atr_14", "rsi_14"),
            spark_session_factory=lambda _app_name, _master: pytest.fail(
                "Spark should not start when required L1 columns are missing"
            ),
        )


def test_source_frame_job_fails_closed_when_spark_runtime_is_unavailable(
    tmp_path: Path,
) -> None:
    bar_path, indicator_path = _write_empty_l0_l1_tables(
        tmp_path, indicator_columns={"atr_14": "double", "rsi_14": "double"}
    )

    def _missing_spark(_app_name: str, _master: str) -> object:
        raise RuntimeError("spark unavailable")

    with pytest.raises(RuntimeError, match="spark unavailable"):
        run_research_derived_source_frames_spark_job(
            bar_views_path=bar_path,
            indicator_frames_path=indicator_path,
            output_dir=tmp_path,
            dataset_version="dataset-v1",
            contour_id="native_tradable",
            indicator_set_version="indicators-v1",
            derived_profile_version="core_v1",
            source_indicator_columns=("atr_14", "rsi_14"),
            spark_session_factory=_missing_spark,
        )


def test_windowed_source_frame_refresh_requires_existing_output_before_spark(
    tmp_path: Path,
) -> None:
    bar_path, indicator_path = _write_empty_l0_l1_tables(
        tmp_path, indicator_columns={"atr_14": "double"}
    )

    with pytest.raises(RuntimeError, match="windowed derived source-frame refresh requires"):
        run_research_derived_source_frames_spark_job(
            bar_views_path=bar_path,
            indicator_frames_path=indicator_path,
            output_dir=tmp_path / "derived",
            dataset_version="dataset-v1",
            contour_id="pit_active_front",
            indicator_set_version="indicators-v1",
            derived_profile_version="core_v1",
            source_indicator_columns=("atr_14",),
            refresh_windows=[
                {
                    "instrument_id": "BR",
                    "timeframe": "15m",
                    "start_ts": "2026-06-10T10:00:00Z",
                    "end_ts": "2026-06-10T10:15:00Z",
                }
            ],
            spark_session_factory=lambda _app_name, _master: pytest.fail(
                "Spark should not start for incomplete windowed source-frame refresh"
            ),
        )


def test_source_frame_scope_filter_groups_keep_per_window_time_bounds() -> None:
    refresh_windows = job._normalize_refresh_windows(  # type: ignore[attr-defined]
        [
            {
                "instrument_id": "BR",
                "timeframe": "15m",
                "start_ts": "2026-06-10T10:00:00Z",
                "end_ts": "2026-06-10T10:15:00Z",
            },
            {
                "instrument_id": "Si",
                "timeframe": "1h",
                "start_ts": "2026-06-11T11:00:00Z",
                "end_ts": "2026-06-11T12:00:00Z",
            },
        ]
    )

    groups = job._scope_filter_groups(  # type: ignore[attr-defined]
        dataset_version="dataset-v1",
        contour_id="pit_active_front",
        indicator_set_version="indicators-v1",
        timeframes=("15m", "1h"),
        dataset_instrument_ids=("BR", "Si"),
        refresh_windows=refresh_windows,
    )
    condition = job._scoped_delete_disjunction(groups)  # type: ignore[attr-defined]

    assert groups == [
        [
            ("dataset_version", "=", "dataset-v1"),
            ("contour_id", "=", "pit_active_front"),
            ("indicator_set_version", "=", "indicators-v1"),
            ("timeframe", "in", ("15m",)),
            ("instrument_id", "in", ("BR",)),
            ("ts", ">=", "2026-06-10T10:00:00Z"),
            ("ts", "<=", "2026-06-10T10:15:00Z"),
        ],
        [
            ("dataset_version", "=", "dataset-v1"),
            ("contour_id", "=", "pit_active_front"),
            ("indicator_set_version", "=", "indicators-v1"),
            ("timeframe", "in", ("1h",)),
            ("instrument_id", "in", ("Si",)),
            ("ts", ">=", "2026-06-11T11:00:00Z"),
            ("ts", "<=", "2026-06-11T12:00:00Z"),
        ],
    ]
    assert " OR " in condition
    assert "ts >= '2026-06-10T10:00:00Z'" in condition
    assert "ts <= '2026-06-11T12:00:00Z'" in condition


def test_source_frame_writer_does_not_empty_append_before_scoped_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[str, object]] = []

    class FakeWriter:
        def format(self, value: str) -> "FakeWriter":
            calls.append(("format", value))
            return self

        def mode(self, value: str) -> "FakeWriter":
            calls.append(("mode", value))
            return self

        def option(self, key: str, value: object) -> "FakeWriter":
            calls.append(("option", (key, value)))
            return self

        def save(self, path: str) -> None:
            calls.append(("save", path))

    class FakeDataFrame:
        sparkSession = object()

        @property
        def write(self) -> FakeWriter:
            return FakeWriter()

        def limit(self, _row_count: int) -> "FakeDataFrame":
            raise AssertionError("source-frame writer must not append an empty Delta batch")

    class FakeDeltaTable:
        def delete(self, condition: str) -> None:
            calls.append(("delete", condition))

    class FakeDeltaTableFactory:
        @staticmethod
        def forPath(_spark_session: object, path: str) -> FakeDeltaTable:
            calls.append(("forPath", path))
            return FakeDeltaTable()

    delta_module = types.ModuleType("delta")
    delta_tables_module = types.ModuleType("delta.tables")
    delta_tables_module.DeltaTable = FakeDeltaTableFactory
    monkeypatch.setitem(sys.modules, "delta", delta_module)
    monkeypatch.setitem(sys.modules, "delta.tables", delta_tables_module)
    monkeypatch.setattr(job, "has_delta_log", lambda _path: True)
    monkeypatch.setattr(job, "_cast_to_source_contract", lambda *_args, **_kwargs: FakeDataFrame())

    job._write_source_frame_table(  # type: ignore[attr-defined]
        dataframe=types.SimpleNamespace(sparkSession=object()),
        table_path=tmp_path / "research_derived_source_frames.delta",
        replace_scope=[
            ("dataset_version", "=", "dataset-v1"),
            ("contour_id", "=", "pit_active_front"),
            ("indicator_set_version", "=", "indicators-v1"),
            ("instrument_id", "in", ("BR",)),
            ("timeframe", "in", ("15m",)),
            ("ts", ">=", "2026-06-10T10:00:00Z"),
        ],
        source_indicator_columns=("atr_14",),
    )

    expected_delete = (
        "dataset_version = 'dataset-v1' AND contour_id = 'pit_active_front' "
        "AND indicator_set_version = 'indicators-v1' AND instrument_id IN ('BR') "
        "AND timeframe IN ('15m') AND ts >= '2026-06-10T10:00:00Z'"
    )
    assert ("delete", expected_delete) in calls
    assert [call[0] for call in calls].count("save") == 1
