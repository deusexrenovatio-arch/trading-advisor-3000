from __future__ import annotations

import hashlib
import json
import math
from dataclasses import replace
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import (
    RollMapEntry,
    SessionCalendarEntry,
)
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    delta_equals_predicate,
    delta_table_columns,
    read_delta_table_rows,
    replace_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.bar_usage_policy import (
    INDICATOR_USAGE_POLICY_ID,
)
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    ResearchBarView,
    ResearchDatasetManifest,
    research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.datasets import (
    materialize_research_dataset as _materialize_research_dataset_manifest,
)
from trading_advisor_3000.product_plane.research.datasets.bar_usage import BAR_USAGE_POLICY_ID
from trading_advisor_3000.product_plane.research.derived_indicators import (
    DerivedIndicatorProfile,
    current_derived_indicator_profile,
    research_derived_indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.derived_indicators import (
    materialize as derived_materialize_module,
)
from trading_advisor_3000.product_plane.research.derived_indicators.materialize import (
    materialize_derived_indicator_frames,
    reload_derived_indicator_frames,
)
from trading_advisor_3000.product_plane.research.derived_indicators.source_frames import (
    DERIVED_SOURCE_FRAME_DELTA,
    DERIVED_SOURCE_FRAME_TABLE,
    research_derived_source_frame_store_contract,
)
from trading_advisor_3000.product_plane.research.indicators import (
    IndicatorParameter,
    IndicatorProfile,
    IndicatorSpec,
    default_indicator_profile,
    indicator_store_contract,
    materialize_indicator_frames,
)
from trading_advisor_3000.product_plane.research.indicators.materialize import (
    reload_indicator_frames,
)


def _canonical_bar(
    *, contract_id: str = "BR-6.26", instrument_id: str = "BR", index: int, close: float
) -> CanonicalBar:
    day = 1 + (index // 48)
    hour = 9 + ((index % 48) // 4)
    minute = (index % 4) * 15
    ts = f"2026-03-{day:02d}T{hour:02d}:{minute:02d}:00Z"
    return CanonicalBar.from_dict(
        {
            "contract_id": contract_id,
            "instrument_id": instrument_id,
            "timeframe": "15m",
            "ts": ts,
            "open": close - 0.2,
            "high": close + 0.4,
            "low": close - 0.5,
            "close": close,
            "volume": 1000 + index * 5,
            "open_interest": 20000 + index,
        }
    )


def materialize_research_dataset(
    *,
    manifest_seed: ResearchDatasetManifest,
    bars: list[CanonicalBar],
    session_calendar: list[SessionCalendarEntry],
    roll_map: list[RollMapEntry],
    output_dir: Path,
) -> dict[str, object]:
    contract = research_dataset_store_contract()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = manifest_seed
    calendar_by_key = {
        (row.instrument_id, row.timeframe, row.session_date): row for row in session_calendar
    }
    active_by_key = {
        (row.instrument_id, row.session_date): row.active_contract_id for row in roll_map
    }
    sorted_bars = sorted(
        bars,
        key=lambda row: (
            row.instrument_id,
            row.contract_id,
            str(row.timeframe.value),
            row.ts,
        ),
    )
    previous_close_by_series: dict[tuple[str, str], float] = {}
    index_by_series: dict[tuple[str, str], int] = {}
    view_rows: list[dict[str, object]] = []
    for bar in sorted_bars:
        timeframe = str(bar.timeframe.value)
        session_date = bar.ts[:10]
        calendar = calendar_by_key[(bar.instrument_id, timeframe, session_date)]
        active_contract_id = active_by_key.get((bar.instrument_id, session_date), bar.contract_id)
        series_id = (
            bar.instrument_id if manifest.series_mode == "continuous_front" else bar.contract_id
        )
        series_key = (series_id, timeframe)
        previous_close = previous_close_by_series.get(series_key)
        ret_1 = None if previous_close in {None, 0.0} else (bar.close / previous_close) - 1.0
        log_ret_1 = None if previous_close in {None, 0.0} else math.log(bar.close / previous_close)
        bar_index = index_by_series.get(series_key, 0)
        view = ResearchBarView(
            dataset_version=manifest.dataset_version,
            contour_id=manifest.contour_id,
            contract_id=bar.contract_id,
            instrument_id=bar.instrument_id,
            timeframe=timeframe,
            ts=bar.ts,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            open_interest=bar.open_interest,
            session_date=session_date,
            session_open_ts=calendar.session_open_ts,
            session_close_ts=calendar.session_close_ts,
            active_contract_id=active_contract_id,
            ret_1=ret_1,
            log_ret_1=log_ret_1,
            true_range=max(
                bar.high - bar.low,
                abs(bar.high - (previous_close or bar.close)),
                abs(bar.low - (previous_close or bar.close)),
            ),
            hl_range=bar.high - bar.low,
            oc_range=bar.close - bar.open,
            bar_index=bar_index,
            slice_role="train",
            series_id=series_id,
            series_mode=manifest.series_mode,
            price_space="continuous" if manifest.series_mode == "continuous_front" else "native",
        )
        view_rows.append(view.to_dict())
        previous_close_by_series[series_key] = bar.close
        index_by_series[series_key] = bar_index + 1

    bars_hash = (
        hashlib.sha256(
            json.dumps(view_rows, sort_keys=True, separators=(",", ":"), default=str).encode(
                "utf-8"
            )
        )
        .hexdigest()[:16]
        .upper()
    )
    split_params = manifest.split_params
    if not split_params and manifest.split_method == "full" and view_rows:
        split_params = {
            "windows": [
                {
                    "window_id": "full-01",
                    "train_start_ts": min(str(row["ts"]) for row in view_rows),
                    "train_end_ts": max(str(row["ts"]) for row in view_rows),
                    "test_start_ts": None,
                    "test_end_ts": None,
                }
            ]
        }
    manifest = replace(manifest, bars_hash=bars_hash, split_params=split_params)

    grouped: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in view_rows:
        grouped.setdefault((str(row["instrument_id"]), str(row["timeframe"])), []).append(row)
    tree_rows = [
        {
            "dataset_version": manifest.dataset_version,
            "contour_id": manifest.contour_id,
            "universe_id": manifest.universe_id,
            "asset_class": "futures",
            "asset_group": instrument_id,
            "internal_id": instrument_id,
            "instrument_id": instrument_id,
            "source_instrument_id": instrument_id,
            "contract_ids_json": sorted({str(row["contract_id"]) for row in rows}),
            "active_contract_ids_json": sorted({str(row["active_contract_id"]) for row in rows}),
            "timeframes_json": [timeframe],
            "row_count": len(rows),
            "first_ts": min(str(row["ts"]) for row in rows),
            "last_ts": max(str(row["ts"]) for row in rows),
            "source_bars_hash": "test-fixture",
            "lineage_key": manifest.lineage_key(),
            "created_at": "2026-04-29T00:00:00Z",
        }
        for (instrument_id, timeframe), rows in sorted(grouped.items())
    ]
    predicate = delta_equals_predicate(
        {"dataset_version": manifest.dataset_version, "contour_id": manifest.contour_id}
    )
    replace_delta_table_rows(
        table_path=output_dir / "research_bar_views.delta",
        rows=view_rows,
        columns=contract["research_bar_views"]["columns"],
        predicate=predicate,
    )
    replace_delta_table_rows(
        table_path=output_dir / "research_instrument_tree.delta",
        rows=tree_rows,
        columns=contract["research_instrument_tree"]["columns"],
        predicate=predicate,
    )
    return _materialize_research_dataset_manifest(
        manifest_seed=manifest,
        output_dir=output_dir,
        bar_view_count=len(view_rows),
        instrument_tree_count=len(tree_rows),
        output_paths={
            "research_bar_views": (output_dir / "research_bar_views.delta").as_posix(),
            "research_instrument_tree": (output_dir / "research_instrument_tree.delta").as_posix(),
            "research_datasets": (output_dir / "research_datasets.delta").as_posix(),
        },
    )


def _write_test_derived_source_frames(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    source_frame_output_dir: Path,
    dataset_version: str,
    contour_id: str,
    indicator_set_version: str,
    derived_profile_version: str,
    source_indicator_columns: tuple[str, ...],
) -> dict[str, object]:
    missing_columns = tuple(
        column
        for column in source_indicator_columns
        if column
        not in set(delta_table_columns(indicator_output_dir / "research_indicator_frames.delta"))
    )
    if missing_columns:
        raise ValueError(
            "derived source-frame requires source indicator columns: " + ", ".join(missing_columns)
        )
    bar_rows = read_delta_table_rows(
        dataset_output_dir / "research_bar_views.delta",
        filters=[("dataset_version", "=", dataset_version), ("contour_id", "=", contour_id)],
    )
    indicator_rows = read_delta_table_rows(
        indicator_output_dir / "research_indicator_frames.delta",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", contour_id),
            ("indicator_set_version", "=", indicator_set_version),
        ],
    )
    join_keys = (
        "dataset_version",
        "contour_id",
        "series_mode",
        "series_id",
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
    )
    indicators_by_key: dict[tuple[object, ...], dict[str, object]] = {}
    duplicate_indicator_keys: set[tuple[object, ...]] = set()
    for row in indicator_rows:
        key = tuple(row.get(column) for column in join_keys)
        if key in indicators_by_key:
            duplicate_indicator_keys.add(key)
            continue
        indicators_by_key[key] = row
    assert not duplicate_indicator_keys

    source_rows = []
    joined_row_count = len(bar_rows)
    for bar in bar_rows:
        indicator = indicators_by_key[tuple(bar.get(key) for key in join_keys)]
        source_rows.append(
            {
                **bar,
                "indicator_set_version": indicator_set_version,
                "derived_profile_version": derived_profile_version,
                **{column: indicator.get(column) for column in source_indicator_columns},
                "indicator_profile_version": indicator["profile_version"],
                "indicator_source_bars_hash": indicator["source_bars_hash"],
                "indicator_source_dataset_bars_hash": indicator.get("source_dataset_bars_hash", ""),
                "indicator_row_count": indicator["row_count"],
                "indicator_warmup_span": indicator["warmup_span"],
                "indicator_null_warmup_span": indicator["null_warmup_span"],
                "indicator_created_at": indicator["created_at"],
                "indicator_output_columns_hash": indicator.get("output_columns_hash", ""),
                "source_indicator_columns_hash": "TEST",
                "source_l0_delta_version": 0,
                "source_l1_delta_version": 0,
                "source_l0_delta_hash": "test-l0",
                "source_l1_delta_hash": "test-l1",
                "l0_row_count": len(bar_rows),
                "l1_row_count": len(indicator_rows),
                "joined_row_count": joined_row_count,
                "duplicate_indicator_key_count": 0,
                "missing_indicator_key_count": 0,
                "source_bars_hash": "",
                "source_indicators_hash": "",
                "source_frame_created_at": "2026-04-29T00:00:00Z",
            }
        )
    contract = research_derived_source_frame_store_contract(
        source_indicator_columns=source_indicator_columns
    )
    write_delta_table_rows(
        table_path=source_frame_output_dir / DERIVED_SOURCE_FRAME_DELTA,
        rows=source_rows,
        columns=contract[DERIVED_SOURCE_FRAME_TABLE]["columns"],
    )
    return {
        "status": "PASS",
        "rows_by_table": {DERIVED_SOURCE_FRAME_TABLE: len(source_rows)},
        "source_delta_versions": {"research_bar_views": 0, "research_indicator_frames": 0},
        "source_delta_hashes": {
            "research_bar_views": "test-l0",
            "research_indicator_frames": "test-l1",
        },
        "output_paths": {
            DERIVED_SOURCE_FRAME_TABLE: (
                source_frame_output_dir / DERIVED_SOURCE_FRAME_DELTA
            ).as_posix()
        },
        "delta_manifest": contract,
    }


@pytest.fixture(autouse=True)
def _fixture_source_frame_runner(monkeypatch: pytest.MonkeyPatch):
    def _fake_source_frame_runner(**kwargs: object) -> dict[str, object]:
        return _write_test_derived_source_frames(
            dataset_output_dir=Path(str(kwargs["bar_views_path"])).parent,
            indicator_output_dir=Path(str(kwargs["indicator_frames_path"])).parent,
            source_frame_output_dir=Path(str(kwargs["output_dir"])),
            dataset_version=str(kwargs["dataset_version"]),
            contour_id=str(kwargs["contour_id"]),
            indicator_set_version=str(kwargs["indicator_set_version"]),
            derived_profile_version=str(kwargs["derived_profile_version"]),
            source_indicator_columns=tuple(kwargs["source_indicator_columns"]),  # type: ignore[arg-type]
        )

    monkeypatch.setattr(
        derived_materialize_module,
        "run_research_derived_source_frames_spark_job",
        _fake_source_frame_runner,
        raising=False,
    )


def test_indicator_materialization_and_reload_from_dataset_layer(tmp_path: Path) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(72)]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
        SessionCalendarEntry(
            "BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"
        ),
    ]
    roll_map = [
        RollMapEntry("BR", "2026-03-01", "BR-6.26", "test"),
        RollMapEntry("BR", "2026-03-02", "BR-6.26", "test"),
    ]

    dataset_dir = tmp_path / "dataset"
    indicator_dir = tmp_path / "indicators"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-v3",
            dataset_name="indicator-ready sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-02T20:45:00Z",
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )

    report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
    )
    assert report["indicator_row_count"] == 72
    assert report["profile_version"] == "core_v1"
    assert report["bar_usage_policy_id"] == BAR_USAGE_POLICY_ID
    assert report["indicator_usage_policy_id"] == INDICATOR_USAGE_POLICY_ID
    assert report["refreshed_partition_count"] == 1
    assert (Path(str(report["output_paths"]["research_indicator_frames"])) / "_delta_log").exists()

    rows = reload_indicator_frames(
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
    )
    assert len(rows) == 72
    assert rows[-1].values["ema_20"] is not None
    assert rows[-1].values["macd_12_26_9"] is not None
    assert rows[-1].warmup_span >= 50
    assert rows[-1].profile_version == "core_v1"
    assert rows[-1].null_warmup_span >= 0


def test_indicator_materialization_recomputes_only_affected_partitions(tmp_path: Path) -> None:
    bars_v1 = [
        *[
            _canonical_bar(
                contract_id="BR-6.26", instrument_id="BR", index=index, close=80.0 + index * 0.2
            )
            for index in range(60)
        ],
        *[
            _canonical_bar(
                contract_id="Si-6.26", instrument_id="Si", index=index, close=90000.0 + index * 2
            )
            for index in range(60)
        ],
    ]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
        SessionCalendarEntry(
            "Si", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
        SessionCalendarEntry(
            "BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"
        ),
        SessionCalendarEntry(
            "Si", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"
        ),
    ]
    roll_map = [
        RollMapEntry("BR", "2026-03-01", "BR-6.26", "test"),
        RollMapEntry("Si", "2026-03-01", "Si-6.26", "test"),
        RollMapEntry("BR", "2026-03-02", "BR-6.26", "test"),
        RollMapEntry("Si", "2026-03-02", "Si-6.26", "test"),
    ]

    dataset_dir = tmp_path / "dataset-inc"
    indicator_dir = tmp_path / "indicators-inc"
    manifest = ResearchDatasetManifest(
        dataset_version="dataset-inc-v1",
        dataset_name="incremental sample",
        universe_id="moex-futures",
        timeframes=("15m",),
        base_timeframe="15m",
        start_ts="2026-03-01T09:00:00Z",
        end_ts="2026-03-02T11:45:00Z",
        warmup_bars=0,
        split_method="full",
        code_version="test",
    )
    materialize_research_dataset(
        manifest_seed=manifest,
        bars=bars_v1,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    first_report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-inc-v1",
        indicator_set_version="indicators-v1",
    )
    assert first_report["refreshed_partition_count"] == 2

    first_rows = reload_indicator_frames(
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-inc-v1",
        indicator_set_version="indicators-v1",
    )
    first_br_created_at = {
        row.ts: row.created_at for row in first_rows if row.instrument_id == "BR"
    }
    first_si_created_at = {
        row.ts: row.created_at for row in first_rows if row.instrument_id == "Si"
    }

    bars_v2 = [
        row
        if not (row.instrument_id == "BR" and row.ts == "2026-03-02T11:45:00Z")
        else CanonicalBar.from_dict(
            {
                "contract_id": row.contract_id,
                "instrument_id": row.instrument_id,
                "timeframe": row.timeframe.value,
                "ts": row.ts,
                "open": row.open,
                "high": row.high + 5.5,
                "low": row.low,
                "close": row.close + 5.0,
                "volume": row.volume + 100,
                "open_interest": row.open_interest,
            }
        )
        for row in bars_v1
    ]
    materialize_research_dataset(
        manifest_seed=manifest,
        bars=bars_v2,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    second_report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-inc-v1",
        indicator_set_version="indicators-v1",
    )
    assert second_report["refreshed_partition_count"] == 1
    assert second_report["reused_partition_count"] == 1

    second_rows = reload_indicator_frames(
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-inc-v1",
        indicator_set_version="indicators-v1",
    )
    second_br_created_at = {
        row.ts: row.created_at for row in second_rows if row.instrument_id == "BR"
    }
    second_si_created_at = {
        row.ts: row.created_at for row in second_rows if row.instrument_id == "Si"
    }
    assert first_si_created_at == second_si_created_at
    assert first_br_created_at != second_br_created_at


def test_indicator_planning_uses_delta_native_aggregates_not_python_row_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bars = [
        *[
            _canonical_bar(
                contract_id="BR-6.26",
                instrument_id="BR",
                index=index,
                close=80.0 + index * 0.2,
            )
            for index in range(60)
        ],
        *[
            _canonical_bar(
                contract_id="Si-6.26",
                instrument_id="Si",
                index=index,
                close=90000.0 + index * 2,
            )
            for index in range(60)
        ],
    ]
    session_calendar = [
        SessionCalendarEntry(
            "BR",
            "15m",
            "2026-03-01",
            "2026-03-01T09:00:00Z",
            "2026-03-01T20:45:00Z",
        ),
        SessionCalendarEntry(
            "Si",
            "15m",
            "2026-03-01",
            "2026-03-01T09:00:00Z",
            "2026-03-01T20:45:00Z",
        ),
        SessionCalendarEntry(
            "BR",
            "15m",
            "2026-03-02",
            "2026-03-02T09:00:00Z",
            "2026-03-02T20:45:00Z",
        ),
        SessionCalendarEntry(
            "Si",
            "15m",
            "2026-03-02",
            "2026-03-02T09:00:00Z",
            "2026-03-02T20:45:00Z",
        ),
    ]
    roll_map = [
        RollMapEntry("BR", "2026-03-01", "BR-6.26", "test"),
        RollMapEntry("Si", "2026-03-01", "Si-6.26", "test"),
        RollMapEntry("BR", "2026-03-02", "BR-6.26", "test"),
        RollMapEntry("Si", "2026-03-02", "Si-6.26", "test"),
    ]
    dataset_dir = tmp_path / "dataset-delta-native-planning"
    indicator_dir = tmp_path / "indicators-delta-native-planning"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-delta-native-planning-v1",
            dataset_name="delta native planning sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-01T23:45:00Z",
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )

    def fail_python_row_batch_planner(*args: object, **kwargs: object) -> object:
        raise AssertionError("indicator planning must not iterate Delta rows in Python")

    import trading_advisor_3000.product_plane.research.indicators.materialize as materialize_module
    import trading_advisor_3000.product_plane.research.indicators.store as store_module

    monkeypatch.setattr(
        materialize_module,
        "iter_delta_table_row_batches",
        fail_python_row_batch_planner,
        raising=False,
    )
    monkeypatch.setattr(
        store_module,
        "iter_delta_table_row_batches",
        fail_python_row_batch_planner,
        raising=False,
    )

    first_report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-delta-native-planning-v1",
        indicator_set_version="indicators-v1",
    )
    second_report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-delta-native-planning-v1",
        indicator_set_version="indicators-v1",
    )

    assert first_report["refreshed_partition_count"] == 2
    assert second_report["reused_partition_count"] == 2
    assert second_report["refreshed_partition_count"] == 0


def test_indicator_materialization_streams_fresh_partitions_into_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bars = [
        *[
            _canonical_bar(
                contract_id="BR-6.26",
                instrument_id="BR",
                index=index,
                close=80.0 + index * 0.2,
            )
            for index in range(60)
        ],
        *[
            _canonical_bar(
                contract_id="Si-6.26",
                instrument_id="Si",
                index=index,
                close=90000.0 + index * 2,
            )
            for index in range(60)
        ],
    ]
    session_calendar = [
        SessionCalendarEntry(
            "BR",
            "15m",
            "2026-03-01",
            "2026-03-01T09:00:00Z",
            "2026-03-01T20:45:00Z",
        ),
        SessionCalendarEntry(
            "Si",
            "15m",
            "2026-03-01",
            "2026-03-01T09:00:00Z",
            "2026-03-01T20:45:00Z",
        ),
        SessionCalendarEntry(
            "BR",
            "15m",
            "2026-03-02",
            "2026-03-02T09:00:00Z",
            "2026-03-02T20:45:00Z",
        ),
        SessionCalendarEntry(
            "Si",
            "15m",
            "2026-03-02",
            "2026-03-02T09:00:00Z",
            "2026-03-02T20:45:00Z",
        ),
    ]
    roll_map = [
        RollMapEntry("BR", "2026-03-01", "BR-6.26", "test"),
        RollMapEntry("Si", "2026-03-01", "Si-6.26", "test"),
        RollMapEntry("BR", "2026-03-02", "BR-6.26", "test"),
        RollMapEntry("Si", "2026-03-02", "Si-6.26", "test"),
    ]
    dataset_dir = tmp_path / "dataset-streaming-indicators"
    indicator_dir = tmp_path / "indicators-streaming-indicators"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-streaming-indicators-v1",
            dataset_name="streaming indicator sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-01T23:45:00Z",
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )

    import trading_advisor_3000.product_plane.research.indicators.materialize as materialize_module

    loaded_partitions: list[str] = []
    loaded_at_writer_entry: list[str] = []
    loaded_after_first_batch: list[str] = []
    real_load_partition_rows = materialize_module._load_bar_partition_rows

    def tracking_load_partition_rows(*args: object, **kwargs: object) -> object:
        partition = kwargs["partition"]
        loaded_partitions.append(partition.instrument_id)
        return real_load_partition_rows(*args, **kwargs)

    def fake_build_partition_rows(*args: object, **kwargs: object) -> list[object]:
        return [object()]

    def capture_writer(*args: object, **kwargs: object) -> tuple[dict[str, str], int, int]:
        loaded_at_writer_entry.extend(loaded_partitions)
        row_batches = iter(kwargs["row_batches"])
        first_batch = next(row_batches)
        loaded_after_first_batch.extend(loaded_partitions)
        return (
            {"research_indicator_frames": (indicator_dir / "stub.delta").as_posix()},
            len(first_batch),
            1,
        )

    monkeypatch.setattr(
        materialize_module,
        "_load_bar_partition_rows",
        tracking_load_partition_rows,
    )
    monkeypatch.setattr(
        materialize_module,
        "_build_partition_rows",
        fake_build_partition_rows,
    )
    monkeypatch.setattr(
        materialize_module,
        "write_indicator_frame_batches",
        capture_writer,
    )

    report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-streaming-indicators-v1",
        indicator_set_version="indicators-v1",
    )

    assert report["refreshed_partition_count"] == 2
    assert loaded_at_writer_entry == []
    assert loaded_after_first_batch == ["BR"]


def test_indicator_refresh_reuses_planning_series_for_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bars_v1 = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(20)]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
    ]
    roll_map = [RollMapEntry("BR", "2026-03-01", "BR-6.26", "test")]
    dataset_dir = tmp_path / "dataset-refresh-load-reuse"
    indicator_dir = tmp_path / "indicators-refresh-load-reuse"
    manifest = ResearchDatasetManifest(
        dataset_version="dataset-refresh-load-reuse-v1",
        dataset_name="refresh load reuse sample",
        universe_id="moex-futures",
        timeframes=("15m",),
        base_timeframe="15m",
        start_ts="2026-03-01T09:00:00Z",
        end_ts="2026-03-01T13:45:00Z",
        warmup_bars=0,
        split_method="full",
        code_version="test",
    )
    profile = IndicatorProfile(
        version="test_refresh_load_reuse_v1",
        description="small profile without volume profile",
        indicators=(
            IndicatorSpec(
                indicator_id="sma_3",
                category="trend",
                operation_key="sma",
                parameters=(IndicatorParameter("length", 3),),
                required_input_columns=("close",),
                output_columns=("sma_3",),
                warmup_bars=3,
            ),
        ),
    )
    materialize_research_dataset(
        manifest_seed=manifest,
        bars=bars_v1,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-refresh-load-reuse-v1",
        indicator_set_version="indicators-v1",
        profile=profile,
    )

    bars_v2 = [
        row
        if row.ts != "2026-03-01T13:45:00Z"
        else CanonicalBar.from_dict(
            {
                "contract_id": row.contract_id,
                "instrument_id": row.instrument_id,
                "timeframe": row.timeframe.value,
                "ts": row.ts,
                "open": row.open,
                "high": row.high + 1.0,
                "low": row.low,
                "close": row.close + 1.0,
                "volume": row.volume,
                "open_interest": row.open_interest,
            }
        )
        for row in bars_v1
    ]
    materialize_research_dataset(
        manifest_seed=manifest,
        bars=bars_v2,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )

    import trading_advisor_3000.product_plane.research.indicators.materialize as materialize_module

    loaded_partitions: list[str] = []
    real_load_partition_rows = materialize_module._load_bar_partition_rows

    def tracking_load_partition_rows(*args: object, **kwargs: object) -> object:
        loaded_partitions.append(kwargs["partition"].instrument_id)
        return real_load_partition_rows(*args, **kwargs)

    monkeypatch.setattr(
        materialize_module,
        "_load_bar_partition_rows",
        tracking_load_partition_rows,
    )

    report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-refresh-load-reuse-v1",
        indicator_set_version="indicators-v1",
        profile=profile,
    )

    assert report["refreshed_partition_count"] == 1
    assert loaded_partitions == ["BR"]


def test_indicator_materialization_blocks_existing_writer_lock(tmp_path: Path) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(8)]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
    ]
    roll_map = [RollMapEntry("BR", "2026-03-01", "BR-6.26", "test")]
    dataset_dir = tmp_path / "dataset-writer-lock"
    indicator_dir = tmp_path / "indicators-writer-lock"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-writer-lock-v1",
            dataset_name="writer lock sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-01T10:45:00Z",
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    indicator_dir.mkdir(parents=True)
    (indicator_dir / ".research_indicator_frames.lock.json").write_text(
        json.dumps({"holder": "other-run"}),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="active research_indicator_frames writer"):
        materialize_indicator_frames(
            dataset_output_dir=dataset_dir,
            indicator_output_dir=indicator_dir,
            dataset_version="dataset-writer-lock-v1",
            indicator_set_version="indicators-v1",
        )

    assert not (indicator_dir / "research_indicator_frames.delta" / "_delta_log").exists()


def test_continuous_front_partition_counts_span_roll_contracts(tmp_path: Path) -> None:
    import trading_advisor_3000.product_plane.research.indicators.materialize as materialize_module

    bars = [
        *[
            _canonical_bar(
                contract_id="BR-3.26",
                instrument_id="BR",
                index=index,
                close=78.0 + index * 0.1,
            )
            for index in range(4)
        ],
        *[
            _canonical_bar(
                contract_id="BR-6.26",
                instrument_id="BR",
                index=index,
                close=80.0 + index * 0.1,
            )
            for index in range(4, 10)
        ],
    ]
    session_calendar = [
        SessionCalendarEntry(
            "BR",
            "15m",
            "2026-03-01",
            "2026-03-01T09:00:00Z",
            "2026-03-01T20:45:00Z",
        ),
    ]
    roll_map = [RollMapEntry("BR", "2026-03-01", "BR-6.26", "test")]
    dataset_dir = tmp_path / "dataset-continuous-front-counts"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-continuous-front-counts-v1",
            dataset_name="continuous front partition count sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            contour_id="pit_active_front",
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-01T11:15:00Z",
            series_mode="continuous_front",
            continuous_front_policy=ContinuousFrontPolicy.from_config({}),
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )

    counts = materialize_module._load_bar_partition_counts(
        dataset_output_dir=dataset_dir,
        dataset_version="dataset-continuous-front-counts-v1",
        contour_id="pit_active_front",
        indicator_set_version="indicators-v1",
        series_mode="continuous_front",
    )

    assert list(counts.values()) == [10]
    partition = next(iter(counts))
    assert partition.contract_id is None
    assert partition.series_mode == "continuous_front"


def test_indicator_materialization_extends_profile_without_recomputing_existing_columns(
    tmp_path: Path,
) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(40)]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
    ]
    roll_map = [RollMapEntry("BR", "2026-03-01", "BR-6.26", "test")]
    dataset_dir = tmp_path / "dataset-profile-extension"
    indicator_dir = tmp_path / "indicators-profile-extension"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-profile-extension-v1",
            dataset_name="indicator profile extension sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-01T18:45:00Z",
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    base_profile = IndicatorProfile(
        version="test_profile_v1",
        description="small base profile",
        indicators=(
            IndicatorSpec(
                indicator_id="sma_10",
                category="trend",
                operation_key="sma",
                parameters=(IndicatorParameter("length", 10),),
                required_input_columns=("close",),
                output_columns=("sma_10",),
                warmup_bars=10,
            ),
        ),
    )
    extended_profile = IndicatorProfile(
        version="test_profile_v2",
        description="small extended profile",
        indicators=(
            *base_profile.indicators,
            IndicatorSpec(
                indicator_id="ema_10",
                category="trend",
                operation_key="ema",
                parameters=(IndicatorParameter("length", 10),),
                required_input_columns=("close",),
                output_columns=("ema_10",),
                warmup_bars=10,
            ),
        ),
    )

    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-profile-extension-v1",
        indicator_set_version="indicators-v1",
        profile=base_profile,
    )
    first_rows = reload_indicator_frames(
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-profile-extension-v1",
        indicator_set_version="indicators-v1",
    )
    first_created_at = {row.ts: row.created_at for row in first_rows}
    first_sma = {row.ts: row.values["sma_10"] for row in first_rows}

    report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-profile-extension-v1",
        indicator_set_version="indicators-v1",
        profile=extended_profile,
    )

    assert report["refreshed_partition_count"] == 1
    assert report["extended_partition_count"] == 1
    assert report["recomputed_partition_count"] == 0
    second_rows = reload_indicator_frames(
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-profile-extension-v1",
        indicator_set_version="indicators-v1",
    )
    assert {row.ts: row.created_at for row in second_rows} == first_created_at
    assert {row.ts: row.values["sma_10"] for row in second_rows} == first_sma
    assert second_rows[-1].values["ema_10"] is not None
    assert second_rows[-1].profile_version == "test_profile_v2"


def test_continuous_front_profile_extension_recomputes_existing_columns(
    tmp_path: Path,
) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(40)]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
    ]
    roll_map = [RollMapEntry("BR", "2026-03-01", "BR-6.26", "test")]
    dataset_dir = tmp_path / "dataset-cf-profile-extension"
    indicator_dir = tmp_path / "indicators-cf-profile-extension"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-cf-profile-extension-v1",
            dataset_name="continuous front profile extension sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            contour_id="pit_active_front",
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-01T18:45:00Z",
            series_mode="continuous_front",
            continuous_front_policy=ContinuousFrontPolicy.from_config({}),
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    base_profile = IndicatorProfile(
        version="test_cf_profile_v1",
        description="small continuous-front base profile",
        indicators=(
            IndicatorSpec(
                indicator_id="sma_10",
                category="trend",
                operation_key="sma",
                parameters=(IndicatorParameter("length", 10),),
                required_input_columns=("close",),
                output_columns=("sma_10",),
                warmup_bars=10,
            ),
        ),
    )
    extended_profile = IndicatorProfile(
        version="test_cf_profile_v2",
        description="small continuous-front extended profile",
        indicators=(
            *base_profile.indicators,
            IndicatorSpec(
                indicator_id="ema_10",
                category="trend",
                operation_key="ema",
                parameters=(IndicatorParameter("length", 10),),
                required_input_columns=("close",),
                output_columns=("ema_10",),
                warmup_bars=10,
            ),
        ),
    )

    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-cf-profile-extension-v1",
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=base_profile,
    )
    first_rows = read_delta_table_rows(
        indicator_dir / "research_indicator_frames.delta",
        filters=[
            ("dataset_version", "=", "dataset-cf-profile-extension-v1"),
            ("contour_id", "=", "pit_active_front"),
            ("indicator_set_version", "=", "indicators-v1"),
        ],
    )
    same_profile_report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-cf-profile-extension-v1",
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=base_profile,
    )
    assert same_profile_report["refreshed_partition_count"] == 0
    assert same_profile_report["reused_partition_count"] == 1
    assert same_profile_report["recomputed_partition_count"] == 0
    same_profile_rows = reload_indicator_frames(
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-cf-profile-extension-v1",
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
    )
    assert all(row.values["sma_10"] != -999.0 for row in same_profile_rows)

    corrupted_rows = [{**row, "sma_10": -999.0} for row in first_rows]
    replace_delta_table_rows(
        table_path=indicator_dir / "research_indicator_frames.delta",
        rows=corrupted_rows,
        columns=indicator_store_contract(profile=base_profile)["research_indicator_frames"][
            "columns"
        ],
        predicate=delta_equals_predicate(
            {
                "dataset_version": "dataset-cf-profile-extension-v1",
                "contour_id": "pit_active_front",
                "indicator_set_version": "indicators-v1",
            }
        ),
    )

    report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-cf-profile-extension-v1",
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=extended_profile,
    )

    assert report["refreshed_partition_count"] == 1
    assert report["extended_partition_count"] == 0
    assert report["recomputed_partition_count"] == 1
    second_rows = reload_indicator_frames(
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-cf-profile-extension-v1",
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
    )
    assert all(row.values["sma_10"] != -999.0 for row in second_rows)
    assert second_rows[-1].values["ema_10"] is not None


def test_continuous_front_full_reuse_skips_bar_partition_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trading_advisor_3000.product_plane.research.indicators.materialize as materialize_module

    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(20)]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T13:45:00Z"
        ),
    ]
    roll_map = [RollMapEntry("BR", "2026-03-01", "BR-6.26", "test")]
    dataset_dir = tmp_path / "dataset-cf-full-reuse-fast-path"
    indicator_dir = tmp_path / "indicators-cf-full-reuse-fast-path"
    profile = IndicatorProfile(
        version="test_cf_full_reuse_v1",
        description="small continuous-front full reuse profile",
        indicators=(
            IndicatorSpec(
                indicator_id="sma_3",
                category="trend",
                operation_key="sma",
                parameters=(IndicatorParameter("length", 3),),
                required_input_columns=("close",),
                output_columns=("sma_3",),
                warmup_bars=3,
            ),
        ),
    )
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-cf-full-reuse-fast-path-v1",
            dataset_name="continuous front full reuse fast path sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            contour_id="pit_active_front",
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-01T13:45:00Z",
            series_mode="continuous_front",
            continuous_front_policy=ContinuousFrontPolicy.from_config({}),
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-cf-full-reuse-fast-path-v1",
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=profile,
    )

    monkeypatch.setattr(
        materialize_module,
        "_load_bar_partition_counts",
        lambda **_kwargs: pytest.fail("full reuse must not scan research_bar_views"),
    )
    monkeypatch.setattr(
        materialize_module,
        "_load_bar_partition_rows",
        lambda **_kwargs: pytest.fail("full reuse must not load bar partition rows"),
    )

    report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-cf-full-reuse-fast-path-v1",
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=profile,
    )

    assert report["refreshed_partition_count"] == 0
    assert report["reused_partition_count"] == 1
    assert report["recomputed_partition_count"] == 0
    assert report["write_batch_count"] == 0


def test_indicator_reuse_ignores_stale_partitions_outside_dataset_manifest_timeframes(
    tmp_path: Path,
) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(20)]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T13:45:00Z"
        ),
    ]
    roll_map = [RollMapEntry("BR", "2026-03-01", "BR-6.26", "test")]
    dataset_dir = tmp_path / "dataset-cf-stale-timeframe"
    indicator_dir = tmp_path / "indicators-cf-stale-timeframe"
    dataset_version = "dataset-cf-stale-timeframe-v1"
    profile = IndicatorProfile(
        version="test_cf_stale_scope_v1",
        description="small continuous-front stale scope profile",
        indicators=(
            IndicatorSpec(
                indicator_id="sma_3",
                category="trend",
                operation_key="sma",
                parameters=(IndicatorParameter("length", 3),),
                required_input_columns=("close",),
                output_columns=("sma_3",),
                warmup_bars=3,
            ),
        ),
    )
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version=dataset_version,
            dataset_name="continuous front stale timeframe sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            contour_id="pit_active_front",
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-01T13:45:00Z",
            series_mode="continuous_front",
            continuous_front_policy=ContinuousFrontPolicy.from_config({}),
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version=dataset_version,
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=profile,
    )

    bar_contract = research_dataset_store_contract()["research_bar_views"]["columns"]
    stale_bar = read_delta_table_rows(
        dataset_dir / "research_bar_views.delta",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", "pit_active_front"),
            ("timeframe", "=", "15m"),
        ],
    )[0]
    stale_bar.update(
        {
            "timeframe": "1d",
            "ts": "2026-03-09T00:00:00Z",
            "session_date": "2026-03-09",
            "session_close_ts": None,
            "bar_start_ts": "2026-03-09T00:00:00Z",
            "bar_end_ts": "2026-03-09T23:59:00Z",
            "bar_index": 0,
        }
    )
    write_delta_table_rows(
        table_path=dataset_dir / "research_bar_views.delta",
        rows=[stale_bar],
        columns=bar_contract,
        mode="append",
    )

    indicator_contract = indicator_store_contract(profile=profile)["research_indicator_frames"][
        "columns"
    ]
    stale_indicator = read_delta_table_rows(
        indicator_dir / "research_indicator_frames.delta",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", "pit_active_front"),
            ("indicator_set_version", "=", "indicators-v1"),
            ("timeframe", "=", "15m"),
        ],
    )[0]
    stale_indicator.update(
        {
            "timeframe": "1d",
            "ts": "2026-03-09T00:00:00Z",
            "source_dataset_bars_hash": "STALE_DATASET_HASH",
            "source_bars_hash": "STALE_PARTITION_HASH",
            "row_count": 1,
        }
    )
    write_delta_table_rows(
        table_path=indicator_dir / "research_indicator_frames.delta",
        rows=[stale_indicator],
        columns=indicator_contract,
        mode="append",
    )

    report = materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version=dataset_version,
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=profile,
    )

    assert report["reuse_plan_mode"] == "metadata_fast_path"
    assert report["refreshed_partition_count"] == 0
    assert report["reused_partition_count"] == 1
    assert report["write_batch_count"] == 0


def test_derived_indicator_materialization_recomputes_only_affected_partitions(
    tmp_path: Path,
) -> None:
    bars_v1 = [
        *[
            _canonical_bar(
                contract_id="BR-6.26", instrument_id="BR", index=index, close=80.0 + index * 0.2
            )
            for index in range(60)
        ],
        *[
            _canonical_bar(
                contract_id="Si-6.26", instrument_id="Si", index=index, close=90000.0 + index * 2
            )
            for index in range(60)
        ],
    ]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
        SessionCalendarEntry(
            "Si", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
        SessionCalendarEntry(
            "BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"
        ),
        SessionCalendarEntry(
            "Si", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"
        ),
    ]
    roll_map = [
        RollMapEntry("BR", "2026-03-01", "BR-6.26", "test"),
        RollMapEntry("Si", "2026-03-01", "Si-6.26", "test"),
        RollMapEntry("BR", "2026-03-02", "BR-6.26", "test"),
        RollMapEntry("Si", "2026-03-02", "Si-6.26", "test"),
    ]
    dataset_dir = tmp_path / "dataset-derived-inc"
    indicator_dir = tmp_path / "indicators-derived-inc"
    derived_dir = tmp_path / "derived-inc"
    manifest = ResearchDatasetManifest(
        dataset_version="dataset-derived-inc-v1",
        dataset_name="derived incremental sample",
        universe_id="moex-futures",
        timeframes=("15m",),
        base_timeframe="15m",
        start_ts="2026-03-01T09:00:00Z",
        end_ts="2026-03-02T11:45:00Z",
        warmup_bars=0,
        split_method="full",
        code_version="test",
    )
    materialize_research_dataset(
        manifest_seed=manifest,
        bars=bars_v1,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-derived-inc-v1",
        indicator_set_version="indicators-v1",
    )
    first_report = materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-inc-v1",
        indicator_set_version="indicators-v1",
    )
    assert first_report["refreshed_partition_count"] == 2
    assert first_report["reused_partition_count"] == 0
    assert first_report["bar_usage_policy_id"] == BAR_USAGE_POLICY_ID
    assert first_report["indicator_usage_policy_id"] == INDICATOR_USAGE_POLICY_ID

    first_rows = reload_derived_indicator_frames(
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-inc-v1",
        indicator_set_version="indicators-v1",
    )
    first_br_created_at = {
        row.ts: row.created_at for row in first_rows if row.instrument_id == "BR"
    }
    first_si_created_at = {
        row.ts: row.created_at for row in first_rows if row.instrument_id == "Si"
    }

    bars_v2 = [
        row
        if not (row.instrument_id == "BR" and row.ts == "2026-03-02T11:45:00Z")
        else CanonicalBar.from_dict(
            {
                "contract_id": row.contract_id,
                "instrument_id": row.instrument_id,
                "timeframe": row.timeframe.value,
                "ts": row.ts,
                "open": row.open,
                "high": row.high + 5.5,
                "low": row.low,
                "close": row.close + 5.0,
                "volume": row.volume + 100,
                "open_interest": row.open_interest,
            }
        )
        for row in bars_v1
    ]
    materialize_research_dataset(
        manifest_seed=manifest,
        bars=bars_v2,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-derived-inc-v1",
        indicator_set_version="indicators-v1",
    )
    second_report = materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-inc-v1",
        indicator_set_version="indicators-v1",
    )
    assert second_report["derived_indicator_row_count"] == len(bars_v2)
    assert second_report["refreshed_row_count"] == 60
    assert second_report["refreshed_partition_count"] == 1
    assert second_report["reused_partition_count"] == 1
    assert second_report["deleted_partition_count"] == 0

    second_rows = reload_derived_indicator_frames(
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-inc-v1",
        indicator_set_version="indicators-v1",
    )
    second_br_created_at = {
        row.ts: row.created_at for row in second_rows if row.instrument_id == "BR"
    }
    second_si_created_at = {
        row.ts: row.created_at for row in second_rows if row.instrument_id == "Si"
    }
    assert len(second_rows) == len(bars_v2)
    assert first_si_created_at == second_si_created_at
    assert first_br_created_at != second_br_created_at


def test_derived_indicator_materialization_reuses_when_unrelated_base_indicator_is_added(
    tmp_path: Path,
) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(60)]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
        SessionCalendarEntry(
            "BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"
        ),
    ]
    roll_map = [
        RollMapEntry("BR", "2026-03-01", "BR-6.26", "test"),
        RollMapEntry("BR", "2026-03-02", "BR-6.26", "test"),
    ]
    dataset_dir = tmp_path / "dataset-derived-base-extension"
    indicator_dir = tmp_path / "indicators-derived-base-extension"
    derived_dir = tmp_path / "derived-base-extension"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-derived-base-extension-v1",
            dataset_name="derived base extension sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-02T11:45:00Z",
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-derived-base-extension-v1",
        indicator_set_version="indicators-v1",
    )
    materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-base-extension-v1",
        indicator_set_version="indicators-v1",
    )
    first_rows = reload_derived_indicator_frames(
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-base-extension-v1",
        indicator_set_version="indicators-v1",
    )
    first_created_at = {row.ts: row.created_at for row in first_rows}

    base_profile = default_indicator_profile()
    extended_base_profile = IndicatorProfile(
        version="core_v1_plus_unrelated",
        description="Core profile plus an unrelated indicator not consumed by derived formulas.",
        indicators=(
            *base_profile.indicators,
            IndicatorSpec(
                indicator_id="sma_3_unrelated",
                category="trend",
                operation_key="sma",
                parameters=(IndicatorParameter("length", 3),),
                required_input_columns=("close",),
                output_columns=("sma_3_unrelated",),
                warmup_bars=3,
            ),
        ),
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-derived-base-extension-v1",
        indicator_set_version="indicators-v1",
        profile=extended_base_profile,
    )
    report = materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-base-extension-v1",
        indicator_set_version="indicators-v1",
    )

    assert report["refreshed_partition_count"] == 0
    assert report["reused_partition_count"] == 1
    second_rows = reload_derived_indicator_frames(
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-base-extension-v1",
        indicator_set_version="indicators-v1",
    )
    assert {row.ts: row.created_at for row in second_rows} == first_created_at


def test_derived_indicator_materialization_fails_when_required_source_columns_are_absent(
    tmp_path: Path,
) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(60)]
    session_calendar = [
        SessionCalendarEntry(
            "BR",
            "15m",
            "2026-03-01",
            "2026-03-01T09:00:00Z",
            "2026-03-01T20:45:00Z",
        ),
        SessionCalendarEntry(
            "BR",
            "15m",
            "2026-03-02",
            "2026-03-02T09:00:00Z",
            "2026-03-02T20:45:00Z",
        ),
    ]
    roll_map = [
        RollMapEntry("BR", "2026-03-01", "BR-6.26", "test"),
        RollMapEntry("BR", "2026-03-02", "BR-6.26", "test"),
    ]
    dataset_dir = tmp_path / "dataset-derived-missing-source"
    indicator_dir = tmp_path / "indicators-derived-missing-source"
    derived_dir = tmp_path / "derived-missing-source"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-derived-missing-source-v1",
            dataset_name="derived missing source sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-02T11:45:00Z",
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    minimal_indicator_profile = IndicatorProfile(
        version="minimal_for_derived_failure",
        description="Minimal profile missing required derived source columns.",
        indicators=(
            IndicatorSpec(
                indicator_id="ema_20_only",
                category="trend",
                operation_key="ema",
                parameters=(IndicatorParameter("length", 20),),
                required_input_columns=("close",),
                output_columns=("ema_20",),
                warmup_bars=20,
            ),
        ),
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-derived-missing-source-v1",
        indicator_set_version="indicators-v1",
        profile=minimal_indicator_profile,
    )

    with pytest.raises(
        ValueError,
        match="derived indicator materialization requires source indicator columns",
    ):
        materialize_derived_indicator_frames(
            dataset_output_dir=dataset_dir,
            indicator_output_dir=indicator_dir,
            derived_indicator_output_dir=derived_dir,
            dataset_version="dataset-derived-missing-source-v1",
            indicator_set_version="indicators-v1",
        )


def test_derived_indicator_materialization_extends_profile_without_recomputing_existing_columns(
    tmp_path: Path,
) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(60)]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
        SessionCalendarEntry(
            "BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"
        ),
    ]
    roll_map = [
        RollMapEntry("BR", "2026-03-01", "BR-6.26", "test"),
        RollMapEntry("BR", "2026-03-02", "BR-6.26", "test"),
    ]
    dataset_dir = tmp_path / "dataset-derived-profile-extension"
    indicator_dir = tmp_path / "indicators-derived-profile-extension"
    derived_dir = tmp_path / "derived-profile-extension"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-derived-profile-extension-v1",
            dataset_name="derived profile extension sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-02T11:45:00Z",
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-derived-profile-extension-v1",
        indicator_set_version="indicators-v1",
    )
    materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-profile-extension-v1",
        indicator_set_version="indicators-v1",
    )
    noop_report = materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-profile-extension-v1",
        indicator_set_version="indicators-v1",
    )
    assert noop_report["refreshed_partition_count"] == 0
    assert noop_report["loaded_indicator_partition_count"] == 0
    first_rows = reload_derived_indicator_frames(
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-profile-extension-v1",
        indicator_set_version="indicators-v1",
    )
    first_created_at = {row.ts: row.created_at for row in first_rows}
    first_session_vwap = {row.ts: row.values["session_vwap"] for row in first_rows}

    base_derived_profile = current_derived_indicator_profile()
    extended_derived_profile = DerivedIndicatorProfile(
        version="core_v1_plus_new_derived",
        description="Core derived profile plus one new formula column.",
        output_columns=(*base_derived_profile.output_columns, "distance_to_close_atr"),
        warmup_bars=base_derived_profile.warmup_bars,
    )
    report = materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-profile-extension-v1",
        indicator_set_version="indicators-v1",
        profile=extended_derived_profile,
    )

    assert report["refreshed_partition_count"] == 1
    assert report["extended_partition_count"] == 1
    assert report["recomputed_partition_count"] == 0
    second_rows = reload_derived_indicator_frames(
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-profile-extension-v1",
        indicator_set_version="indicators-v1",
    )
    assert {row.ts: row.created_at for row in second_rows} == first_created_at
    assert {row.ts: row.values["session_vwap"] for row in second_rows} == first_session_vwap
    assert second_rows[-1].values["distance_to_close_atr"] == 0.0
    assert second_rows[-1].profile_version == "core_v1_plus_new_derived"


def test_continuous_front_derived_profile_extension_reuses_same_profile_and_recomputes_new_profile(
    tmp_path: Path,
) -> None:
    bars = [
        _canonical_bar(
            contract_id="BR-6.26" if index < 48 else "BR-9.26",
            index=index,
            close=80.0 + index * 0.25,
        )
        for index in range(60)
    ]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
        SessionCalendarEntry(
            "BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"
        ),
    ]
    roll_map = [
        RollMapEntry("BR", "2026-03-01", "BR-6.26", "test"),
        RollMapEntry("BR", "2026-03-02", "BR-9.26", "test"),
    ]
    dataset_dir = tmp_path / "dataset-cf-derived-profile-extension"
    indicator_dir = tmp_path / "indicators-cf-derived-profile-extension"
    derived_dir = tmp_path / "derived-cf-profile-extension"
    dataset_version = "dataset-cf-derived-profile-extension-v1"

    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version=dataset_version,
            dataset_name="continuous front derived profile extension sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            contour_id="pit_active_front",
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-02T11:45:00Z",
            series_mode="continuous_front",
            continuous_front_policy=ContinuousFrontPolicy.from_config({}),
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version=dataset_version,
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
    )
    base_derived_profile = current_derived_indicator_profile()
    materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version=dataset_version,
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=base_derived_profile,
    )
    first_rows = read_delta_table_rows(
        derived_dir / "research_derived_indicator_frames.delta",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", "pit_active_front"),
            ("indicator_set_version", "=", "indicators-v1"),
        ],
    )
    same_profile_report = materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version=dataset_version,
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=base_derived_profile,
    )
    assert same_profile_report["derived_indicator_row_count"] == 60
    assert same_profile_report["refreshed_partition_count"] == 0
    assert same_profile_report["reused_partition_count"] == 1
    assert same_profile_report["recomputed_partition_count"] == 0
    repaired_rows = read_delta_table_rows(
        derived_dir / "research_derived_indicator_frames.delta",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", "pit_active_front"),
            ("indicator_set_version", "=", "indicators-v1"),
        ],
    )
    assert repaired_rows == first_rows

    corrupted_rows = [{**row, "session_vwap": -999.0} for row in repaired_rows]
    replace_delta_table_rows(
        table_path=derived_dir / "research_derived_indicator_frames.delta",
        rows=corrupted_rows,
        columns=research_derived_indicator_store_contract(profile=base_derived_profile)[
            "research_derived_indicator_frames"
        ]["columns"],
        predicate=delta_equals_predicate(
            {
                "dataset_version": dataset_version,
                "contour_id": "pit_active_front",
                "indicator_set_version": "indicators-v1",
            }
        ),
    )

    extended_derived_profile = DerivedIndicatorProfile(
        version="core_v1_plus_new_cf_derived",
        description="Core continuous-front derived profile plus one new formula column.",
        output_columns=(*base_derived_profile.output_columns, "distance_to_close_atr"),
        warmup_bars=base_derived_profile.warmup_bars,
    )
    report = materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version=dataset_version,
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=extended_derived_profile,
    )

    assert report["derived_indicator_row_count"] == 60
    assert report["refreshed_partition_count"] == 1
    assert report["extended_partition_count"] == 0
    assert report["recomputed_partition_count"] == 1
    second_rows = read_delta_table_rows(
        derived_dir / "research_derived_indicator_frames.delta",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", "pit_active_front"),
            ("indicator_set_version", "=", "indicators-v1"),
        ],
    )
    assert all(row["session_vwap"] != -999.0 for row in second_rows)
    assert second_rows[-1]["distance_to_close_atr"] == 0.0
    assert second_rows[-1]["profile_version"] == "core_v1_plus_new_cf_derived"


def test_derived_materialization_uses_source_frame_without_l0_l1_python_join(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(60)]
    session_calendar = [
        SessionCalendarEntry(
            "BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"
        ),
        SessionCalendarEntry(
            "BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"
        ),
    ]
    roll_map = [
        RollMapEntry("BR", "2026-03-01", "BR-6.26", "test"),
        RollMapEntry("BR", "2026-03-02", "BR-6.26", "test"),
    ]
    dataset_dir = tmp_path / "dataset-source-frame"
    indicator_dir = tmp_path / "indicators-source-frame"
    derived_dir = tmp_path / "derived-source-frame"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="dataset-source-frame-v1",
            dataset_name="derived source-frame sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-01T09:00:00Z",
            end_ts="2026-03-02T11:45:00Z",
            warmup_bars=0,
            split_method="full",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=dataset_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        dataset_version="dataset-source-frame-v1",
        indicator_set_version="indicators-v1",
    )

    def _fake_source_frame_runner(**kwargs: object) -> dict[str, object]:
        return _write_test_derived_source_frames(
            dataset_output_dir=Path(str(kwargs["bar_views_path"])).parent,
            indicator_output_dir=Path(str(kwargs["indicator_frames_path"])).parent,
            source_frame_output_dir=Path(str(kwargs["output_dir"])),
            dataset_version=str(kwargs["dataset_version"]),
            contour_id=str(kwargs["contour_id"]),
            indicator_set_version=str(kwargs["indicator_set_version"]),
            derived_profile_version=str(kwargs["derived_profile_version"]),
            source_indicator_columns=tuple(kwargs["source_indicator_columns"]),  # type: ignore[arg-type]
        )

    def _blocked_loader(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("production L2 must read ready source-frame partitions")

    monkeypatch.setattr(
        derived_materialize_module,
        "run_research_derived_source_frames_spark_job",
        _fake_source_frame_runner,
        raising=False,
    )
    monkeypatch.setattr(derived_materialize_module, "_load_bar_partition_rows", _blocked_loader)
    monkeypatch.setattr(
        derived_materialize_module,
        "load_indicator_partition_rows",
        _blocked_loader,
        raising=False,
    )

    report = materialize_derived_indicator_frames(
        dataset_output_dir=dataset_dir,
        indicator_output_dir=indicator_dir,
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-source-frame-v1",
        indicator_set_version="indicators-v1",
    )

    assert report["derived_indicator_row_count"] == len(bars)
    assert report["source_frame_row_count"] == len(bars)
