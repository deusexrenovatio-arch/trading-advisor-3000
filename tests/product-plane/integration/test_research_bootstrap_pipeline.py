from __future__ import annotations

import math
import hashlib
import json
from dataclasses import replace
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane import run_sample_backfill
from trading_advisor_3000.product_plane.data_plane.canonical import (
    RollMapEntry,
    SessionCalendarEntry,
)
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    delta_equals_predicate,
    iter_delta_table_row_batches,
    read_filtered_delta_table_rows,
    read_small_delta_table_rows,
    replace_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    ResearchBarView,
    ResearchDatasetManifest,
    materialize_research_dataset as _materialize_research_dataset_manifest,
    research_dataset_store_contract,
)

ROOT = Path(__file__).resolve().parents[3]
RAW_FIXTURE = (
    ROOT / "tests" / "product-plane" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"
)


def _read_batched_delta_rows(table_path: Path) -> list[dict[str, object]]:
    return [row for batch in iter_delta_table_row_batches(table_path) for row in batch]


def _load_canonical_context(
    output_dir: Path,
) -> tuple[list[CanonicalBar], list[SessionCalendarEntry], list[RollMapEntry]]:
    bars = [
        CanonicalBar.from_dict(row)
        for row in _read_batched_delta_rows(output_dir / "canonical_bars.delta")
    ]
    session_calendar = [
        SessionCalendarEntry(
            instrument_id=str(row["instrument_id"]),
            timeframe=str(row["timeframe"]),
            session_date=str(row["session_date"]),
            session_open_ts=str(row["session_open_ts"]),
            session_close_ts=str(row["session_close_ts"]),
        )
        for row in _read_batched_delta_rows(output_dir / "canonical_session_calendar.delta")
    ]
    roll_map = [
        RollMapEntry(
            instrument_id=str(row["instrument_id"]),
            session_date=str(row["session_date"]),
            active_contract_id=str(row["active_contract_id"]),
            reason=str(row["reason"]),
        )
        for row in _read_batched_delta_rows(output_dir / "canonical_roll_map.delta")
    ]
    return bars, session_calendar, roll_map


def _load_research_dataset(output_dir: Path, dataset_version: str) -> dict[str, object]:
    filters = [("dataset_version", "=", dataset_version)]
    manifests = [
        row
        for row in read_small_delta_table_rows(output_dir / "research_datasets.delta")
        if row.get("dataset_version") == dataset_version
    ]
    instrument_tree = [
        row
        for row in read_small_delta_table_rows(output_dir / "research_instrument_tree.delta")
        if row.get("dataset_version") == dataset_version
    ]
    bar_views = [
        ResearchBarView.from_dict(row)
        for row in read_filtered_delta_table_rows(
            output_dir / "research_bar_views.delta", filters=filters
        )
    ]
    return {
        "dataset_manifest": manifests[0],
        "instrument_tree": instrument_tree,
        "bar_views": bar_views,
    }


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
        series_id = bar.instrument_id if manifest.series_mode == "continuous_front" else bar.contract_id
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
            true_range=max(bar.high - bar.low, abs(bar.high - (previous_close or bar.close)), abs(bar.low - (previous_close or bar.close))),
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

    bars_hash = hashlib.sha256(
        json.dumps(view_rows, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()[:16].upper()
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


def test_research_dataset_materialization_and_reload_by_dataset_version(tmp_path: Path) -> None:
    canonical_output_dir = tmp_path / "canonical"
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_output_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )
    bars, session_calendar, roll_map = _load_canonical_context(canonical_output_dir)

    research_output_dir = tmp_path / "research"
    report = materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="research-dataset-v1",
            dataset_name="sample contract dataset",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-16T10:00:00Z",
            end_ts="2026-03-16T10:00:00Z",
            warmup_bars=0,
            split_method="holdout",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=research_output_dir,
    )

    assert report["bar_view_count"] == 2
    assert (Path(str(report["output_paths"]["research_datasets"])) / "_delta_log").exists()
    assert (Path(str(report["output_paths"]["research_bar_views"])) / "_delta_log").exists()

    loaded = _load_research_dataset(research_output_dir, "research-dataset-v1")
    assert loaded["dataset_manifest"]["dataset_version"] == "research-dataset-v1"
    assert len(loaded["bar_views"]) == 2
    assert all(isinstance(row, ResearchBarView) for row in loaded["bar_views"])
    assert {row.contract_id for row in loaded["bar_views"]} == {"BR-6.26", "Si-6.26"}


def test_research_dataset_materialization_supports_continuous_front_mode(tmp_path: Path) -> None:
    bars = [
        CanonicalBar.from_dict(
            {
                "contract_id": "BR-6.26",
                "instrument_id": "BR",
                "timeframe": "15m",
                "ts": "2026-03-16T10:00:00Z",
                "open": 82.0,
                "high": 82.5,
                "low": 81.8,
                "close": 82.4,
                "volume": 1000,
                "open_interest": 150,
            }
        ),
        CanonicalBar.from_dict(
            {
                "contract_id": "BR-7.26",
                "instrument_id": "BR",
                "timeframe": "15m",
                "ts": "2026-03-17T10:00:00Z",
                "open": 82.5,
                "high": 83.0,
                "low": 82.2,
                "close": 82.8,
                "volume": 1200,
                "open_interest": 260,
            }
        ),
    ]
    report = materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="research-cf-v1",
            dataset_name="continuous front sample",
            universe_id="moex-futures",
            timeframes=("15m",),
            contour_id="pit_active_front",
            source_table="continuous_front_bars",
            base_timeframe="15m",
            start_ts="2026-03-16T10:00:00Z",
            end_ts="2026-03-17T10:00:00Z",
            series_mode="continuous_front",
            split_method="full",
            warmup_bars=0,
            continuous_front_policy=ContinuousFrontPolicy(),
            code_version="test",
        ),
        bars=bars,
        session_calendar=[
            SessionCalendarEntry(
                "BR", "15m", "2026-03-16", "2026-03-16T10:00:00Z", "2026-03-16T23:45:00Z"
            ),
            SessionCalendarEntry(
                "BR", "15m", "2026-03-17", "2026-03-17T10:00:00Z", "2026-03-17T23:45:00Z"
            ),
        ],
        roll_map=[
            RollMapEntry("BR", "2026-03-16", "BR-6.26", "test"),
            RollMapEntry("BR", "2026-03-17", "BR-7.26", "test"),
        ],
        output_dir=tmp_path / "research-cf",
    )

    loaded = _load_research_dataset(tmp_path / "research-cf", "research-cf-v1")
    assert [row.active_contract_id for row in loaded["bar_views"]] == ["BR-6.26", "BR-7.26"]
    assert report["dataset_manifest"]["split_params_json"]["windows"][0]["window_id"] == "full-01"
