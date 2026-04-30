from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane import run_sample_backfill
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    ResearchBarView,
    ResearchDatasetManifest,
    materialize_research_dataset,
)


ROOT = Path(__file__).resolve().parents[3]
RAW_FIXTURE = ROOT / "tests" / "product-plane" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"


def _load_canonical_context(output_dir: Path) -> tuple[list[CanonicalBar], list[SessionCalendarEntry], list[RollMapEntry]]:
    bars = [CanonicalBar.from_dict(row) for row in read_delta_table_rows(output_dir / "canonical_bars.delta")]
    session_calendar = [
        SessionCalendarEntry(
            instrument_id=str(row["instrument_id"]),
            timeframe=str(row["timeframe"]),
            session_date=str(row["session_date"]),
            session_open_ts=str(row["session_open_ts"]),
            session_close_ts=str(row["session_close_ts"]),
        )
        for row in read_delta_table_rows(output_dir / "canonical_session_calendar.delta")
    ]
    roll_map = [
        RollMapEntry(
            instrument_id=str(row["instrument_id"]),
            session_date=str(row["session_date"]),
            active_contract_id=str(row["active_contract_id"]),
            reason=str(row["reason"]),
        )
        for row in read_delta_table_rows(output_dir / "canonical_roll_map.delta")
    ]
    return bars, session_calendar, roll_map


def _load_research_dataset(output_dir: Path, dataset_version: str) -> dict[str, object]:
    filters = [("dataset_version", "=", dataset_version)]
    manifests = read_delta_table_rows(output_dir / "research_datasets.delta", filters=filters)
    instrument_tree = read_delta_table_rows(output_dir / "research_instrument_tree.delta", filters=filters)
    bar_views = [
        ResearchBarView.from_dict(row)
        for row in read_delta_table_rows(output_dir / "research_bar_views.delta", filters=filters)
    ]
    return {
        "dataset_manifest": manifests[0],
        "instrument_tree": instrument_tree,
        "bar_views": bar_views,
    }


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
            SessionCalendarEntry("BR", "15m", "2026-03-16", "2026-03-16T10:00:00Z", "2026-03-16T23:45:00Z"),
            SessionCalendarEntry("BR", "15m", "2026-03-17", "2026-03-17T10:00:00Z", "2026-03-17T23:45:00Z"),
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
