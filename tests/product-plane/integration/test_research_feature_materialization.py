from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.research.datasets import ResearchDatasetManifest, load_materialized_research_dataset, materialize_research_dataset
from trading_advisor_3000.product_plane.research.features import materialize_feature_frames, reload_feature_frames
from trading_advisor_3000.product_plane.research.indicators import materialize_indicator_frames


def _ts(start: datetime, *, hours: int = 0, minutes: int = 0) -> str:
    return (start + timedelta(hours=hours, minutes=minutes)).replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")


def _build_canonical_context() -> tuple[list[CanonicalBar], list[SessionCalendarEntry], list[RollMapEntry]]:
    start = datetime(2026, 3, 16, 0, 0, tzinfo=UTC)
    bars: list[CanonicalBar] = []
    for hour_index in range(60):
        hour_start = start + timedelta(hours=hour_index)
        base_close = 80.0 + hour_index * 0.18
        bars.append(
            CanonicalBar.from_dict(
                {
                    "contract_id": "BR-6.26",
                    "instrument_id": "BR",
                    "timeframe": "1h",
                    "ts": hour_start.isoformat().replace("+00:00", "Z"),
                    "open": base_close - 0.3,
                    "high": base_close + 0.7,
                    "low": base_close - 0.9,
                    "close": base_close,
                    "volume": 2500 + hour_index * 20,
                    "open_interest": 20000 + hour_index,
                }
            )
        )
        for quarter in range(4):
            quarter_start = hour_start + timedelta(minutes=15 * quarter)
            close = base_close + (quarter - 1.5) * 0.08
            bars.append(
                CanonicalBar.from_dict(
                    {
                        "contract_id": "BR-6.26",
                        "instrument_id": "BR",
                        "timeframe": "15m",
                        "ts": quarter_start.isoformat().replace("+00:00", "Z"),
                        "open": close - 0.2,
                        "high": close + 0.5,
                        "low": close - 0.6,
                        "close": close,
                        "volume": 900 + hour_index * 10 + quarter * 15,
                        "open_interest": 15000 + hour_index * 4 + quarter,
                    }
                )
            )

    session_dates = sorted({bar.ts[:10] for bar in bars})
    session_calendar = [
        SessionCalendarEntry(
            instrument_id="BR",
            timeframe=timeframe,
            session_date=session_date,
            session_open_ts=f"{session_date}T00:00:00Z",
            session_close_ts=f"{session_date}T23:45:00Z" if timeframe == "15m" else f"{session_date}T23:00:00Z",
        )
        for session_date in session_dates
        for timeframe in ("15m", "1h")
    ]
    roll_map = [
        RollMapEntry(
            instrument_id="BR",
            session_date=session_date,
            active_contract_id="BR-6.26",
            reason="test",
        )
        for session_date in session_dates
    ]
    return bars, session_calendar, roll_map


def test_feature_materialization_builds_versioned_feature_frames_and_reuses_unchanged_partitions(tmp_path: Path) -> None:
    bars, session_calendar, roll_map = _build_canonical_context()
    research_dir = tmp_path / "research"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="feature-dataset-v1",
            dataset_name="feature integration",
            universe_id="moex-futures",
            timeframes=("15m", "1h"),
            base_timeframe="15m",
            start_ts=None,
            end_ts=None,
            split_method="full",
            warmup_bars=0,
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=research_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=research_dir,
        indicator_output_dir=research_dir,
        dataset_version="feature-dataset-v1",
        indicator_set_version="indicators-v1",
        profile_version="core_v1",
    )

    first_report = materialize_feature_frames(
        dataset_output_dir=research_dir,
        indicator_output_dir=research_dir,
        feature_output_dir=research_dir,
        dataset_version="feature-dataset-v1",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        profile_version="core_v1",
    )
    second_report = materialize_feature_frames(
        dataset_output_dir=research_dir,
        indicator_output_dir=research_dir,
        feature_output_dir=research_dir,
        dataset_version="feature-dataset-v1",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        profile_version="core_v1",
    )

    assert first_report["refreshed_partition_count"] == 2
    assert first_report["reused_partition_count"] == 0
    assert second_report["refreshed_partition_count"] == 0
    assert second_report["reused_partition_count"] == 2

    loaded_dataset = load_materialized_research_dataset(output_dir=research_dir, dataset_version="feature-dataset-v1")
    loaded_rows = reload_feature_frames(
        feature_output_dir=research_dir,
        dataset_version="feature-dataset-v1",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
    )
    assert loaded_dataset["dataset_manifest"]["dataset_version"] == "feature-dataset-v1"
    assert loaded_rows
    assert (Path(str(first_report["output_paths"]["research_feature_frames"])) / "_delta_log").exists()

    ltf_rows = [row for row in loaded_rows if row.timeframe == "15m"]
    htf_rows = [row for row in loaded_rows if row.timeframe == "1h"]
    assert ltf_rows and htf_rows

    tail = ltf_rows[-1]
    assert tail.profile_version == "core_v1"
    assert tail.values["session_vwap"] is not None
    assert tail.values["trend_strength"] is not None
    assert tail.values["bb_width_20_2"] is not None
    assert tail.values["rvol_20"] is not None
    assert tail.values["htf_rsi_14"] is not None
    assert tail.values["htf_trend_state_code"] in {-1, 0, 1}
