from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.research.datasets import ResearchDatasetManifest, materialize_research_dataset
from trading_advisor_3000.product_plane.research.derived_indicators import (
    DerivedIndicatorProfile,
    current_derived_indicator_profile,
    materialize_derived_indicator_frames,
    reload_derived_indicator_frames,
)
from trading_advisor_3000.product_plane.research.indicators import (
    IndicatorParameter,
    IndicatorProfile,
    IndicatorSpec,
    default_indicator_profile,
    materialize_indicator_frames,
    reload_indicator_frames,
)


def _canonical_bar(*, contract_id: str = "BR-6.26", instrument_id: str = "BR", index: int, close: float) -> CanonicalBar:
    ts = f"2026-03-{1 + (index // 48):02d}T{9 + ((index % 48) // 4):02d}:{((index % 4) * 15):02d}:00Z"
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


def test_indicator_materialization_and_reload_from_dataset_layer(tmp_path: Path) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(72)]
    session_calendar = [
        SessionCalendarEntry("BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"),
        SessionCalendarEntry("BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"),
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
        *[_canonical_bar(contract_id="BR-6.26", instrument_id="BR", index=index, close=80.0 + index * 0.2) for index in range(60)],
        *[_canonical_bar(contract_id="Si-6.26", instrument_id="Si", index=index, close=90000.0 + index * 2) for index in range(60)],
    ]
    session_calendar = [
        SessionCalendarEntry("BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"),
        SessionCalendarEntry("Si", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"),
        SessionCalendarEntry("BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"),
        SessionCalendarEntry("Si", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"),
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
    first_br_created_at = {row.ts: row.created_at for row in first_rows if row.instrument_id == "BR"}
    first_si_created_at = {row.ts: row.created_at for row in first_rows if row.instrument_id == "Si"}

    bars_v2 = [
        row if not (row.instrument_id == "BR" and row.ts == "2026-03-02T11:45:00Z") else CanonicalBar.from_dict(
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
    second_br_created_at = {row.ts: row.created_at for row in second_rows if row.instrument_id == "BR"}
    second_si_created_at = {row.ts: row.created_at for row in second_rows if row.instrument_id == "Si"}
    assert first_si_created_at == second_si_created_at
    assert first_br_created_at != second_br_created_at


def test_indicator_materialization_extends_profile_without_recomputing_existing_columns(tmp_path: Path) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(40)]
    session_calendar = [
        SessionCalendarEntry("BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"),
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


def test_derived_indicator_materialization_recomputes_only_affected_partitions(tmp_path: Path) -> None:
    bars_v1 = [
        *[_canonical_bar(contract_id="BR-6.26", instrument_id="BR", index=index, close=80.0 + index * 0.2) for index in range(60)],
        *[_canonical_bar(contract_id="Si-6.26", instrument_id="Si", index=index, close=90000.0 + index * 2) for index in range(60)],
    ]
    session_calendar = [
        SessionCalendarEntry("BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"),
        SessionCalendarEntry("Si", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"),
        SessionCalendarEntry("BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"),
        SessionCalendarEntry("Si", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"),
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

    first_rows = reload_derived_indicator_frames(
        derived_indicator_output_dir=derived_dir,
        dataset_version="dataset-derived-inc-v1",
        indicator_set_version="indicators-v1",
    )
    first_br_created_at = {row.ts: row.created_at for row in first_rows if row.instrument_id == "BR"}
    first_si_created_at = {row.ts: row.created_at for row in first_rows if row.instrument_id == "Si"}

    bars_v2 = [
        row if not (row.instrument_id == "BR" and row.ts == "2026-03-02T11:45:00Z") else CanonicalBar.from_dict(
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
    second_br_created_at = {row.ts: row.created_at for row in second_rows if row.instrument_id == "BR"}
    second_si_created_at = {row.ts: row.created_at for row in second_rows if row.instrument_id == "Si"}
    assert len(second_rows) == len(bars_v2)
    assert first_si_created_at == second_si_created_at
    assert first_br_created_at != second_br_created_at


def test_derived_indicator_materialization_reuses_when_unrelated_base_indicator_is_added(tmp_path: Path) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(60)]
    session_calendar = [
        SessionCalendarEntry("BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"),
        SessionCalendarEntry("BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"),
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


def test_derived_indicator_materialization_extends_profile_without_recomputing_existing_columns(tmp_path: Path) -> None:
    bars = [_canonical_bar(index=index, close=80.0 + index * 0.25) for index in range(60)]
    session_calendar = [
        SessionCalendarEntry("BR", "15m", "2026-03-01", "2026-03-01T09:00:00Z", "2026-03-01T20:45:00Z"),
        SessionCalendarEntry("BR", "15m", "2026-03-02", "2026-03-02T09:00:00Z", "2026-03-02T20:45:00Z"),
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
