from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.contracts import CanonicalBar, Timeframe
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    HoldoutSplitConfig,
    ResearchDatasetManifest,
    build_research_bar_views,
    build_research_dataset_manifest,
    research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.continuous_front import ContinuousFrontResearchBar


def _bar(*, contract_id: str, instrument_id: str, ts: str, close: float, oi: int, timeframe: Timeframe = Timeframe.M15) -> CanonicalBar:
    return CanonicalBar(
        contract_id=contract_id,
        instrument_id=instrument_id,
        timeframe=timeframe,
        ts=ts,
        open=close - 0.2,
        high=close + 0.4,
        low=close - 0.5,
        close=close,
        volume=1000,
        open_interest=oi,
    )


def _calendar(*, instrument_id: str, timeframe: str, session_date: str, open_ts: str, close_ts: str) -> SessionCalendarEntry:
    return SessionCalendarEntry(
        instrument_id=instrument_id,
        timeframe=timeframe,
        session_date=session_date,
        session_open_ts=open_ts,
        session_close_ts=close_ts,
    )


def _roll(*, instrument_id: str, session_date: str, active_contract_id: str) -> RollMapEntry:
    return RollMapEntry(
        instrument_id=instrument_id,
        session_date=session_date,
        active_contract_id=active_contract_id,
        reason="test",
    )


def test_contract_mode_dataset_builds_warmup_aware_bar_views() -> None:
    bars = [
        _bar(contract_id="BR-6.26", instrument_id="BR", ts="2026-03-16T09:00:00Z", close=82.2, oi=100),
        _bar(contract_id="BR-6.26", instrument_id="BR", ts="2026-03-16T09:15:00Z", close=82.5, oi=105),
        _bar(contract_id="BR-6.26", instrument_id="BR", ts="2026-03-16T09:30:00Z", close=82.9, oi=110),
    ]
    manifest_seed = ResearchDatasetManifest(
        dataset_version="dataset-v2",
        dataset_name="BR contract slice",
        universe_id="moex-futures",
        timeframes=("15m",),
        base_timeframe="15m",
        start_ts="2026-03-16T09:15:00Z",
        end_ts="2026-03-16T09:30:00Z",
        warmup_bars=1,
        split_method="holdout",
        code_version="test",
    )
    views = build_research_bar_views(
        dataset_version=manifest_seed.dataset_version,
        bars=bars,
        session_calendar=[
            _calendar(
                instrument_id="BR",
                timeframe="15m",
                session_date="2026-03-16",
                open_ts="2026-03-16T09:00:00Z",
                close_ts="2026-03-16T09:45:00Z",
            )
        ],
        roll_map=[_roll(instrument_id="BR", session_date="2026-03-16", active_contract_id="BR-6.26")],
        manifest=manifest_seed,
    )

    assert [row.ts for row in views] == [
        "2026-03-16T09:00:00Z",
        "2026-03-16T09:15:00Z",
        "2026-03-16T09:30:00Z",
    ]
    assert [row.slice_role for row in views] == ["warmup", "analysis", "analysis"]
    assert views[0].ret_1 is None
    assert views[1].bar_index == 1
    assert views[2].true_range >= views[2].hl_range

    manifest = build_research_dataset_manifest(
        manifest_seed=manifest_seed,
        bars=bars,
        selected_views=views,
        split_config=HoldoutSplitConfig(holdout_ratio=0.5),
    )
    payload = manifest.to_dict()
    assert payload["dataset_name"] == "BR contract slice"
    assert payload["base_timeframe"] == "15m"
    assert payload["bars_hash"]
    assert payload["split_params_json"]["analysis_row_count"] == 2
    assert payload["split_params_json"]["windows"][0]["window_id"] == "holdout-01"


def test_continuous_front_uses_roll_map_across_contract_boundary() -> None:
    bars = [
        _bar(contract_id="BR-6.26", instrument_id="BR", ts="2026-03-16T09:00:00Z", close=82.0, oi=200),
        _bar(contract_id="BR-7.26", instrument_id="BR", ts="2026-03-17T09:00:00Z", close=82.8, oi=260),
    ]
    manifest = ResearchDatasetManifest(
        dataset_version="dataset-cf-v1",
        dataset_name="BR continuous front",
        universe_id="moex-futures",
        timeframes=("15m",),
        base_timeframe="15m",
        start_ts="2026-03-16T09:00:00Z",
        end_ts="2026-03-17T09:00:00Z",
        series_mode="continuous_front",
        warmup_bars=0,
        split_method="full",
        continuous_front_policy=ContinuousFrontPolicy(),
        code_version="test",
    )
    views = build_research_bar_views(
        dataset_version=manifest.dataset_version,
        bars=bars,
        session_calendar=[
            _calendar(instrument_id="BR", timeframe="15m", session_date="2026-03-16", open_ts="2026-03-16T09:00:00Z", close_ts="2026-03-16T23:45:00Z"),
            _calendar(instrument_id="BR", timeframe="15m", session_date="2026-03-17", open_ts="2026-03-17T09:00:00Z", close_ts="2026-03-17T23:45:00Z"),
        ],
        roll_map=[
            _roll(instrument_id="BR", session_date="2026-03-16", active_contract_id="BR-6.26"),
            _roll(instrument_id="BR", session_date="2026-03-17", active_contract_id="BR-7.26"),
        ],
        manifest=manifest,
    )

    assert [row.contract_id for row in views] == ["BR-6.26", "BR-7.26"]
    assert [row.active_contract_id for row in views] == ["BR-6.26", "BR-7.26"]
    assert [row.bar_index for row in views] == [0, 1]


def test_continuous_front_policy_rejects_unknown_session_timezone() -> None:
    with pytest.raises(ValueError, match="unsupported continuous_front session_timezone"):
        ContinuousFrontPolicy(session_timezone="Europe/Moscw")


def test_continuous_front_preserves_intraday_roll_rows_in_research_views() -> None:
    bars = [
        _bar(contract_id="BRK2@MOEX", instrument_id="FUT_BR", ts="2022-03-21T10:00:00Z", close=98.0, oi=300),
        _bar(contract_id="BRM2@MOEX", instrument_id="FUT_BR", ts="2022-03-21T10:15:00Z", close=100.5, oi=420),
    ]
    manifest = ResearchDatasetManifest(
        dataset_version="dataset-cf-intraday-v1",
        dataset_name="BR continuous front",
        universe_id="moex-futures",
        timeframes=("15m",),
        base_timeframe="15m",
        start_ts="2022-03-21T10:00:00Z",
        end_ts="2022-03-21T10:15:00Z",
        series_mode="continuous_front",
        warmup_bars=0,
        split_method="full",
        continuous_front_policy=ContinuousFrontPolicy(),
        code_version="test",
    )
    views = build_research_bar_views(
        dataset_version=manifest.dataset_version,
        bars=bars,
        session_calendar=[
            _calendar(
                instrument_id="FUT_BR",
                timeframe="15m",
                session_date="2022-03-21",
                open_ts="2022-03-21T10:00:00Z",
                close_ts="2022-03-21T23:45:00Z",
            ),
        ],
        roll_map=[
            _roll(instrument_id="FUT_BR", session_date="2022-03-21", active_contract_id="BRK2@MOEX"),
            _roll(instrument_id="FUT_BR", session_date="2022-03-21", active_contract_id="BRM2@MOEX"),
        ],
        manifest=manifest,
    )

    assert [row.ts for row in views] == ["2022-03-21T10:00:00Z", "2022-03-21T10:15:00Z"]
    assert [row.contract_id for row in views] == ["BRK2@MOEX", "BRM2@MOEX"]
    assert [row.active_contract_id for row in views] == ["BRK2@MOEX", "BRM2@MOEX"]


def test_continuous_front_roll_metadata_survives_research_bar_views() -> None:
    bars = [
        ContinuousFrontResearchBar(
            contract_id="BRM2@MOEX",
            instrument_id="FUT_BR",
            timeframe=Timeframe.M15,
            ts="2022-03-21T10:30:00Z",
            open=101.0,
            high=102.0,
            low=100.5,
            close=101.5,
            volume=1200,
            open_interest=420,
            series_id="FUT_BR|15m|continuous_front",
            series_mode="continuous_front",
            active_contract_id="BRM2@MOEX",
            previous_contract_id="BRK2@MOEX",
            candidate_contract_id="BRM2@MOEX",
            roll_epoch=1,
            roll_event_id="CFR-test-0001",
            is_roll_bar=True,
            is_first_bar_after_roll=True,
            bars_since_roll=0,
            native_open=101.0,
            native_high=102.0,
            native_low=100.5,
            native_close=101.5,
            continuous_open=101.0,
            continuous_high=102.0,
            continuous_low=100.5,
            continuous_close=101.5,
            adjustment_mode="additive",
            cumulative_additive_offset=0.0,
            ratio_factor=None,
            price_space="continuous_backward_current_anchor_additive",
        )
    ]
    manifest = ResearchDatasetManifest(
        dataset_version="dataset-cf-metadata-v1",
        dataset_name="BR continuous front",
        universe_id="moex-futures",
        timeframes=("15m",),
        base_timeframe="15m",
        start_ts="2022-03-21T10:30:00Z",
        end_ts="2022-03-21T10:30:00Z",
        series_mode="continuous_front",
        warmup_bars=0,
        split_method="full",
        continuous_front_policy=ContinuousFrontPolicy(),
        code_version="test",
    )

    views = build_research_bar_views(
        dataset_version=manifest.dataset_version,
        bars=bars,
        session_calendar=[
            _calendar(
                instrument_id="FUT_BR",
                timeframe="15m",
                session_date="2022-03-21",
                open_ts="2022-03-21T10:00:00Z",
                close_ts="2022-03-21T23:45:00Z",
            ),
        ],
        roll_map=[],
        manifest=manifest,
    )

    row = views[0]
    assert row.series_id == "FUT_BR|15m|continuous_front"
    assert row.series_mode == "continuous_front"
    assert row.roll_epoch == 1
    assert row.roll_event_id == "CFR-test-0001"
    assert row.is_roll_bar is True
    assert row.active_contract_id == "BRM2@MOEX"
    assert row.previous_contract_id == "BRK2@MOEX"
    assert row.native_close == 101.5
    assert row.continuous_close == 101.5
    assert row.execution_close == 101.5
    assert row.price_space == "continuous_backward_current_anchor_additive"


def test_dataset_store_contract_contains_required_stage2_fields() -> None:
    contract = research_dataset_store_contract()
    assert set(contract) == {"research_datasets", "research_instrument_tree", "research_bar_views"}
    dataset_columns = contract["research_datasets"]["columns"]
    bar_columns = contract["research_bar_views"]["columns"]
    assert {"dataset_version", "dataset_name", "series_mode", "bars_hash", "split_params_json"} <= set(dataset_columns)
    assert {
        "dataset_version",
        "contract_id",
        "instrument_id",
        "session_open_ts",
        "session_close_ts",
        "active_contract_id",
        "series_id",
        "series_mode",
        "roll_epoch",
        "roll_event_id",
        "is_roll_bar",
        "bars_since_roll",
        "price_space",
        "native_close",
        "continuous_close",
        "execution_close",
        "ret_1",
        "true_range",
        "bar_index",
        "slice_role",
    } <= set(bar_columns)
