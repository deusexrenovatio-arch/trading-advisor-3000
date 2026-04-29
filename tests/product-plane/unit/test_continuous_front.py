from __future__ import annotations

from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.research import continuous_front as continuous_front_module
from trading_advisor_3000.product_plane.research.continuous_front import (
    continuous_front_store_contract,
    load_continuous_front_as_research_context,
)


def _front_bar_row(
    *,
    dataset_version: str,
    ts: str,
    active_contract_id: str,
    close: float,
    previous_contract_id: str | None = None,
    candidate_contract_id: str | None = None,
    roll_epoch: int = 0,
    roll_event_id: str | None = None,
    is_roll_bar: bool = False,
    bars_since_roll: int = 0,
) -> dict[str, object]:
    return {
        "dataset_version": dataset_version,
        "roll_policy_version": "liquidity_oi_v1",
        "adjustment_policy_version": "additive_close_to_close_v1",
        "instrument_id": "BR",
        "timeframe": "15m",
        "ts": ts,
        "active_contract_id": active_contract_id,
        "previous_contract_id": previous_contract_id,
        "candidate_contract_id": candidate_contract_id or active_contract_id,
        "roll_epoch": roll_epoch,
        "roll_event_id": roll_event_id,
        "is_roll_bar": is_roll_bar,
        "is_first_bar_after_roll": is_roll_bar,
        "bars_since_roll": bars_since_roll,
        "native_open": close - 0.2,
        "native_high": close + 0.5,
        "native_low": close - 0.5,
        "native_close": close,
        "native_volume": 1000,
        "native_open_interest": 2000,
        "continuous_open": close - 0.2,
        "continuous_high": close + 0.5,
        "continuous_low": close - 0.5,
        "continuous_close": close,
        "adjustment_mode": "additive",
        "cumulative_additive_offset": 0.0,
        "ratio_factor": None,
        "price_space": "continuous_backward_current_anchor_additive",
        "causality_watermark_ts": ts,
        "input_row_count": 2,
        "created_at": "2026-04-29T00:00:00Z",
    }


def test_continuous_front_module_has_no_python_materializer_surface() -> None:
    assert not hasattr(continuous_front_module, "build_continuous_front_tables")
    assert not hasattr(continuous_front_module, "materialize_continuous_front")
    assert not hasattr(continuous_front_module, "_rank_candidate")
    assert not hasattr(continuous_front_module, "_candidate_passes_switch_rules")
    assert not hasattr(continuous_front_module, "_write_tables")
    assert not hasattr(continuous_front_module, "_promote_delta_tables")


def test_load_continuous_front_preserves_bar_level_intraday_roll_metadata(tmp_path: Path) -> None:
    dataset_version = "cf-loader-v1"
    output_dir = tmp_path / "front"
    contract = continuous_front_store_contract()
    write_delta_table_rows(
        table_path=output_dir / "continuous_front_bars.delta",
        rows=[
            _front_bar_row(
                dataset_version=dataset_version,
                ts="2022-03-21T10:00:00Z",
                active_contract_id="BRK2@MOEX",
                close=98.0,
            ),
            _front_bar_row(
                dataset_version=dataset_version,
                ts="2022-03-21T10:15:00Z",
                active_contract_id="BRM2@MOEX",
                previous_contract_id="BRK2@MOEX",
                close=101.5,
                roll_epoch=1,
                roll_event_id="CFR-cf_loader_v1-BR-15m-0001",
                is_roll_bar=True,
            ),
        ],
        columns=dict(contract["continuous_front_bars"]["columns"]),
    )

    bars, roll_map = load_continuous_front_as_research_context(
        continuous_front_output_dir=output_dir,
        dataset_version=dataset_version,
        timeframes=("15m",),
    )

    by_ts = {row.ts: row for row in bars}
    assert set(by_ts) == {"2022-03-21T10:00:00Z", "2022-03-21T10:15:00Z"}
    assert by_ts["2022-03-21T10:00:00Z"].active_contract_id == "BRK2@MOEX"
    assert by_ts["2022-03-21T10:15:00Z"].active_contract_id == "BRM2@MOEX"
    assert by_ts["2022-03-21T10:15:00Z"].previous_contract_id == "BRK2@MOEX"
    assert by_ts["2022-03-21T10:15:00Z"].roll_event_id == "CFR-cf_loader_v1-BR-15m-0001"
    assert by_ts["2022-03-21T10:15:00Z"].is_first_bar_after_roll is True
    assert {entry.active_contract_id for entry in roll_map} == {"BRK2@MOEX", "BRM2@MOEX"}


def test_load_continuous_front_fails_closed_when_dataset_has_no_rows(tmp_path: Path) -> None:
    output_dir = tmp_path / "front"
    contract = continuous_front_store_contract()
    write_delta_table_rows(
        table_path=output_dir / "continuous_front_bars.delta",
        rows=[],
        columns=dict(contract["continuous_front_bars"]["columns"]),
    )

    with pytest.raises(RuntimeError, match="has no rows"):
        load_continuous_front_as_research_context(
            continuous_front_output_dir=output_dir,
            dataset_version="missing-v1",
        )
