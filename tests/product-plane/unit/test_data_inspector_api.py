from __future__ import annotations

from importlib.resources import files
from pathlib import Path

from fastapi.testclient import TestClient

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.interfaces.data_inspector import (
    DATA_INSPECTOR_LAYERS,
    create_app,
)


def _write_delta(table_path: Path, rows: list[dict[str, object]], columns: dict[str, str]) -> None:
    write_delta_table_rows(table_path=table_path, rows=rows, columns=columns)


def _read_packaged_html() -> str:
    return (
        files("trading_advisor_3000")
        .joinpath("product_plane")
        .joinpath("interfaces")
        .joinpath("static")
        .joinpath("data_inspector.html")
        .read_text(encoding="utf-8")
    )


def test_data_inspector_registry_matches_current_data_layers() -> None:
    assert set(DATA_INSPECTOR_LAYERS) == {
        "raw",
        "canonical",
        "indicators",
        "derived_indicators",
    }
    assert "contour_id" in DATA_INSPECTOR_LAYERS["indicators"].default_columns
    assert "series_mode" in DATA_INSPECTOR_LAYERS["derived_indicators"].default_columns
    assert DATA_INSPECTOR_LAYERS["raw"].scan_table_timeframes
    assert DATA_INSPECTOR_LAYERS["canonical"].scan_table_timeframes


def test_data_inspector_html_asset_is_packaged() -> None:
    html = _read_packaged_html()

    assert "<!doctype html>" in html
    assert "TA3000 Data Inspector" in html


def test_data_inspector_serves_html_with_direct_file_guidance(tmp_path: Path) -> None:
    client = TestClient(create_app(data_root=tmp_path))
    response = client.get("/")

    assert response.status_code == 200
    assert 'window.location.protocol === "file:"' in response.text
    assert "http://127.0.0.1:8765/" in response.text


def test_data_inspector_options_match_current_timeframes_and_parameters(tmp_path: Path) -> None:
    _write_delta(
        tmp_path / DATA_INSPECTOR_LAYERS["raw"].options_relative_path,
        [
            {
                "internal_id": "FUT_BR",
                "instrument_id": "FUT_BR",
                "contract_ids_json": '["BRM6@MOEX", "BRN6@MOEX"]',
                "timeframes_json": '["15m"]',
            }
        ],
        {
            "internal_id": "string",
            "instrument_id": "string",
            "contract_ids_json": "string",
            "timeframes_json": "string",
        },
    )
    _write_delta(
        tmp_path / DATA_INSPECTOR_LAYERS["raw"].relative_path,
        [
            {
                "internal_id": "FUT_BR",
                "finam_symbol": "BRM6@MOEX",
                "timeframe": "1m",
                "ts_open": "2026-05-20T09:00:00Z",
                "close": 104.2,
            },
            {
                "internal_id": "FUT_BR",
                "finam_symbol": "BRM6@MOEX",
                "timeframe": "1h",
                "ts_open": "2026-05-20T09:00:00Z",
                "close": 104.4,
            },
        ],
        {
            "internal_id": "string",
            "finam_symbol": "string",
            "timeframe": "string",
            "ts_open": "timestamp",
            "close": "double",
        },
    )
    _write_delta(
        tmp_path / DATA_INSPECTOR_LAYERS["canonical"].relative_path,
        [
            {
                "contract_id": "BRM6@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "1m",
                "ts": "2026-05-20T09:00:00Z",
                "close": 104.4,
            },
            {
                "contract_id": "BRM6@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "5m",
                "ts": "2026-05-20T09:00:00Z",
                "close": 104.6,
            },
        ],
        {
            "contract_id": "string",
            "instrument_id": "string",
            "timeframe": "string",
            "ts": "timestamp",
            "close": "double",
        },
    )
    _write_delta(
        tmp_path / DATA_INSPECTOR_LAYERS["indicators"].relative_path,
        [
            {
                "dataset_version": "moex-current",
                "contour_id": "pit_active_front",
                "series_mode": "continuous_front",
                "series_id": "FUT_BR",
                "indicator_set_version": "indicators-v1",
                "profile_version": "core_v1",
                "contract_id": "BRM6@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-05-20T09:00:00Z",
                "rsi_14": 53.5,
                "vp_poc_price": 104.0,
            }
        ],
        {
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
            "rsi_14": "double",
            "vp_poc_price": "double",
        },
    )
    _write_delta(
        tmp_path / DATA_INSPECTOR_LAYERS["derived_indicators"].relative_path,
        [
            {
                "dataset_version": "moex-current",
                "contour_id": "pit_active_front",
                "series_mode": "continuous_front",
                "series_id": "FUT_BR",
                "indicator_set_version": "indicators-v1",
                "derived_indicator_set_version": "derived-v1",
                "profile_version": "core_v1",
                "contract_id": "BRM6@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-05-20T09:00:00Z",
                "distance_to_session_vwap": 1.25,
            }
        ],
        {
            "dataset_version": "string",
            "contour_id": "string",
            "series_mode": "string",
            "series_id": "string",
            "indicator_set_version": "string",
            "derived_indicator_set_version": "string",
            "profile_version": "string",
            "contract_id": "string",
            "instrument_id": "string",
            "timeframe": "string",
            "ts": "timestamp",
            "distance_to_session_vwap": "double",
        },
    )

    with TestClient(create_app(data_root=tmp_path)) as client:
        raw_options = client.get("/api/data-inspector/options", params={"layer": "raw"}).json()
        assert raw_options["timeframes"] == ["1h", "1m"]
        assert raw_options["parameter_values_by_instrument"]["finam_symbol"] == {
            "FUT_BR": ["BRM6@MOEX", "BRN6@MOEX"],
        }

        canonical_options = client.get(
            "/api/data-inspector/options", params={"layer": "canonical"}
        ).json()
        assert canonical_options["timeframes"] == ["1m", "5m"]

        indicator_options = client.get(
            "/api/data-inspector/options", params={"layer": "indicators"}
        ).json()
        assert indicator_options["parameter_values"]["contour_id"] == ["pit_active_front"]
        assert indicator_options["parameter_values"]["series_mode"] == ["continuous_front"]

        derived_options = client.get(
            "/api/data-inspector/options", params={"layer": "derived_indicators"}
        ).json()
        assert derived_options["parameter_values"]["derived_indicator_set_version"] == [
            "derived-v1",
        ]


def test_data_inspector_sorts_rows_by_time_desc_by_default_and_selected_column(
    tmp_path: Path,
) -> None:
    _write_delta(
        tmp_path / DATA_INSPECTOR_LAYERS["canonical"].relative_path,
        [
            {
                "contract_id": "BRM6@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-05-20T09:00:00Z",
                "close": 104.0,
            },
            {
                "contract_id": "BRM6@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-05-20T09:30:00Z",
                "close": 101.0,
            },
            {
                "contract_id": "BRM6@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-05-20T09:15:00Z",
                "close": 103.0,
            },
        ],
        {
            "contract_id": "string",
            "instrument_id": "string",
            "timeframe": "string",
            "ts": "timestamp",
            "close": "double",
        },
    )

    with TestClient(create_app(data_root=tmp_path)) as client:
        default_rows = client.get(
            "/api/data-inspector/rows",
            params={
                "layer": "canonical",
                "instrument": "FUT_BR",
                "timeframe": "15m",
                "series": "BRM6@MOEX",
                "limit": 2,
            },
        ).json()
        assert default_rows["sort"] == {"column": "ts", "direction": "desc"}
        assert [row["ts"] for row in default_rows["rows"]] == [
            "2026-05-20T09:30:00Z",
            "2026-05-20T09:15:00Z",
        ]

        close_sorted_rows = client.get(
            "/api/data-inspector/rows",
            params={
                "layer": "canonical",
                "instrument": "FUT_BR",
                "timeframe": "15m",
                "series": "BRM6@MOEX",
                "sort_by": "close",
                "sort_direction": "asc",
            },
        ).json()
        assert close_sorted_rows["sort"] == {"column": "close", "direction": "asc"}
        assert [row["close"] for row in close_sorted_rows["rows"]] == [
            101.0,
            103.0,
            104.0,
        ]

        missing_sort = client.get(
            "/api/data-inspector/rows",
            params={
                "layer": "canonical",
                "instrument": "FUT_BR",
                "timeframe": "15m",
                "sort_by": "missing_column",
            },
        )
        assert missing_sort.status_code == 400
