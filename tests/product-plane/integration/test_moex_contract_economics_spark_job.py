from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.moex.economics import (
    moex_economics_store_contract,
)
from trading_advisor_3000.product_plane.data_plane.schemas.delta import (
    historical_data_delta_schema_manifest,
)
from trading_advisor_3000.spark_jobs import research_bar_views_job
from trading_advisor_3000.spark_jobs.canonical_bars_job import _create_spark_session
from trading_advisor_3000.spark_jobs.moex_contract_economics_job import (
    run_moex_contract_economics_spark_job,
)


def _columns(table_name: str) -> dict[str, str]:
    return dict(moex_economics_store_contract()[table_name]["columns"])


def _configure_windows_spark_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hadoop_home = Path("D:/CodexHome/runtime/hadoop-winutils-3.3.6")
    if not (hadoop_home / "bin" / "winutils.exe").exists():
        hadoop_home = Path("D:/CodexHome/runtime/hadoop-winutils")
    if not (hadoop_home / "bin" / "hadoop.dll").exists():
        hadoop_home = Path.cwd() / ".tmp" / "hadoop-winutils"
    monkeypatch.setenv("HADOOP_HOME", hadoop_home.as_posix())
    monkeypatch.setenv("TA3000_SPARK_RUNTIME_ROOT", (tmp_path / "spark-runtime").as_posix())


def _write_single_day_raw_economics_fixture(
    *,
    raw_root: Path,
    trade_date: str,
    risk_trade_date: str | None = None,
    contract_id: str = "BRN6",
    assetcode: str = "BR",
    quote_currency: str = "USD",
    fx_rate: float = 71.9077,
    settle_price: float = 93.99,
    official_margin: float = 17_721.61,
    official_step_price: float | None = None,
    min_step: float = 0.01,
    lot_volume: float = 10.0,
    historical_market: dict[str, object] | None = None,
    fx_clearing_type: str = "tc",
    expiration_date: str = "2026-07-15",
    mode: str = "overwrite",
    write_shared_inputs: bool = True,
) -> dict[str, Path]:
    risk_trade_date = risk_trade_date or trade_date
    raw_contracts = raw_root / "raw_moex_contract_securities.delta"
    raw_fx = raw_root / "raw_moex_indicative_fx_rates.delta"
    raw_limits = raw_root / "raw_moex_rms_limits.delta"
    raw_staticparams = raw_root / "raw_moex_rms_staticparams.delta"
    write_delta_table_rows(
        table_path=raw_contracts,
        columns=_columns("raw_moex_contract_securities"),
        rows=[
            {
                "source_id": "moex_iss_forts_securities",
                "source_url": "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json",
                "source_document_id": f"{contract_id}-{trade_date}",
                "source_document_hash": f"contracts-hash-{trade_date}-{contract_id}",
                "fetched_at_utc": f"{trade_date}T19:01:00Z",
                "engine": "futures",
                "market": "forts",
                "board": "RFUD",
                "moex_secid": contract_id,
                "trade_date": trade_date,
                "assetcode": assetcode,
                "contract_shortname": contract_id,
                "last_trade_date": expiration_date,
                "last_del_date": expiration_date,
                "min_step": min_step,
                "lot_volume": lot_volume,
                "official_step_price": (
                    None
                    if historical_market is not None
                    else min_step * lot_volume * fx_rate
                    if official_step_price is None
                    else official_step_price
                ),
                "official_initial_margin": official_margin,
                "last_settle_price": settle_price,
                "raw_payload_json": json.dumps(
                    {
                        "history": historical_market,
                        "description": {
                            "ASSETCODE": assetcode,
                            "LOTSIZE": str(lot_volume),
                        },
                        "inference": {
                            "min_step_source": "current_secid",
                            "lot_volume_source": "description_lotsize",
                        },
                    }
                    if historical_market is not None
                    else {
                        "SECID": contract_id,
                        "ASSETCODE": assetcode,
                        "MINSTEP": min_step,
                        "LOTVOLUME": lot_volume,
                        "LASTSETTLEPRICE": settle_price,
                        "INITIALMARGIN": official_margin,
                        "CURRENCYID": quote_currency,
                        "MATDATE": expiration_date,
                    }
                ),
            }
        ],
        mode=mode,
    )
    paths = {
        "contracts": raw_contracts,
        "fx": raw_fx,
        "limits": raw_limits,
        "staticparams": raw_staticparams,
    }
    if not write_shared_inputs:
        return paths
    write_delta_table_rows(
        table_path=raw_fx,
        columns=_columns("raw_moex_indicative_fx_rates"),
        rows=[
            {
                "source_id": "moex_iss_indicativerates",
                "source_url": "https://iss.moex.com/iss/statistics/engines/futures/markets/indicativerates/securities.json",
                "source_document_id": f"USD-RUB-{trade_date}-{fx_clearing_type}",
                "source_document_hash": f"fx-hash-{trade_date}",
                "fetched_at_utc": f"{trade_date}T19:02:00Z",
                "trade_date": trade_date,
                "trade_time": "19:00:00",
                "fx_pair": "USD/RUB",
                "clearing_type": fx_clearing_type,
                "rate": fx_rate,
                "raw_payload_json": json.dumps(
                    {
                        "tradedate": trade_date,
                        "tradetime": "19:00:00",
                        "secid": "USD/RUB",
                        "rate": fx_rate,
                        "clearing": fx_clearing_type,
                    }
                ),
            }
        ],
        mode=mode,
    )
    write_delta_table_rows(
        table_path=raw_limits,
        columns=_columns("raw_moex_rms_limits"),
        rows=[
            {
                "source_id": "ncc_derivatives_limits",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": f"{assetcode}-{risk_trade_date}-limits",
                "source_document_hash": f"limits-hash-{risk_trade_date}-{assetcode}",
                "fetched_at_utc": f"{risk_trade_date}T19:03:00Z",
                "trade_date": risk_trade_date,
                "assetcode": assetcode,
                "mr1": 0.12,
                "mr2": 0.0,
                "mr3": 0.0,
                "lk1": 0.0,
                "lk2": 0.0,
                "title": assetcode,
                "group_title": "Fixture",
                "update_time": f"{risk_trade_date}T19:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": assetcode, "MR1": 0.12}),
            }
        ],
        mode=mode,
    )
    write_delta_table_rows(
        table_path=raw_staticparams,
        columns=_columns("raw_moex_rms_staticparams"),
        rows=[
            {
                "source_id": "ncc_derivatives_staticparams",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": f"{assetcode}-{risk_trade_date}-staticparams",
                "source_document_hash": f"staticparams-hash-{risk_trade_date}-{assetcode}",
                "fetched_at_utc": f"{risk_trade_date}T19:03:00Z",
                "trade_date": risk_trade_date,
                "assetcode": assetcode,
                "radius_pct": 15.0,
                "update_time": f"{risk_trade_date}T19:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": assetcode, "RADIUS": 15.0}),
            }
        ],
        mode=mode,
    )
    return paths


def test_contract_economics_spark_job_materializes_canonical_side_tables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)

    raw_root = tmp_path / "raw"
    raw_contracts = raw_root / "raw_moex_contract_securities.delta"
    raw_fx = raw_root / "raw_moex_indicative_fx_rates.delta"
    raw_limits = raw_root / "raw_moex_rms_limits.delta"
    raw_staticparams = raw_root / "raw_moex_rms_staticparams.delta"
    write_delta_table_rows(
        table_path=raw_contracts,
        columns=_columns("raw_moex_contract_securities"),
        rows=[
            {
                "source_id": "moex_iss_forts_securities",
                "source_url": "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json",
                "source_document_id": "BRN6-2026-06-11",
                "source_document_hash": "contracts-hash",
                "fetched_at_utc": "2026-06-11T19:01:00Z",
                "engine": "futures",
                "market": "forts",
                "board": "RFUD",
                "moex_secid": "BRN6",
                "trade_date": "2026-06-11",
                "assetcode": "BR",
                "contract_shortname": "Brent",
                "last_trade_date": "2026-07-15",
                "last_del_date": "2026-07-15",
                "min_step": 0.01,
                "lot_volume": 10,
                "official_step_price": 7.19077,
                "official_initial_margin": 17_721.61,
                "last_settle_price": 93.99,
                "raw_payload_json": json.dumps(
                    {
                        "SECID": "BRN6",
                        "ASSETCODE": "BR",
                        "MINSTEP": 0.01,
                        "LOTVOLUME": 10,
                        "STEPPRICE": 7.19077,
                        "INITIALMARGIN": 17_721.61,
                        "LASTSETTLEPRICE": 93.99,
                        "CURRENCYID": "USD",
                        "MATDATE": "2026-07-15",
                    }
                ),
            }
        ],
    )
    write_delta_table_rows(
        table_path=raw_fx,
        columns=_columns("raw_moex_indicative_fx_rates"),
        rows=[
            {
                "source_id": "moex_iss_indicativerates",
                "source_url": "https://iss.moex.com/iss/statistics/engines/futures/markets/indicativerates/securities.json",
                "source_document_id": "USD-RUB-2026-06-11-mc",
                "source_document_hash": "fx-hash-mc",
                "fetched_at_utc": "2026-06-11T14:02:00Z",
                "trade_date": "2026-06-11",
                "trade_time": "14:00:00",
                "fx_pair": "USD/RUB",
                "clearing_type": "mc",
                "rate": 70.0000,
                "raw_payload_json": json.dumps(
                    {
                        "tradedate": "2026-06-11",
                        "tradetime": "14:00:00",
                        "secid": "USD/RUB",
                        "rate": 70.0000,
                        "clearing": "mc",
                    }
                ),
            },
            {
                "source_id": "moex_iss_indicativerates",
                "source_url": "https://iss.moex.com/iss/statistics/engines/futures/markets/indicativerates/securities.json",
                "source_document_id": "USD-RUB-2026-06-11-tc",
                "source_document_hash": "fx-hash",
                "fetched_at_utc": "2026-06-11T19:02:00Z",
                "trade_date": "2026-06-11",
                "trade_time": "19:00:00",
                "fx_pair": "USD/RUB",
                "clearing_type": "tc",
                "rate": 71.9077,
                "raw_payload_json": json.dumps(
                    {
                        "tradedate": "2026-06-11",
                        "tradetime": "19:00:00",
                        "secid": "USD/RUB",
                        "rate": 71.9077,
                        "clearing": "tc",
                    }
                ),
            },
        ],
    )
    write_delta_table_rows(
        table_path=raw_limits,
        columns=_columns("raw_moex_rms_limits"),
        rows=[
            {
                "source_id": "ncc_derivatives_limits",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": "BR-2026-06-11-limits",
                "source_document_hash": "limits-hash",
                "fetched_at_utc": "2026-06-11T19:03:00Z",
                "trade_date": "2026-06-11",
                "assetcode": "BR",
                "mr1": 0.12,
                "mr2": 0.0,
                "mr3": 0.0,
                "lk1": 0.0,
                "lk2": 0.0,
                "title": "Brent",
                "group_title": "Oil",
                "update_time": "2026-06-11T19:00:00Z",
                "raw_payload_json": json.dumps(
                    {
                        "ASSETCODE": "BR",
                        "MR1": 0.12,
                    }
                ),
            }
        ],
    )
    write_delta_table_rows(
        table_path=raw_staticparams,
        columns=_columns("raw_moex_rms_staticparams"),
        rows=[
            {
                "source_id": "ncc_derivatives_staticparams",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": "BR-2026-06-11-staticparams",
                "source_document_hash": "staticparams-hash",
                "fetched_at_utc": "2026-06-11T19:03:00Z",
                "trade_date": "2026-06-11",
                "assetcode": "BR",
                "radius_pct": None,
                "update_time": "2026-06-11T19:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": "BR"}),
            }
        ],
    )

    output_dir = tmp_path / "canonical"
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_contracts,
        raw_fx_rates_path=raw_fx,
        raw_rms_limits_path=raw_limits,
        raw_rms_staticparams_path=raw_staticparams,
        output_dir=output_dir,
        run_id="economics-fixture",
        report_path=tmp_path / "evidence" / "contract-economics-report.json",
    )

    assert report["status"] == "PASS"
    assert report["row_counts"]["canonical_fx_rates"] == 4
    assert report["row_counts"]["canonical_asset_risk_parameters"] == 1
    assert report["row_counts"]["canonical_contract_economics"] == 1
    assert report["defaulted_radius_rows"] == 1
    assert report["runtime_profile"]["transform_runtime"] == "spark_sql_delta"
    assert report["runtime_profile"]["delta_writer"] == "spark_delta"
    assert (output_dir / "canonical_contract_economics.delta" / "_delta_log").exists()
    rows = read_delta_table_rows(output_dir / "canonical_contract_economics.delta", limit=5)
    assert len(rows) == 1
    row = rows[0]
    assert row["contract_id"] == "BRN6"
    assert row["clearing_type"] == "mc"
    assert row["step_price_rub"] == pytest.approx(7.19077)
    assert row["margin_required_estimate"] == pytest.approx(17_721.61)


def test_contract_economics_spark_job_uses_official_stepprice_for_si(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2026-07-09",
        contract_id="SiU6",
        assetcode="SI",
        quote_currency="RUB",
        fx_rate=76.4026,
        settle_price=77_426.0,
        official_margin=12_316.83,
        official_step_price=1.0,
        min_step=1.0,
        lot_volume=1_000.0,
    )

    output_dir = tmp_path / "canonical"
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        run_id="economics-si-stepprice-fixture",
    )

    row = read_delta_table_rows(output_dir / "canonical_contract_economics.delta", limit=5)[0]
    assert report["status"] == "PASS"
    assert row["step_price_rub"] == pytest.approx(1.0)
    assert row["radius_pct"] == pytest.approx(15.0)
    assert row["radius_source"] == "source"
    assert row["margin_formula_base"] == pytest.approx(9_291.12)
    assert row["margin_required_estimate"] == pytest.approx(12_316.83)


def test_contract_economics_spark_job_zeros_radius_for_pure_rub_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2026-07-09",
        contract_id="SBERU6",
        assetcode="SBER",
        quote_currency="RUB",
        fx_rate=1.0,
        settle_price=100.0,
        official_margin=1.0,
        official_step_price=1.0,
        min_step=1.0,
        lot_volume=1.0,
    )

    output_dir = tmp_path / "canonical"
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        run_id="economics-pure-rub-radius-fixture",
    )

    row = read_delta_table_rows(output_dir / "canonical_contract_economics.delta", limit=1)[0]
    assert report["status"] == "PASS"
    assert row["radius_pct"] == pytest.approx(0.0)
    assert row["radius_source"] == "not_applicable_pure_rub"
    assert row["margin_formula_base"] == pytest.approx(12.0)
    assert row["margin_radius_adjusted"] == pytest.approx(12.0)
    assert row["margin_required_estimate"] == pytest.approx(1.0)


def test_contract_economics_spark_job_reconstructs_historical_br_stepprice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2024-06-14",
        contract_id="BRN4",
        assetcode="BR",
        quote_currency="USD",
        fx_rate=89.0658,
        fx_clearing_type="mc",
        settle_price=82.84,
        official_margin=17_721.61,
        official_step_price=None,
        min_step=0.01,
        lot_volume=10.0,
        historical_market={
            "VALUE": 10_595_079_985.34,
            "VOLUME": 143_606,
            "WAPRICE": 82.84,
        },
    )

    output_dir = tmp_path / "canonical"
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        run_id="economics-historical-br-stepprice",
    )

    row = read_delta_table_rows(output_dir / "canonical_contract_economics.delta", limit=1)[0]
    source_flags = json.loads(str(row["source_flags_json"]))
    assert report["status"] == "PASS"
    assert row["official_step_price"] is None
    assert row["lot_volume"] == pytest.approx(10.0)
    assert row["step_price_rub"] == pytest.approx(8.90658)
    assert row["tick_value_currency"] == pytest.approx(0.1)
    assert source_flags["step_price_source"] == "contract_parameter_regime"
    assert source_flags["step_price_rule_id"] == "BR-2022-2026-v1"
    profile = report["step_price_rule_profiles"][0]
    assert profile["min_step"] == pytest.approx(0.01)
    assert profile["lot_volume"] == pytest.approx(10.0)
    assert profile["tick_value_quote"] == pytest.approx(0.1)
    assert set(profile) == {
        "rule_id",
        "assetcode",
        "quote_currency",
        "min_step",
        "lot_volume",
        "tick_value_quote",
        "rule_rows",
        "validation_rows",
        "validation_p95_relative_error",
        "validation_max_relative_error",
    }
    assert profile["validation_rows"] == 1


def test_contract_economics_spark_job_calibrates_history_by_asset_and_maturity_rank(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    raw_root = tmp_path / "raw"

    fixture = {
        "assetcode": "BR",
        "quote_currency": "USD",
        "fx_rate": 100.0,
        "settle_price": 100.0,
        "official_step_price": 10.0,
        "min_step": 0.01,
        "lot_volume": 10.0,
        "fx_clearing_type": "mc",
    }
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=raw_root,
        trade_date="2026-07-09",
        contract_id="BRN6",
        official_margin=27_600.0,
        expiration_date="2026-07-15",
        **fixture,
    )
    _write_single_day_raw_economics_fixture(
        raw_root=raw_root,
        trade_date="2026-07-09",
        contract_id="BRU6",
        official_margin=41_400.0,
        expiration_date="2026-09-17",
        mode="append",
        write_shared_inputs=False,
        **fixture,
    )

    historical_market = {"VALUE": 100_000.0, "VOLUME": 1.0, "WAPRICE": 100.0}
    for index, (contract_id, expiration_date) in enumerate(
        (
            ("BRN4", "2024-07-18"),
            ("BRU4", "2024-09-19"),
            ("BRZ4", "2024-12-19"),
        )
    ):
        _write_single_day_raw_economics_fixture(
            raw_root=raw_root,
            trade_date="2024-06-14",
            contract_id=contract_id,
            official_margin=0.0,
            expiration_date=expiration_date,
            historical_market=historical_market,
            mode="append",
            write_shared_inputs=index == 0,
            **fixture,
        )

    output_dir = tmp_path / "canonical"
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        run_id="economics-margin-calibration",
    )

    rows = read_delta_table_rows(output_dir / "canonical_contract_economics.delta", limit=10)
    historical = {
        int(row["maturity_rank"]): row
        for row in rows
        if str(row["economics_session_date"]) == "2024-06-14"
    }
    assert report["status"] == "PASS"
    assert report["margin_calibration_as_of_date"] == "2026-07-09"
    assert report["calibrated_margin_rows"] == 3
    assert report["nonzero_margin_buffer_rows"] == 0
    assert historical[1]["margin_calibration_factor"] == pytest.approx(2.0)
    assert historical[1]["margin_calibration_rank"] == 1
    assert historical[1]["margin_calibration_source"] == "latest_official_asset_rank"
    assert historical[1]["margin_required_estimate"] == pytest.approx(27_600.0)
    assert historical[2]["margin_calibration_factor"] == pytest.approx(3.0)
    assert historical[2]["margin_calibration_rank"] == 2
    assert historical[2]["margin_required_estimate"] == pytest.approx(41_400.0)
    assert historical[3]["margin_calibration_factor"] == pytest.approx(3.0)
    assert historical[3]["margin_calibration_rank"] == 2
    assert historical[3]["margin_calibration_source"] == "latest_official_nearest_rank"
    assert historical[3]["margin_required_estimate"] == pytest.approx(41_400.0)
    assert all(row["margin_buffer_pct"] == pytest.approx(0.0) for row in rows)


def test_contract_economics_spark_job_rejects_historical_lotvolume_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2024-06-14",
        contract_id="BRN4",
        assetcode="BR",
        quote_currency="USD",
        fx_rate=89.0658,
        fx_clearing_type="mc",
        settle_price=82.84,
        official_margin=17_721.61,
        official_step_price=None,
        min_step=0.01,
        lot_volume=11.0,
        historical_market={
            "VALUE": 10_595_079_985.34,
            "VOLUME": 143_606,
            "WAPRICE": 82.84,
        },
    )

    with pytest.raises(
        RuntimeError,
        match=r"LOTVOLUME expected=10\.0 observed=11\.0",
    ):
        run_moex_contract_economics_spark_job(
            raw_contract_specs_path=raw_paths["contracts"],
            raw_fx_rates_path=raw_paths["fx"],
            raw_rms_limits_path=raw_paths["limits"],
            raw_rms_staticparams_path=raw_paths["staticparams"],
            output_dir=tmp_path / "canonical",
            run_id="economics-historical-br-lotvolume-mismatch",
        )


def test_contract_economics_spark_job_rejects_missing_latest_stepprice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2026-07-09",
        official_step_price=0.0,
    )

    with pytest.raises(RuntimeError, match=r"STEPPRICE=1"):
        run_moex_contract_economics_spark_job(
            raw_contract_specs_path=raw_paths["contracts"],
            raw_fx_rates_path=raw_paths["fx"],
            raw_rms_limits_path=raw_paths["limits"],
            raw_rms_staticparams_path=raw_paths["staticparams"],
            output_dir=tmp_path / "canonical",
            run_id="economics-missing-latest-stepprice",
        )


def test_contract_economics_spark_job_deduplicates_rms_snapshots_by_update_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)

    trade_date = "2026-01-05"
    assetcode = "BR"
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date=trade_date,
        assetcode=assetcode,
        official_margin=20_000.0,
        fx_clearing_type="mc",
    )
    write_delta_table_rows(
        table_path=raw_paths["limits"],
        columns=_columns("raw_moex_rms_limits"),
        mode="append",
        rows=[
            {
                "source_id": "ncc_derivatives_limits",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": f"{assetcode}-{trade_date}-limits-old",
                "source_document_hash": "limits-hash-old",
                "fetched_at_utc": f"{trade_date}T09:03:00Z",
                "trade_date": trade_date,
                "assetcode": assetcode,
                "mr1": 0.10,
                "mr2": 0.0,
                "mr3": 0.0,
                "lk1": 0.0,
                "lk2": 0.0,
                "title": assetcode,
                "group_title": "Fixture",
                "update_time": f"{trade_date}T09:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": assetcode, "MR1": 0.10}),
            },
            {
                "source_id": "ncc_derivatives_limits",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": f"{assetcode}-{trade_date}-limits-latest",
                "source_document_hash": "limits-hash-latest",
                "fetched_at_utc": f"{trade_date}T20:03:00Z",
                "trade_date": trade_date,
                "assetcode": assetcode,
                "mr1": 0.15,
                "mr2": 0.0,
                "mr3": 0.0,
                "lk1": 0.0,
                "lk2": 0.0,
                "title": assetcode,
                "group_title": "Fixture",
                "update_time": f"{trade_date}T20:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": assetcode, "MR1": 0.15}),
            },
        ],
    )
    write_delta_table_rows(
        table_path=raw_paths["staticparams"],
        columns=_columns("raw_moex_rms_staticparams"),
        mode="append",
        rows=[
            {
                "source_id": "ncc_derivatives_staticparams",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": f"{assetcode}-{trade_date}-staticparams-old",
                "source_document_hash": "staticparams-hash-old",
                "fetched_at_utc": f"{trade_date}T09:03:00Z",
                "trade_date": trade_date,
                "assetcode": assetcode,
                "radius_pct": 11.0,
                "update_time": f"{trade_date}T09:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": assetcode, "RADIUS": 11.0}),
            },
            {
                "source_id": "ncc_derivatives_staticparams",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": f"{assetcode}-{trade_date}-staticparams-latest",
                "source_document_hash": "staticparams-hash-latest",
                "fetched_at_utc": f"{trade_date}T20:03:00Z",
                "trade_date": trade_date,
                "assetcode": assetcode,
                "radius_pct": 21.0,
                "update_time": f"{trade_date}T20:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": assetcode, "RADIUS": 21.0}),
            },
        ],
    )

    output_dir = tmp_path / "canonical"
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        run_id="economics-rms-dedupe-fixture",
        report_path=tmp_path / "evidence" / "contract-economics-report.json",
    )

    assert report["status"] == "PASS"
    assert report["row_counts"]["canonical_asset_risk_parameters"] == 1
    assert report["row_counts"]["canonical_contract_economics"] == 1

    risk_rows = read_delta_table_rows(output_dir / "canonical_asset_risk_parameters.delta", limit=5)
    assert len(risk_rows) == 1
    risk_row = risk_rows[0]
    assert risk_row["mr1"] == pytest.approx(0.15)
    assert risk_row["radius_pct"] == pytest.approx(21.0)
    assert risk_row["source_limits_hash"] == "limits-hash-latest"
    assert risk_row["source_staticparams_hash"] == "staticparams-hash-latest"

    economics_rows = read_delta_table_rows(
        output_dir / "canonical_contract_economics.delta", limit=5
    )
    assert len(economics_rows) == 1
    economics_row = economics_rows[0]
    assert economics_row["mr1"] == pytest.approx(0.15)
    assert economics_row["radius_pct"] == pytest.approx(21.0)
    assert economics_row["margin_formula_base"] == pytest.approx(93.99 * 719.077 * 0.15)


def test_contract_economics_spark_job_closes_effective_intervals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    raw_root = tmp_path / "raw"
    raw_contracts = raw_root / "raw_moex_contract_securities.delta"
    raw_fx = raw_root / "raw_moex_indicative_fx_rates.delta"
    raw_limits = raw_root / "raw_moex_rms_limits.delta"
    raw_staticparams = raw_root / "raw_moex_rms_staticparams.delta"
    canonical_calendar = tmp_path / "canonical_session_calendar.delta"

    contract_rows = []
    fx_rows = []
    limit_rows = []
    static_rows = []
    for trade_date, settle_price, margin, fx_rate, official_step_price in (
        ("2026-06-11", 93.99, 17_721.61, 71.9077, None),
        ("2026-06-12", 94.25, 17_900.00, 72.1000, 7.21),
    ):
        contract_rows.append(
            {
                "source_id": "moex_iss_forts_securities",
                "source_url": "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json",
                "source_document_id": f"BRN6-{trade_date}",
                "source_document_hash": f"contracts-hash-{trade_date}",
                "fetched_at_utc": f"{trade_date}T19:01:00Z",
                "engine": "futures",
                "market": "forts",
                "board": "RFUD",
                "moex_secid": "BRN6",
                "trade_date": trade_date,
                "assetcode": "BR",
                "contract_shortname": "Brent",
                "last_trade_date": "2026-07-15",
                "last_del_date": "2026-07-15",
                "min_step": 0.01,
                "lot_volume": 10,
                "official_step_price": official_step_price,
                "official_initial_margin": margin,
                "last_settle_price": settle_price,
                "raw_payload_json": json.dumps({"SECID": "BRN6", "ASSETCODE": "BR"}),
            }
        )
        fx_rows.append(
            {
                "source_id": "moex_iss_indicativerates",
                "source_url": "https://iss.moex.com/iss/statistics/engines/futures/markets/indicativerates/securities.json",
                "source_document_id": f"USD-RUB-{trade_date}-mc",
                "source_document_hash": f"fx-hash-{trade_date}",
                "fetched_at_utc": f"{trade_date}T19:02:00Z",
                "trade_date": trade_date,
                "trade_time": "19:00:00",
                "fx_pair": "USD/RUB",
                "clearing_type": "mc",
                "rate": fx_rate,
                "raw_payload_json": json.dumps(
                    {"tradedate": trade_date, "secid": "USD/RUB", "rate": fx_rate}
                ),
            }
        )
        limit_rows.append(
            {
                "source_id": "ncc_derivatives_limits",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": f"BR-{trade_date}-limits",
                "source_document_hash": f"limits-hash-{trade_date}",
                "fetched_at_utc": f"{trade_date}T19:03:00Z",
                "trade_date": trade_date,
                "assetcode": "BR",
                "mr1": 0.12,
                "mr2": 0.0,
                "mr3": 0.0,
                "lk1": 0.0,
                "lk2": 0.0,
                "title": "Brent",
                "group_title": "Oil",
                "update_time": f"{trade_date}T19:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": "BR", "MR1": 0.12}),
            }
        )
        static_rows.append(
            {
                "source_id": "ncc_derivatives_staticparams",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": f"BR-{trade_date}-staticparams",
                "source_document_hash": f"staticparams-hash-{trade_date}",
                "fetched_at_utc": f"{trade_date}T19:03:00Z",
                "trade_date": trade_date,
                "assetcode": "BR",
                "radius_pct": 15.0,
                "update_time": f"{trade_date}T19:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": "BR", "RADIUS": 15.0}),
            }
        )

    fx_rows = [row for row in fx_rows if row["trade_date"] == "2026-06-12"]
    limit_rows = [row for row in limit_rows if row["trade_date"] == "2026-06-12"]
    static_rows = [row for row in static_rows if row["trade_date"] == "2026-06-12"]

    write_delta_table_rows(
        table_path=raw_contracts,
        columns=_columns("raw_moex_contract_securities"),
        rows=contract_rows,
    )
    write_delta_table_rows(
        table_path=raw_fx,
        columns=_columns("raw_moex_indicative_fx_rates"),
        rows=fx_rows,
    )
    write_delta_table_rows(
        table_path=raw_limits,
        columns=_columns("raw_moex_rms_limits"),
        rows=limit_rows,
    )
    write_delta_table_rows(
        table_path=raw_staticparams,
        columns=_columns("raw_moex_rms_staticparams"),
        rows=static_rows,
    )
    write_delta_table_rows(
        table_path=canonical_calendar,
        columns={
            "instrument_id": "string",
            "timeframe": "string",
            "session_date": "date",
            "session_open_ts": "timestamp",
            "session_close_ts": "timestamp",
            "session_class": "string",
        },
        rows=[
            {
                "instrument_id": "FUT_BR",
                "timeframe": "1d",
                "session_date": "2026-06-16",
                "session_open_ts": "2026-06-16T07:00:00Z",
                "session_close_ts": "2026-06-16T21:00:00Z",
                "session_class": "regular",
            }
        ],
    )

    output_dir = tmp_path / "canonical"
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_contracts,
        raw_fx_rates_path=raw_fx,
        raw_rms_limits_path=raw_limits,
        raw_rms_staticparams_path=raw_staticparams,
        output_dir=output_dir,
        canonical_session_calendar_path=canonical_calendar,
        run_id="economics-intervals",
    )

    assert report["status"] == "PASS"
    assert report["step_price_unavailable_rows"] == 1
    assert report["missing_economics_rows"] == 0
    assert report["formula_margin_dominates_rows"] == 0
    assert report["historical_missing_input_counts"]["FX"] == 1
    assert report["historical_missing_input_counts"]["MR1"] == 1
    assert report["zero_or_negative_effective_intervals"] == 0
    assert report["duplicate_effective_from_rows"] == 0
    rows = sorted(
        read_delta_table_rows(output_dir / "canonical_contract_economics.delta", limit=10),
        key=lambda row: str(row["effective_from_ts"]),
    )
    assert len(rows) == 2
    assert rows[0]["economics_session_date"] == "2026-06-11"
    assert rows[0]["effective_session_date"] == "2026-06-12"
    assert rows[0]["effective_from_ts"] == "2026-06-12T00:00:00Z"
    assert rows[0]["effective_to_ts"] == "2026-06-13T00:00:00Z"
    assert rows[0]["step_price_rub"] is None
    assert rows[0]["margin_required_estimate"] == pytest.approx(17_721.61)
    assert rows[0]["model_quality"] == "official_initial_margin"
    assert json.loads(str(rows[0]["source_flags_json"]))["effective_session_source"] == (
        "calendar_gap_fallback"
    )
    assert rows[1]["economics_session_date"] == "2026-06-12"
    assert rows[1]["effective_session_date"] == "2026-06-13"
    assert rows[1]["effective_from_ts"] == "2026-06-13T00:00:00Z"
    assert rows[1]["effective_to_ts"] is None


def test_contract_economics_spark_job_merges_canonical_updates_without_losing_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)

    raw_root = tmp_path / "raw"
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=raw_root,
        trade_date="2026-06-11",
        settle_price=93.99,
        official_margin=17_721.61,
        fx_rate=71.9077,
        fx_clearing_type="mc",
    )
    output_dir = tmp_path / "canonical"
    run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        run_id="economics-merge-first",
    )

    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=raw_root,
        trade_date="2026-06-12",
        settle_price=94.25,
        official_margin=17_900.00,
        fx_rate=72.1000,
        fx_clearing_type="mc",
    )
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        run_id="economics-merge-second",
    )

    rows = sorted(
        read_delta_table_rows(output_dir / "canonical_contract_economics.delta", limit=10),
        key=lambda row: str(row["economics_session_date"]),
    )
    assert report["row_counts"]["canonical_contract_economics"] == 2
    assert [row["economics_session_date"] for row in rows] == [
        "2026-06-11",
        "2026-06-12",
    ]


def test_contract_economics_spark_job_deduplicates_latest_risk_snapshot_before_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)

    raw_root = tmp_path / "raw"
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=raw_root,
        trade_date="2026-06-11",
        settle_price=93.99,
        official_margin=17_721.61,
        fx_rate=71.9077,
        fx_clearing_type="mc",
    )
    write_delta_table_rows(
        table_path=raw_paths["limits"],
        columns=_columns("raw_moex_rms_limits"),
        mode="append",
        rows=[
            {
                "source_id": "ncc_derivatives_limits",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": "BR-2026-06-11-limits-newer",
                "source_document_hash": "limits-hash-2026-06-11-BR-newer",
                "fetched_at_utc": "2026-06-11T20:03:00Z",
                "trade_date": "2026-06-11",
                "assetcode": "BR",
                "mr1": 0.33,
                "mr2": 0.39,
                "mr3": 0.46,
                "lk1": 0.0,
                "lk2": 0.0,
                "title": "BR",
                "group_title": "Fixture",
                "update_time": "2026-06-11T20:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": "BR", "MR1": 0.33}),
            }
        ],
    )
    write_delta_table_rows(
        table_path=raw_paths["staticparams"],
        columns=_columns("raw_moex_rms_staticparams"),
        mode="append",
        rows=[
            {
                "source_id": "ncc_derivatives_staticparams",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": "BR-2026-06-11-staticparams-newer",
                "source_document_hash": "staticparams-hash-2026-06-11-BR-newer",
                "fetched_at_utc": "2026-06-11T20:03:00Z",
                "trade_date": "2026-06-11",
                "assetcode": "BR",
                "radius_pct": 25.0,
                "update_time": "2026-06-11T20:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": "BR", "RADIUS": 25.0}),
            }
        ],
    )

    output_dir = tmp_path / "canonical"
    for run_id in ("economics-risk-dedupe-first", "economics-risk-dedupe-second"):
        report = run_moex_contract_economics_spark_job(
            raw_contract_specs_path=raw_paths["contracts"],
            raw_fx_rates_path=raw_paths["fx"],
            raw_rms_limits_path=raw_paths["limits"],
            raw_rms_staticparams_path=raw_paths["staticparams"],
            output_dir=output_dir,
            run_id=run_id,
        )

    risk_rows = read_delta_table_rows(
        output_dir / "canonical_asset_risk_parameters.delta",
        limit=10,
    )
    economics_rows = read_delta_table_rows(
        output_dir / "canonical_contract_economics.delta",
        limit=10,
    )
    assert report["row_counts"]["canonical_asset_risk_parameters"] == 1
    assert report["row_counts"]["canonical_contract_economics"] == 1
    assert len(risk_rows) == 1
    assert len(economics_rows) == 1
    assert risk_rows[0]["mr1"] == pytest.approx(0.33)
    assert risk_rows[0]["radius_pct"] == pytest.approx(25.0)
    assert economics_rows[0]["mr1"] == pytest.approx(0.33)
    assert economics_rows[0]["radius_pct"] == pytest.approx(25.0)


def test_contract_economics_spark_job_uses_latest_prior_risk_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)

    raw_root = tmp_path / "raw"
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=raw_root,
        trade_date="2026-06-26",
        risk_trade_date="2026-06-25",
        settle_price=93.99,
        official_margin=17_721.61,
        fx_rate=71.9077,
    )
    write_delta_table_rows(
        table_path=raw_paths["limits"],
        columns=_columns("raw_moex_rms_limits"),
        mode="append",
        rows=[
            {
                "source_id": "ncc_derivatives_limits",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": "BR-2026-06-27-limits",
                "source_document_hash": "limits-hash-2026-06-27-BR",
                "fetched_at_utc": "2026-06-27T19:03:00Z",
                "trade_date": "2026-06-27",
                "assetcode": "BR",
                "mr1": 0.99,
                "mr2": 0.99,
                "mr3": 0.99,
                "lk1": 0.0,
                "lk2": 0.0,
                "title": "BR",
                "group_title": "Fixture",
                "update_time": "2026-06-27T19:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": "BR", "MR1": 0.99}),
            }
        ],
    )
    write_delta_table_rows(
        table_path=raw_paths["staticparams"],
        columns=_columns("raw_moex_rms_staticparams"),
        mode="append",
        rows=[
            {
                "source_id": "ncc_derivatives_staticparams",
                "source_url": "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
                "source_document_id": "BR-2026-06-27-staticparams",
                "source_document_hash": "staticparams-hash-2026-06-27-BR",
                "fetched_at_utc": "2026-06-27T19:03:00Z",
                "trade_date": "2026-06-27",
                "assetcode": "BR",
                "radius_pct": 99.0,
                "update_time": "2026-06-27T19:00:00Z",
                "raw_payload_json": json.dumps({"ASSETCODE": "BR", "RADIUS": 99.0}),
            }
        ],
    )

    output_dir = tmp_path / "canonical"
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        run_id="economics-prior-risk",
    )

    risk_rows = sorted(
        read_delta_table_rows(output_dir / "canonical_asset_risk_parameters.delta", limit=10),
        key=lambda row: str(row["risk_session_date"]),
    )
    economics_rows = read_delta_table_rows(
        output_dir / "canonical_contract_economics.delta",
        limit=10,
    )
    assert report["row_counts"]["canonical_asset_risk_parameters"] == 2
    assert report["row_counts"]["canonical_contract_economics"] == 1
    assert [row["risk_session_date"] for row in risk_rows] == ["2026-06-25", "2026-06-27"]
    assert economics_rows[0]["economics_session_date"] == "2026-06-26"
    assert economics_rows[0]["mr1"] == pytest.approx(0.12)
    assert economics_rows[0]["radius_pct"] == pytest.approx(15.0)
    assert json.loads(economics_rows[0]["source_flags_json"])["risk_session_date"] == "2026-06-25"


def test_contract_economics_spark_job_deduplicates_fx_source_before_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)

    raw_root = tmp_path / "raw"
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=raw_root,
        trade_date="2026-06-26",
        settle_price=93.99,
        official_margin=17_721.61,
        fx_rate=71.9077,
    )
    duplicate_fx_row = read_delta_table_rows(raw_paths["fx"], limit=1)[0]
    write_delta_table_rows(
        table_path=raw_paths["fx"],
        columns=_columns("raw_moex_indicative_fx_rates"),
        mode="append",
        rows=[duplicate_fx_row],
    )

    output_dir = tmp_path / "canonical"
    for run_id in ("economics-fx-dedupe-first", "economics-fx-dedupe-second"):
        report = run_moex_contract_economics_spark_job(
            raw_contract_specs_path=raw_paths["contracts"],
            raw_fx_rates_path=raw_paths["fx"],
            raw_rms_limits_path=raw_paths["limits"],
            raw_rms_staticparams_path=raw_paths["staticparams"],
            output_dir=output_dir,
            run_id=run_id,
        )

    fx_rows = sorted(
        read_delta_table_rows(output_dir / "canonical_fx_rates.delta", limit=10),
        key=lambda row: str(row["fx_pair"]),
    )
    assert report["row_counts"]["canonical_fx_rates"] == 2
    assert [(row["rate_date"], row["fx_pair"], row["clearing_type"]) for row in fx_rows] == [
        ("2026-06-26", "RUB/RUB", "tc"),
        ("2026-06-26", "USD/RUB", "tc"),
    ]
    assert report["row_counts"]["canonical_contract_economics"] == 1


def test_contract_economics_spark_job_ignores_legacy_non_session_contract_dates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)

    raw_root = tmp_path / "raw"
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=raw_root,
        trade_date="2026-06-19",
        contract_id="BRM6",
        assetcode="BR",
        settle_price=77.5,
        official_margin=10_000.0,
        fx_rate=78.5,
    )
    for trade_date in ("2026-06-20", "2026-06-21"):
        write_delta_table_rows(
            table_path=raw_paths["contracts"],
            columns=_columns("raw_moex_contract_securities"),
            mode="append",
            rows=[
                {
                    "source_id": "moex_iss_forts_securities",
                    "source_url": (
                        "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json"
                    ),
                    "source_document_id": f"BRM6-{trade_date}",
                    "source_document_hash": f"legacy-weekend-contracts-{trade_date}",
                    "fetched_at_utc": f"{trade_date}T07:00:00Z",
                    "engine": "futures",
                    "market": "forts",
                    "board": "RFUD",
                    "moex_secid": "BRM6",
                    "trade_date": trade_date,
                    "assetcode": "BR",
                    "contract_shortname": "BR-6.26",
                    "last_trade_date": "2026-07-15",
                    "last_del_date": "2026-07-15",
                    "min_step": 0.01,
                    "lot_volume": 10.0,
                    "official_step_price": 7.85,
                    "official_initial_margin": 10_000.0,
                    "last_settle_price": 77.5,
                    "raw_payload_json": json.dumps(
                        {
                            "SECID": "BRM6",
                            "ASSETCODE": "BR",
                            "MINSTEP": 0.01,
                            "LOTVOLUME": 10.0,
                            "LASTSETTLEPRICE": 77.5,
                            "INITIALMARGIN": 10_000.0,
                            "CURRENCYID": "USD",
                            "MATDATE": "2026-07-15",
                        }
                    ),
                }
            ],
        )

    calendar_path = tmp_path / "canonical_session_calendar.delta"
    calendar_columns = historical_data_delta_schema_manifest()["canonical_session_calendar"][
        "columns"
    ]
    write_delta_table_rows(
        table_path=calendar_path,
        columns=calendar_columns,
        rows=[
            {
                "instrument_id": "FUT_BR",
                "timeframe": "1d",
                "session_date": "2026-06-19",
                "session_open_ts": "2026-06-19T10:00:00Z",
                "session_close_ts": "2026-06-19T18:45:00Z",
                "session_class": "regular",
            },
            {
                "instrument_id": "FUT_BR",
                "timeframe": "1d",
                "session_date": "2026-06-22",
                "session_open_ts": "2026-06-22T10:00:00Z",
                "session_close_ts": "2026-06-22T18:45:00Z",
                "session_class": "regular",
            },
        ],
    )

    output_dir = tmp_path / "canonical"
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        canonical_session_calendar_path=calendar_path,
        run_id="economics-ignore-weekend-contract-dates",
    )

    rows = read_delta_table_rows(output_dir / "canonical_contract_economics.delta", limit=10)
    assert report["row_counts"]["canonical_contract_economics"] == 1
    assert [str(row["economics_session_date"]) for row in rows] == ["2026-06-19"]
    assert rows[0]["effective_session_date"] == "2026-06-22"


def test_contract_economics_spark_job_zeros_buffer_for_linked_rub_quote(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)

    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2026-06-11",
        contract_id="SIM6",
        assetcode="SI",
        quote_currency="RUB",
        fx_rate=71.9077,
        settle_price=90_000.0,
        official_margin=10_000.0,
        min_step=1.0,
        lot_volume=1.0,
    )
    output_dir = tmp_path / "canonical"
    run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        run_id="economics-linked-rub",
    )

    rows = read_delta_table_rows(output_dir / "canonical_contract_economics.delta", limit=5)
    assert len(rows) == 1
    assert rows[0]["quote_currency"] == "RUB"
    assert rows[0]["fx_rate_to_rub"] == pytest.approx(1.0)
    assert rows[0]["margin_buffer_pct"] == pytest.approx(0.0)


def test_research_execution_economics_join_uses_latest_asof_without_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    economics_path = tmp_path / "canonical" / "canonical_contract_economics.delta"
    base_row = {
        "contract_id": "BRN6",
        "instrument_id": "FUT_BR",
        "moex_secid": "BRN6",
        "assetcode": "BR",
        "economics_session_date": "2026-06-11",
        "effective_session_date": "2026-06-11",
        "clearing_type": "mc",
        "effective_from_ts": "2026-06-11T19:00:00Z",
        "effective_to_ts": None,
        "min_step": 0.01,
        "lot_volume": 10.0,
        "quote_currency": "USD",
        "fx_rate_to_rub": 71.9,
        "tick_value_currency": 0.1,
        "step_price_rub": 7.19,
        "official_step_price": 7.19,
        "official_initial_margin": 17_700.0,
        "last_settle_price": 94.0,
        "mr1": 0.12,
        "radius_pct": 15.0,
        "radius_source": "source",
        "margin_formula_base": 8_100.0,
        "margin_radius_adjusted": 9_315.0,
        "margin_required_no_buffer": 17_700.0,
        "margin_buffer_pct": 0.05,
        "margin_required_estimate": 18_585.0,
        "maturity_rank": 1,
        "days_to_expiry": 34,
        "expiration_date": "2026-07-15",
        "model_version": "v1",
        "buffer_policy_version": "buffer-v1",
        "model_quality": "estimated",
        "source_flags_json": "{}",
        "source_document_hashes_json": "{}",
        "created_at": "2026-06-11T19:05:00Z",
    }
    newer_row = {
        **base_row,
        "economics_session_date": "2026-06-12",
        "effective_session_date": "2026-06-12",
        "effective_from_ts": "2026-06-12T19:00:00Z",
        "step_price_rub": 7.21,
        "margin_required_estimate": 18_795.0,
        "created_at": "2026-06-12T19:05:00Z",
    }
    write_delta_table_rows(
        table_path=economics_path,
        columns=_columns("canonical_contract_economics"),
        rows=[base_row, newer_row],
    )

    spark = _create_spark_session("ta3000-research-economics-asof-test", "local[2]")
    try:
        bars = spark.sql(
            """
            SELECT
              'BRN6@MOEX' AS contract_id,
              CAST('2026-06-13 10:00:00' AS TIMESTAMP) AS ts
            """
        )
        rows = research_bar_views_job._with_execution_economics(  # type: ignore[attr-defined]
            spark=spark,
            dataframe=bars,
            canonical_contract_economics_path=economics_path,
            contract_column="contract_id",
        ).collect()
    finally:
        spark.stop()

    assert len(rows) == 1
    assert rows[0]["contract_id"] == "BRN6@MOEX"
    assert rows[0]["execution_step_price_rub"] == pytest.approx(7.21)
    assert rows[0]["execution_margin_required_estimate"] == pytest.approx(18_795.0)


def test_research_weekly_execution_economics_uses_bar_end_asof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    economics_path = tmp_path / "canonical" / "canonical_contract_economics.delta"
    base_row = {
        "contract_id": "BRN6",
        "instrument_id": "FUT_BR",
        "moex_secid": "BRN6",
        "assetcode": "BR",
        "economics_session_date": "2026-06-08",
        "effective_session_date": "2026-06-08",
        "clearing_type": "mc",
        "effective_from_ts": "2026-06-08T19:00:00Z",
        "effective_to_ts": None,
        "min_step": 0.01,
        "lot_volume": 10.0,
        "quote_currency": "USD",
        "fx_rate_to_rub": 71.9,
        "tick_value_currency": 0.1,
        "step_price_rub": 7.19,
        "official_step_price": 7.19,
        "official_initial_margin": 17_700.0,
        "last_settle_price": 94.0,
        "mr1": 0.12,
        "radius_pct": 15.0,
        "radius_source": "source",
        "margin_formula_base": 8_100.0,
        "margin_radius_adjusted": 9_315.0,
        "margin_required_no_buffer": 17_700.0,
        "margin_buffer_pct": 0.05,
        "margin_required_estimate": 18_585.0,
        "maturity_rank": 1,
        "days_to_expiry": 34,
        "expiration_date": "2026-07-15",
        "model_version": "v1",
        "buffer_policy_version": "buffer-v1",
        "model_quality": "estimated",
        "source_flags_json": "{}",
        "source_document_hashes_json": "{}",
        "created_at": "2026-06-08T19:05:00Z",
    }
    newer_row = {
        **base_row,
        "economics_session_date": "2026-06-12",
        "effective_session_date": "2026-06-12",
        "effective_from_ts": "2026-06-12T19:00:00Z",
        "step_price_rub": 7.25,
        "margin_required_estimate": 19_125.0,
        "created_at": "2026-06-12T19:05:00Z",
    }
    write_delta_table_rows(
        table_path=economics_path,
        columns=_columns("canonical_contract_economics"),
        rows=[base_row, newer_row],
    )

    spark = _create_spark_session("ta3000-research-weekly-economics-asof-test", "local[2]")
    try:
        weekly_bars = spark.sql(
            """
            SELECT
              'BRN6' AS contract_id,
              '1w' AS timeframe,
              CAST('2026-06-08 00:00:00' AS TIMESTAMP) AS ts,
              CAST('2026-06-14 23:59:59' AS TIMESTAMP) AS bar_end_ts
            """
        )
        rows = research_bar_views_job._with_execution_economics(  # type: ignore[attr-defined]
            spark=spark,
            dataframe=weekly_bars,
            canonical_contract_economics_path=economics_path,
            contract_column="contract_id",
        ).collect()
    finally:
        spark.stop()

    assert len(rows) == 1
    assert rows[0]["execution_step_price_rub"] == pytest.approx(7.25)
    assert rows[0]["execution_margin_required_estimate"] == pytest.approx(19_125.0)


def test_research_execution_economics_propagates_across_session_timeframes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    economics_path = tmp_path / "canonical" / "canonical_contract_economics.delta"
    economics_row = {
        "contract_id": "BRN6",
        "instrument_id": "FUT_BR",
        "moex_secid": "BRN6",
        "assetcode": "BR",
        "economics_session_date": "2026-06-11",
        "effective_session_date": "2026-06-12",
        "clearing_type": "tc",
        "effective_from_ts": "2026-06-11T19:00:00Z",
        "effective_to_ts": None,
        "min_step": 0.01,
        "lot_volume": 10.0,
        "quote_currency": "USD",
        "fx_rate_to_rub": 71.9,
        "tick_value_currency": 0.1,
        "step_price_rub": 7.19,
        "official_step_price": 7.19,
        "official_initial_margin": 17_700.0,
        "last_settle_price": 94.0,
        "mr1": 0.12,
        "radius_pct": 15.0,
        "radius_source": "source",
        "margin_formula_base": 8_100.0,
        "margin_radius_adjusted": 9_315.0,
        "margin_required_no_buffer": 17_700.0,
        "margin_buffer_pct": 0.05,
        "margin_required_estimate": 18_585.0,
        "maturity_rank": 1,
        "days_to_expiry": 34,
        "expiration_date": "2026-07-15",
        "model_version": "v1",
        "buffer_policy_version": "buffer-v1",
        "model_quality": "estimated",
        "source_flags_json": "{}",
        "source_document_hashes_json": "{}",
        "created_at": "2026-06-11T19:05:00Z",
    }
    write_delta_table_rows(
        table_path=economics_path,
        columns=_columns("canonical_contract_economics"),
        rows=[economics_row],
    )

    spark = _create_spark_session("ta3000-research-session-timeframe-economics-test", "local[2]")
    try:
        bars = spark.sql(
            """
            SELECT
              'BRN6' AS contract_id,
              timeframe,
              CAST(ts AS TIMESTAMP) AS ts,
              CAST(bar_end_ts AS TIMESTAMP) AS bar_end_ts
            FROM VALUES
              ('1m', '2026-06-12 10:00:00', '2026-06-12 10:00:59'),
              ('5m', '2026-06-12 10:00:00', '2026-06-12 10:04:59'),
              ('15m', '2026-06-12 10:00:00', '2026-06-12 10:14:59'),
              ('1h', '2026-06-12 10:00:00', '2026-06-12 10:59:59'),
              ('4h', '2026-06-12 10:00:00', '2026-06-12 13:59:59'),
              ('1d', '2026-06-12 00:00:00', '2026-06-12 23:59:59')
            AS t(timeframe, ts, bar_end_ts)
            """
        )
        rows = [
            row.asDict(recursive=True)
            for row in research_bar_views_job._with_execution_economics(  # type: ignore[attr-defined]
                spark=spark,
                dataframe=bars,
                canonical_contract_economics_path=economics_path,
                contract_column="contract_id",
            )
            .orderBy("timeframe")
            .toLocalIterator()
        ]
    finally:
        spark.stop()

    assert {row["timeframe"] for row in rows} == {"1m", "5m", "15m", "1h", "4h", "1d"}
    assert all(row["execution_step_price_rub"] == pytest.approx(7.19) for row in rows)
    assert all(row["execution_margin_required_estimate"] == pytest.approx(18_585.0) for row in rows)
