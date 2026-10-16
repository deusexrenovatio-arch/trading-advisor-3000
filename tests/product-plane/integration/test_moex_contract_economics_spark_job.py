from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.moex.economics import (
    MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES,
    moex_economics_store_contract,
)
from trading_advisor_3000.product_plane.data_plane.schemas.delta import (
    historical_data_delta_schema_manifest,
)
from trading_advisor_3000.spark_jobs import research_bar_views_job
from trading_advisor_3000.spark_jobs.canonical_bars_job import _create_spark_session
from trading_advisor_3000.spark_jobs.moex_contract_economics_job import (
    _asset_risk_parameters_frame,
    _contract_parameter_regimes_frame,
    _contract_specs_frame,
    _economics_frame,
    _fail_closed_on_missing_inputs,
    _fail_closed_on_step_price_rule_validation,
    _fx_rates_frame,
    _with_latest_margin_calibration,
    _write_spark_delta_table,
    run_moex_contract_economics_spark_job,
)


def _columns(table_name: str) -> dict[str, str]:
    return dict(moex_economics_store_contract()[table_name]["columns"])


def test_economics_writer_skips_equal_rows_and_preserves_unchanged_lineage(tmp_path, monkeypatch):
    from deltalake import DeltaTable
    from pyspark.sql import functions as F

    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw", trade_date="2026-06-26"
    )
    spark = _create_spark_session("economics-change-only-writer", "local[2]")
    output = tmp_path / "canonical_fx_rates.delta"
    try:
        original = _fx_rates_frame(spark, paths["fx"]).withColumn(
            "created_at", F.lit("2026-06-26T20:00:00Z").cast("timestamp")
        )
        _write_spark_delta_table(original, table_path=output, table_name="canonical_fx_rates")
        version = DeltaTable(str(output)).version()
        repeat = original.withColumn("created_at", F.lit("2026-06-27T20:00:00Z").cast("timestamp"))
        _write_spark_delta_table(repeat, table_path=output, table_name="canonical_fx_rates")
        assert DeltaTable(str(output)).version() == version
        changed = repeat.withColumn(
            "rate_to_rub",
            F.when(F.col("fx_pair") == "USD/RUB", F.col("rate_to_rub") + 1).otherwise(
                F.col("rate_to_rub")
            ),
        )
        _write_spark_delta_table(changed, table_path=output, table_name="canonical_fx_rates")
        rows = {row["fx_pair"]: row for row in read_delta_table_rows(output, limit=10)}
        assert rows["USD/RUB"]["rate_to_rub"] == pytest.approx(72.9077)
        assert rows["RUB/RUB"]["created_at"] == "2026-06-26T20:00:00Z"
        assert rows["USD/RUB"]["created_at"] == "2026-06-27T20:00:00Z"
        lineage = changed.withColumn(
            "source_document_hash",
            F.when(F.col("fx_pair") == "USD/RUB", F.lit("new-evidence")).otherwise(
                F.col("source_document_hash")
            ),
        )
        _write_spark_delta_table(lineage, table_path=output, table_name="canonical_fx_rates")
        assert (
            next(
                row
                for row in read_delta_table_rows(output, limit=10)
                if row["fx_pair"] == "USD/RUB"
            )["source_document_hash"]
            == "new-evidence"
        )
    finally:
        spark.stop()


def test_economics_scope_uses_accepted_intervals_and_survives_retry(tmp_path, monkeypatch):
    from deltalake import DeltaTable
    from pyspark.sql import functions as F

    from trading_advisor_3000.product_plane.common.data_snapshots import capture_table
    from trading_advisor_3000.product_plane.data_plane.moex.economics import (
        MOEX_CONTRACT_ECONOMICS_MODEL_VERSION,
    )
    from trading_advisor_3000.spark_jobs.moex_contract_economics_job import (
        _economics_downstream_scope,
    )

    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    spark = _create_spark_session("economics-accepted-change-scope", "local[2]")
    bars_path = tmp_path / "canonical_bars.delta"
    try:
        bars = spark.createDataFrame(
            [
                ("BRN6@MOEX", "BR", "15m", stamp)
                for stamp in (
                    "2026-06-01T09:00:00Z",
                    "2026-06-02T10:15:00Z",
                    "2026-06-02T13:00:00Z",
                    "2026-06-03T09:00:00Z",
                )
            ],
            "contract_id string, instrument_id string, timeframe string, ts string",
        ).withColumn("ts", F.col("ts").cast("timestamp"))
        bars.write.format("delta").save(str(bars_path))
        bars_version = DeltaTable(str(bars_path)).version()
        source = spark.createDataFrame(
            [
                (
                    "BRN6",
                    "FUT_BR",
                    "2026-06-02",
                    "tc",
                    "2026-06-02T10:00:00Z",
                    "2026-06-02T11:00:00Z",
                    1.0,
                )
            ],
            "contract_id string, instrument_id string, economics_session_date string, "
            "clearing_type string, effective_from_ts string, effective_to_ts string, "
            "step_price_rub double",
        )
        source = source.withColumn(
            "model_version", F.lit(MOEX_CONTRACT_ECONOMICS_MODEL_VERSION)
        ).withColumn("created_at", F.current_timestamp())
        for case in ("unchanged", "shift", "delete", "partial", "uncovered", "historical"):
            output = tmp_path / case / "canonical_contract_economics.delta"
            case_source = source
            if case == "uncovered":
                case_source = source.withColumn("contract_id", F.lit("ABSENT"))
            elif case == "historical":
                case_source = source.withColumn(
                    "effective_from_ts", F.lit("2025-06-02T10:00:00Z")
                ).withColumn("effective_to_ts", F.lit("2025-06-02T11:00:00Z"))
            _write_spark_delta_table(
                case_source, table_path=output, table_name="canonical_contract_economics"
            )
            accepted = capture_table(output)
            if case == "shift":
                shifted = source.withColumn(
                    "effective_from_ts", F.lit("2026-06-02T12:00:00Z")
                ).withColumn("effective_to_ts", F.lit(None).cast("string"))
                _write_spark_delta_table(
                    shifted, table_path=output, table_name="canonical_contract_economics"
                )
            elif case in {"delete", "uncovered", "historical"}:
                # Explicit authoritative deletion, never inferred from a partial response.
                DeltaTable(str(output)).delete()
            elif case == "partial":
                _write_spark_delta_table(
                    source.limit(0), table_path=output, table_name="canonical_contract_economics"
                )
            scope = _economics_downstream_scope(
                spark=spark,
                table_path=output,
                previous_reference=accepted,
                canonical_bars_path=bars_path,
            )
            assert scope["full_refresh"] is False
            if case in {"unchanged", "partial"}:
                assert scope["windows"] == []
            elif case in {"uncovered", "historical"}:
                assert scope["windows"] == []
                assert len(scope["effective_intervals"]) == 1
                interval = scope["effective_intervals"][0]
                assert interval["contract_id"] == ("ABSENT" if case == "uncovered" else "BRN6")
                assert interval["effective_from_ts"].startswith(
                    "2025-" if case == "historical" else "2026-"
                )
            else:
                assert scope["windows"] == [
                    {
                        "contract_id": "BRN6",
                        "instrument_id": "BR",
                        "timeframe": "15m",
                        "start_ts": "2026-06-02T09:45:00.000000Z",
                        "end_ts": "2026-06-03T09:00:00.000000Z",
                    }
                ]
                # A failed Data publication keeps the accepted old pin. A retry
                # must report the same dirty scope even after outputs committed.
                assert (
                    _economics_downstream_scope(
                        spark=spark,
                        table_path=output,
                        previous_reference=accepted,
                        canonical_bars_path=bars_path,
                    )
                    == scope
                )
        assert DeltaTable(str(bars_path)).version() == bars_version
        # The mutable output path must use its captured version, even inside a
        # materializer's writable scope. Reject an already advanced owner pin.
        from trading_advisor_3000.spark_jobs import moex_contract_economics_job as job

        reads = []
        original_read = job.read_spark_delta

        def bound_read(session, path, *, version=None):
            if Path(path) == output:
                reads.append(version)
            return original_read(session, path, version=version)

        monkeypatch.setattr(job, "read_spark_delta", bound_read)
        _economics_downstream_scope(
            spark=spark,
            table_path=output,
            previous_reference=accepted,
            canonical_bars_path=bars_path,
        )
        assert reads == [accepted["delta_version"], DeltaTable(str(output)).version()]
        with pytest.raises(ValueError, match="output changed"):
            _economics_downstream_scope(
                spark=spark,
                table_path=output,
                previous_reference=accepted,
                canonical_bars_path=bars_path,
                output_reference=accepted,
            )
        assert (
            _economics_downstream_scope(
                spark=spark,
                table_path=output,
                previous_reference=None,
                canonical_bars_path=bars_path,
            )["full_refresh"]
            is True
        )
    finally:
        spark.stop()


def test_launch_risk_requires_publication_before_effective_interval(tmp_path, monkeypatch):
    pytest.importorskip("pyspark.sql")
    from pyspark.sql import functions as F

    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw", trade_date="2026-06-26"
    )
    cases = [
        ("EARLY", "2026-06-27", None, "2026-06-27 00:31:00", 0.07),
        ("EXACT", "2026-06-27", "2026-06-27T00:00:00Z", None, 0.08),
        ("LATE", "2026-06-27", "2026-06-27T00:00:01Z", None, 0.09),
        ("UNKNOWN", "2026-06-27", None, None, 0.10),
        ("PRIOR", "2026-06-25", "2026-06-25T19:00:00Z", None, 0.12),
        ("PRIOR", "2026-06-27", "2026-06-26T21:31:00Z", None, 0.99),
    ]
    write_delta_table_rows(
        table_path=paths["limits"],
        columns=_columns("raw_moex_rms_limits"),
        rows=[
            {
                "assetcode": asset,
                "trade_date": day,
                "mr1": mr1,
                "update_time": stamp,
                "source_document_hash": f"{asset}-{day}",
                "raw_payload_json": json.dumps({"updatetime": local_stamp}),
            }
            for asset, day, stamp, local_stamp, mr1 in cases
        ],
    )
    spark = _create_spark_session("launch-risk-publication", "local[2]")
    try:
        specs = (
            _contract_specs_frame(spark, paths["contracts"])
            .withColumn(
                "assetcode",
                F.explode(
                    F.array(*[F.lit(s) for s in ["EARLY", "EXACT", "LATE", "UNKNOWN", "PRIOR"]])
                ),
            )
            .withColumn("contract_id", F.col("assetcode"))
        )
        risk = _asset_risk_parameters_frame(spark, paths["limits"], paths["staticparams"])
        rows = {
            r.assetcode: r.asDict()
            for r in _economics_frame(specs, _fx_rates_frame(spark, paths["fx"]), risk).collect()
        }
        assert rows["EARLY"]["mr1"] == pytest.approx(0.07)
        assert rows["EXACT"]["mr1"] == pytest.approx(0.08)
        assert rows["LATE"]["mr1"] is None
        assert rows["UNKNOWN"]["mr1"] is None
        assert rows["PRIOR"]["mr1"] == pytest.approx(0.12)
        flags = json.loads(rows["EARLY"]["source_flags_json"])
        assert flags["risk_selection"] == "first_published_before_effective_session"
        assert flags["risk_available_from_ts"].startswith("2026-06-26T21:31:00")
        assert (
            json.loads(rows["EARLY"]["source_document_hashes_json"])["limits"] == "EARLY-2026-06-27"
        )
    finally:
        spark.stop()


@pytest.mark.parametrize("static_history", ["absent", "eligible", "late_only"])
def test_launch_risk_keeps_latest_eligible_same_day_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, static_history: str
) -> None:
    pytest.importorskip("pyspark.sql")
    from pyspark.sql import functions as F

    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw", trade_date="2026-06-26"
    )
    write_delta_table_rows(
        table_path=paths["limits"],
        columns=_columns("raw_moex_rms_limits"),
        rows=[
            {
                "assetcode": "LAUNCH",
                "trade_date": "2026-06-27",
                "mr1": mr1,
                "update_time": stamp,
                "source_document_hash": source_hash,
            }
            for stamp, mr1, source_hash in [
                ("2026-06-26T23:58:00Z", 0.06, "older-eligible-limit"),
                ("2026-06-26T23:59:00Z", 0.071, "eligible-limit-a"),
                ("2026-06-26T23:59:00Z", 0.07, "eligible-limit-z"),
                ("2026-06-27T00:01:00Z", 0.99, "later-limit"),
            ]
        ],
    )
    if static_history != "absent":
        write_delta_table_rows(
            table_path=paths["staticparams"],
            columns=_columns("raw_moex_rms_staticparams"),
            rows=[
                {
                    "assetcode": "LAUNCH",
                    "trade_date": "2026-06-27",
                    "radius_pct": radius,
                    "update_time": stamp,
                    "source_document_hash": source_hash,
                }
                for stamp, radius, source_hash in [
                    *(
                        [("2026-06-26T23:58:00Z", 0.5, "eligible-static")]
                        if static_history == "eligible"
                        else []
                    ),
                    ("2026-06-27T00:01:00Z", 3.5, "later-static"),
                ]
            ],
        )
    spark = _create_spark_session("launch-risk-same-day-history", "local[2]")
    try:
        specs = (
            _contract_specs_frame(spark, paths["contracts"])
            .withColumn("assetcode", F.lit("LAUNCH"))
            .withColumn("contract_id", F.lit("LAUNCH"))
        )
        risk = _asset_risk_parameters_frame(spark, paths["limits"], paths["staticparams"])
        daily = risk.where(F.col("assetcode") == "LAUNCH")
        assert daily.count() == 1
        assert daily.first().mr1 == pytest.approx(0.99)
        canonical_path = tmp_path / "canonical-risk.delta"
        _write_spark_delta_table(
            daily,
            table_path=canonical_path,
            table_name="canonical_asset_risk_parameters",
        )
        canonical = read_delta_table_rows(canonical_path)
        assert len(canonical) == 1
        assert canonical[0]["mr1"] == pytest.approx(0.99)
        assert canonical[0]["source_limits_hash"] == "later-limit"
        assert "__limits_history" not in canonical[0]
        assert "__static_history" not in canonical[0]

        fx = _fx_rates_frame(spark, paths["fx"])
        row = _economics_frame(specs, fx, risk).first()
        flags = json.loads(row.source_flags_json)
        if static_history == "late_only":
            assert row.mr1 is None
            assert flags["risk_selection"] == "unavailable"
        else:
            assert row.mr1 == pytest.approx(0.07)
            hashes = json.loads(row.source_document_hashes_json)
            assert hashes["limits"] == "eligible-limit-z"
            assert flags["risk_selection"] == "first_published_before_effective_session"
            assert flags["risk_available_from_ts"].startswith("2026-06-26T23:59:00")
            if static_history == "eligible":
                assert row.radius_pct == pytest.approx(0.5)
                assert hashes["staticparams"] == "eligible-static"

        prior_specs = specs.withColumn("trade_date", F.to_date(F.lit("2026-06-29")))
        prior_fx = fx.withColumn("rate_date", F.to_date(F.lit("2026-06-29")))
        prior = _economics_frame(prior_specs, prior_fx, risk).first()
        assert prior.mr1 == pytest.approx(0.99)
        assert json.loads(prior.source_document_hashes_json)["limits"] == "later-limit"
        assert json.loads(prior.source_flags_json)["risk_selection"] == "latest_prior_source_date"
    finally:
        spark.stop()


def _configure_windows_spark_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name != "nt":
        return
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


def test_economics_fx_excludes_cross_pairs_and_keeps_rub_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw", trade_date="2020-01-27", fx_clearing_type="mc"
    )
    original = read_delta_table_rows(paths["fx"], limit=1)[0]
    write_delta_table_rows(
        table_path=paths["fx"],
        columns=_columns("raw_moex_indicative_fx_rates"),
        mode="append",
        rows=[
            {**original, "fx_pair": pair, "rate": rate, "source_document_hash": pair}
            for pair, rate in [("USD/JPY", 108.914), ("USD/CNY", 6.9334)]
        ],
    )
    spark = _create_spark_session("test-rub-quote-filter", "local[2]")
    try:
        rates = _fx_rates_frame(spark, paths["fx"])
        rows = {row.fx_pair: row.rate_to_rub for row in rates.collect()}
        assert rows == {"USD/RUB": 71.9077, "RUB/RUB": 1.0}
        assert (
            rates.groupBy("base_currency", "rate_date", "clearing_type")
            .count()
            .where("count > 1")
            .count()
            == 0
        )
    finally:
        spark.stop()


@pytest.mark.parametrize("nested", [False, True])
def test_economics_reads_turnover_from_both_history_payload_shapes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, nested: bool
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    history = {"VALUE": 71000.0, "VOLUME": 10.0, "WAPRICE": 100.0}
    paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw", trade_date="2020-01-27", historical_market=history
    )
    if not nested:
        row = read_delta_table_rows(paths["contracts"], limit=1)[0]
        row["raw_payload_json"] = json.dumps(history)
        write_delta_table_rows(
            table_path=paths["contracts"],
            columns=_columns("raw_moex_contract_securities"),
            rows=[row],
        )
    spark = _create_spark_session("test-history-payload-shape", "local[2]")
    try:
        row = _contract_specs_frame(spark, paths["contracts"]).first()
        assert (row.history_value_rub, row.history_volume, row.history_waprice) == (
            71000.0,
            10.0,
            100.0,
        )
    finally:
        spark.stop()


def test_margin_quality_is_unavailable_when_formula_is_missing_despite_calibration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    spark = _create_spark_session("test-incomplete-margin-quality", "local[2]")
    try:
        frame = spark.createDataFrame(
            [
                ("current", "BR", "2026-07-28", "mc", 1, 100.0, 50.0),
                ("missing", "BR", "2020-01-27", "mc", 1, None, None),
                ("zero", "BR", "2020-01-27", "mc", 2, None, 0.0),
                ("valid", "BR", "2020-01-27", "mc", 1, None, 25.0),
                ("nearest", "BR", "2020-01-27", "mc", 2, None, 25.0),
            ],
            "contract_id string, assetcode string, economics_session_date string, "
            "clearing_type string, maturity_rank int, official_initial_margin double, "
            "margin_radius_adjusted double",
        )
        rows = {r.contract_id: r for r in _with_latest_margin_calibration(frame).collect()}
        for key in ("missing", "zero"):
            assert rows[key].margin_calibration_factor == 2.0
            assert rows[key].margin_required_estimate is None
            assert rows[key].model_quality == "unavailable"
        assert rows["current"].model_quality == "official_initial_margin"
        assert rows["valid"].model_quality == "calibrated_asset_rank"
        assert rows["nearest"].model_quality == "calibrated_nearest_rank"
        assert rows["valid"].margin_required_estimate == 50.0
    finally:
        spark.stop()


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


@pytest.mark.parametrize(
    ("trade_date", "rule_id"),
    [
        ("2024-06-14", "BR-2022-2026-v1"),
        ("2020-01-27", "BR-pre2022-v1"),
        ("2018-06-26", "BR-pre2020-v1"),
    ],
)
def test_contract_economics_spark_job_reconstructs_historical_br_stepprice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    trade_date: str,
    rule_id: str,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date=trade_date,
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
    assert source_flags["step_price_rule_id"] == rule_id
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


def test_contract_economics_spark_job_atomically_replaces_obsolete_model_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)

    raw_root = tmp_path / "raw"
    raw_paths = _write_single_day_raw_economics_fixture(
        raw_root=raw_root,
        trade_date="2026-06-11",
        fx_clearing_type="mc",
    )
    output_dir = tmp_path / "canonical"
    target_path = output_dir / "canonical_contract_economics.delta"
    write_delta_table_rows(
        table_path=target_path,
        columns=_columns("canonical_contract_economics"),
        rows=[
            {
                "contract_id": "BRN6",
                "instrument_id": "FUT_BR",
                "moex_secid": "BRN6",
                "assetcode": "BR",
                "economics_session_date": "2026-06-11",
                "effective_session_date": "2026-06-12",
                "clearing_type": "tc",
                "effective_from_ts": "2026-06-12T00:00:00Z",
                "effective_to_ts": None,
                "min_step": 0.01,
                "lot_volume": 10.0,
                "quote_currency": "USD",
                "fx_rate_to_rub": 71.9077,
                "tick_value_currency": 0.1,
                "step_price_rub": 7.19077,
                "official_step_price": 7.19077,
                "official_initial_margin": 17_721.61,
                "last_settle_price": 93.99,
                "mr1": 0.12,
                "radius_pct": 15.0,
                "radius_source": "source",
                "margin_formula_base": 8_100.0,
                "margin_radius_adjusted": 9_315.0,
                "margin_required_no_buffer": 17_721.61,
                "margin_buffer_pct": 0.05,
                "margin_required_estimate": 18_607.69,
                "maturity_rank": 1,
                "days_to_expiry": 34,
                "expiration_date": "2026-07-15",
                "model_version": "moex_contract_economics_v1",
                "buffer_policy_version": "buffer-v1",
                "model_quality": "estimated",
                "source_flags_json": "{}",
                "source_document_hashes_json": "{}",
                "created_at": "2026-06-11T19:05:00Z",
            }
        ],
    )
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["contracts"],
        raw_fx_rates_path=raw_paths["fx"],
        raw_rms_limits_path=raw_paths["limits"],
        raw_rms_staticparams_path=raw_paths["staticparams"],
        output_dir=output_dir,
        run_id="economics-model-v6-replacement",
    )

    rows = read_delta_table_rows(target_path, limit=10)
    assert report["obsolete_model_rows_replaced"] == 1
    assert report["row_counts"]["canonical_contract_economics"] == 1
    assert len(rows) == 1
    assert rows[0]["model_version"] == "moex_contract_economics_v6"
    assert rows[0]["clearing_type"] == "mc"


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


def test_contract_economics_explicit_calibration_without_later_official_snapshot(
    tmp_path, monkeypatch
):
    from datetime import date

    from deltalake import DeltaTable

    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2026-08-14",
        contract_id="BRZ6",
        expiration_date="2026-12-15",
        settle_price=100.0,
        official_margin=200.0,
    )
    _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2026-10-05",
        contract_id="BRZ6",
        expiration_date="2026-12-15",
        settle_price=120.0,
        official_margin=None,
        mode="append",
    )
    raw_versions = {name: DeltaTable(str(path)).version() for name, path in paths.items()}
    output = tmp_path / "canonical"
    kwargs = {
        "raw_contract_specs_path": paths["contracts"],
        "raw_fx_rates_path": paths["fx"],
        "raw_rms_limits_path": paths["limits"],
        "raw_rms_staticparams_path": paths["staticparams"],
        "output_dir": output,
    }
    with pytest.raises(RuntimeError, match="INITIALMARGIN=1"):
        run_moex_contract_economics_spark_job(**kwargs)
    assert not output.exists()
    report = run_moex_contract_economics_spark_job(
        **kwargs,
        allow_calibrated_latest_margin=True,
        margin_calibration_as_of_date=date(2026, 8, 14),
        report_path=tmp_path / "explicit-calibration.json",
    )
    assert report["status"] == "PASS"
    assert report["allow_calibrated_latest_margin"] is True
    assert report["requested_margin_calibration_as_of_date"] == "2026-08-14"
    assert report["missing_required_input_counts"]["INITIALMARGIN"] == 1
    rows = read_delta_table_rows(
        table_path=output / "canonical_contract_economics.delta",
        columns=[
            "economics_session_date",
            "official_initial_margin",
            "margin_required_estimate",
            "margin_calibration_as_of_date",
            "model_quality",
            "margin_buffer_pct",
        ],
        limit=10,
    )
    assert len(rows) == 2
    by_date = {str(row["economics_session_date"]): row for row in rows}
    assert by_date["2026-08-14"]["official_initial_margin"] == 200.0
    assert by_date["2026-08-14"]["margin_required_estimate"] == 200.0
    tail = by_date["2026-10-05"]
    assert tail["official_initial_margin"] is None
    assert tail["margin_required_estimate"] == pytest.approx(240.0)
    assert str(tail["margin_calibration_as_of_date"]) == "2026-08-14"
    assert tail["model_quality"] == "calibrated_asset_rank"
    assert tail["margin_buffer_pct"] == 0.0
    assert raw_versions == {name: DeltaTable(str(path)).version() for name, path in paths.items()}
    assert all((Path(path) / "_delta_log").is_dir() for path in report["output_paths"].values())


def _bounded_regimes():
    from dataclasses import replace

    return tuple(
        replace(
            rule,
            rule_id=f"{rule.assetcode}-bounded-tail-20261005-v1",
            effective_from="2026-07-29",
            effective_to="2026-10-05",
            validation_method="dated_official_tc",
        )
        for rule in MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES[:13]
    )


def test_parameter_metadata_native_preserves_defaults_and_literal_strings(tmp_path, monkeypatch):
    from dataclasses import replace

    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    spark = _create_spark_session("parameter-metadata", "local[2]")
    try:
        rows = {r.rule_id: r.asDict() for r in _contract_parameter_regimes_frame(spark).collect()}
        assert len(rows) == 32
        for r in MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES:
            assert rows[r.rule_id]["rule_min_step"] == r.min_step
            assert rows[r.rule_id]["rule_tick_value_quote"] == r.tick_value_quote
            assert rows[r.rule_id]["rule_validation_method"] == "turnover"
        literal = replace(_bounded_regimes()[0], source_document_id="fixture'\\literal")
        extra = (
            _contract_parameter_regimes_frame(spark, (literal,))
            .where(f"rule_id = '{literal.rule_id}'")
            .first()
        )
        assert extra.rule_source_document_id == "fixture'\\literal"
    finally:
        spark.stop()


def test_parameter_owning_reader_pins_zero_when_current_advances(tmp_path, monkeypatch):
    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.data_plane.moex.contract_parameter_continuation import (
        bind_contract_parameter_input,
    )

    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw", trade_date="2026-07-29", expiration_date="2026-12-15"
    )
    original = DeltaTable(str(paths["contracts"]))
    _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2026-10-05",
        expiration_date="2026-12-15",
        mode="append",
        write_shared_inputs=False,
    )
    verified = {"prepared_input_table_id": original.metadata().id, "prepared_input_version": 0}
    assert bind_contract_parameter_input(paths["contracts"], verified) == 0
    spark = _create_spark_session("parameter-pinned-reader", "local[2]")
    try:
        assert _contract_specs_frame(spark, paths["contracts"]).count() == 2
        pinned = _contract_specs_frame(spark, paths["contracts"], source_version=0).collect()
        assert len(pinned) == 1
        assert pinned[0].trade_date.isoformat() == "2026-07-29"
        assert pinned[0].last_settle_price == 93.99
        assert DeltaTable(str(paths["contracts"])).version() == 1
    finally:
        spark.stop()


@pytest.mark.parametrize(
    "day,inside",
    [("2026-07-28", False), ("2026-07-29", True), ("2026-10-05", True), ("2026-10-06", False)],
)
def test_parameter_interval_native_is_inclusive_and_not_renewed(tmp_path, monkeypatch, day, inside):
    pytest.importorskip("pyspark.sql")
    from pyspark.sql import functions as F

    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw", trade_date=day, expiration_date="2026-12-15"
    )
    spark = _create_spark_session("parameter-boundary", "local[2]")
    try:
        specs = _contract_specs_frame(spark, paths["contracts"]).withColumn(
            "official_step_price", F.lit(None).cast("double")
        )
        fx = _fx_rates_frame(spark, paths["fx"])
        fx = fx.unionByName(
            fx.where(F.col("base_currency") == "USD").withColumn("clearing_type", F.lit("mc"))
        )
        risk = _asset_risk_parameters_frame(spark, paths["limits"], paths["staticparams"])
        frame = _economics_frame(
            specs, fx, risk, additional_contract_parameter_regimes=(_bounded_regimes()[0],)
        ).cache()
        row = frame.first()
        assert (row["__step_price_rule_id"] is not None) == inside
        if inside:
            assert row.step_price_rub == pytest.approx(0.1 * 71.9077)
        else:
            assert row.step_price_rub is None
            with pytest.raises(RuntimeError, match="STEPPRICE=1"):
                _fail_closed_on_missing_inputs(frame)
        frame.unpersist()
    finally:
        spark.stop()


@pytest.mark.parametrize(
    "clearing,error,passes", [("tc", 0.0, True), ("mc", 0.0, False), ("tc", 0.002, False)]
)
def test_parameter_dated_tc_gate_validates_official_source(
    tmp_path, monkeypatch, clearing, error, passes
):
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2026-08-14",
        fx_clearing_type=clearing,
        official_step_price=0.1 * 71.9077 * (1 + error),
        expiration_date="2026-12-15",
    )
    spark = _create_spark_session("parameter-dated-tc", "local[2]")
    try:
        frame = _economics_frame(
            _contract_specs_frame(spark, paths["contracts"]),
            _fx_rates_frame(spark, paths["fx"]),
            _asset_risk_parameters_frame(spark, paths["limits"], paths["staticparams"]),
            additional_contract_parameter_regimes=(_bounded_regimes()[0],),
        ).cache()
        assert frame.first()["__step_price_source"] == "moex_stepprice"
        if passes:
            profiles = _fail_closed_on_step_price_rule_validation(
                frame, additional_regimes=(_bounded_regimes()[0],)
            )
            assert profiles[0]["validation_rows"] == 1
            assert profiles[0]["validation_method"] == "dated_official_tc"
            with pytest.raises(RuntimeError, match="GOLD-bounded"):
                _fail_closed_on_step_price_rule_validation(
                    frame, additional_regimes=_bounded_regimes()[:2]
                )
        else:
            with pytest.raises(RuntimeError, match="rule validation failed"):
                _fail_closed_on_step_price_rule_validation(
                    frame, additional_regimes=(_bounded_regimes()[0],)
                )
        frame.unpersist()
    finally:
        spark.stop()


@pytest.mark.parametrize("rub_rule", [False, True])
def test_parameter_validation_uses_rule_currency_and_rub_identity(tmp_path, monkeypatch, rub_rule):
    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    rule = next(r for r in _bounded_regimes() if r.assetcode == ("MIX" if rub_rule else "BR"))
    paths = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "raw",
        trade_date="2026-08-14",
        assetcode=rule.assetcode,
        quote_currency="RUB",
        min_step=rule.min_step,
        lot_volume=rule.lot_volume,
        official_step_price=25.0 if rub_rule else 0.1,
        fx_rate=70.0,
        fx_clearing_type="mc" if rub_rule else "tc",
        expiration_date="2026-12-15",
    )
    spark = _create_spark_session("parameter-rule-currency", "local[2]")
    try:
        frame = _economics_frame(
            _contract_specs_frame(spark, paths["contracts"]),
            _fx_rates_frame(spark, paths["fx"]),
            _asset_risk_parameters_frame(spark, paths["limits"], paths["staticparams"]),
            additional_contract_parameter_regimes=(rule,),
        ).cache()
        if rub_rule:
            assert (
                _fail_closed_on_step_price_rule_validation(frame, additional_regimes=(rule,))[0][
                    "validation_p95_relative_error"
                ]
                == 0.0
            )
        else:
            # Rule USD must use USD TC70, never spec RUB's identity1.
            with pytest.raises(RuntimeError, match="rule validation failed"):
                _fail_closed_on_step_price_rule_validation(frame, additional_regimes=(rule,))
        frame.unpersist()
    finally:
        spark.stop()


def test_parameter_full_admission_native_preserves_schema_and_calibration(tmp_path, monkeypatch):
    import hashlib
    from dataclasses import asdict, replace
    from datetime import date

    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.data_plane.moex import (
        contract_parameter_continuation as admission,
    )

    pytest.importorskip("pyspark.sql")
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    template = _write_single_day_raw_economics_fixture(
        raw_root=tmp_path / "template", trade_date="2026-07-29", expiration_date="2026-12-15"
    )
    source_templates = {
        name: read_delta_table_rows(path, limit=1)[0] for name, path in template.items()
    }
    raw = tmp_path / "raw"
    paths = {name: raw / path.name for name, path in template.items()}
    rules = _bounded_regimes()
    rows = {name: [] for name in paths}
    for day in ("2026-07-29", "2026-08-14", "2026-10-05"):
        for rule in rules:
            margin = None if day == "2026-10-05" else 200.0
            step = (
                None
                if day == "2026-10-05"
                else rule.tick_value_quote * (1 if rule.quote_currency == "RUB" else 70)
            )
            contract = {
                **source_templates["contracts"],
                "trade_date": day,
                "moex_secid": rule.assetcode + "Z6",
                "assetcode": rule.assetcode,
                "min_step": rule.min_step,
                "lot_volume": rule.lot_volume,
                "official_step_price": step,
                "official_initial_margin": margin,
                "last_settle_price": 120.0 if day == "2026-10-05" else 100.0,
                "source_document_hash": f"fixture-{day}-{rule.assetcode}",
                "raw_payload_json": json.dumps({"CURRENCYID": rule.quote_currency}),
            }
            rows["contracts"].append(contract)
            for name in ("limits", "staticparams"):
                rows[name].append(
                    {
                        **source_templates[name],
                        "trade_date": day,
                        "assetcode": rule.assetcode,
                        "update_time": day + "T19:00:00Z",
                        "source_document_hash": f"{name}-{day}-{rule.assetcode}",
                    }
                )
        for clearing, rate in (("tc", 70.0), ("mc", 60.0)):
            rows["fx"].append(
                {
                    **source_templates["fx"],
                    "trade_date": day,
                    "clearing_type": clearing,
                    "rate": rate,
                    "source_document_hash": f"fx-{day}-{clearing}",
                }
            )
    for name, table_name in (
        ("contracts", "raw_moex_contract_securities"),
        ("fx", "raw_moex_indicative_fx_rates"),
        ("limits", "raw_moex_rms_limits"),
        ("staticparams", "raw_moex_rms_staticparams"),
    ):
        write_delta_table_rows(
            table_path=paths[name], rows=rows[name], columns=_columns(table_name)
        )
    dated = {
        "status": "PASS",
        "validation_method": "dated_official_tc",
        "snapshot_version": 134,
        "dated_official_rows": 674,
        "extrapolation_scope": ["2026-07-29", "2026-10-05"],
        "inference_not_daily_official": True,
        "turnover_proxy_rejected_due_to_nonidentical_trade_baskets": True,
        "profiles": [
            {
                "assetcode": r.assetcode,
                "rows": 111
                if r.assetcode == "WHEAT"
                else 67
                if r.assetcode == "BR"
                else 64
                if r.assetcode == "NG"
                else 72
                if r.assetcode in {"MIX", "MXI"}
                else 36,
                "p95": 0.0,
                "maximum": 0.0,
            }
            for r in rules
        ],
    }
    dated_path = tmp_path / "dated.json"
    dated_path.write_text(json.dumps(dated), encoding="utf-8")
    dated_sha = hashlib.sha256(dated_path.read_bytes()).hexdigest()
    rules = tuple(
        replace(r, source_document_id=r.source_document_id + ";sha256:" + dated_sha) for r in rules
    )
    rules_path = tmp_path / "rules.json"
    rules_path.write_text(json.dumps([asdict(r) for r in rules]), encoding="utf-8")
    original = {
        name: (DeltaTable(str(path)).metadata().id, DeltaTable(str(path)).version())
        for name, path in paths.items()
    }
    # Fixture-only admitted definition: production admission never accepts caller pins.
    monkeypatch.setattr(
        admission,
        "_ADMITTED_BUNDLE",
        replace(
            admission._ADMITTED_BUNDLE,
            regimes_sha256=hashlib.sha256(rules_path.read_bytes()).hexdigest(),
            evidence_sha256=dated_sha,
            prepared_table_id=original["contracts"][0],
        ),
    )
    output = tmp_path / "canonical"
    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=paths["contracts"],
        raw_fx_rates_path=paths["fx"],
        raw_rms_limits_path=paths["limits"],
        raw_rms_staticparams_path=paths["staticparams"],
        output_dir=output,
        allow_calibrated_latest_margin=True,
        margin_calibration_as_of_date=date(2026, 8, 14),
        additional_contract_parameter_regimes=rules,
        contract_parameter_evidence=admission.MoexContractParameterEvidence(
            "bounded-tail-20261005-v1", rules_path, dated_path
        ),
        report_path=tmp_path / "report.json",
    )
    assert report["status"] == "PASS"
    assert len(report["additional_contract_parameter_regimes"]) == 13
    assert report["contract_parameter_evidence"]["prepared_input_version"] == 0
    assert sum(p["validation_rows"] for p in report["step_price_rule_profiles"]) == 26
    assert all(
        p["validation_method"] == "dated_official_tc" for p in report["step_price_rule_profiles"]
    )
    table = output / "canonical_contract_economics.delta"
    canonical = read_delta_table_rows(table, limit=40)
    assert len(canonical) == 39
    assert set(DeltaTable(str(table)).schema().to_pyarrow().names) == set(
        _columns("canonical_contract_economics")
    )
    for row in canonical:
        assert (
            json.loads(row["source_flags_json"])["step_price_rule_validation_method"]
            == "dated_official_tc"
        )
        expected_margin = 200.0
        if str(row["economics_session_date"]) == "2026-10-05":
            expected_margin = 200.0 * (120.0 / 100.0)
            if row["quote_currency"] == "USD":
                expected_margin *= 60.0 / 70.0
        assert row["margin_required_estimate"] == pytest.approx(expected_margin)
        if str(row["economics_session_date"]) == "2026-10-05":
            assert row["official_initial_margin"] is None
            assert str(row["margin_calibration_as_of_date"]) == "2026-08-14"
    assert original == {
        name: (DeltaTable(str(path)).metadata().id, DeltaTable(str(path)).version())
        for name, path in paths.items()
    }
