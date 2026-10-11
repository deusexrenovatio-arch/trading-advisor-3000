from __future__ import annotations

from datetime import date

import pytest

from trading_advisor_3000.spark_jobs.canonical_bars_job import _create_spark_session
from trading_advisor_3000.spark_jobs.moex_contract_economics_job import (
    _fail_closed_on_missing_inputs,
    _with_latest_margin_calibration,
)

CALIBRATION_DATE = date(2026, 8, 14)
LATEST_DATE = date(2026, 10, 5)


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark.sql")
    session = _create_spark_session("explicit-historical-margin-gate", "local[2]")
    yield session
    session.stop()


def _source(spark, *, later_official=False):
    extra = ", ('BR_LATER', '2026-09-01', 'BR', 1, 100D, 9900D)" if later_official else ""
    return spark.sql(
        "SELECT contract_id, CAST(day AS DATE) AS economics_session_date, "
        "assetcode, rank AS maturity_rank, formula AS margin_radius_adjusted, "
        "official AS official_initial_margin, 'tc' AS clearing_type, "
        "1D AS min_step, 1D AS lot_volume, 1D AS fx_rate_to_rub, "
        "1D AS mr1, 1D AS last_settle_price, 1D AS step_price_rub, "
        "1D AS official_step_price FROM VALUES "
        "('BR_ONE', '2026-08-14', 'BR', 1, 100D, 200D), "
        "('BR_THREE', '2026-08-14', 'BR', 3, 100D, 400D), "
        "('BR_TAIL', '2026-10-05', 'BR', 2, 150D, CAST(NULL AS DOUBLE))"
        + extra
        + " AS input(contract_id, day, assetcode, rank, formula, official)"
    )


def _calibrated(spark, **kwargs):
    return _with_latest_margin_calibration(
        _source(spark, **kwargs), margin_calibration_as_of_date=CALIBRATION_DATE
    )


def _gate(frame, calibration_date=CALIBRATION_DATE):
    return _fail_closed_on_missing_inputs(
        frame,
        allow_calibrated_latest_margin=True,
        margin_calibration_as_of_date=calibration_date,
    )


def test_explicit_calibration_uses_exact_date_and_nearest_rank(spark):
    frame = _calibrated(spark)
    rows = {row.contract_id: row.asDict() for row in frame.collect()}
    assert rows["BR_ONE"]["margin_required_estimate"] == 200.0
    assert rows["BR_THREE"]["margin_required_estimate"] == 400.0
    assert rows["BR_ONE"]["model_quality"] == "official_initial_margin"
    tail = rows["BR_TAIL"]
    assert tail["margin_required_estimate"] == 300.0
    assert tail["margin_calibration_rank"] == 1
    assert tail["margin_calibration_factor"] == 2.0
    assert tail["margin_calibration_as_of_date"] == CALIBRATION_DATE
    assert tail["margin_calibration_source"] == "latest_official_nearest_rank"
    assert tail["model_quality"] == "calibrated_nearest_rank"
    assert tail["official_initial_margin"] is None
    assert tail["margin_buffer_pct"] == 0.0
    latest, historical = _gate(frame)
    assert latest["INITIALMARGIN"] == historical["INITIALMARGIN"] == 1


def test_explicit_calibration_does_not_use_a_later_official_snapshot(spark):
    frame = _calibrated(spark, later_official=True)
    tail = frame.where("contract_id = 'BR_TAIL'").first()
    assert tail.margin_required_estimate == 300.0
    assert tail.margin_calibration_as_of_date == CALIBRATION_DATE
    _gate(frame)


def test_explicit_calibration_missing_exact_date_fails_closed(spark):
    absent = date(2026, 8, 13)
    frame = _with_latest_margin_calibration(_source(spark), margin_calibration_as_of_date=absent)
    tail = frame.where("contract_id = 'BR_TAIL'").first()
    assert tail.margin_required_estimate is None
    assert tail.model_quality == "unavailable"
    with pytest.raises(RuntimeError, match="CALIBRATED_INITIALMARGIN=1"):
        _gate(frame, absent)


def test_latest_official_margin_is_required_by_default(spark):
    frame = _with_latest_margin_calibration(_source(spark))
    with pytest.raises(RuntimeError, match="INITIALMARGIN=1"):
        _fail_closed_on_missing_inputs(frame)


def test_historical_mode_requires_an_explicit_date(spark):
    with pytest.raises(ValueError, match="explicit calibration date"):
        _fail_closed_on_missing_inputs(
            _with_latest_margin_calibration(_source(spark)),
            allow_calibrated_latest_margin=True,
        )


@pytest.mark.parametrize("formula", [None, 0.0, -1.0])
def test_missing_or_nonpositive_formula_stays_unavailable(spark, formula):
    from pyspark.sql import functions as F

    source = _source(spark).withColumn(
        "margin_radius_adjusted",
        F.when(F.col("contract_id") == "BR_TAIL", F.lit(formula).cast("double")).otherwise(
            F.col("margin_radius_adjusted")
        ),
    )
    frame = _with_latest_margin_calibration(source, margin_calibration_as_of_date=CALIBRATION_DATE)
    tail = frame.where("contract_id = 'BR_TAIL'").first()
    assert tail.margin_required_estimate is None
    assert tail.model_quality == "unavailable"
    with pytest.raises(RuntimeError, match="CALIBRATED_INITIALMARGIN=1"):
        _gate(frame)


@pytest.mark.parametrize(
    "column,value",
    [
        ("margin_calibration_factor", None),
        ("margin_calibration_factor", 0.0),
        ("margin_calibration_factor", -1.0),
        ("margin_calibration_factor", float("nan")),
        ("margin_calibration_factor", float("inf")),
        ("margin_required_estimate", None),
        ("margin_required_estimate", 0.0),
        ("margin_required_estimate", -1.0),
        ("margin_required_estimate", float("nan")),
        ("margin_required_estimate", float("inf")),
        ("margin_calibration_as_of_date", date(2026, 8, 13)),
        ("margin_calibration_as_of_date", None),
        ("margin_calibration_source", "unverified_source"),
        ("model_quality", "unavailable"),
    ],
)
def test_historical_margin_gate_rejects_invalid_model_metadata(spark, column, value):
    from pyspark.sql import functions as F

    frame = _calibrated(spark)
    dtype = frame.schema[column].dataType
    changed = frame.withColumn(
        column,
        F.when(F.col("contract_id") == "BR_TAIL", F.lit(value).cast(dtype)).otherwise(
            F.col(column)
        ),
    )
    with pytest.raises(RuntimeError, match="CALIBRATED_INITIALMARGIN=1"):
        _gate(changed)


@pytest.mark.parametrize(
    "column,label",
    [
        ("min_step", "MINSTEP"),
        ("lot_volume", "LOTVOLUME"),
        ("fx_rate_to_rub", "FX"),
        ("mr1", "MR1"),
        ("last_settle_price", "LASTSETTLEPRICE"),
        ("step_price_rub", "STEPPRICE"),
    ],
)
def test_calibration_does_not_relax_other_required_inputs(spark, column, label):
    from pyspark.sql import functions as F

    frame = _calibrated(spark).withColumn(
        column, F.when(F.col("contract_id") != "BR_TAIL", F.col(column))
    )
    with pytest.raises(RuntimeError, match=f"{label}=1"):
        _gate(frame)


def test_explicit_calibration_has_no_cross_asset_fallback(spark):
    from pyspark.sql import functions as F

    source = _source(spark).withColumn(
        "assetcode",
        F.when(F.col("contract_id") == "BR_TAIL", F.lit("SI")).otherwise(F.col("assetcode")),
    )
    frame = _with_latest_margin_calibration(source, margin_calibration_as_of_date=CALIBRATION_DATE)
    assert frame.where("contract_id = 'BR_TAIL'").first().margin_required_estimate is None
    with pytest.raises(RuntimeError, match="CALIBRATED_INITIALMARGIN=1"):
        _gate(frame)


def test_calibration_duplicate_pairs_keep_existing_fail_closed_report(spark):
    from pyspark.sql import functions as F

    from trading_advisor_3000.spark_jobs.moex_contract_economics_job import (
        _margin_calibration_report,
    )

    source = _source(spark)
    duplicate = source.where("contract_id = 'BR_ONE'").withColumn(
        "contract_id", F.lit("BR_ONE_DUPLICATE")
    )
    frame = _with_latest_margin_calibration(
        source.unionByName(duplicate), margin_calibration_as_of_date=CALIBRATION_DATE
    )
    with pytest.raises(RuntimeError, match="duplicate"):
        _margin_calibration_report(frame)


def test_historical_margin_gate_rejects_future_calibration_date(spark):
    from pyspark.sql import functions as F

    future = date(2026, 10, 6)
    frame = _calibrated(spark).withColumn("margin_calibration_as_of_date", F.lit(future))
    with pytest.raises(RuntimeError, match="CALIBRATED_INITIALMARGIN=1"):
        _gate(frame, future)
