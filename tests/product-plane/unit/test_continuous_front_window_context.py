"""Direct time bounds must select outputs without resetting causal roll state."""

from dataclasses import replace
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.research.datasets import ContinuousFrontPolicy
from trading_advisor_3000.spark_jobs import continuous_front_job as job


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2026-01-02T00:00:00Z", None),
        (None, "2026-01-03T00:00:00Z"),
        ("2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"),
    ],
)
def test_time_bounds_preserve_the_source_context(monkeypatch, start, end):
    pytest.importorskip("pyspark.sql")

    class SourceInspected(Exception):
        pass

    def inspect_source(**kwargs):
        assert kwargs["start_ts"] is None
        assert kwargs["end_ts"] is None
        assert kwargs["instrument_ids"] == ("FUT_BR",)
        assert kwargs["timeframes"] == ("15m",)
        raise SourceInspected

    monkeypatch.setattr(job, "_load_filtered_bars", inspect_source)
    with pytest.raises(SourceInspected):
        job._build_spark_native_tables(
            spark=None,
            canonical_bars_path=Path("unused.delta"),
            dataset_version="context-contract",
            policy=ContinuousFrontPolicy(),
            run_id="context-contract",
            instrument_ids=("FUT_BR",),
            timeframes=("15m",),
            start_ts=start,
            end_ts=end,
        )


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    # This is a native Spark transform test. Its sources are in-memory canonical
    # frames; no Delta connector, download, or production table is involved.
    sql = pytest.importorskip("pyspark.sql")
    root = tmp_path_factory.mktemp("cf-window-context")
    session = (
        sql.SparkSession.builder.master("local[2]")
        .appName("cf-window-context")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.local.dir", str(root / "local"))
        .config("spark.sql.warehouse.dir", str(root / "warehouse"))
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.parametrize("mode", ["liquidity_oi_v1", "calendar_expiry_v1"])
def test_direct_window_keeps_full_roll_context_and_l1_append(spark, monkeypatch, tmp_path, mode):
    from pyspark.sql import functions as F

    if mode == "liquidity_oi_v1":
        rows = [
            ("BRK2@MOEX", "2022-03-21 10:00:00", 100.0, 300),
            ("BRM2@MOEX", "2022-03-21 10:00:00", 110.0, 100),
            ("BRK2@MOEX", "2022-03-21 10:15:00", 101.0, 120),
            ("BRM2@MOEX", "2022-03-21 10:15:00", 111.0, 420),
            ("BRK2@MOEX", "2022-03-21 10:30:00", 102.0, 100),
            ("BRM2@MOEX", "2022-03-21 10:30:00", 112.0, 430),
            ("BRM2@MOEX", "2022-03-21 10:45:00", 113.0, 440),
        ]
        policy = ContinuousFrontPolicy(
            roll_policy_version="front_liquidity_oi_v1",
            roll_policy_mode=mode,
            primary_metric="open_interest",
            secondary_metric="volume",
            confirmation_bars=1,
            switch_timing="next_tradable_bar_after_decision_watermark",
            tie_breaker="higher_open_interest_then_volume_then_contract_id",
            reference_price_policy="decision_bar_close",
        )
        start, end = "2022-03-21 10:45:00", "2022-03-21 10:45:00"
    else:
        rows = [
            ("BRU5@MOEX", "2025-09-23 06:00:00", 100.0, 0),
            ("BRU5@MOEX", "2025-09-24 06:00:00", 101.0, 0),
            ("BRV5@MOEX", "2025-09-24 06:00:00", 111.0, 0),
            ("BRX5@MOEX", "2025-09-24 06:00:00", 211.0, 0),
            ("BRU5@MOEX", "2025-09-25 06:00:00", 102.0, 0),
            ("BRV5@MOEX", "2025-09-25 06:00:00", 112.0, 0),
            ("BRX5@MOEX", "2025-09-25 06:00:00", 212.0, 0),
            ("BRU5@MOEX", "2025-09-26 06:00:00", 103.0, 0),
            ("BRV5@MOEX", "2025-09-26 06:00:00", 113.0, 0),
            ("BRV5@MOEX", "2025-10-01 06:00:00", 114.0, 0),
        ]
        policy = ContinuousFrontPolicy(calendar_roll_offset_trading_days=2)
        start, end = "2025-09-25 00:00:00", "2025-09-26 23:59:59"
    bars = spark.createDataFrame(rows, ["contract_id", "time", "price", "oi"]).select(
        "contract_id",
        F.lit("FUT_BR").alias("instrument_id"),
        F.lit("15m").alias("timeframe"),
        F.col("time").cast("timestamp").alias("ts"),
        F.col("price").alias("open"),
        (F.col("price") + 1.0).alias("high"),
        (F.col("price") - 1.0).alias("low"),
        F.col("price").alias("close"),
        F.lit(100).cast("long").alias("volume"),
        F.col("oi").cast("long").alias("open_interest"),
    )

    def source(**kwargs):
        selected = bars
        if kwargs.get("start_ts"):
            selected = selected.where(F.col("ts") >= F.lit(kwargs["start_ts"]).cast("timestamp"))
        if kwargs.get("end_ts"):
            selected = selected.where(F.col("ts") <= F.lit(kwargs["end_ts"]).cast("timestamp"))
        return selected

    monkeypatch.setattr(job, "_load_filtered_bars", source)
    options = dict(
        spark=spark,
        canonical_bars_path=tmp_path / "unused.delta",
        dataset_version="window-context",
        policy=policy,
        run_id="window-context",
        instrument_ids=("FUT_BR",),
        timeframes=("15m",),
    )
    full = job._build_spark_native_tables(**options, start_ts=None, end_ts=None)
    expected = [r.asDict() for r in full["continuous_front_bars"].orderBy("ts").collect()]
    scoped = job._build_spark_native_tables(**options, start_ts=start, end_ts=end)
    actual = [r.asDict() for r in scoped["continuous_front_bars"].orderBy("ts").collect()]
    lower, upper = start.replace(" ", "T"), end.replace(" ", "T")
    wanted = [r for r in expected if lower <= r["ts"].isoformat() <= upper]
    assert actual and len(actual) == len(wanted)
    assert all(r["roll_epoch"] == 1 for r in wanted)
    columns = (
        "ts",
        "active_contract_id",
        "previous_contract_id",
        "roll_epoch",
        "roll_event_id",
        "bars_since_roll",
        "native_close",
    )
    assert [{c: r[c] for c in columns} for r in actual] == [
        {c: r[c] for c in columns} for r in wanted
    ]
    for name in ("continuous_front_roll_events", "continuous_front_adjustment_ladder"):
        # Reading the prefix must not append historical sidecars outside the
        # writer's bounded replacement predicate.
        assert scoped[name].collect() == []

    # Feed the real producer's bounded example to L1. Its state must survive
    # serialization and produce the same values as a full calculation of this
    # small example; the historical corrupted market snapshot is never edited.
    from test_research_indicator_layer import _continuous_view, _sma2_profile

    from trading_advisor_3000.product_plane.research.indicators import materialize as m

    research = [
        replace(
            _continuous_view(
                ts_index=i,
                close=r["native_close"],
                roll_epoch=r["roll_epoch"],
                active_contract_id=r["active_contract_id"],
            ),
            ts=r["ts"].isoformat() + "Z",
        )
        for i, r in enumerate(expected)
    ]
    ladder = tuple(
        dict(
            instrument_id="BR",
            timeframe="15m",
            roll_sequence=int(r.roll_sequence),
            effective_ts=r.effective_ts.isoformat() + "Z",
            additive_gap=float(r.additive_gap),
        )
        for r in full["continuous_front_adjustment_ladder"].collect()
    )
    kwargs = dict(
        dataset_version="dataset-v3",
        indicator_set_version="test",
        profile=_sma2_profile(),
        series_mode="continuous_front",
        adjustment_ladder_rows=ladder,
    )
    old = m._build_partition_rows(series=research[:-1], **kwargs)
    old = [m.IndicatorFrameRow.from_dict(r.to_dict()) for r in old]
    diagnostics = {}
    tail = m._build_partition_rows(
        series=research, existing_rows=old, compute_diagnostics=diagnostics, **kwargs
    )
    oracle = m._build_partition_rows(series=research, **kwargs)
    assert diagnostics.get("cf_tail_partition_count") == 1, diagnostics
    assert [r.values for r in tail] == [r.values for r in oracle]
