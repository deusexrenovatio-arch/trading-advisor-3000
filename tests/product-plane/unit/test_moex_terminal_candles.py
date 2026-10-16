from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.moex import foundation
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import MoexCandle

CASES = [
    (1, "1m", "2026-05-04 18:59:00", "2026-05-04 18:59:59"),
    (60, "1h", "2026-05-04 18:00:00", "2026-05-04 18:59:59"),
    (24, "1d", "2026-05-04 00:00:00", "2026-05-04 23:59:59"),
    (7, "1w", "2026-06-15 00:00:00", "2026-06-21 00:00:00"),
]


@pytest.mark.parametrize("mode", ["baseline", "bootstrap"])
@pytest.mark.parametrize("interval,timeframe,begin,end", CASES)
def test_ingest_retains_official_terminal_candle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    interval: int,
    timeframe: str,
    begin: str,
    end: str,
) -> None:
    opening = foundation._parse_moex_datetime(begin)
    closing = foundation._parse_moex_datetime(end)
    coverage = foundation.DiscoveryRecord(
        internal_id="FUT_BR",
        finam_symbol="BRK6@MOEX",
        moex_engine="futures",
        moex_market="forts",
        moex_board="RFUD",
        moex_secid="BRK6",
        asset_group="commodity",
        requested_target_timeframes=timeframe,
        source_interval=interval,
        source_timeframe=timeframe,
        coverage_begin_utc=foundation._to_iso_utc(opening - timedelta(days=30)),
        coverage_end_utc=foundation._to_iso_utc(opening),
        discovered_at_utc="2026-07-01T00:00:00Z",
        discovery_url="https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRK6/candleborders.json",
    )
    candle = MoexCandle(
        open=114.09, close=114.09, high=114.09, low=114.09, volume=22, begin=begin, end=end
    )
    captured: dict = {}

    class Client:
        def iter_candles(self, **kwargs):
            captured["fetched"] = True
            yield candle

    def writer(**kwargs):
        captured.update(kwargs)
        captured["rows"] = [
            json.loads(line) for line in kwargs["source_rows_path"].read_text().splitlines()
        ]
        return {
            "status": "PASS",
            "source_rows": len(captured["rows"]),
            "incremental_rows": len(captured["rows"]),
            "deduplicated_rows": 0,
            "stale_rows": 0,
            "changed_windows": [],
        }

    # The preceding candle must not be treated as covering a known terminal
    # candle merely because it is within the low-liquidity watermark grace.
    monkeypatch.setattr(
        foundation,
        "compute_raw_watermarks_spark_delta",
        lambda **kwargs: {
            ("FUT_BR", timeframe, "BRK6"): foundation._to_iso_utc(opening - timedelta(seconds=1))
        },
    )
    monkeypatch.setattr(foundation, "run_moex_raw_ingest_spark_delta_job", writer)
    kwargs = dict(
        client=Client(),
        coverage=[coverage],
        table_path=tmp_path / "raw" / "bars.delta",
        run_id="terminal",
        ingest_till_utc="2026-07-01T00:00:00Z",
        stability_lag_minutes=20,
        refresh_overlap_minutes=0,
    )
    if mode == "baseline":
        foundation.ingest_moex_baseline_window(**kwargs, refresh_window_days=30)
    else:
        foundation.ingest_moex_bootstrap_window(**kwargs, bootstrap_window_days=30)
    assert captured.get("fetched") is True
    assert len(captured["rows"]) == 1
    assert captured["rows"][0]["ts_close"] == foundation._to_iso_utc(closing)
    assert foundation._parse_iso_utc(captured["window_scopes"][0]["window_end_utc"]) >= closing


@pytest.mark.parametrize("mode", ["baseline", "bootstrap"])
def test_terminal_extension_keeps_stability_cutoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    record = foundation.DiscoveryRecord(
        internal_id="FUT_BR",
        finam_symbol="BRK6@MOEX",
        moex_engine="futures",
        moex_market="forts",
        moex_board="RFUD",
        moex_secid="BRK6",
        asset_group="commodity",
        requested_target_timeframes="1m",
        source_interval=1,
        source_timeframe="1m",
        coverage_begin_utc="2026-05-04T15:58:00Z",
        coverage_end_utc="2026-05-04T15:59:00Z",
        discovered_at_utc="2026-05-04T16:19:30Z",
        discovery_url="https://iss.moex.com/BRK6/candleborders.json",
    )
    candles = [
        MoexCandle(
            open=1,
            high=1,
            low=1,
            close=1,
            volume=1,
            begin="2026-05-04 18:58:00",
            end="2026-05-04 18:58:59",
        ),
        MoexCandle(
            open=1,
            high=1,
            low=1,
            close=1,
            volume=1,
            begin="2026-05-04 18:59:00",
            end="2026-05-04 18:59:59",
        ),
    ]
    captured = {}

    class Client:
        def iter_candles(self, **kwargs):
            yield from candles

    def writer(**kwargs):
        captured["rows"] = [
            json.loads(line) for line in kwargs["source_rows_path"].read_text().splitlines()
        ]
        captured["scopes"] = kwargs["window_scopes"]
        return {
            "status": "PASS",
            "source_rows": len(captured["rows"]),
            "incremental_rows": len(captured["rows"]),
            "deduplicated_rows": 0,
            "stale_rows": 0,
            "changed_windows": [],
        }

    monkeypatch.setattr(foundation, "compute_raw_watermarks_spark_delta", lambda **kwargs: {})
    monkeypatch.setattr(foundation, "run_moex_raw_ingest_spark_delta_job", writer)
    kwargs = dict(
        client=Client(),
        coverage=[record],
        table_path=tmp_path / "raw" / "bars.delta",
        run_id="stable",
        ingest_till_utc="2026-05-04T16:19:30Z",
        stability_lag_minutes=20,
        refresh_overlap_minutes=0,
    )
    if mode == "baseline":
        foundation.ingest_moex_baseline_window(**kwargs, refresh_window_days=1)
    else:
        foundation.ingest_moex_bootstrap_window(**kwargs, bootstrap_window_days=1)
    assert len(captured["rows"]) == 1
    assert captured["rows"][0]["ts_close"] == "2026-05-04T15:58:59Z"
    assert captured["scopes"][0]["window_end_utc"] == "2026-05-04T15:59:30Z"


@pytest.mark.parametrize("offset_hours", [0, 3])
def test_nightly_closed_day_reaches_native_daily_filter(tmp_path, monkeypatch, offset_hours):
    from datetime import UTC, datetime, timezone

    from dagster import build_schedule_context

    from trading_advisor_3000.dagster_defs import moex_historical_assets as assets

    monkeypatch.setattr(assets, "_route_artifact_root", lambda: tmp_path)
    monkeypatch.setattr(assets, "registered_history_refresh_kwargs", lambda root: {})
    monkeypatch.setattr(assets, "_baseline_economics_options", lambda config: {})
    scheduled = datetime(2026, 10, 15, 23, tzinfo=UTC).astimezone(
        timezone(timedelta(hours=offset_hours))
    )
    tick = assets.moex_baseline_daily_update_schedule.evaluate_tick(
        build_schedule_context(scheduled_execution_time=scheduled)
    )
    config = tick.run_requests[0].run_config["ops"]["moex_baseline_update"]["config"]
    assert config["ingest_till_utc"] == "2026-10-15T21:00:00Z"
    # Live/raw callers still retain their stability delay.
    generic = assets.build_moex_baseline_update_run_config(baseline_root=tmp_path, run_id="generic")
    assert generic["ops"]["moex_baseline_update"]["config"]["stability_lag_minutes"] == 20
    record = foundation.DiscoveryRecord(
        internal_id="FUT_BR",
        finam_symbol="BRX6@MOEX",
        moex_engine="futures",
        moex_market="forts",
        moex_board="RFUD",
        moex_secid="BRX6",
        asset_group="commodity",
        requested_target_timeframes="1d",
        source_interval=24,
        source_timeframe="1d",
        coverage_begin_utc="2026-10-13T21:00:00Z",
        coverage_end_utc="2026-10-15T21:00:00Z",
        discovered_at_utc="2026-10-15T23:00:00Z",
        discovery_url="https://iss.moex.com/BRX6/candleborders.json",
    )

    class Client:
        def iter_candles(self, **kwargs):
            # The source may return both the last closed and the ongoing day.
            for day in (15, 16):
                yield MoexCandle(
                    open=1,
                    high=1,
                    low=1,
                    close=1,
                    volume=1,
                    begin=f"2026-10-{day} 00:00:00",
                    end=f"2026-10-{day} 23:59:59",
                )

    captured = {}

    def writer(**kwargs):
        captured["rows"] = [
            json.loads(line) for line in kwargs["source_rows_path"].read_text().splitlines()
        ]
        return {"source_rows": len(captured["rows"])}

    # Execute the actual timestamp conversion, coverage and candle filtering;
    # only the subsequent Spark/Delta write is isolated in this unit regression.
    monkeypatch.setattr(foundation, "compute_raw_watermarks_spark_delta", lambda **kwargs: {})
    monkeypatch.setattr(foundation, "run_moex_raw_ingest_spark_delta_job", writer)
    foundation.ingest_moex_baseline_window(
        client=Client(),
        coverage=[record],
        table_path=tmp_path / "raw" / "bars.delta",
        run_id="closed-day",
        ingest_till_utc=config["ingest_till_utc"],
        stability_lag_minutes=config["stability_lag_minutes"],
        refresh_window_days=7,
        refresh_overlap_minutes=180,
    )
    assert [row["ts_close"] for row in captured["rows"]] == ["2026-10-15T20:59:59Z"]
