from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import (
    FuturesSessionScheduleRow,
)
from trading_advisor_3000.product_plane.data_plane.moex.session_schedule import (
    CANDLE_INFERRED_SESSION_POLICY_ID,
    DEFAULT_PUBLIC_RULE_CATALOG_PATH,
    OFFICIAL_FUTURES_SESSION_SOURCE_ID,
    PUBLIC_RECONSTRUCTED_SESSION_POLICY_ID,
    materialize_official_session_schedule_for_changed_windows,
    materialize_reconstructed_session_schedule_for_changed_windows,
    normalize_raw_session_schedule_rows,
)

RAW_COLUMNS: dict[str, str] = {
    "internal_id": "string",
    "finam_symbol": "string",
    "timeframe": "string",
    "source_interval": "int",
    "ts_open": "timestamp",
    "ts_close": "timestamp",
    "open": "double",
    "high": "double",
    "low": "double",
    "close": "double",
    "volume": "bigint",
    "open_interest": "bigint",
    "ingest_run_id": "string",
    "ingested_at_utc": "timestamp",
    "provenance_json": "json",
}


class FakeClient:
    def fetch_futures_session_schedule(self, **_kwargs):
        return [
            FuturesSessionScheduleRow(
                tradedate="2026-04-21",
                secid="-",
                boardid="-",
                session_type="morning_session",
                time_from="2026-04-21 09:00:00",
                time_till="2026-04-21 10:00:00",
                updatetime="2026-04-20 20:00:00",
                payload={"source": "fixture", "seq": 1},
            ),
            FuturesSessionScheduleRow(
                tradedate="2026-04-21",
                secid="-",
                boardid="-",
                session_type="main_session",
                time_from="2026-04-21 10:00:00",
                time_till="2026-04-21 18:50:00",
                updatetime="2026-04-20 20:00:00",
                payload={"source": "fixture", "seq": 2},
            ),
        ]


def _rule_catalog(
    path: Path,
    *,
    effective_from: str = "2026-04-21",
    calendar_exceptions: list[dict[str, object]] | None = None,
) -> Path:
    payload = {
        "catalog_id": "test-session-rules",
        "timezone": "Europe/Moscow",
        "rules": [
            {
                "rule_id": "test-public-rule",
                "effective_from": effective_from,
                "effective_to": "2026-04-21",
                "weekdays": [1, 2, 3, 4, 5],
                "applies_to": {"engine": "futures", "market": "forts"},
                "source_title": "fixture public source",
                "source_date": "2026-04-20",
                "source_urls": ["https://www.moex.com/test-public-source"],
                "source_quality": "public_moex_page",
                "confidence": "high",
                "trading_intervals": [
                    {
                        "type": "morning_session",
                        "time_from": "09:00:00",
                        "time_till": "10:00:00",
                        "session_class": "regular",
                    }
                ],
            }
        ],
        "calendar_exceptions": calendar_exceptions or [],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _changed_window(date_text: str = "2026-04-21", *, days: int = 1) -> list[dict[str, object]]:
    session_date = date.fromisoformat(date_text)
    moscow_tz = ZoneInfo("Europe/Moscow")
    start_utc = datetime.combine(session_date, time.min, tzinfo=moscow_tz).astimezone(UTC)
    end_utc = datetime.combine(
        session_date + timedelta(days=days),
        time.min,
        tzinfo=moscow_tz,
    ).astimezone(UTC)
    return [
        {
            "internal_id": "FUT_BR",
            "source_timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRM6@MOEX",
            "window_start_utc": _iso(start_utc),
            "window_end_utc": _iso(end_utc),
        }
    ]


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def test_materialize_official_session_schedule_builds_raw_and_canonical_delta(
    tmp_path: Path,
) -> None:
    raw_schedule_path = tmp_path / "raw_moex_session_schedule.delta"
    intervals_path = tmp_path / "canonical_session_intervals.delta"

    report = materialize_official_session_schedule_for_changed_windows(
        client=FakeClient(),
        changed_windows=[
            {
                "internal_id": "FUT_BR",
                "source_timeframe": "1m",
                "source_interval": 1,
                "moex_secid": "BRM6@MOEX",
                "window_start_utc": "2026-04-21T00:00:00Z",
                "window_end_utc": "2026-04-21T23:59:00Z",
            }
        ],
        mappings=[],
        raw_schedule_path=raw_schedule_path,
        canonical_session_intervals_path=intervals_path,
        fetched_at_utc="2026-04-21T00:00:00Z",
    )

    assert report["status"] == "PASS"
    assert report["source_id"] == OFFICIAL_FUTURES_SESSION_SOURCE_ID
    assert has_delta_log(raw_schedule_path)
    assert has_delta_log(intervals_path)

    raw_rows = read_delta_table_rows(raw_schedule_path, limit=10)
    interval_rows = read_delta_table_rows(intervals_path, limit=10)
    assert len(raw_rows) == 2
    assert [row["interval_seq"] for row in interval_rows] == [1, 2]
    assert interval_rows[0]["expected_open_ts"] == "2026-04-21T06:00:00Z"
    assert {row["instrument_id"] for row in interval_rows} == {"FUT_BR"}
    assert {row["source_id"] for row in interval_rows} == {OFFICIAL_FUTURES_SESSION_SOURCE_ID}


def test_reconstructed_session_schedule_uses_static_rule_catalog_without_client(
    tmp_path: Path,
) -> None:
    raw_schedule_path = tmp_path / "raw_moex_session_schedule.delta"
    intervals_path = tmp_path / "canonical_session_intervals.delta"

    report = materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window(),
        mappings=[],
        raw_schedule_path=raw_schedule_path,
        canonical_session_intervals_path=intervals_path,
        rule_catalog_path=_rule_catalog(tmp_path / "rules.json"),
        allow_candle_inference=False,
        fetched_at_utc="2026-04-21T00:00:00Z",
    )

    interval_rows = read_delta_table_rows(intervals_path, limit=10)
    raw_rows = read_delta_table_rows(raw_schedule_path, limit=10)
    assert report["mode"] == "static_reconstructed"
    assert report["public_rule_rows"] == 1
    assert report["candle_inferred_rows"] == 0
    assert interval_rows[0]["policy_id"] == PUBLIC_RECONSTRUCTED_SESSION_POLICY_ID
    assert interval_rows[0]["expected_open_ts"] == "2026-04-21T06:00:00Z"
    assert "fixture public source" in raw_rows[0]["raw_payload_json"]


def test_reconstructed_session_schedule_fails_closed_without_rule_catalog(
    tmp_path: Path,
) -> None:
    missing_catalog = tmp_path / "missing-rules.json"
    try:
        materialize_reconstructed_session_schedule_for_changed_windows(
            changed_windows=_changed_window(),
            mappings=[],
            raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
            canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
            rule_catalog_path=missing_catalog,
            allow_candle_inference=False,
        )
    except FileNotFoundError as exc:
        assert "static rule catalog" in str(exc)
    else:
        raise AssertionError("missing rule catalog must fail closed")


def test_reconstructed_session_schedule_fails_closed_for_weekday_rule_gap(
    tmp_path: Path,
) -> None:
    try:
        materialize_reconstructed_session_schedule_for_changed_windows(
            changed_windows=_changed_window("2026-04-21", days=2),
            mappings=[],
            raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
            canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
            rule_catalog_path=_rule_catalog(tmp_path / "rules.json"),
            allow_candle_inference=False,
        )
    except ValueError as exc:
        assert "missing weekday coverage" in str(exc)
        assert "BRM6:2026-04-22" in str(exc)
    else:
        raise AssertionError("weekday rule gaps must fail closed")


def test_reconstructed_session_schedule_records_closed_holiday_without_intervals(
    tmp_path: Path,
) -> None:
    raw_schedule_path = tmp_path / "raw_moex_session_schedule.delta"
    intervals_path = tmp_path / "canonical_session_intervals.delta"

    report = materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2026-04-21"),
        mappings=[],
        raw_schedule_path=raw_schedule_path,
        canonical_session_intervals_path=intervals_path,
        rule_catalog_path=_rule_catalog(
            tmp_path / "rules.json",
            calendar_exceptions=[
                {
                    "exception_id": "test-closed-holiday",
                    "action": "closed",
                    "dates": ["2026-04-21"],
                    "source_title": "fixture closed source",
                    "source_urls": ["https://www.moex.com/test-closed"],
                    "applies_to": {"engine": "futures", "market": "forts"},
                }
            ],
        ),
        allow_candle_inference=False,
        fetched_at_utc="2026-04-21T00:00:00Z",
    )

    raw_rows = read_delta_table_rows(raw_schedule_path, limit=10)
    interval_rows = read_delta_table_rows(intervals_path, limit=10)
    payload = json.loads(str(raw_rows[0]["raw_payload_json"]))
    assert report["closed_calendar_rows"] == 1
    assert payload["trading_status"] == "closed"
    assert payload["exception_id"] == "test-closed-holiday"
    assert interval_rows == []


def test_reconstructed_session_schedule_applies_special_working_weekend(
    tmp_path: Path,
) -> None:
    report = materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2026-04-25"),
        mappings=[],
        raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
        rule_catalog_path=_rule_catalog(
            tmp_path / "rules.json",
            calendar_exceptions=[
                {
                    "exception_id": "test-working-saturday",
                    "action": "special_open",
                    "dates": ["2026-04-25"],
                    "source_title": "fixture working saturday",
                    "source_urls": ["https://www.moex.com/test-open"],
                    "applies_to": {"engine": "futures", "market": "forts"},
                    "trading_intervals": [
                        {
                            "type": "weekend_session",
                            "time_from": "10:00:00",
                            "time_till": "19:00:00",
                            "session_class": "weekend",
                        }
                    ],
                }
            ],
        ),
        allow_candle_inference=False,
        fetched_at_utc="2026-04-25T00:00:00Z",
    )

    interval_rows = read_delta_table_rows(
        tmp_path / "canonical_session_intervals.delta",
        limit=10,
    )
    assert report["calendar_exception_ids"] == ["test-working-saturday"]
    assert interval_rows[0]["session_class"] == "weekend"
    assert interval_rows[0]["expected_open_ts"] == "2026-04-25T07:00:00Z"


def test_reconstructed_session_schedule_records_default_closed_weekend(
    tmp_path: Path,
) -> None:
    report = materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2026-04-26"),
        mappings=[],
        raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
        rule_catalog_path=_rule_catalog(tmp_path / "rules.json"),
        allow_candle_inference=False,
        fetched_at_utc="2026-04-26T00:00:00Z",
    )

    raw_rows = read_delta_table_rows(tmp_path / "raw_moex_session_schedule.delta", limit=10)
    interval_rows = read_delta_table_rows(
        tmp_path / "canonical_session_intervals.delta",
        limit=10,
    )
    payload = json.loads(str(raw_rows[0]["raw_payload_json"]))
    assert report["closed_calendar_rows"] == 1
    assert payload["exception_id"] == "moex-forts-default-weekend-closed"
    assert interval_rows == []


def test_default_catalog_preserves_2022_futures_regime_change(tmp_path: Path) -> None:
    before_path = tmp_path / "before"
    after_path = tmp_path / "after"

    materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2022-08-15"),
        mappings=[],
        raw_schedule_path=before_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=before_path / "canonical_session_intervals.delta",
        rule_catalog_path=DEFAULT_PUBLIC_RULE_CATALOG_PATH,
        allow_candle_inference=False,
        fetched_at_utc="2022-08-15T00:00:00Z",
    )
    materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2022-09-13"),
        mappings=[],
        raw_schedule_path=after_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=after_path / "canonical_session_intervals.delta",
        rule_catalog_path=DEFAULT_PUBLIC_RULE_CATALOG_PATH,
        allow_candle_inference=False,
        fetched_at_utc="2022-09-13T00:00:00Z",
    )

    before_rows = read_delta_table_rows(
        before_path / "canonical_session_intervals.delta",
        limit=10,
    )
    after_rows = read_delta_table_rows(
        after_path / "canonical_session_intervals.delta",
        limit=10,
    )
    assert [row["interval_type"] for row in before_rows] == [
        "main_session",
        "evening_session",
    ]
    assert [row["interval_type"] for row in after_rows] == [
        "morning_session",
        "main_session",
        "evening_session",
    ]
    assert after_rows[0]["expected_open_ts"] == "2022-09-13T06:00:00Z"


def test_default_catalog_covers_current_2020_start_regime(tmp_path: Path) -> None:
    materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2020-06-18"),
        mappings=[],
        raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
        rule_catalog_path=DEFAULT_PUBLIC_RULE_CATALOG_PATH,
        allow_candle_inference=False,
        fetched_at_utc="2020-06-18T00:00:00Z",
    )

    interval_rows = read_delta_table_rows(
        tmp_path / "canonical_session_intervals.delta",
        limit=10,
    )
    assert [row["interval_type"] for row in interval_rows] == [
        "main_session",
        "evening_session",
    ]
    assert interval_rows[0]["expected_open_ts"] == "2020-06-18T07:00:00Z"
    assert interval_rows[1]["expected_open_ts"] == "2020-06-18T16:05:00Z"


def test_default_catalog_covers_2021_morning_regime(tmp_path: Path) -> None:
    materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2021-03-01"),
        mappings=[],
        raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
        rule_catalog_path=DEFAULT_PUBLIC_RULE_CATALOG_PATH,
        allow_candle_inference=False,
        fetched_at_utc="2021-03-01T00:00:00Z",
    )

    interval_rows = read_delta_table_rows(
        tmp_path / "canonical_session_intervals.delta",
        limit=10,
    )
    assert [row["interval_type"] for row in interval_rows] == [
        "morning_session",
        "main_session",
        "evening_session",
    ]
    assert interval_rows[0]["expected_open_ts"] == "2021-03-01T04:00:00Z"
    assert interval_rows[2]["expected_open_ts"] == "2021-03-01T16:00:00Z"


def test_default_catalog_preserves_legacy_option_day_evening_delay(tmp_path: Path) -> None:
    materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2022-02-23"),
        mappings=[],
        raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
        rule_catalog_path=DEFAULT_PUBLIC_RULE_CATALOG_PATH,
        allow_candle_inference=False,
        fetched_at_utc="2022-02-23T00:00:00Z",
    )

    interval_rows = read_delta_table_rows(
        tmp_path / "canonical_session_intervals.delta",
        limit=10,
    )
    assert interval_rows[2]["expected_open_ts"] == "2022-02-23T16:05:00Z"


def test_default_catalog_preserves_legacy_standard_evening_start(tmp_path: Path) -> None:
    materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2022-02-21"),
        mappings=[],
        raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
        rule_catalog_path=DEFAULT_PUBLIC_RULE_CATALOG_PATH,
        allow_candle_inference=False,
        fetched_at_utc="2022-02-21T00:00:00Z",
    )

    interval_rows = read_delta_table_rows(
        tmp_path / "canonical_session_intervals.delta",
        limit=10,
    )
    assert interval_rows[2]["expected_open_ts"] == "2022-02-21T16:00:00Z"


def test_default_catalog_preserves_2022_02_25_evening_without_morning(
    tmp_path: Path,
) -> None:
    materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2022-02-25"),
        mappings=[],
        raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
        rule_catalog_path=DEFAULT_PUBLIC_RULE_CATALOG_PATH,
        allow_candle_inference=False,
        fetched_at_utc="2022-02-25T00:00:00Z",
    )

    interval_rows = read_delta_table_rows(
        tmp_path / "canonical_session_intervals.delta",
        limit=10,
    )
    assert [row["interval_type"] for row in interval_rows] == [
        "main_session",
        "evening_session",
    ]
    assert interval_rows[0]["expected_open_ts"] == "2022-02-25T07:00:00Z"
    assert interval_rows[1]["expected_open_ts"] == "2022-02-25T16:00:00Z"


def test_default_catalog_uses_updated_2026_02_23_holiday_weekend_session(
    tmp_path: Path,
) -> None:
    report = materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2026-02-23"),
        mappings=[],
        raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
        rule_catalog_path=DEFAULT_PUBLIC_RULE_CATALOG_PATH,
        allow_candle_inference=False,
        fetched_at_utc="2026-02-23T00:00:00Z",
    )

    raw_rows = read_delta_table_rows(tmp_path / "raw_moex_session_schedule.delta", limit=10)
    interval_rows = read_delta_table_rows(
        tmp_path / "canonical_session_intervals.delta",
        limit=10,
    )
    payload = json.loads(str(raw_rows[0]["raw_payload_json"]))
    assert report["calendar_exception_ids"] == ["moex-forts-holiday-weekend-session-2026-02-23"]
    assert payload["exception_id"] == "moex-forts-holiday-weekend-session-2026-02-23"
    assert interval_rows[0]["interval_type"] == "holiday_weekend_session"
    assert interval_rows[0]["expected_open_ts"] == "2026-02-23T07:00:00Z"
    assert interval_rows[0]["expected_close_ts"] == "2026-02-23T16:00:00Z"


def test_reconstructed_session_schedule_uses_candle_inference_for_rule_gap(
    tmp_path: Path,
) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    start = datetime(2026, 4, 22, 6, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for minute in [0, 1, 2, 20, 21]:
        ts_open = start + timedelta(minutes=minute)
        rows.append(
            {
                "internal_id": "FUT_BR",
                "finam_symbol": "BRM6@MOEX",
                "timeframe": "1m",
                "source_interval": 1,
                "ts_open": _iso(ts_open),
                "ts_close": _iso(ts_open + timedelta(minutes=1)),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 10,
                "open_interest": None,
                "ingest_run_id": "raw-ingest-fixture",
                "ingested_at_utc": _iso(ts_open + timedelta(minutes=1)),
                "provenance_json": {"source_provider": "moex_iss"},
            }
        )
    write_delta_table_rows(table_path=raw_table_path, rows=rows, columns=RAW_COLUMNS)

    report = materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=_changed_window("2026-04-22"),
        mappings=[],
        raw_table_path=raw_table_path,
        raw_schedule_path=tmp_path / "raw_moex_session_schedule.delta",
        canonical_session_intervals_path=tmp_path / "canonical_session_intervals.delta",
        rule_catalog_path=_rule_catalog(tmp_path / "rules.json"),
        allow_candle_inference=True,
        fetched_at_utc="2026-04-22T00:00:00Z",
    )

    interval_rows = read_delta_table_rows(
        tmp_path / "canonical_session_intervals.delta",
        limit=10,
    )
    assert report["public_rule_rows"] == 0
    assert report["candle_inferred_rows"] == 2
    assert [row["policy_id"] for row in interval_rows] == [
        CANDLE_INFERRED_SESSION_POLICY_ID,
        CANDLE_INFERRED_SESSION_POLICY_ID,
    ]


def test_normalize_session_schedule_keeps_moscow_trade_date_for_utc_boundary() -> None:
    interval_rows = normalize_raw_session_schedule_rows(
        [
            {
                "source_id": OFFICIAL_FUTURES_SESSION_SOURCE_ID,
                "source_document_hash": "sha256:fixture",
                "moex_secid": "BRM6@MOEX",
                "trade_date": "2026-04-22",
                "raw_payload_json": (
                    '{"type":"evening_session",'
                    '"time_from":"2026-04-22 00:15:00",'
                    '"time_till":"2026-04-22 01:00:00"}'
                ),
            }
        ],
        instrument_by_moex_secid={"BRM6@MOEX": "FUT_BR"},
    )

    assert interval_rows[0]["session_date"] == "2026-04-22"
    assert interval_rows[0]["expected_open_ts"] == "2026-04-21T21:15:00Z"
    assert interval_rows[0]["expected_close_ts"] == "2026-04-21T22:00:00Z"
