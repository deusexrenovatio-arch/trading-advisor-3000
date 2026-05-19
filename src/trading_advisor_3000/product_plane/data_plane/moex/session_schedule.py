from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    iter_delta_table_row_batches,
    replace_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.moex.foundation import MappingRecord
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import (
    FuturesSessionScheduleRow,
    MoexISSClient,
)
from trading_advisor_3000.product_plane.data_plane.schemas import (
    historical_data_delta_schema_manifest,
)

OFFICIAL_FUTURES_SESSION_SOURCE_ID = "moex-iss-calendars-futures-session"
OFFICIAL_FUTURES_SESSION_SOURCE_URL = "https://iss.moex.com/iss/calendars/futures/session"
OFFICIAL_FUTURES_SESSION_POLICY_ID = "moex-official-futures-session.v1"
PUBLIC_RECONSTRUCTED_SESSION_SOURCE_ID = "moex-public-reconstructed-futures-session"
PUBLIC_RECONSTRUCTED_SESSION_POLICY_ID = "moex-public-reconstructed-futures-session.v1"
CANDLE_INFERRED_SESSION_SOURCE_ID = "moex-observed-candle-inferred-session"
CANDLE_INFERRED_SESSION_POLICY_ID = "moex-observed-candle-inferred-session.v1"
DEFAULT_PUBLIC_RULE_CATALOG_PATH = (
    Path(__file__).resolve().parents[5]
    / "configs"
    / "moex_foundation"
    / "session_schedule_rules.v1.json"
)
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_date(value: object, *, field_name: str) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"`{field_name}` must be non-empty date")
    return date.fromisoformat(text[:10])


def _parse_datetime(value: object, *, field_name: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"`{field_name}` must be non-empty datetime")
    normalized = text.replace(" ", "T")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=MOSCOW_TZ)
    return parsed.astimezone(UTC).replace(microsecond=0)


def _json_hash(payload: object) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def _quote_delta_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _or_predicate(rows: Iterable[dict[str, object]], key_columns: tuple[str, ...]) -> str:
    clauses: list[str] = []
    seen: set[tuple[str, ...]] = set()
    for row in rows:
        key = tuple(str(row[column]) for column in key_columns)
        if key in seen:
            continue
        seen.add(key)
        clauses.append(
            "("
            + " AND ".join(
                f"{column} = {_quote_delta_literal(row[column])}" for column in key_columns
            )
            + ")"
        )
    if not clauses:
        raise ValueError("delta replace predicate requires at least one scoped key")
    return " OR ".join(clauses)


def _changed_window_dates(changed_windows: list[dict[str, object]]) -> tuple[date, date]:
    dates: list[date] = []
    for window in changed_windows:
        start = _parse_datetime(window.get("window_start_utc"), field_name="window_start_utc")
        end = _parse_datetime(window.get("window_end_utc"), field_name="window_end_utc")
        effective_end = end - timedelta(seconds=1) if end > start else end
        dates.extend(
            [start.astimezone(MOSCOW_TZ).date(), effective_end.astimezone(MOSCOW_TZ).date()]
        )
    if not dates:
        raise ValueError("official session schedule refresh requires non-empty changed windows")
    return min(dates), max(dates)


def _iter_dates(date_from: date, date_till: date) -> Iterable[date]:
    current = date_from
    while current <= date_till:
        yield current
        current += timedelta(days=1)


def _mapping_by_key(mappings: Iterable[MappingRecord]) -> dict[tuple[str, str], MappingRecord]:
    result: dict[tuple[str, str], MappingRecord] = {}
    for mapping in mappings:
        result[(mapping.internal_id, mapping.moex_secid)] = mapping
    return result


def _scope_from_changed_windows(
    *,
    changed_windows: list[dict[str, object]],
    mappings: Iterable[MappingRecord],
) -> list[dict[str, str]]:
    mapping_lookup = _mapping_by_key(mappings)
    scope: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for window in changed_windows:
        internal_id = str(window.get("internal_id", "")).strip()
        moex_secid = str(window.get("moex_secid", "")).strip()
        if not internal_id:
            continue
        mapping = mapping_lookup.get((internal_id, moex_secid))
        engine = str(window.get("moex_engine") or getattr(mapping, "moex_engine", "futures"))
        market = str(window.get("moex_market") or getattr(mapping, "moex_market", "forts"))
        board = str(window.get("moex_board") or getattr(mapping, "moex_board", "RFUD"))
        secid = str(getattr(mapping, "moex_secid", "") or moex_secid).strip()
        if secid.endswith("@MOEX"):
            secid = secid[:-5]
        key = (internal_id, secid)
        if not secid or key in seen:
            continue
        seen.add(key)
        scope.append(
            {
                "instrument_id": internal_id,
                "engine": engine,
                "market": market,
                "board": board,
                "moex_secid": secid,
            }
        )
    if not scope:
        raise ValueError("official session schedule refresh cannot resolve affected instruments")
    return scope


def _raw_schedule_rows(
    *,
    schedule_rows: list[FuturesSessionScheduleRow],
    affected_scope: list[dict[str, str]],
    fetched_at_utc: str,
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for row in schedule_rows:
        row_date = _parse_date(row.tradedate, field_name="tradedate").isoformat()
        for scope in affected_scope:
            row_secid = row.secid.strip()
            row_board = row.boardid.strip()
            if row_secid not in {"", "-", scope["moex_secid"]}:
                continue
            if row_board not in {"", "-", scope["board"]}:
                continue
            payload = {
                "tradedate": row.tradedate,
                "secid": row.secid,
                "boardid": row.boardid,
                "type": row.session_type,
                "time_from": row.time_from,
                "time_till": row.time_till,
                "updatetime": row.updatetime,
                "payload": row.payload,
            }
            result.append(
                {
                    "source_id": OFFICIAL_FUTURES_SESSION_SOURCE_ID,
                    "source_url": OFFICIAL_FUTURES_SESSION_SOURCE_URL,
                    "source_document_id": f"{row_date}:{scope['moex_secid']}:{row.session_type}",
                    "source_document_hash": _json_hash(payload),
                    "fetched_at_utc": fetched_at_utc,
                    "engine": scope["engine"],
                    "market": scope["market"],
                    "board": scope["board"],
                    "moex_secid": scope["moex_secid"],
                    "trade_date": row_date,
                    "raw_payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                }
            )
    return result


def _load_rule_catalog(path: Path) -> dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(
            f"MOEX session reconstruction requires a static rule catalog: {path.as_posix()}"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"MOEX session rule catalog must be JSON object: {path.as_posix()}")
    rules = payload.get("rules")
    if not isinstance(rules, list) or not rules:
        raise ValueError("MOEX session rule catalog must contain non-empty `rules`")
    return payload


def _rule_value_matches(rule_value: object, expected: str) -> bool:
    if rule_value is None:
        return True
    if isinstance(rule_value, list):
        return expected in {str(item).strip() for item in rule_value}
    return str(rule_value).strip() in {"", "*", expected}


def _rule_applies_to_scope(rule: dict[str, object], scope: dict[str, str]) -> bool:
    applies_to = rule.get("applies_to")
    if not isinstance(applies_to, dict):
        return True
    return (
        _rule_value_matches(applies_to.get("engine"), scope["engine"])
        and _rule_value_matches(applies_to.get("market"), scope["market"])
        and _rule_value_matches(applies_to.get("board"), scope["board"])
    )


def _rule_applies_to_date(rule: dict[str, object], session_date: date) -> bool:
    effective_from = _parse_date(rule.get("effective_from"), field_name="effective_from")
    raw_effective_to = rule.get("effective_to")
    effective_to = (
        _parse_date(raw_effective_to, field_name="effective_to") if raw_effective_to else None
    )
    excluded_dates = {
        item.isoformat()
        for item in _catalog_dates(
            rule.get("excluded_dates", []),
            field_name="excluded_dates",
        )
    }
    if session_date.isoformat() in excluded_dates:
        return False
    weekdays = rule.get("weekdays")
    if isinstance(weekdays, list):
        allowed_weekdays = {int(item) for item in weekdays}
        if session_date.isoweekday() not in allowed_weekdays:
            return False
    return session_date >= effective_from and (effective_to is None or session_date <= effective_to)


def _rules_for_date_and_scope(
    catalog: dict[str, object],
    *,
    session_date: date,
    scope: dict[str, str],
) -> list[dict[str, object]]:
    rules = catalog.get("rules")
    if not isinstance(rules, list):
        return []
    return [
        dict(rule)
        for rule in rules
        if isinstance(rule, dict)
        and _rule_applies_to_scope(rule, scope)
        and _rule_applies_to_date(rule, session_date)
    ]


def _catalog_dates(value: object, *, field_name: str) -> list[date]:
    if value in (None, ""):
        return []
    if not isinstance(value, list):
        raise ValueError(f"`{field_name}` must be a list of ISO dates or date ranges")
    result: list[date] = []
    for item in value:
        if isinstance(item, dict):
            start = _parse_date(item.get("from"), field_name=f"{field_name}.from")
            end = _parse_date(item.get("to"), field_name=f"{field_name}.to")
            if end < start:
                raise ValueError(f"`{field_name}` date range end must be >= start")
            result.extend(_iter_dates(start, end))
        else:
            result.append(_parse_date(item, field_name=field_name))
    return result


def _exception_applies_to_date(exception: dict[str, object], session_date: date) -> bool:
    dates = _catalog_dates(exception.get("dates", []), field_name="calendar_exceptions.dates")
    if session_date in dates:
        return True
    for item in _catalog_dates(
        exception.get("date_ranges", []),
        field_name="calendar_exceptions.date_ranges",
    ):
        if session_date == item:
            return True
    return False


def _calendar_exceptions_for_date_and_scope(
    catalog: dict[str, object],
    *,
    session_date: date,
    scope: dict[str, str],
) -> list[dict[str, object]]:
    exceptions = catalog.get("calendar_exceptions", [])
    if not isinstance(exceptions, list):
        raise ValueError("MOEX session rule catalog `calendar_exceptions` must be a list")
    return [
        dict(exception)
        for exception in exceptions
        if isinstance(exception, dict)
        and _rule_applies_to_scope(exception, scope)
        and _exception_applies_to_date(exception, session_date)
    ]


def _parse_local_time(value: object, *, field_name: str) -> time:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"`{field_name}` must be non-empty time")
    return time.fromisoformat(text)


def _split_local_interval(
    *,
    session_date: date,
    start_time: time,
    end_time: time,
) -> Iterable[tuple[date, datetime, datetime]]:
    current_start = datetime.combine(session_date, start_time, tzinfo=MOSCOW_TZ)
    interval_end = datetime.combine(session_date, end_time, tzinfo=MOSCOW_TZ)
    if interval_end <= current_start:
        interval_end += timedelta(days=1)

    while current_start < interval_end:
        next_midnight = datetime.combine(
            current_start.date() + timedelta(days=1),
            time.min,
            tzinfo=MOSCOW_TZ,
        )
        current_end = min(interval_end, next_midnight)
        yield current_start.date(), current_start, current_end
        current_start = current_end


def _format_local_datetime(value: datetime) -> str:
    return value.astimezone(MOSCOW_TZ).replace(tzinfo=None, microsecond=0).isoformat(" ")


def _source_urls(source: dict[str, object]) -> list[str]:
    return [str(item).strip() for item in source.get("source_urls", []) if str(item).strip()]


def _source_url(source: dict[str, object]) -> str:
    urls = _source_urls(source)
    return urls[0] if urls else "static://moex-session-rule-catalog"


def _raw_calendar_exception_evidence_row(
    *,
    catalog: dict[str, object],
    exception: dict[str, object],
    scope: dict[str, str],
    session_date: date,
    fetched_at_utc: str,
) -> dict[str, object]:
    source_urls = _source_urls(exception)
    payload = {
        "catalog_id": catalog.get("catalog_id"),
        "exception_id": exception.get("exception_id"),
        "trading_status": exception.get("action", "open_standard"),
        "source_title": exception.get("source_title"),
        "source_date": exception.get("source_date"),
        "source_quality": exception.get("source_quality", "public_calendar"),
        "confidence": exception.get("confidence", "medium"),
        "source_urls": source_urls,
        "notes": exception.get("notes"),
        "policy_id": PUBLIC_RECONSTRUCTED_SESSION_POLICY_ID,
    }
    return {
        "source_id": PUBLIC_RECONSTRUCTED_SESSION_SOURCE_ID,
        "source_url": _source_url(exception),
        "source_document_id": (
            f"{exception.get('exception_id')}:{session_date.isoformat()}:{scope['moex_secid']}:calendar"
        ),
        "source_document_hash": _json_hash(payload),
        "fetched_at_utc": fetched_at_utc,
        "engine": scope["engine"],
        "market": scope["market"],
        "board": scope["board"],
        "moex_secid": scope["moex_secid"],
        "trade_date": session_date.isoformat(),
        "raw_payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
    }


def _default_weekend_closed_exception() -> dict[str, object]:
    return {
        "exception_id": "moex-forts-default-weekend-closed",
        "action": "closed",
        "source_title": "MOEX public weekend trading rules: no explicit weekend session",
        "source_date": "2026-05-18",
        "source_urls": [
            "https://www.moex.com/n91270",
            "https://www.moex.com/n95564",
        ],
        "source_quality": "public_moex_news",
        "confidence": "medium",
        "notes": (
            "Weekend dates are closed by default unless an explicit public weekend trading "
            "rule or exception applies."
        ),
    }


def _append_interval_raw_rows(
    *,
    result: list[dict[str, object]],
    catalog: dict[str, object],
    source: dict[str, object],
    source_key: str,
    intervals: object,
    scope: dict[str, str],
    session_date: date,
    fetched_at_utc: str,
) -> None:
    if not isinstance(intervals, list):
        raise ValueError(f"MOEX session source `{source_key}` missing intervals")
    source_urls = _source_urls(source)
    for interval_index, interval in enumerate(intervals, start=1):
        if not isinstance(interval, dict):
            raise ValueError(f"MOEX session source `{source_key}` has invalid interval")
        start_time = _parse_local_time(
            interval.get("time_from"),
            field_name="time_from",
        )
        end_time = _parse_local_time(
            interval.get("time_till"),
            field_name="time_till",
        )
        for part_index, (trade_date, part_start, part_end) in enumerate(
            _split_local_interval(
                session_date=session_date,
                start_time=start_time,
                end_time=end_time,
            ),
            start=1,
        ):
            payload = {
                "catalog_id": catalog.get("catalog_id"),
                "rule_id": source.get("rule_id"),
                "exception_id": source.get("exception_id"),
                "source_title": source.get("source_title"),
                "source_date": source.get("source_date"),
                "source_quality": source.get("source_quality", "public_rule"),
                "confidence": source.get("confidence", "medium"),
                "source_urls": source_urls,
                "type": interval.get("type") or interval.get("interval_type"),
                "time_from": _format_local_datetime(part_start),
                "time_till": _format_local_datetime(part_end),
                "session_class": interval.get("session_class", "regular"),
                "policy_id": PUBLIC_RECONSTRUCTED_SESSION_POLICY_ID,
                "split_part": part_index,
            }
            source_document_id = (
                f"{source_key}:{trade_date.isoformat()}:{scope['moex_secid']}:"
                f"{interval_index}:{part_index}"
            )
            result.append(
                {
                    "source_id": PUBLIC_RECONSTRUCTED_SESSION_SOURCE_ID,
                    "source_url": _source_url(source),
                    "source_document_id": source_document_id,
                    "source_document_hash": _json_hash(payload),
                    "fetched_at_utc": fetched_at_utc,
                    "engine": scope["engine"],
                    "market": scope["market"],
                    "board": scope["board"],
                    "moex_secid": scope["moex_secid"],
                    "trade_date": trade_date.isoformat(),
                    "raw_payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                }
            )


def _raw_rows_from_public_rules(
    *,
    catalog: dict[str, object],
    affected_scope: list[dict[str, str]],
    date_from: date,
    date_till: date,
    fetched_at_utc: str,
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for scope in affected_scope:
        for session_date in _iter_dates(date_from, date_till):
            exceptions = _calendar_exceptions_for_date_and_scope(
                catalog,
                session_date=session_date,
                scope=scope,
            )
            closed_exceptions = [
                exception
                for exception in exceptions
                if str(exception.get("action", "")).strip() == "closed"
            ]
            if closed_exceptions:
                result.append(
                    _raw_calendar_exception_evidence_row(
                        catalog=catalog,
                        exception=closed_exceptions[-1],
                        scope=scope,
                        session_date=session_date,
                        fetched_at_utc=fetched_at_utc,
                    )
                )
                continue
            interval_exceptions = [
                exception
                for exception in exceptions
                if isinstance(exception.get("trading_intervals"), list)
            ]
            if interval_exceptions:
                for exception in interval_exceptions:
                    _append_interval_raw_rows(
                        result=result,
                        catalog=catalog,
                        source=exception,
                        source_key=str(exception.get("exception_id")),
                        intervals=exception.get("trading_intervals"),
                        scope=scope,
                        session_date=session_date,
                        fetched_at_utc=fetched_at_utc,
                    )
                continue
            for exception in exceptions:
                result.append(
                    _raw_calendar_exception_evidence_row(
                        catalog=catalog,
                        exception=exception,
                        scope=scope,
                        session_date=session_date,
                        fetched_at_utc=fetched_at_utc,
                    )
                )
            rules = _rules_for_date_and_scope(
                catalog,
                session_date=session_date,
                scope=scope,
            )
            if not rules and session_date.isoweekday() > 5:
                result.append(
                    _raw_calendar_exception_evidence_row(
                        catalog=catalog,
                        exception=_default_weekend_closed_exception(),
                        scope=scope,
                        session_date=session_date,
                        fetched_at_utc=fetched_at_utc,
                    )
                )
                continue
            for rule in rules:
                _append_interval_raw_rows(
                    result=result,
                    catalog=catalog,
                    source=rule,
                    source_key=str(rule.get("rule_id")),
                    intervals=rule.get("trading_intervals"),
                    scope=scope,
                    session_date=session_date,
                    fetched_at_utc=fetched_at_utc,
                )
    return result


def _scope_dates_covered_by_raw_rows(raw_rows: list[dict[str, object]]) -> set[tuple[str, str]]:
    return {
        (
            str(row["moex_secid"]),
            _parse_date(row["trade_date"], field_name="trade_date").isoformat(),
        )
        for row in raw_rows
    }


def _missing_weekday_scope_dates(
    *,
    affected_scope: list[dict[str, str]],
    date_from: date,
    date_till: date,
    covered_scope_dates: set[tuple[str, str]],
) -> list[tuple[str, str]]:
    missing: list[tuple[str, str]] = []
    for scope in affected_scope:
        for session_date in _iter_dates(date_from, date_till):
            if session_date.isoweekday() > 5:
                continue
            key = (scope["moex_secid"], session_date.isoformat())
            if key not in covered_scope_dates:
                missing.append(key)
    return missing


def _interval_replace_scope_rows(
    *,
    affected_scope: list[dict[str, str]],
    date_from: date,
    date_till: date,
) -> list[dict[str, object]]:
    return [
        {
            "instrument_id": scope["instrument_id"],
            "session_date": session_date.isoformat(),
        }
        for scope in affected_scope
        for session_date in _iter_dates(date_from, date_till)
    ]


def _raw_rows_from_candle_inference(
    *,
    raw_table_path: Path,
    affected_scope: list[dict[str, str]],
    date_from: date,
    date_till: date,
    covered_scope_dates: set[tuple[str, str]],
    fetched_at_utc: str,
    gap_threshold_minutes: int = 10,
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for scope in affected_scope:
        for session_date in _iter_dates(date_from, date_till):
            if (scope["moex_secid"], session_date.isoformat()) in covered_scope_dates:
                continue
            local_start = datetime.combine(session_date, time.min, tzinfo=MOSCOW_TZ)
            local_end = local_start + timedelta(days=1)
            utc_start = local_start.astimezone(UTC)
            utc_end = local_end.astimezone(UTC)
            rows: list[dict[str, object]] = []
            for batch in iter_delta_table_row_batches(
                raw_table_path,
                filters=[
                    ("internal_id", "=", scope["instrument_id"]),
                    ("timeframe", "=", "1m"),
                    ("ts_open", ">=", utc_start),
                    ("ts_open", "<", utc_end),
                ],
            ):
                for row in batch:
                    raw_symbol = str(row.get("finam_symbol") or "").strip()
                    allowed_symbols = {scope["moex_secid"], f"{scope['moex_secid']}@MOEX"}
                    if raw_symbol and raw_symbol not in allowed_symbols:
                        continue
                    rows.append(row)
            if not rows:
                continue

            ordered = sorted(
                rows, key=lambda item: _parse_datetime(item["ts_open"], field_name="ts_open")
            )
            groups: list[list[dict[str, object]]] = []
            for row in ordered:
                if not groups:
                    groups.append([row])
                    continue
                previous = groups[-1][-1]
                previous_close = _parse_datetime(previous["ts_close"], field_name="ts_close")
                current_open = _parse_datetime(row["ts_open"], field_name="ts_open")
                if current_open - previous_close > timedelta(minutes=gap_threshold_minutes):
                    groups.append([row])
                else:
                    groups[-1].append(row)

            for seq, group in enumerate(groups, start=1):
                first_open = _parse_datetime(group[0]["ts_open"], field_name="ts_open")
                last_close = _parse_datetime(group[-1]["ts_close"], field_name="ts_close")
                payload = {
                    "type": "observed_1m_cluster",
                    "time_from": _format_local_datetime(first_open),
                    "time_till": _format_local_datetime(last_close),
                    "session_class": "observed_inferred",
                    "policy_id": CANDLE_INFERRED_SESSION_POLICY_ID,
                    "gap_threshold_minutes": gap_threshold_minutes,
                    "source_row_count": len(group),
                    "raw_table_path": raw_table_path.as_posix(),
                }
                result.append(
                    {
                        "source_id": CANDLE_INFERRED_SESSION_SOURCE_ID,
                        "source_url": f"delta://{raw_table_path.as_posix()}",
                        "source_document_id": (
                            f"observed:{session_date.isoformat()}:{scope['moex_secid']}:{seq}"
                        ),
                        "source_document_hash": _json_hash(payload),
                        "fetched_at_utc": fetched_at_utc,
                        "engine": scope["engine"],
                        "market": scope["market"],
                        "board": scope["board"],
                        "moex_secid": scope["moex_secid"],
                        "trade_date": session_date.isoformat(),
                        "raw_payload_json": json.dumps(
                            payload,
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                    }
                )
    return result


def normalize_raw_session_schedule_rows(
    raw_rows: list[dict[str, object]],
    *,
    instrument_by_moex_secid: dict[str, str],
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in raw_rows:
        moex_secid = str(row.get("moex_secid", "")).strip()
        instrument_id = instrument_by_moex_secid.get(moex_secid)
        if not instrument_id:
            continue
        trade_date = _parse_date(row.get("trade_date"), field_name="trade_date").isoformat()
        grouped.setdefault((instrument_id, trade_date), []).append(row)

    intervals: list[dict[str, object]] = []
    for (instrument_id, session_date), rows in sorted(grouped.items()):
        parsed: list[tuple[datetime, datetime, dict[str, object]]] = []
        for row in rows:
            payload = json.loads(str(row["raw_payload_json"]))
            if not payload.get("time_from") and payload.get("trading_status"):
                continue
            parsed.append(
                (
                    _parse_datetime(payload.get("time_from"), field_name="time_from"),
                    _parse_datetime(payload.get("time_till"), field_name="time_till"),
                    row,
                )
            )
        for seq, (open_ts, close_ts, row) in enumerate(
            sorted(parsed, key=lambda item: (item[0], item[1])), start=1
        ):
            payload = json.loads(str(row["raw_payload_json"]))
            session_type = str(payload.get("type") or "regular").strip()
            session_class = str(payload.get("session_class") or "regular").strip()
            policy_id = str(payload.get("policy_id") or OFFICIAL_FUTURES_SESSION_POLICY_ID).strip()
            intervals.append(
                {
                    "instrument_id": instrument_id,
                    "session_date": session_date,
                    "interval_id": f"{instrument_id}-{session_date}-{seq:02d}-{session_type}",
                    "interval_seq": seq,
                    "expected_open_ts": open_ts.isoformat().replace("+00:00", "Z"),
                    "expected_close_ts": close_ts.isoformat().replace("+00:00", "Z"),
                    "session_class": session_class,
                    "interval_type": session_type,
                    "policy_id": policy_id,
                    "source_id": str(row["source_id"]),
                    "source_document_hash": str(row["source_document_hash"]),
                }
            )
    return intervals


def _write_session_schedule_tables(
    *,
    raw_rows: list[dict[str, object]],
    interval_rows: list[dict[str, object]],
    interval_replace_scope_rows: list[dict[str, object]] | None = None,
    raw_schedule_path: Path,
    canonical_session_intervals_path: Path,
) -> None:
    manifest = historical_data_delta_schema_manifest()
    raw_columns = manifest["raw_moex_session_schedule"]["columns"]
    interval_columns = manifest["canonical_session_intervals"]["columns"]
    if has_delta_log(raw_schedule_path):
        replace_delta_table_rows(
            table_path=raw_schedule_path,
            rows=raw_rows,
            columns=raw_columns,
            predicate=_or_predicate(
                raw_rows,
                ("engine", "market", "board", "moex_secid", "trade_date"),
            ),
        )
    else:
        write_delta_table_rows(
            table_path=raw_schedule_path,
            rows=raw_rows,
            columns=raw_columns,
        )
    if has_delta_log(canonical_session_intervals_path):
        replace_scope_rows = interval_rows or interval_replace_scope_rows or []
        replace_delta_table_rows(
            table_path=canonical_session_intervals_path,
            rows=interval_rows,
            columns=interval_columns,
            predicate=_or_predicate(replace_scope_rows, ("instrument_id", "session_date")),
        )
    else:
        write_delta_table_rows(
            table_path=canonical_session_intervals_path,
            rows=interval_rows,
            columns=interval_columns,
        )


def materialize_reconstructed_session_schedule_for_changed_windows(
    *,
    changed_windows: list[dict[str, object]],
    mappings: Iterable[MappingRecord],
    raw_schedule_path: Path,
    canonical_session_intervals_path: Path,
    raw_table_path: Path | None = None,
    rule_catalog_path: Path = DEFAULT_PUBLIC_RULE_CATALOG_PATH,
    allow_candle_inference: bool = True,
    fetched_at_utc: str | None = None,
) -> dict[str, object]:
    if not changed_windows:
        return {
            "status": "PASS-NOOP",
            "mode": "static_reconstructed",
            "raw_schedule_rows": 0,
            "canonical_session_interval_rows": 0,
        }

    catalog = _load_rule_catalog(rule_catalog_path)
    date_from, date_till = _changed_window_dates(changed_windows)
    affected_scope = _scope_from_changed_windows(
        changed_windows=changed_windows,
        mappings=mappings,
    )
    fetched_at_utc = fetched_at_utc or _utc_now_iso()
    raw_rows = _raw_rows_from_public_rules(
        catalog=catalog,
        affected_scope=affected_scope,
        date_from=date_from,
        date_till=date_till,
        fetched_at_utc=fetched_at_utc,
    )
    inferred_rows = 0
    if allow_candle_inference and raw_table_path is not None:
        inferred = _raw_rows_from_candle_inference(
            raw_table_path=raw_table_path,
            affected_scope=affected_scope,
            date_from=date_from,
            date_till=date_till,
            covered_scope_dates=_scope_dates_covered_by_raw_rows(raw_rows),
            fetched_at_utc=fetched_at_utc,
        )
        inferred_rows = len(inferred)
        raw_rows.extend(inferred)
    covered_scope_dates = _scope_dates_covered_by_raw_rows(raw_rows)
    missing_weekdays = _missing_weekday_scope_dates(
        affected_scope=affected_scope,
        date_from=date_from,
        date_till=date_till,
        covered_scope_dates=covered_scope_dates,
    )
    if missing_weekdays:
        sample = ", ".join(
            f"{secid}:{session_date}" for secid, session_date in missing_weekdays[:10]
        )
        suffix = "" if len(missing_weekdays) <= 10 else f", +{len(missing_weekdays) - 10} more"
        raise ValueError(
            "MOEX session reconstruction is missing weekday coverage; "
            f"add public rule coverage or enable candle inference for {sample}{suffix}"
        )

    instrument_by_moex_secid = {
        scope["moex_secid"]: scope["instrument_id"] for scope in affected_scope
    }
    interval_rows = normalize_raw_session_schedule_rows(
        raw_rows,
        instrument_by_moex_secid=instrument_by_moex_secid,
    )
    if not interval_rows and not raw_rows:
        raise ValueError(
            "MOEX session reconstruction produced no canonical intervals; "
            "rule catalog coverage and candle inference are empty"
        )

    _write_session_schedule_tables(
        raw_rows=raw_rows,
        interval_rows=interval_rows,
        interval_replace_scope_rows=_interval_replace_scope_rows(
            affected_scope=affected_scope,
            date_from=date_from,
            date_till=date_till,
        ),
        raw_schedule_path=raw_schedule_path,
        canonical_session_intervals_path=canonical_session_intervals_path,
    )
    rule_ids: set[str] = set()
    exception_ids: set[str] = set()
    public_rule_rows = 0
    calendar_exception_rows = 0
    closed_calendar_rows = 0
    for row in raw_rows:
        payload = json.loads(str(row["raw_payload_json"]))
        rule_id = payload.get("rule_id")
        exception_id = payload.get("exception_id")
        if rule_id:
            rule_ids.add(str(rule_id))
            public_rule_rows += 1
        if exception_id:
            exception_ids.add(str(exception_id))
            calendar_exception_rows += 1
        if str(payload.get("trading_status", "")).strip() == "closed":
            closed_calendar_rows += 1
    return {
        "status": "PASS",
        "mode": "static_reconstructed",
        "source_id": PUBLIC_RECONSTRUCTED_SESSION_SOURCE_ID,
        "rule_catalog_path": rule_catalog_path.as_posix(),
        "rule_ids": sorted(rule_ids),
        "calendar_exception_ids": sorted(exception_ids),
        "date_from": date_from.isoformat(),
        "date_till": date_till.isoformat(),
        "affected_instrument_count": len(affected_scope),
        "raw_schedule_path": raw_schedule_path.as_posix(),
        "canonical_session_intervals_path": canonical_session_intervals_path.as_posix(),
        "raw_schedule_rows": len(raw_rows),
        "public_rule_rows": public_rule_rows,
        "calendar_exception_rows": calendar_exception_rows,
        "closed_calendar_rows": closed_calendar_rows,
        "candle_inferred_rows": inferred_rows,
        "canonical_session_interval_rows": len(interval_rows),
    }


def materialize_official_session_schedule_for_changed_windows(
    *,
    client: MoexISSClient,
    changed_windows: list[dict[str, object]],
    mappings: Iterable[MappingRecord],
    raw_schedule_path: Path,
    canonical_session_intervals_path: Path,
    fetched_at_utc: str | None = None,
) -> dict[str, object]:
    if not changed_windows:
        return {
            "status": "PASS-NOOP",
            "raw_schedule_rows": 0,
            "canonical_session_interval_rows": 0,
        }
    date_from, date_till = _changed_window_dates(changed_windows)
    affected_scope = _scope_from_changed_windows(
        changed_windows=changed_windows,
        mappings=mappings,
    )
    fetched_at_utc = fetched_at_utc or _utc_now_iso()
    schedule_rows = client.fetch_futures_session_schedule(
        date_from=date_from,
        date_till=date_till,
    )
    raw_rows = _raw_schedule_rows(
        schedule_rows=schedule_rows,
        affected_scope=affected_scope,
        fetched_at_utc=fetched_at_utc,
    )
    instrument_by_moex_secid = {
        scope["moex_secid"]: scope["instrument_id"] for scope in affected_scope
    }
    interval_rows = normalize_raw_session_schedule_rows(
        raw_rows,
        instrument_by_moex_secid=instrument_by_moex_secid,
    )
    if not interval_rows:
        raise ValueError("official MOEX futures session schedule produced no canonical intervals")

    _write_session_schedule_tables(
        raw_rows=raw_rows,
        interval_rows=interval_rows,
        raw_schedule_path=raw_schedule_path,
        canonical_session_intervals_path=canonical_session_intervals_path,
    )

    return {
        "status": "PASS",
        "source_id": OFFICIAL_FUTURES_SESSION_SOURCE_ID,
        "source_url": OFFICIAL_FUTURES_SESSION_SOURCE_URL,
        "date_from": date_from.isoformat(),
        "date_till": date_till.isoformat(),
        "affected_instrument_count": len(affected_scope),
        "raw_schedule_path": raw_schedule_path.as_posix(),
        "canonical_session_intervals_path": canonical_session_intervals_path.as_posix(),
        "raw_schedule_rows": len(raw_rows),
        "canonical_session_interval_rows": len(interval_rows),
    }
