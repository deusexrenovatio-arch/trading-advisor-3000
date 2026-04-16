from __future__ import annotations

from collections.abc import Callable, Iterator
import json
from datetime import UTC, date, datetime, timedelta
import random
import time
from dataclasses import dataclass
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError


DEFAULT_CANDLE_CHUNK_DAYS_BY_INTERVAL: dict[int, int] = {
    1: 7,
    60: 120,
    24: 365,
    7: 1461,
}
DEFAULT_HISTORY_BOARD_TIMEOUT_SECONDS = 45.0
DEFAULT_HISTORY_BOARD_MAX_RETRIES = 4
DEFAULT_HISTORY_BOARD_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_JITTER_RATIO = 0.25


@dataclass(frozen=True)
class CandleBorder:
    interval: int
    begin: str
    end: str


@dataclass(frozen=True)
class MoexCandle:
    open: float
    close: float
    high: float
    low: float
    volume: int
    begin: str
    end: str


@dataclass(frozen=True)
class HistorySecurity:
    boardid: str
    tradedate: str
    secid: str
    shortname: str
    assetcode: str
    volume: int
    numtrades: int


class MoexRequestError(RuntimeError):
    def __init__(
        self,
        *,
        url: str,
        params: dict[str, str],
        attempts: int,
        last_error: Exception,
    ) -> None:
        self.url = url
        self.params = dict(params)
        self.attempts = attempts
        self.last_error = last_error
        self.last_error_type = type(last_error).__name__
        self.last_error_message = str(last_error)
        super().__init__(f"MOEX request failed after retries: {url} ({last_error})")


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iter_date_windows(date_from: date, date_till: date, *, chunk_days: int) -> Iterator[tuple[date, date]]:
    if chunk_days <= 0:
        raise ValueError("chunk_days must be > 0")
    current = date_from
    while current <= date_till:
        window_till = min(date_till, current + timedelta(days=chunk_days - 1))
        yield current, window_till
        current = window_till + timedelta(days=1)


def _rows_to_dicts(block: dict[str, Any]) -> list[dict[str, Any]]:
    columns = block.get("columns")
    rows = block.get("data")
    if not isinstance(columns, list) or not isinstance(rows, list):
        return []
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        payload: dict[str, Any] = {}
        for idx, name in enumerate(columns):
            if isinstance(name, str) and idx < len(row):
                payload[name] = row[idx]
        result.append(payload)
    return result


def select_interval_borders(payload: dict[str, Any], required_intervals: set[int]) -> dict[int, CandleBorder]:
    borders_block = payload.get("borders")
    if not isinstance(borders_block, dict):
        raise ValueError("MOEX candleborders payload is missing `borders` block")
    interval_map: dict[int, CandleBorder] = {}
    for row in _rows_to_dicts(borders_block):
        raw_interval = row.get("interval")
        raw_begin = row.get("begin")
        raw_end = row.get("end")
        if isinstance(raw_interval, int) and isinstance(raw_begin, str) and isinstance(raw_end, str):
            interval_map[raw_interval] = CandleBorder(interval=raw_interval, begin=raw_begin, end=raw_end)

    missing = sorted(required_intervals - set(interval_map))
    if missing:
        missing_text = ", ".join(str(item) for item in missing)
        raise ValueError(f"MOEX candleborders payload is missing required intervals: {missing_text}")
    return {interval: interval_map[interval] for interval in sorted(required_intervals)}


class MoexISSClient:
    def __init__(
        self,
        *,
        base_url: str = "https://iss.moex.com",
        timeout_seconds: float = 20.0,
        max_retries: int = 2,
        retry_backoff_seconds: float = 0.8,
        history_board_timeout_seconds: float | None = None,
        history_board_max_retries: int | None = None,
        history_board_retry_backoff_seconds: float | None = None,
        retry_jitter_ratio: float = DEFAULT_RETRY_JITTER_RATIO,
        user_agent: str = "trading-advisor-3000/moex-phase01-foundation",
        candle_chunk_days_by_interval: dict[int, int] | None = None,
        request_event_hook: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.history_board_timeout_seconds = max(
            timeout_seconds,
            float(
                DEFAULT_HISTORY_BOARD_TIMEOUT_SECONDS
                if history_board_timeout_seconds is None
                else history_board_timeout_seconds
            ),
        )
        self.history_board_max_retries = max(
            max_retries,
            int(
                DEFAULT_HISTORY_BOARD_MAX_RETRIES
                if history_board_max_retries is None
                else history_board_max_retries
            ),
        )
        self.history_board_retry_backoff_seconds = max(
            retry_backoff_seconds,
            float(
                DEFAULT_HISTORY_BOARD_RETRY_BACKOFF_SECONDS
                if history_board_retry_backoff_seconds is None
                else history_board_retry_backoff_seconds
            ),
        )
        if retry_jitter_ratio < 0:
            raise ValueError("retry_jitter_ratio must be >= 0")
        self.retry_jitter_ratio = float(retry_jitter_ratio)
        self.user_agent = user_agent
        self.request_event_hook = request_event_hook
        merged_chunk_days = dict(DEFAULT_CANDLE_CHUNK_DAYS_BY_INTERVAL)
        if candle_chunk_days_by_interval:
            for interval, chunk_days in candle_chunk_days_by_interval.items():
                normalized_interval = int(interval)
                normalized_chunk_days = int(chunk_days)
                if normalized_chunk_days <= 0:
                    raise ValueError("candle_chunk_days_by_interval values must be > 0")
                merged_chunk_days[normalized_interval] = normalized_chunk_days
        self.candle_chunk_days_by_interval = merged_chunk_days

    def _emit_request_event(self, payload: dict[str, Any]) -> None:
        if self.request_event_hook is None:
            return
        event_payload = dict(payload)
        event_payload.setdefault("emitted_at_utc", _utc_now_iso())
        try:
            self.request_event_hook(event_payload)
        except Exception:
            return

    def _request_policy(self, *, operation: str, context: dict[str, Any]) -> tuple[float, int, float]:
        timeout_seconds = float(self.timeout_seconds)
        max_retries = int(self.max_retries)
        retry_backoff_seconds = float(self.retry_backoff_seconds)
        if operation == "history_board_securities":
            timeout_seconds = float(self.history_board_timeout_seconds)
            page_start_raw = context.get("page_start", 0)
            try:
                page_start = int(page_start_raw)
            except (TypeError, ValueError):
                page_start = 0
            if page_start >= 400:
                timeout_seconds = max(timeout_seconds, self.history_board_timeout_seconds * 1.25)
            max_retries = int(self.history_board_max_retries)
            retry_backoff_seconds = float(self.history_board_retry_backoff_seconds)
        return timeout_seconds, max_retries, retry_backoff_seconds

    def _retry_sleep_seconds(self, *, retry_backoff_seconds: float, attempt: int) -> float:
        if retry_backoff_seconds <= 0:
            return 0.0
        base = retry_backoff_seconds * (2**attempt)
        jitter = random.uniform(0.0, base * self.retry_jitter_ratio) if self.retry_jitter_ratio else 0.0
        return base + jitter

    def _get_json(
        self,
        path: str,
        *,
        params: dict[str, str],
        event_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        query = parse.urlencode(params)
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        last_error: Exception | None = None
        context = dict(event_context or {})
        operation = str(context.get("operation", "http")).strip() or "http"
        timeout_seconds, max_retries, retry_backoff_seconds = self._request_policy(
            operation=operation,
            context=context,
        )
        for attempt in range(max_retries + 1):
            req = request.Request(url, headers={"User-Agent": self.user_agent})
            started = time.perf_counter()
            try:
                with request.urlopen(req, timeout=timeout_seconds) as response:
                    payload = json.load(response)
                if not isinstance(payload, dict):
                    raise ValueError(f"MOEX response must be JSON object: {url}")
                self._emit_request_event(
                    {
                        **context,
                        "event": "moex_http",
                        "status": "success",
                        "url": url,
                        "path": path,
                        "params": dict(params),
                        "attempt": attempt + 1,
                        "attempt_limit": max_retries + 1,
                        "timeout_seconds": timeout_seconds,
                        "retry_backoff_seconds": retry_backoff_seconds,
                        "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                    }
                )
                return payload
            except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError, ValueError) as exc:
                last_error = exc
                will_retry = attempt < max_retries
                retry_sleep_seconds = (
                    self._retry_sleep_seconds(
                        retry_backoff_seconds=retry_backoff_seconds,
                        attempt=attempt,
                    )
                    if will_retry
                    else 0.0
                )
                self._emit_request_event(
                    {
                        **context,
                        "event": "moex_http",
                        "status": "retry" if will_retry else "fail",
                        "url": url,
                        "path": path,
                        "params": dict(params),
                        "attempt": attempt + 1,
                        "attempt_limit": max_retries + 1,
                        "timeout_seconds": timeout_seconds,
                        "retry_backoff_seconds": retry_backoff_seconds,
                        "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "retry_sleep_seconds": retry_sleep_seconds,
                    }
                )
                if attempt >= max_retries:
                    break
                time.sleep(retry_sleep_seconds)
        assert last_error is not None
        raise MoexRequestError(url=url, params=params, attempts=max_retries + 1, last_error=last_error) from last_error

    def fetch_candleborders(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        secid: str,
        required_intervals: set[int],
    ) -> dict[int, CandleBorder]:
        path = f"/iss/engines/{engine}/markets/{market}/boards/{board}/securities/{secid}/candleborders.json"
        payload = self._get_json(
            path,
            params={"iss.meta": "off"},
            event_context={
                "operation": "candleborders",
                "engine": engine,
                "market": market,
                "board": board,
                "secid": secid,
                "required_intervals": sorted(required_intervals),
            },
        )
        return select_interval_borders(payload, required_intervals=required_intervals)

    def _fetch_candles_page(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        secid: str,
        interval: int,
        date_from: date,
        date_till: date,
        start: int,
    ) -> list[MoexCandle]:
        path = f"/iss/engines/{engine}/markets/{market}/boards/{board}/securities/{secid}/candles.json"
        params = {
            "iss.meta": "off",
            "interval": str(interval),
            "from": date_from.isoformat(),
            "till": date_till.isoformat(),
            "start": str(start),
        }
        payload = self._get_json(
            path,
            params=params,
            event_context={
                "operation": "candles_page",
                "engine": engine,
                "market": market,
                "board": board,
                "secid": secid,
                "interval": interval,
                "window_from": date_from.isoformat(),
                "window_till": date_till.isoformat(),
                "page_start": start,
            },
        )
        block = payload.get("candles")
        if not isinstance(block, dict):
            raise ValueError(f"MOEX candles payload is missing `candles` block for {secid}")

        page: list[MoexCandle] = []
        for row in _rows_to_dicts(block):
            raw_open = row.get("open")
            raw_close = row.get("close")
            raw_high = row.get("high")
            raw_low = row.get("low")
            raw_volume = row.get("volume")
            raw_begin = row.get("begin")
            raw_end = row.get("end")
            if not isinstance(raw_begin, str) or not isinstance(raw_end, str):
                continue
            if not all(isinstance(value, (int, float)) for value in (raw_open, raw_close, raw_high, raw_low)):
                continue
            if not isinstance(raw_volume, int):
                continue
            page.append(
                MoexCandle(
                    open=float(raw_open),
                    close=float(raw_close),
                    high=float(raw_high),
                    low=float(raw_low),
                    volume=int(raw_volume),
                    begin=raw_begin,
                    end=raw_end,
                )
            )
        self._emit_request_event(
            {
                "event": "moex_candles_page",
                "status": "parsed",
                "engine": engine,
                "market": market,
                "board": board,
                "secid": secid,
                "interval": interval,
                "window_from": date_from.isoformat(),
                "window_till": date_till.isoformat(),
                "page_start": start,
                "row_count": len(page),
            }
        )
        return page

    def _iter_candles_chunk(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        secid: str,
        interval: int,
        date_from: date,
        date_till: date,
    ) -> Iterator[MoexCandle]:
        start = 0
        while True:
            page = self._fetch_candles_page(
                engine=engine,
                market=market,
                board=board,
                secid=secid,
                interval=interval,
                date_from=date_from,
                date_till=date_till,
                start=start,
            )
            if not page:
                break
            for candle in page:
                yield candle
            start += len(page)

    def _iter_candles_chunk_resilient(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        secid: str,
        interval: int,
        date_from: date,
        date_till: date,
    ) -> Iterator[MoexCandle]:
        try:
            yield from self._iter_candles_chunk(
                engine=engine,
                market=market,
                board=board,
                secid=secid,
                interval=interval,
                date_from=date_from,
                date_till=date_till,
            )
            return
        except MoexRequestError as exc:
            if date_from >= date_till:
                self._emit_request_event(
                    {
                        "event": "moex_candles_chunk",
                        "status": "fail",
                        "engine": engine,
                        "market": market,
                        "board": board,
                        "secid": secid,
                        "interval": interval,
                        "window_from": date_from.isoformat(),
                        "window_till": date_till.isoformat(),
                        "error_type": exc.last_error_type,
                        "error": exc.last_error_message,
                    }
                )
                raise

            span_days = (date_till - date_from).days + 1
            left_span_days = max(span_days // 2, 1)
            left_till = min(date_till, date_from + timedelta(days=left_span_days - 1))
            right_from = left_till + timedelta(days=1)
            if right_from > date_till:
                raise
            self._emit_request_event(
                {
                    "event": "moex_candles_chunk_split",
                    "status": "split",
                    "reason": "request_failure",
                    "engine": engine,
                    "market": market,
                    "board": board,
                    "secid": secid,
                    "interval": interval,
                    "window_from": date_from.isoformat(),
                    "window_till": date_till.isoformat(),
                    "split_left_till": left_till.isoformat(),
                    "split_right_from": right_from.isoformat(),
                    "span_days": span_days,
                    "error_type": exc.last_error_type,
                    "error": exc.last_error_message,
                }
            )
            yield from self._iter_candles_chunk_resilient(
                engine=engine,
                market=market,
                board=board,
                secid=secid,
                interval=interval,
                date_from=date_from,
                date_till=left_till,
            )
            yield from self._iter_candles_chunk_resilient(
                engine=engine,
                market=market,
                board=board,
                secid=secid,
                interval=interval,
                date_from=right_from,
                date_till=date_till,
            )

    def iter_candles(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        secid: str,
        interval: int,
        date_from: date,
        date_till: date,
    ) -> Iterator[MoexCandle]:
        chunk_days = self.candle_chunk_days_by_interval.get(interval)
        if chunk_days is None:
            chunk_days = max((date_till - date_from).days + 1, 1)
        for window_from, window_till in _iter_date_windows(date_from, date_till, chunk_days=chunk_days):
            self._emit_request_event(
                {
                    "event": "moex_candles_chunk",
                    "status": "scheduled",
                    "engine": engine,
                    "market": market,
                    "board": board,
                    "secid": secid,
                    "interval": interval,
                    "window_from": window_from.isoformat(),
                    "window_till": window_till.isoformat(),
                    "chunk_days": chunk_days,
                }
            )
            yield from self._iter_candles_chunk_resilient(
                engine=engine,
                market=market,
                board=board,
                secid=secid,
                interval=interval,
                date_from=window_from,
                date_till=window_till,
            )

    def fetch_candles(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        secid: str,
        interval: int,
        date_from: date,
        date_till: date,
    ) -> list[MoexCandle]:
        return list(
            self.iter_candles(
                engine=engine,
                market=market,
                board=board,
                secid=secid,
                interval=interval,
                date_from=date_from,
                date_till=date_till,
            )
        )

    def fetch_history_board_securities(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        trade_date: date,
    ) -> list[HistorySecurity]:
        path = f"/iss/history/engines/{engine}/markets/{market}/boards/{board}/securities.json"
        start = 0
        securities: list[HistorySecurity] = []
        while True:
            payload = self._get_json(
                path,
                params={
                    "iss.meta": "off",
                    "date": trade_date.isoformat(),
                    "start": str(start),
                },
                event_context={
                    "operation": "history_board_securities",
                    "engine": engine,
                    "market": market,
                    "board": board,
                    "trade_date": trade_date.isoformat(),
                    "page_start": start,
                },
            )
            block = payload.get("history")
            if not isinstance(block, dict):
                raise ValueError(f"MOEX history payload is missing `history` block for {board} {trade_date.isoformat()}")

            page: list[HistorySecurity] = []
            for row in _rows_to_dicts(block):
                secid = str(row.get("SECID", "")).strip()
                boardid = str(row.get("BOARDID", "")).strip()
                tradedate = str(row.get("TRADEDATE", "")).strip()
                if not secid or not boardid or not tradedate:
                    continue
                shortname = str(row.get("SHORTNAME", "")).strip()
                assetcode = str(row.get("ASSETCODE", "")).strip()
                raw_volume = row.get("VOLUME")
                raw_numtrades = row.get("NUMTRADES")
                volume = int(raw_volume) if isinstance(raw_volume, (int, float)) and not isinstance(raw_volume, bool) else 0
                numtrades = (
                    int(raw_numtrades)
                    if isinstance(raw_numtrades, (int, float)) and not isinstance(raw_numtrades, bool)
                    else 0
                )
                page.append(
                    HistorySecurity(
                        boardid=boardid,
                        tradedate=tradedate,
                        secid=secid,
                        shortname=shortname,
                        assetcode=assetcode,
                        volume=max(0, volume),
                        numtrades=max(0, numtrades),
                    )
                )

            if not page:
                break
            self._emit_request_event(
                {
                    "event": "moex_history_page",
                    "status": "parsed",
                    "engine": engine,
                    "market": market,
                    "board": board,
                    "trade_date": trade_date.isoformat(),
                    "page_start": start,
                    "row_count": len(page),
                }
            )
            securities.extend(page)
            start += len(page)
        return securities
