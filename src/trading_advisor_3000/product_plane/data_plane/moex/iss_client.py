from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError


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
        user_agent: str = "trading-advisor-3000/moex-phase01-foundation",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.user_agent = user_agent

    def _get_json(self, path: str, *, params: dict[str, str]) -> dict[str, Any]:
        query = parse.urlencode(params)
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            req = request.Request(url, headers={"User-Agent": self.user_agent})
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    payload = json.load(response)
                if not isinstance(payload, dict):
                    raise ValueError(f"MOEX response must be JSON object: {url}")
                return payload
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(self.retry_backoff_seconds * (2**attempt))
        assert last_error is not None
        raise RuntimeError(f"MOEX request failed after retries: {url} ({last_error})") from last_error

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
        payload = self._get_json(path, params={"iss.meta": "off"})
        return select_interval_borders(payload, required_intervals=required_intervals)

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
        path = f"/iss/engines/{engine}/markets/{market}/boards/{board}/securities/{secid}/candles.json"
        start = 0
        candles: list[MoexCandle] = []
        while True:
            payload = self._get_json(
                path,
                params={
                    "iss.meta": "off",
                    "interval": str(interval),
                    "from": date_from.isoformat(),
                    "till": date_till.isoformat(),
                    "start": str(start),
                },
            )
            block = payload.get("candles")
            if not isinstance(block, dict):
                raise ValueError(f"MOEX candles payload is missing `candles` block for {secid}")

            page = []
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

            if not page:
                break
            candles.extend(page)
            start += len(page)
        return candles

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
            securities.extend(page)
            start += len(page)
        return securities
