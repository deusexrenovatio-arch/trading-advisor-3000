from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from zoneinfo import ZoneInfo


DEFAULT_MOEX_ISS_BASE_URL = "https://iss.moex.com/iss"
DEFAULT_MOEX_MARKET = "forts"
DEFAULT_MOEX_ENGINE = "futures"
DEFAULT_PAGE_SIZE = 100
MOEX_TIMEZONE = ZoneInfo("Europe/Moscow")
TIMEFRAME_TO_INTERVAL = {"5m": 5, "15m": 15, "1h": 60}
MONTH_CODE_BY_NUMBER = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z",
}


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def _http_json(url: str, *, timeout_seconds: float) -> dict[str, Any]:
    request = urllib_request.Request(
        url=url,
        method="GET",
        headers={"Accept": "application/json"},
    )
    with urllib_request.urlopen(request, timeout=timeout_seconds) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError("MOEX ISS response must be an object")
    return payload


def _rows_from_table(payload: dict[str, Any], table_name: str) -> list[dict[str, Any]]:
    table = payload.get(table_name)
    if not isinstance(table, dict):
        raise ValueError(f"missing MOEX ISS table: {table_name}")
    columns = table.get("columns", [])
    data = table.get("data", [])
    if not isinstance(columns, list) or not isinstance(data, list):
        raise ValueError(f"invalid MOEX ISS table structure: {table_name}")
    result: list[dict[str, Any]] = []
    for raw in data:
        if not isinstance(raw, list):
            raise ValueError(f"invalid MOEX ISS row in {table_name}")
        result.append({str(columns[index]): raw[index] for index in range(min(len(columns), len(raw)))})
    return result


def _normalize_moex_ts(raw: str) -> str:
    value = str(raw).strip()
    if not value:
        raise ValueError("MOEX timestamp must be non-empty")
    if "T" in value:
        parsed = datetime.fromisoformat(value)
    else:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=MOEX_TIMEZONE)
    return parsed.astimezone(ZoneInfo("UTC")).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _contract_parts(contract_id: str) -> tuple[str, int, int]:
    if "-" not in contract_id:
        raise ValueError(f"unsupported contract_id format: {contract_id}")
    instrument_id, expiry = contract_id.split("-", 1)
    if "." not in expiry:
        raise ValueError(f"unsupported contract_id format: {contract_id}")
    month_text, year_text = expiry.split(".", 1)
    month = int(month_text)
    year = int(year_text)
    if month not in MONTH_CODE_BY_NUMBER:
        raise ValueError(f"unsupported contract month: {contract_id}")
    return instrument_id, month, year


def derive_moex_secid(contract_id: str) -> str:
    instrument_id, month, year = _contract_parts(contract_id)
    return f"{instrument_id}{MONTH_CODE_BY_NUMBER[month]}{str(year)[-1]}"


def _daily_open_interest_by_date(
    *,
    secid: str,
    from_date: str,
    till_date: str,
    base_url: str,
    timeout_seconds: float,
    page_size: int,
) -> dict[str, int]:
    rows: list[dict[str, Any]] = []
    start = 0
    while True:
        query = urllib_parse.urlencode({"from": from_date, "till": till_date, "start": start})
        url = (
            f"{_normalize_base_url(base_url)}/history/engines/{DEFAULT_MOEX_ENGINE}/markets/"
            f"{DEFAULT_MOEX_MARKET}/securities/{secid}.json?{query}"
        )
        payload = _http_json(url, timeout_seconds=timeout_seconds)
        page_rows = _rows_from_table(payload, "history")
        rows.extend(page_rows)
        if len(page_rows) < page_size:
            break
        start += page_size
    result: dict[str, int] = {}
    for row in rows:
        trade_date = str(row.get("TRADEDATE", "")).strip()
        if not trade_date:
            continue
        open_position = row.get("OPENPOSITION")
        if isinstance(open_position, bool) or not isinstance(open_position, (int, float)):
            continue
        result[trade_date] = int(open_position)
    return result


@dataclass(frozen=True)
class MoexHistoricalFetchResult:
    rows: list[dict[str, object]]
    secid: str
    instrument_id: str
    timeframe: str
    from_date: str
    till_date: str
    source_url: str

    def to_dict(self) -> dict[str, object]:
        return {
            "rows": self.rows,
            "secid": self.secid,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "from_date": self.from_date,
            "till_date": self.till_date,
            "source_url": self.source_url,
        }


def fetch_moex_historical_bars(
    *,
    contract_id: str,
    timeframe: str,
    from_date: str,
    till_date: str,
    base_url: str = DEFAULT_MOEX_ISS_BASE_URL,
    timeout_seconds: float = 20.0,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> MoexHistoricalFetchResult:
    if timeframe not in TIMEFRAME_TO_INTERVAL:
        raise ValueError(f"unsupported timeframe for MOEX ISS fetch: {timeframe}")
    instrument_id, _month, _year = _contract_parts(contract_id)
    secid = derive_moex_secid(contract_id)
    interval = TIMEFRAME_TO_INTERVAL[timeframe]
    open_interest_by_date = _daily_open_interest_by_date(
        secid=secid,
        from_date=from_date,
        till_date=till_date,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        page_size=page_size,
    )

    rows: list[dict[str, object]] = []
    start = 0
    source_url = ""
    while True:
        query = urllib_parse.urlencode(
            {
                "from": from_date,
                "till": till_date,
                "interval": interval,
                "start": start,
            }
        )
        source_url = (
            f"{_normalize_base_url(base_url)}/engines/{DEFAULT_MOEX_ENGINE}/markets/"
            f"{DEFAULT_MOEX_MARKET}/securities/{secid}/candles.json?{query}"
        )
        payload = _http_json(source_url, timeout_seconds=timeout_seconds)
        page_rows = _rows_from_table(payload, "candles")
        for row in page_rows:
            begin = _normalize_moex_ts(str(row.get("begin", "")))
            end = _normalize_moex_ts(str(row.get("end", "")))
            trade_date = begin[:10]
            open_interest = int(open_interest_by_date.get(trade_date, 0))
            rows.append(
                {
                    "contract_id": contract_id,
                    "instrument_id": instrument_id,
                    "timeframe": timeframe,
                    "ts_open": begin,
                    "ts_close": end,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"]),
                    "open_interest": open_interest,
                }
            )
        if len(page_rows) < page_size:
            break
        start += page_size

    return MoexHistoricalFetchResult(
        rows=rows,
        secid=secid,
        instrument_id=instrument_id,
        timeframe=timeframe,
        from_date=from_date,
        till_date=till_date,
        source_url=source_url,
    )
