from __future__ import annotations

from trading_advisor_3000.app.contracts import CanonicalBar


def build_canonical_bars(rows: list[dict[str, object]]) -> list[CanonicalBar]:
    dedup: dict[tuple[str, str, str], tuple[str, CanonicalBar]] = {}
    for row in rows:
        bar = CanonicalBar.from_dict(
            {
                "contract_id": row["contract_id"],
                "instrument_id": row["instrument_id"],
                "timeframe": row["timeframe"],
                "ts": row["ts_open"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "open_interest": row["open_interest"],
            },
        )
        key = (bar.contract_id, bar.timeframe.value, bar.ts)
        ts_close = str(row["ts_close"])
        current = dedup.get(key)
        if current is None or ts_close > current[0]:
            dedup[key] = (ts_close, bar)
    return sorted((item[1] for item in dedup.values()), key=lambda item: (item.contract_id, item.timeframe.value, item.ts))
