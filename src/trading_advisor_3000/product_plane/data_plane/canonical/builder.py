from __future__ import annotations

from dataclasses import dataclass

from trading_advisor_3000.product_plane.contracts import CanonicalBar


@dataclass(frozen=True)
class CanonicalInstrument:
    instrument_id: str

    def to_dict(self) -> dict[str, str]:
        return {"instrument_id": self.instrument_id}


@dataclass(frozen=True)
class CanonicalContract:
    contract_id: str
    instrument_id: str
    first_seen_ts: str
    last_seen_ts: str

    def to_dict(self) -> dict[str, str]:
        return {
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "first_seen_ts": self.first_seen_ts,
            "last_seen_ts": self.last_seen_ts,
        }


@dataclass(frozen=True)
class SessionCalendarEntry:
    instrument_id: str
    timeframe: str
    session_date: str
    session_open_ts: str
    session_close_ts: str

    def to_dict(self) -> dict[str, str]:
        return {
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "session_date": self.session_date,
            "session_open_ts": self.session_open_ts,
            "session_close_ts": self.session_close_ts,
        }


@dataclass(frozen=True)
class RollMapEntry:
    instrument_id: str
    session_date: str
    active_contract_id: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "instrument_id": self.instrument_id,
            "session_date": self.session_date,
            "active_contract_id": self.active_contract_id,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class CanonicalDataset:
    bars: list[CanonicalBar]
    instruments: list[CanonicalInstrument]
    contracts: list[CanonicalContract]
    session_calendar: list[SessionCalendarEntry]
    roll_map: list[RollMapEntry]


def _deduplicate_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    dedup: dict[tuple[str, str, str], tuple[str, dict[str, object]]] = {}
    for row in rows:
        key = (
            str(row["contract_id"]),
            str(row["timeframe"]),
            str(row["ts_open"]),
        )
        ts_close = str(row["ts_close"])
        current = dedup.get(key)
        if current is None or ts_close > current[0]:
            dedup[key] = (ts_close, row)
    return sorted(
        (item[1] for item in dedup.values()),
        key=lambda item: (str(item["contract_id"]), str(item["timeframe"]), str(item["ts_open"])),
    )


def _build_bars(rows: list[dict[str, object]]) -> list[CanonicalBar]:
    return [
        CanonicalBar.from_dict(
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
        for row in rows
    ]


def _build_instruments(rows: list[dict[str, object]]) -> list[CanonicalInstrument]:
    instrument_ids = sorted({str(row["instrument_id"]) for row in rows})
    return [CanonicalInstrument(instrument_id=item) for item in instrument_ids]


def _build_contracts(rows: list[dict[str, object]]) -> list[CanonicalContract]:
    by_contract: dict[str, dict[str, str]] = {}
    for row in rows:
        contract_id = str(row["contract_id"])
        ts_open = str(row["ts_open"])
        ts_close = str(row["ts_close"])
        current = by_contract.get(contract_id)
        if current is None:
            by_contract[contract_id] = {
                "instrument_id": str(row["instrument_id"]),
                "first_seen_ts": ts_open,
                "last_seen_ts": ts_close,
            }
            continue
        if ts_open < current["first_seen_ts"]:
            current["first_seen_ts"] = ts_open
        if ts_close > current["last_seen_ts"]:
            current["last_seen_ts"] = ts_close

    return [
        CanonicalContract(
            contract_id=contract_id,
            instrument_id=item["instrument_id"],
            first_seen_ts=item["first_seen_ts"],
            last_seen_ts=item["last_seen_ts"],
        )
        for contract_id, item in sorted(by_contract.items())
    ]


def _build_session_calendar(rows: list[dict[str, object]]) -> list[SessionCalendarEntry]:
    calendar: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        instrument_id = str(row["instrument_id"])
        timeframe = str(row["timeframe"])
        ts_open = str(row["ts_open"])
        ts_close = str(row["ts_close"])
        session_date = ts_open[:10]
        key = (instrument_id, timeframe, session_date)
        current = calendar.get(key)
        if current is None:
            calendar[key] = {
                "session_open_ts": ts_open,
                "session_close_ts": ts_close,
            }
            continue
        if ts_open < current["session_open_ts"]:
            current["session_open_ts"] = ts_open
        if ts_close > current["session_close_ts"]:
            current["session_close_ts"] = ts_close

    return [
        SessionCalendarEntry(
            instrument_id=key[0],
            timeframe=key[1],
            session_date=key[2],
            session_open_ts=item["session_open_ts"],
            session_close_ts=item["session_close_ts"],
        )
        for key, item in sorted(calendar.items())
    ]


def _build_roll_map(rows: list[dict[str, object]]) -> list[RollMapEntry]:
    by_instrument_day: dict[tuple[str, str], tuple[str, str, int]] = {}
    for row in rows:
        instrument_id = str(row["instrument_id"])
        contract_id = str(row["contract_id"])
        session_date = str(row["ts_open"])[:10]
        ts_close = str(row["ts_close"])
        open_interest = int(row["open_interest"])
        key = (instrument_id, session_date)
        current = by_instrument_day.get(key)
        if current is None:
            by_instrument_day[key] = (contract_id, ts_close, open_interest)
            continue
        current_contract_id, current_ts_close, current_open_interest = current
        should_replace = False
        if open_interest > current_open_interest:
            should_replace = True
        elif open_interest == current_open_interest and ts_close > current_ts_close:
            should_replace = True
        if should_replace:
            by_instrument_day[key] = (contract_id, ts_close, open_interest)
        else:
            by_instrument_day[key] = (current_contract_id, current_ts_close, current_open_interest)

    return [
        RollMapEntry(
            instrument_id=key[0],
            session_date=key[1],
            active_contract_id=value[0],
            reason="max_open_interest_then_latest_ts_close",
        )
        for key, value in sorted(by_instrument_day.items())
    ]


def build_canonical_dataset(rows: list[dict[str, object]]) -> CanonicalDataset:
    deduplicated_rows = _deduplicate_rows(rows)
    bars = _build_bars(deduplicated_rows)
    return CanonicalDataset(
        bars=bars,
        instruments=_build_instruments(deduplicated_rows),
        contracts=_build_contracts(deduplicated_rows),
        session_calendar=_build_session_calendar(deduplicated_rows),
        roll_map=_build_roll_map(deduplicated_rows),
    )


def build_canonical_bars(rows: list[dict[str, object]]) -> list[CanonicalBar]:
    return build_canonical_dataset(rows).bars
