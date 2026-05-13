from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal
from zoneinfo import ZoneInfo

MOEX_TZ = ZoneInfo("Europe/Moscow")
POLICY_VERSION = "moex_bar_interval_policy_v1"

AnchorType = Literal[
    "session_segment",
    "clearing_day",
    "calendar_day",
    "calendar_week",
]

SourceMode = Literal[
    "aggregate_intraday",
    "native_authoritative",
    "aggregate_raw",
]

SessionModel = Literal[
    "legacy_session",
    "unified_session",
]


@dataclass(frozen=True)
class BarIntervalPolicy:
    timeframe: str
    local_date: date
    anchor_type: AnchorType
    source_mode: SourceMode
    session_model: SessionModel
    policy_id: str


def _parse_ts(ts: str | datetime) -> datetime:
    if isinstance(ts, datetime):
        parsed = ts
    elif isinstance(ts, str):
        value = ts.strip()
        if not value:
            raise ValueError("timestamp must be non-empty")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError("timestamp must be str or datetime")
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_timeframe(timeframe: str) -> str:
    value = str(timeframe).strip().lower()
    aliases = {
        "m5": "5m",
        "m15": "15m",
        "h1": "1h",
        "h4": "4h",
        "d1": "1d",
        "w1": "1w",
    }
    return aliases.get(value, value)


def _policy_id(
    *,
    timeframe: str,
    anchor_type: AnchorType,
    source_mode: SourceMode,
    session_model: SessionModel,
) -> str:
    return f"moex:{timeframe}:{anchor_type}:{source_mode}:{session_model}:v1"


def moex_local_date(ts: str | datetime) -> date:
    return _parse_ts(ts).astimezone(MOEX_TZ).date()


def is_native_calendar_d1_period(local_date: date) -> bool:
    return date(2022, 9, 5) <= local_date <= date(2022, 11, 25)


def is_clearing_day_d1_period(local_date: date) -> bool:
    return local_date <= date(2022, 9, 2) or local_date >= date(2022, 11, 28)


def is_unified_session_period(local_date: date) -> bool:
    return local_date >= date(2026, 3, 29)


def resolve_moex_bar_policy(timeframe: str, ts: str | datetime) -> BarIntervalPolicy:
    normalized_timeframe = _normalize_timeframe(timeframe)
    local = moex_local_date(ts)
    session_model: SessionModel = (
        "unified_session" if is_unified_session_period(local) else "legacy_session"
    )

    if normalized_timeframe == "1d":
        if is_native_calendar_d1_period(local):
            anchor_type: AnchorType = "calendar_day"
            source_mode: SourceMode = "native_authoritative"
        else:
            anchor_type = "clearing_day"
            source_mode = "aggregate_intraday"
    elif normalized_timeframe in {"5m", "15m", "1h", "4h"}:
        anchor_type = "session_segment"
        source_mode = "aggregate_intraday"
    elif normalized_timeframe == "1w":
        anchor_type = "calendar_week"
        source_mode = "aggregate_intraday"
    else:
        raise ValueError(f"unsupported MOEX canonical timeframe: {timeframe}")

    return BarIntervalPolicy(
        timeframe=normalized_timeframe,
        local_date=local,
        anchor_type=anchor_type,
        source_mode=source_mode,
        session_model=session_model,
        policy_id=_policy_id(
            timeframe=normalized_timeframe,
            anchor_type=anchor_type,
            source_mode=source_mode,
            session_model=session_model,
        ),
    )
