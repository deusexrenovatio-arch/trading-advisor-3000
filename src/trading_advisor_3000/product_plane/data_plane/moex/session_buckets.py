from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Iterable

from trading_advisor_3000.product_plane.contracts import Timeframe

from .bar_interval_policy import POLICY_VERSION, resolve_moex_bar_policy

CANONICAL_BUCKET_POLICY_VERSION = POLICY_VERSION
TARGET_MINUTES_BY_TIMEFRAME: dict[str, int] = {
    Timeframe.M5.value: 5,
    Timeframe.M15.value: 15,
    Timeframe.H1.value: 60,
    Timeframe.H4.value: 240,
    Timeframe.D1.value: 1440,
    Timeframe.W1.value: 10080,
}
INTRADAY_SOURCE_PREFERENCE = (1, 5, 10, 15, 30, 60, 240)


class CanonicalBucketBuildError(ValueError):
    pass


@dataclass(frozen=True)
class CanonicalBucket:
    contract_id: str
    instrument_id: str
    timeframe: str
    source_interval: int
    target_minutes: int
    bucket_start_ts: str
    bucket_end_ts: str
    canonical_ts: str
    policy_id: str
    anchor_type: str
    source_mode: str
    session_model: str

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "source_interval": self.source_interval,
            "target_minutes": self.target_minutes,
            "bucket_start_ts": self.bucket_start_ts,
            "bucket_end_ts": self.bucket_end_ts,
            "canonical_ts": self.canonical_ts,
            "policy_id": self.policy_id,
            "anchor_type": self.anchor_type,
            "source_mode": self.source_mode,
            "session_model": self.session_model,
        }


def _parse_utc(ts: str) -> datetime:
    parsed = datetime.fromisoformat(str(ts).strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0)


def _format_utc(ts: datetime) -> str:
    return ts.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _floor_to_interval(ts: datetime, *, minutes: int) -> datetime:
    if minutes == TARGET_MINUTES_BY_TIMEFRAME[Timeframe.W1.value]:
        start = datetime(ts.year, ts.month, ts.day, tzinfo=UTC) - timedelta(days=ts.weekday())
        return start
    if minutes == TARGET_MINUTES_BY_TIMEFRAME[Timeframe.D1.value]:
        return datetime(ts.year, ts.month, ts.day, tzinfo=UTC)
    seconds = int(ts.timestamp())
    bucket_seconds = (seconds // (minutes * 60)) * (minutes * 60)
    return datetime.fromtimestamp(bucket_seconds, tz=UTC)


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


def _contract_keys_for_window(
    *,
    window: object,
    available_intervals_by_contract: dict[tuple[str, str], set[int]],
) -> list[tuple[str, str]]:
    internal_id = str(getattr(window, "internal_id")).strip()
    moex_secid = str(getattr(window, "moex_secid", "")).strip()
    matches = [
        (contract_id, instrument_id)
        for contract_id, instrument_id in available_intervals_by_contract
        if instrument_id == internal_id
    ]
    if matches:
        return sorted(matches)
    if moex_secid:
        return [(moex_secid, internal_id)]
    return []


def _select_source_interval(
    *,
    contract_id: str,
    instrument_id: str,
    timeframe: str,
    local_date: object,
    target_minutes: int,
    source_mode: str,
    available_intervals: set[int],
    selected_source_intervals: dict[tuple[str, str, str], int],
) -> int | None:
    if source_mode == "native_authoritative":
        if target_minutes in available_intervals:
            return target_minutes
        raise CanonicalBucketBuildError(
            f"{timeframe} native/calendar policy requires source_interval={target_minutes}; "
            f"contract_id={contract_id}; instrument_id={instrument_id}; local_date={local_date}; "
            f"required_source_mode={source_mode}; available_intervals={sorted(available_intervals)}"
        )

    if source_mode == "aggregate_intraday":
        for source_interval in INTRADAY_SOURCE_PREFERENCE:
            if source_interval in available_intervals and target_minutes % source_interval == 0:
                return source_interval
        selected = selected_source_intervals.get((contract_id, instrument_id, timeframe))
        if (
            selected in available_intervals
            and selected in INTRADAY_SOURCE_PREFERENCE
            and target_minutes % int(selected) == 0
        ):
            return int(selected)
        if timeframe != Timeframe.D1.value:
            return None
        raise CanonicalBucketBuildError(
            f"{timeframe} aggregate policy requires compatible intraday source; "
            f"contract_id={contract_id}; instrument_id={instrument_id}; local_date={local_date}; "
            f"required_source_mode={source_mode}; available_intervals={sorted(available_intervals)}"
        )

    selected = selected_source_intervals.get((contract_id, instrument_id, timeframe))
    if selected in available_intervals:
        return int(selected)
    raise CanonicalBucketBuildError(
        f"{timeframe} policy has no valid source interval; contract_id={contract_id}; "
        f"instrument_id={instrument_id}; local_date={local_date}; "
        f"required_source_mode={source_mode}; "
        f"available_intervals={sorted(available_intervals)}"
    )


def _iter_bucket_starts(
    *,
    window_start: datetime,
    window_end: datetime,
    target_minutes: int,
) -> Iterable[datetime]:
    expanded_start = window_start - timedelta(days=1)
    expanded_end = window_end + timedelta(days=1)
    step = timedelta(minutes=target_minutes)
    cursor = _floor_to_interval(expanded_start, minutes=target_minutes)
    while cursor < expanded_end:
        bucket_end = cursor + step
        if bucket_end > window_start and cursor < window_end:
            yield cursor
        cursor = bucket_end


def build_canonical_bucket_rows(
    *,
    changed_windows: list[object],
    selected_source_intervals: dict[tuple[str, str, str], int],
    available_intervals_by_contract: dict[tuple[str, str], set[int]],
    target_timeframes: tuple[str, ...],
) -> list[CanonicalBucket]:
    rows_by_key: dict[tuple[str, str, str, int, str], CanonicalBucket] = {}
    normalized_timeframes = tuple(_normalize_timeframe(item) for item in target_timeframes)

    for window in changed_windows:
        window_start = _parse_utc(str(getattr(window, "window_start_utc")))
        window_end = _parse_utc(str(getattr(window, "window_end_utc")))
        if window_end <= window_start:
            continue
        for contract_id, instrument_id in _contract_keys_for_window(
            window=window,
            available_intervals_by_contract=available_intervals_by_contract,
        ):
            available_intervals = set(
                available_intervals_by_contract.get((contract_id, instrument_id), set())
            )
            for timeframe in normalized_timeframes:
                target_minutes = TARGET_MINUTES_BY_TIMEFRAME[timeframe]
                for bucket_start in _iter_bucket_starts(
                    window_start=window_start,
                    window_end=window_end,
                    target_minutes=target_minutes,
                ):
                    policy = resolve_moex_bar_policy(timeframe, bucket_start)
                    source_interval = _select_source_interval(
                        contract_id=contract_id,
                        instrument_id=instrument_id,
                        timeframe=timeframe,
                        local_date=policy.local_date,
                        target_minutes=target_minutes,
                        source_mode=policy.source_mode,
                        available_intervals=available_intervals,
                        selected_source_intervals=selected_source_intervals,
                    )
                    if source_interval is None:
                        continue
                    bucket_end = bucket_start + timedelta(minutes=target_minutes)
                    row = CanonicalBucket(
                        contract_id=contract_id,
                        instrument_id=instrument_id,
                        timeframe=timeframe,
                        source_interval=source_interval,
                        target_minutes=target_minutes,
                        bucket_start_ts=_format_utc(bucket_start),
                        bucket_end_ts=_format_utc(bucket_end),
                        canonical_ts=_format_utc(bucket_start),
                        policy_id=policy.policy_id,
                        anchor_type=policy.anchor_type,
                        source_mode=policy.source_mode,
                        session_model=policy.session_model,
                    )
                    rows_by_key[
                        (
                            row.contract_id,
                            row.instrument_id,
                            row.timeframe,
                            row.source_interval,
                            row.bucket_start_ts,
                        )
                    ] = row

    return sorted(
        rows_by_key.values(),
        key=lambda item: (
            item.contract_id,
            item.instrument_id,
            item.timeframe,
            item.source_interval,
            item.bucket_start_ts,
        ),
    )
