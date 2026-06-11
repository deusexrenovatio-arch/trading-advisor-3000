from __future__ import annotations

import calendar
from datetime import UTC, datetime, timedelta
from typing import Any

NESTED_WALK_FORWARD_SCHEME = "nested_walk_forward_v1"

DEFAULT_NESTED_WALK_FORWARD = {
    "outer_train_months": 18,
    "outer_confirmation_months": 3,
    "outer_step_months": 3,
    "inner_train_months": 9,
    "inner_validation_months": 3,
    "inner_step_months": 3,
}


def _parse_utc(value: str, *, field: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"{field} is required for nested walk-forward validation")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0)


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _inclusive_end_to_exclusive(value: datetime) -> datetime:
    if value.time().hour == 23 and value.time().minute == 59 and value.time().second == 59:
        return value + timedelta(seconds=1)
    return value


def _add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + int(months)
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _timeframe_delta(timeframe: str) -> timedelta:
    token = str(timeframe).strip().lower()
    if token.endswith("m"):
        return timedelta(minutes=int(token[:-1]))
    if token.endswith("h"):
        return timedelta(hours=int(token[:-1]))
    if token.endswith("d"):
        return timedelta(days=int(token[:-1]))
    if token.endswith("w"):
        return timedelta(weeks=int(token[:-1]))
    raise ValueError(f"unsupported backtest timeframe for validation: {timeframe}")


def _positive_int(value: object, *, field: str) -> int:
    try:
        resolved = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a positive integer") from exc
    if resolved <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return resolved


def _non_negative_int(value: object, *, field: str) -> int:
    if value is None:
        raise ValueError(f"{field} is required")
    try:
        resolved = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a non-negative integer") from exc
    if resolved < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return resolved


def _window_payload(
    *,
    validation_scheme: str,
    window_id: str,
    outer_fold_id: str,
    inner_fold_id: str,
    fold_role: str,
    optimizer_visible: bool,
    analysis_start: datetime,
    analysis_end: datetime,
    score_start: datetime,
    score_end: datetime,
    purge_start: datetime,
    purge_end: datetime,
    embargo_start: datetime,
    embargo_end: datetime,
    warmup_start: datetime,
) -> dict[str, object]:
    return {
        "validation_scheme": validation_scheme,
        "validation_split_id": window_id,
        "window_id": window_id,
        "outer_fold_id": outer_fold_id,
        "inner_fold_id": inner_fold_id,
        "fold_role": fold_role,
        "optimizer_visible": optimizer_visible,
        "analysis_start_ts": _format_utc(analysis_start),
        "analysis_end_ts": _format_utc(analysis_end),
        "train_start_ts": _format_utc(analysis_start),
        "train_end_ts": _format_utc(analysis_end),
        "score_start_ts": _format_utc(score_start),
        "score_end_ts": _format_utc(score_end),
        "test_start_ts": _format_utc(score_start),
        "test_end_ts": _format_utc(score_end),
        "purge_start_ts": _format_utc(purge_start),
        "purge_end_ts": _format_utc(purge_end),
        "embargo_start_ts": _format_utc(embargo_start),
        "embargo_end_ts": _format_utc(embargo_end),
        "warmup_start_ts": _format_utc(warmup_start),
    }


def build_nested_walk_forward_plan(
    *,
    start_ts: str,
    end_ts: str,
    backtest_timeframe: str,
    warmup_bars: int,
    purge_bars: int | None,
    embargo_bars: int | None,
    outer_train_months: int = DEFAULT_NESTED_WALK_FORWARD["outer_train_months"],
    outer_confirmation_months: int = DEFAULT_NESTED_WALK_FORWARD["outer_confirmation_months"],
    outer_step_months: int = DEFAULT_NESTED_WALK_FORWARD["outer_step_months"],
    inner_train_months: int = DEFAULT_NESTED_WALK_FORWARD["inner_train_months"],
    inner_validation_months: int = DEFAULT_NESTED_WALK_FORWARD["inner_validation_months"],
    inner_step_months: int = DEFAULT_NESTED_WALK_FORWARD["inner_step_months"],
) -> dict[str, object]:
    purge = _non_negative_int(purge_bars, field="purge_bars")
    embargo = _non_negative_int(embargo_bars, field="embargo_bars")
    warmup = _non_negative_int(warmup_bars, field="warmup_bars")
    outer_train = _positive_int(outer_train_months, field="outer_train_months")
    outer_confirmation = _positive_int(outer_confirmation_months, field="outer_confirmation_months")
    outer_step = _positive_int(outer_step_months, field="outer_step_months")
    inner_train = _positive_int(inner_train_months, field="inner_train_months")
    inner_validation = _positive_int(inner_validation_months, field="inner_validation_months")
    inner_step = _positive_int(inner_step_months, field="inner_step_months")

    start = _parse_utc(start_ts, field="start_ts")
    end_exclusive = _inclusive_end_to_exclusive(_parse_utc(end_ts, field="end_ts"))
    if start >= end_exclusive:
        raise ValueError("nested walk-forward start_ts must be before end_ts")

    bar_delta = _timeframe_delta(backtest_timeframe)
    purge_delta = bar_delta * purge
    embargo_delta = bar_delta * embargo
    warmup_delta = bar_delta * warmup

    windows: list[dict[str, object]] = []
    outer_cursor = start
    outer_index = 0
    while _add_months(outer_cursor, outer_train + outer_confirmation) <= end_exclusive:
        outer_index += 1
        outer_fold_id = f"outer-{outer_index:02d}"
        optimization_start = outer_cursor
        optimization_end = _add_months(outer_cursor, outer_train)
        confirmation_end = _add_months(optimization_end, outer_confirmation)

        inner_cursor = optimization_start
        inner_index = 0
        while _add_months(inner_cursor, inner_train + inner_validation) <= optimization_end:
            inner_index += 1
            inner_fold_id = f"inner-{inner_index:02d}"
            train_end = _add_months(inner_cursor, inner_train)
            validation_end = _add_months(train_end, inner_validation)
            score_start = train_end + purge_delta
            if score_start >= validation_end:
                raise ValueError("purge_bars leaves no inner validation score window")
            window_id = f"{outer_fold_id}__{inner_fold_id}__optimization"
            windows.append(
                _window_payload(
                    validation_scheme=NESTED_WALK_FORWARD_SCHEME,
                    window_id=window_id,
                    outer_fold_id=outer_fold_id,
                    inner_fold_id=inner_fold_id,
                    fold_role="optimization_validation",
                    optimizer_visible=True,
                    analysis_start=inner_cursor,
                    analysis_end=train_end,
                    score_start=score_start,
                    score_end=validation_end,
                    purge_start=train_end,
                    purge_end=score_start,
                    embargo_start=validation_end,
                    embargo_end=validation_end + embargo_delta,
                    warmup_start=inner_cursor - warmup_delta,
                )
            )
            inner_cursor = _add_months(inner_cursor, inner_step)

        if inner_index == 0:
            raise ValueError("insufficient history for nested walk-forward inner folds")

        confirmation_score_start = optimization_end + purge_delta
        if confirmation_score_start >= confirmation_end:
            raise ValueError("purge_bars leaves no confirmation score window")
        windows.append(
            _window_payload(
                validation_scheme=NESTED_WALK_FORWARD_SCHEME,
                window_id=f"{outer_fold_id}__confirmation",
                outer_fold_id=outer_fold_id,
                inner_fold_id="",
                fold_role="confirmation",
                optimizer_visible=False,
                analysis_start=optimization_start,
                analysis_end=optimization_end,
                score_start=confirmation_score_start,
                score_end=confirmation_end,
                purge_start=optimization_end,
                purge_end=confirmation_score_start,
                embargo_start=confirmation_end,
                embargo_end=confirmation_end + embargo_delta,
                warmup_start=optimization_start - warmup_delta,
            )
        )
        outer_cursor = _add_months(outer_cursor, outer_step)

    if outer_index == 0:
        raise ValueError("insufficient history for nested walk-forward outer folds")

    return {
        "scheme": NESTED_WALK_FORWARD_SCHEME,
        "config": {
            "outer_train_months": outer_train,
            "outer_confirmation_months": outer_confirmation,
            "outer_step_months": outer_step,
            "inner_train_months": inner_train,
            "inner_validation_months": inner_validation,
            "inner_step_months": inner_step,
            "warmup_bars": warmup,
            "purge_bars": purge,
            "embargo_bars": embargo,
            "backtest_timeframe": str(backtest_timeframe),
        },
        "outer_fold_count": outer_index,
        "inner_fold_count": sum(
            1 for row in windows if row["fold_role"] == "optimization_validation"
        ),
        "windows": windows,
    }


def validation_windows_from_plan(plan: dict[str, Any] | None) -> tuple[dict[str, object], ...]:
    if not plan:
        return tuple()
    windows = plan.get("windows", ())
    if not isinstance(windows, list | tuple):
        return tuple()
    return tuple(dict(item) for item in windows if isinstance(item, dict))
