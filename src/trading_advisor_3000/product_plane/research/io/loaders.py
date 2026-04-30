from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_frame

from .cache import ResearchCacheKey, ResearchFrameCache


KEY_COLUMNS = ("contract_id", "instrument_id", "timeframe", "ts")
BAR_METADATA_COLUMNS = (
    "dataset_version",
    "session_date",
    "session_open_ts",
    "session_close_ts",
    "active_contract_id",
    "bar_index",
)
INDICATOR_METADATA_COLUMNS = ("dataset_version", "indicator_set_version")
DERIVED_METADATA_COLUMNS = ("dataset_version", "indicator_set_version", "derived_indicator_set_version")


@dataclass(frozen=True)
class ResearchSliceRequest:
    dataset_version: str
    indicator_set_version: str
    derived_indicator_set_version: str = "derived-v1"
    timeframe: str = ""
    contract_ids: tuple[str, ...] = ()
    instrument_ids: tuple[str, ...] = ()
    analysis_only: bool = True
    warmup_bars: int = 0
    price_columns: tuple[str, ...] = ()
    indicator_columns: tuple[str, ...] = ()
    derived_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class ResearchSeriesFrame:
    contract_id: str
    instrument_id: str
    timeframe: str
    frame: pd.DataFrame


def _dedupe(values: tuple[str, ...] | list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in result:
            result.append(text)
    return result


def _delta_filters(
    request: ResearchSliceRequest,
    *,
    include_indicator_version: bool = False,
    include_derived_version: bool = False,
) -> list[tuple[str, str, object]]:
    filters: list[tuple[str, str, object]] = [("dataset_version", "=", request.dataset_version)]
    if request.analysis_only and not include_indicator_version and not include_derived_version:
        filters.append(("slice_role", "=", "analysis"))
    if request.timeframe:
        filters.append(("timeframe", "=", request.timeframe))
    if request.contract_ids:
        values = sorted(set(request.contract_ids))
        filters.append(("contract_id", "=", values[0]) if len(values) == 1 else ("contract_id", "in", values))
    if request.instrument_ids:
        values = sorted(set(request.instrument_ids))
        filters.append(("instrument_id", "=", values[0]) if len(values) == 1 else ("instrument_id", "in", values))
    if include_indicator_version:
        filters.append(("indicator_set_version", "=", request.indicator_set_version))
    if include_derived_version:
        filters.append(("derived_indicator_set_version", "=", request.derived_indicator_set_version))
    return filters


def _projected_columns(
    *,
    metadata_columns: tuple[str, ...],
    payload_columns: tuple[str, ...],
    filter_columns: tuple[str, ...] = (),
) -> list[str] | None:
    if not payload_columns:
        return None
    return _dedupe([*metadata_columns, *KEY_COLUMNS, *filter_columns, *payload_columns])


def _indicator_payload_columns(frame: pd.DataFrame) -> list[str]:
    reserved = {
        "dataset_version",
        "indicator_set_version",
        "profile_version",
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        "source_bars_hash",
        "source_dataset_bars_hash",
        "row_count",
        "warmup_span",
        "null_warmup_span",
        "created_at",
        "output_columns_hash",
    }
    return [column for column in frame.columns if column not in reserved]


def _derived_payload_columns(frame: pd.DataFrame, *, existing: set[str]) -> list[str]:
    reserved = {
        "dataset_version",
        "indicator_set_version",
        "derived_indicator_set_version",
        "profile_version",
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        "source_bars_hash",
        "source_dataset_bars_hash",
        "source_indicators_hash",
        "row_count",
        "warmup_span",
        "null_warmup_span",
        "created_at",
        "output_columns_hash",
    }
    return [column for column in frame.columns if column not in reserved and column not in existing]


def load_backtest_frames(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    derived_indicator_output_dir: Path,
    request: ResearchSliceRequest,
    cache: ResearchFrameCache | None = None,
) -> tuple[tuple[ResearchSeriesFrame, ...], str, bool]:
    filter_tokens = (
        *sorted(request.contract_ids),
        *sorted(request.instrument_ids),
        request.dataset_version,
        request.indicator_set_version,
        request.derived_indicator_set_version,
        "analysis" if request.analysis_only else "all",
        str(request.warmup_bars),
        "price:" + ",".join(sorted(request.price_columns)),
        "indicator:" + ",".join(sorted(request.indicator_columns)),
        "derived:" + ",".join(sorted(request.derived_columns)),
    )
    cache_key = ResearchCacheKey(
        scope="stage5-backtest",
        version_keys=filter_tokens,
        timeframe=request.timeframe or "all",
    )
    cache_id = cache_key.cache_id()
    if cache is not None:
        cached = cache.get(cache_key)
        if cached is not None:
            return cached, cache_id, True

    bar_frame = read_delta_table_frame(
        dataset_output_dir / "research_bar_views.delta",
        columns=_projected_columns(
            metadata_columns=BAR_METADATA_COLUMNS,
            payload_columns=request.price_columns,
        ),
        filters=_delta_filters(request),
    )
    indicator_frame = (
        read_delta_table_frame(
            indicator_output_dir / "research_indicator_frames.delta",
            columns=_projected_columns(
                metadata_columns=INDICATOR_METADATA_COLUMNS,
                payload_columns=request.indicator_columns,
            ),
            filters=_delta_filters(request, include_indicator_version=True),
        )
        if request.indicator_columns
        else pd.DataFrame()
    )
    derived_frame = (
        read_delta_table_frame(
            derived_indicator_output_dir / "research_derived_indicator_frames.delta",
            columns=_projected_columns(
                metadata_columns=DERIVED_METADATA_COLUMNS,
                payload_columns=request.derived_columns,
            ),
            filters=_delta_filters(
                request,
                include_indicator_version=True,
                include_derived_version=True,
            ),
        )
        if request.derived_columns
        else pd.DataFrame()
    )
    if bar_frame.empty:
        return tuple(), cache_id, False

    slices: list[ResearchSeriesFrame] = []
    for (contract_id, instrument_id, timeframe), base_frame in bar_frame.groupby(
        ["contract_id", "instrument_id", "timeframe"], sort=True
    ):
        series = base_frame.sort_values("ts").reset_index(drop=True)
        merged = series.copy()
        if not indicator_frame.empty:
            local_indicators = indicator_frame[
                (indicator_frame["contract_id"] == contract_id)
                & (indicator_frame["instrument_id"] == instrument_id)
                & (indicator_frame["timeframe"] == timeframe)
            ]
            if not local_indicators.empty:
                indicator_columns = _indicator_payload_columns(local_indicators)
                merged = merged.merge(
                    local_indicators[["contract_id", "instrument_id", "timeframe", "ts", *indicator_columns]],
                    on=["contract_id", "instrument_id", "timeframe", "ts"],
                    how="left",
                    validate="one_to_one",
                )
        if not derived_frame.empty:
            local_derived = derived_frame[
                (derived_frame["contract_id"] == contract_id)
                & (derived_frame["instrument_id"] == instrument_id)
                & (derived_frame["timeframe"] == timeframe)
            ]
            if not local_derived.empty:
                derived_columns = _derived_payload_columns(local_derived, existing=set(merged.columns))
                merged = merged.merge(
                    local_derived[["contract_id", "instrument_id", "timeframe", "ts", *derived_columns]],
                    on=["contract_id", "instrument_id", "timeframe", "ts"],
                    how="left",
                    validate="one_to_one",
                )
        merged.index = pd.to_datetime(merged["ts"], utc=True)
        slices.append(
            ResearchSeriesFrame(
                contract_id=str(contract_id),
                instrument_id=str(instrument_id),
                timeframe=str(timeframe),
                frame=merged,
            )
        )

    result = tuple(slices)
    if cache is not None:
        cache.set(cache_key, result)
    return result, cache_id, False
