from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trading_advisor_3000.product_plane.research.datasets import load_materialized_research_dataset
from trading_advisor_3000.product_plane.research.derived_indicators import reload_derived_indicator_frames
from trading_advisor_3000.product_plane.research.indicators import reload_indicator_frames

from .cache import ResearchCacheKey, ResearchFrameCache


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


@dataclass(frozen=True)
class ResearchSeriesFrame:
    contract_id: str
    instrument_id: str
    timeframe: str
    frame: pd.DataFrame
    series_id: str = ""
    series_mode: str = "contract"
    signal_frame: pd.DataFrame | None = None
    execution_frame: pd.DataFrame | None = None


def _matches_filters(
    *,
    contract_id: str,
    instrument_id: str,
    timeframe: str,
    request: ResearchSliceRequest,
) -> bool:
    if request.timeframe and timeframe != request.timeframe:
        return False
    if request.contract_ids and contract_id not in request.contract_ids:
        return False
    if request.instrument_ids and instrument_id not in request.instrument_ids:
        return False
    return True


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


def _dataset_series_mode(dataset_manifest: dict[str, object]) -> str:
    return str(dataset_manifest.get("series_mode") or "contract")


def _series_group_columns(bar_frame: pd.DataFrame, *, series_mode: str) -> list[str]:
    if series_mode == "continuous_front":
        if "series_id" not in bar_frame.columns:
            bar_frame["series_id"] = (
                bar_frame["instrument_id"].astype(str) + "|" + bar_frame["timeframe"].astype(str) + "|continuous_front"
            )
        return ["series_id", "instrument_id", "timeframe"]
    return ["contract_id", "instrument_id", "timeframe"]


def _local_rows_for_series(
    frame: pd.DataFrame,
    *,
    series_mode: str,
    contract_id: str,
    instrument_id: str,
    timeframe: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    mask = (frame["instrument_id"] == instrument_id) & (frame["timeframe"] == timeframe)
    if series_mode != "continuous_front":
        mask = mask & (frame["contract_id"] == contract_id)
    return frame[mask]


def _merge_keys(*, series_mode: str) -> list[str]:
    if series_mode == "continuous_front":
        return ["instrument_id", "timeframe", "ts"]
    return ["contract_id", "instrument_id", "timeframe", "ts"]


def _build_execution_frame(signal_frame: pd.DataFrame, *, series_mode: str) -> pd.DataFrame:
    execution = signal_frame.copy()
    if series_mode != "continuous_front":
        return execution
    for source, target in (
        ("execution_open", "open"),
        ("execution_high", "high"),
        ("execution_low", "low"),
        ("execution_close", "close"),
    ):
        if source in execution.columns:
            execution[target] = pd.to_numeric(execution[source], errors="coerce")
    return execution


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

    loaded_dataset = load_materialized_research_dataset(
        output_dir=dataset_output_dir,
        dataset_version=request.dataset_version,
    )
    series_mode = _dataset_series_mode(dict(loaded_dataset["dataset_manifest"]))
    bar_rows = [
        row
        for row in loaded_dataset["bar_views"]
        if (not request.analysis_only or row.slice_role == "analysis")
        and _matches_filters(
            contract_id=row.contract_id,
            instrument_id=row.instrument_id,
            timeframe=row.timeframe,
            request=request,
        )
    ]
    indicator_rows = [
        row
        for row in reload_indicator_frames(
            indicator_output_dir=indicator_output_dir,
            dataset_version=request.dataset_version,
            indicator_set_version=request.indicator_set_version,
        )
        if _matches_filters(
            contract_id=row.contract_id,
            instrument_id=row.instrument_id,
            timeframe=row.timeframe,
            request=request,
        )
    ]
    derived_rows = [
        row
        for row in reload_derived_indicator_frames(
            derived_indicator_output_dir=derived_indicator_output_dir,
            dataset_version=request.dataset_version,
            indicator_set_version=request.indicator_set_version,
            derived_indicator_set_version=request.derived_indicator_set_version,
        )
        if _matches_filters(
            contract_id=row.contract_id,
            instrument_id=row.instrument_id,
            timeframe=row.timeframe,
            request=request,
        )
    ]

    bar_frame = pd.DataFrame([row.to_dict() for row in bar_rows])
    indicator_frame = pd.DataFrame([row.to_dict() for row in indicator_rows])
    derived_frame = pd.DataFrame([row.to_dict() for row in derived_rows])
    if bar_frame.empty:
        return tuple(), cache_id, False

    slices: list[ResearchSeriesFrame] = []
    group_columns = _series_group_columns(bar_frame, series_mode=series_mode)
    for group_key, base_frame in bar_frame.groupby(group_columns, sort=True):
        if series_mode == "continuous_front":
            series_id, instrument_id, timeframe = group_key
            contract_id = "continuous-front"
        else:
            contract_id, instrument_id, timeframe = group_key
            series_id = str(contract_id)
        series = base_frame.sort_values("ts").reset_index(drop=True)
        merged = series.copy()
        if not indicator_frame.empty:
            local_indicators = _local_rows_for_series(
                indicator_frame,
                series_mode=series_mode,
                contract_id=str(contract_id),
                instrument_id=str(instrument_id),
                timeframe=str(timeframe),
            )
            if not local_indicators.empty:
                indicator_columns = _indicator_payload_columns(local_indicators)
                merged = merged.merge(
                    local_indicators[[*_merge_keys(series_mode=series_mode), *indicator_columns]],
                    on=_merge_keys(series_mode=series_mode),
                    how="left",
                    validate="one_to_one",
                )
        if not derived_frame.empty:
            local_derived = _local_rows_for_series(
                derived_frame,
                series_mode=series_mode,
                contract_id=str(contract_id),
                instrument_id=str(instrument_id),
                timeframe=str(timeframe),
            )
            if not local_derived.empty:
                derived_columns = _derived_payload_columns(local_derived, existing=set(merged.columns))
                merged = merged.merge(
                    local_derived[[*_merge_keys(series_mode=series_mode), *derived_columns]],
                    on=_merge_keys(series_mode=series_mode),
                    how="left",
                    validate="one_to_one",
                )
        merged.index = pd.to_datetime(merged["ts"], utc=True)
        execution_frame = _build_execution_frame(merged, series_mode=series_mode)
        slices.append(
            ResearchSeriesFrame(
                contract_id=str(contract_id),
                instrument_id=str(instrument_id),
                timeframe=str(timeframe),
                frame=merged,
                series_id=str(series_id),
                series_mode=series_mode,
                signal_frame=merged,
                execution_frame=execution_frame,
            )
        )

    result = tuple(slices)
    if cache is not None:
        cache.set(cache_key, result)
    return result, cache_id, False
