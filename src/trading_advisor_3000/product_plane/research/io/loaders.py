from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trading_advisor_3000.product_plane.research.datasets import load_materialized_research_dataset
from trading_advisor_3000.product_plane.research.features import reload_feature_frames
from trading_advisor_3000.product_plane.research.indicators import reload_indicator_frames

from .cache import ResearchCacheKey, ResearchFrameCache


@dataclass(frozen=True)
class ResearchSliceRequest:
    dataset_version: str
    indicator_set_version: str
    feature_set_version: str
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
        "row_count",
        "warmup_span",
        "null_warmup_span",
        "created_at",
    }
    return [column for column in frame.columns if column not in reserved]


def _feature_payload_columns(frame: pd.DataFrame, *, existing: set[str]) -> list[str]:
    reserved = {
        "dataset_version",
        "indicator_set_version",
        "feature_set_version",
        "profile_version",
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        "source_bars_hash",
        "source_indicators_hash",
        "row_count",
        "warmup_span",
        "null_warmup_span",
        "created_at",
    }
    return [column for column in frame.columns if column not in reserved and column not in existing]


def load_backtest_frames(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    feature_output_dir: Path,
    request: ResearchSliceRequest,
    cache: ResearchFrameCache | None = None,
) -> tuple[tuple[ResearchSeriesFrame, ...], str, bool]:
    filter_tokens = (
        *sorted(request.contract_ids),
        *sorted(request.instrument_ids),
        request.dataset_version,
        request.indicator_set_version,
        request.feature_set_version,
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
    feature_rows = [
        row
        for row in reload_feature_frames(
            feature_output_dir=feature_output_dir,
            dataset_version=request.dataset_version,
            indicator_set_version=request.indicator_set_version,
            feature_set_version=request.feature_set_version,
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
    feature_frame = pd.DataFrame([row.to_dict() for row in feature_rows])
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
        if not feature_frame.empty:
            local_features = feature_frame[
                (feature_frame["contract_id"] == contract_id)
                & (feature_frame["instrument_id"] == instrument_id)
                & (feature_frame["timeframe"] == timeframe)
            ]
            if not local_features.empty:
                feature_columns = _feature_payload_columns(local_features, existing=set(merged.columns))
                merged = merged.merge(
                    local_features[["contract_id", "instrument_id", "timeframe", "ts", *feature_columns]],
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
