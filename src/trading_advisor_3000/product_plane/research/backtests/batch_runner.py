from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from trading_advisor_3000.product_plane.research.datasets import load_materialized_research_dataset
from trading_advisor_3000.product_plane.research.io.cache import ResearchFrameCache
from trading_advisor_3000.product_plane.research.io.loaders import ResearchSliceRequest, load_backtest_frames
from trading_advisor_3000.product_plane.research.strategies import StrategyRegistry, build_phase1_strategy_registry

from .engine import BacktestEngineConfig, run_backtest_series
from .results import phase5_backtest_store_contract, write_backtest_artifacts


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _chunked[T](items: list[T], chunk_size: int) -> tuple[tuple[T, ...], ...]:
    return tuple(
        tuple(items[index : index + chunk_size])
        for index in range(0, len(items), chunk_size)
    ) or (tuple(),)


def _manifest_split_windows(dataset_manifest: dict[str, object]) -> tuple[dict[str, object], ...]:
    raw = dataset_manifest.get("split_params_json")
    if isinstance(raw, str) and raw.strip():
        payload = json.loads(raw)
    elif isinstance(raw, dict):
        payload = raw
    else:
        payload = {}
    windows = payload.get("windows", []) if isinstance(payload, dict) else []
    return tuple(item for item in windows if isinstance(item, dict))


@dataclass(frozen=True)
class BacktestBatchRequest:
    dataset_version: str
    indicator_set_version: str
    feature_set_version: str
    strategy_versions: tuple[str, ...]
    combination_count: int
    param_batch_size: int = 25
    series_batch_size: int = 4
    timeframe: str = ""
    contract_ids: tuple[str, ...] = ()
    instrument_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.strategy_versions:
            raise ValueError("strategy_versions must not be empty")
        if self.combination_count <= 0:
            raise ValueError("combination_count must be positive")
        if self.param_batch_size <= 0:
            raise ValueError("param_batch_size must be positive")
        if self.series_batch_size <= 0:
            raise ValueError("series_batch_size must be positive")

    def batch_id(self) -> str:
        payload = "|".join(
            (
                self.dataset_version,
                self.indicator_set_version,
                self.feature_set_version,
                *self.strategy_versions,
                str(self.combination_count),
                str(self.param_batch_size),
                str(self.series_batch_size),
                self.timeframe,
                *self.contract_ids,
                *self.instrument_ids,
            )
        )
        return "BTBATCH-" + _stable_hash(payload)


def run_backtest_batch(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    feature_output_dir: Path,
    output_dir: Path,
    request: BacktestBatchRequest,
    engine_config: BacktestEngineConfig | None = None,
    strategy_registry: StrategyRegistry | None = None,
    cache: ResearchFrameCache | None = None,
) -> dict[str, object]:
    registry = strategy_registry or build_phase1_strategy_registry()
    engine_config = engine_config or BacktestEngineConfig()
    dataset_manifest = load_materialized_research_dataset(
        output_dir=dataset_output_dir,
        dataset_version=request.dataset_version,
    )["dataset_manifest"]
    split_windows = _manifest_split_windows(dataset_manifest)
    series_frames, cache_id, cache_hit = load_backtest_frames(
        dataset_output_dir=dataset_output_dir,
        indicator_output_dir=indicator_output_dir,
        feature_output_dir=feature_output_dir,
        request=ResearchSliceRequest(
            dataset_version=request.dataset_version,
            indicator_set_version=request.indicator_set_version,
            feature_set_version=request.feature_set_version,
            timeframe=request.timeframe,
            contract_ids=request.contract_ids,
            instrument_ids=request.instrument_ids,
        ),
        cache=cache,
    )
    selected_series = list(series_frames)
    if not selected_series:
        raise ValueError("no materialized research series matched the backtest request")

    batch_id = request.batch_id()
    all_run_rows: list[dict[str, object]] = []
    all_stat_rows: list[dict[str, object]] = []
    all_trade_rows: list[dict[str, object]] = []
    all_order_rows: list[dict[str, object]] = []
    all_drawdown_rows: list[dict[str, object]] = []
    total_combinations = 0

    for strategy_version in request.strategy_versions:
        strategy_spec = registry.get(strategy_version)
        combinations = list(strategy_spec.parameter_combinations())[: request.combination_count]
        total_combinations += len(combinations)
        for series_chunk in _chunked(selected_series, request.series_batch_size):
            if not series_chunk:
                continue
            for param_chunk in _chunked(combinations, request.param_batch_size):
                for series in series_chunk:
                    for params in param_chunk:
                        result = run_backtest_series(
                            series=series,
                            strategy_spec=strategy_spec,
                            params=params,
                            config=engine_config,
                            backtest_batch_id=batch_id,
                            dataset_version=request.dataset_version,
                            indicator_set_version=request.indicator_set_version,
                            feature_set_version=request.feature_set_version,
                            split_windows=split_windows,
                        )
                        all_run_rows.extend(result["run_rows"])
                        all_stat_rows.extend(result["stat_rows"])
                        all_trade_rows.extend(result["trade_rows"])
                        all_order_rows.extend(result["order_rows"])
                        all_drawdown_rows.extend(result["drawdown_rows"])

    batch_row = {
        "backtest_batch_id": batch_id,
        "dataset_version": request.dataset_version,
        "indicator_set_version": request.indicator_set_version,
        "feature_set_version": request.feature_set_version,
        "strategy_catalog_version": registry.catalog_version(),
        "engine_name": engine_config.engine_name,
        "param_batch_size": request.param_batch_size,
        "series_batch_size": request.series_batch_size,
        "combination_count": total_combinations,
        "series_count": len(selected_series),
        "cache_id": cache_id,
        "cache_hit": 1 if cache_hit else 0,
        "created_at": all_stat_rows[0]["created_at"] if all_stat_rows else "1970-01-01T00:00:00Z",
    }
    output_paths = write_backtest_artifacts(
        output_dir=output_dir,
        batch_rows=[batch_row],
        run_rows=all_run_rows,
        stat_rows=all_stat_rows,
        trade_rows=all_trade_rows,
        order_rows=all_order_rows,
        drawdown_rows=all_drawdown_rows,
    )
    return {
        "backtest_batch": batch_row,
        "run_rows": all_run_rows,
        "stat_rows": all_stat_rows,
        "trade_rows": all_trade_rows,
        "order_rows": all_order_rows,
        "drawdown_rows": all_drawdown_rows,
        "cache_id": cache_id,
        "cache_hit": cache_hit,
        "delta_manifest": phase5_backtest_store_contract(),
        "output_paths": output_paths,
    }
