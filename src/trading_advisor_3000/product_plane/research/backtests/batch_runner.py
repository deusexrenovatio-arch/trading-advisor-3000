from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from trading_advisor_3000.product_plane.research.datasets import load_materialized_research_dataset
from trading_advisor_3000.product_plane.research.io.cache import ResearchFrameCache
from trading_advisor_3000.product_plane.research.io.loaders import ResearchSliceRequest, load_backtest_frames
from trading_advisor_3000.product_plane.research.strategies import StrategyRegistry, build_strategy_registry

from .engine import BacktestEngineConfig, run_backtest_series
from .results import backtest_store_contract, write_backtest_artifacts


T = TypeVar("T")


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _chunked(items: list[T], chunk_size: int) -> tuple[tuple[T, ...], ...]:
    return tuple(
        tuple(items[index : index + chunk_size])
        for index in range(0, len(items), chunk_size)
    ) or (tuple(),)


def _manifest_split_windows(dataset_manifest: dict[str, object]) -> tuple[dict[str, object], ...]:
    raw = dataset_manifest.get("split_params_json")
    if isinstance(raw, str) and raw.strip():
        import json

        payload = json.loads(raw)
    elif isinstance(raw, dict):
        payload = raw
    else:
        payload = {}
    windows = payload.get("windows", []) if isinstance(payload, dict) else []
    return tuple(item for item in windows if isinstance(item, dict))


def _manifest_hash(payload: dict[str, object]) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class BacktestStrategyInstance:
    strategy_instance_id: str
    strategy_template_id: str
    family_id: str
    family_key: str
    strategy_version_label: str
    execution_mode: str
    parameter_values: dict[str, object]
    manifest_hash: str

    def __post_init__(self) -> None:
        for field_name in (
            "strategy_instance_id",
            "strategy_template_id",
            "family_id",
            "family_key",
            "strategy_version_label",
            "execution_mode",
            "manifest_hash",
        ):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"{field_name} must be non-empty")


@dataclass(frozen=True)
class EphemeralStrategySpace:
    strategy_space_id: str
    strategy_instances: tuple[BacktestStrategyInstance, ...]

    def __post_init__(self) -> None:
        if not self.strategy_space_id.strip():
            raise ValueError("strategy_space_id must not be empty")
        if not self.strategy_instances:
            raise ValueError("strategy_instances must not be empty")


@dataclass(frozen=True)
class BacktestBatchRequest:
    campaign_run_id: str
    strategy_space_id: str
    dataset_version: str
    indicator_set_version: str
    strategy_instances: tuple[BacktestStrategyInstance, ...]
    combination_count: int
    derived_indicator_set_version: str = "derived-v1"
    param_batch_size: int = 25
    series_batch_size: int = 4
    timeframe: str = ""
    contract_ids: tuple[str, ...] = ()
    instrument_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.campaign_run_id.strip():
            raise ValueError("campaign_run_id must not be empty")
        if not self.strategy_space_id.strip():
            raise ValueError("strategy_space_id must not be empty")
        if not self.strategy_instances:
            raise ValueError("strategy_instances must not be empty")
        if self.combination_count <= 0:
            raise ValueError("combination_count must be positive")
        if self.param_batch_size <= 0:
            raise ValueError("param_batch_size must be positive")
        if self.series_batch_size <= 0:
            raise ValueError("series_batch_size must be positive")

    def batch_id(self) -> str:
        payload = "|".join(
            (
                self.campaign_run_id,
                self.strategy_space_id,
                self.dataset_version,
                self.indicator_set_version,
                self.derived_indicator_set_version,
                *[instance.strategy_instance_id for instance in self.strategy_instances],
                str(self.combination_count),
                str(self.param_batch_size),
                str(self.series_batch_size),
                self.timeframe,
                *self.contract_ids,
                *self.instrument_ids,
            )
        )
        return "BTBATCH-" + _stable_hash(payload)


def build_ephemeral_strategy_space(
    *,
    strategy_version_labels: tuple[str, ...],
    instances_per_strategy: int,
    strategy_registry: StrategyRegistry | None = None,
) -> EphemeralStrategySpace:
    registry = strategy_registry or build_strategy_registry()
    if not strategy_version_labels:
        raise ValueError("strategy_version_labels must not be empty")
    if instances_per_strategy <= 0:
        raise ValueError("instances_per_strategy must be positive")

    strategy_instances: list[BacktestStrategyInstance] = []
    for strategy_version_label in strategy_version_labels:
        strategy_spec = registry.get(strategy_version_label)
        family_id = "sfam_" + _stable_hash(strategy_spec.family)
        strategy_template_id = "stpl_" + _stable_hash(
            f"{strategy_spec.family}|{strategy_spec.version}|{strategy_spec.execution_mode}"
        )
        parameter_sets = registry.parameter_combinations(strategy_version_label)[:instances_per_strategy]
        for parameter_values in parameter_sets:
            manifest_payload = {
                "strategy_version_label": strategy_spec.version,
                "family_key": strategy_spec.family,
                "execution_mode": strategy_spec.execution_mode,
                "parameter_values": parameter_values,
            }
            manifest_hash = _manifest_hash(manifest_payload)
            strategy_instances.append(
                BacktestStrategyInstance(
                    strategy_instance_id=f"sinst_{manifest_hash}",
                    strategy_template_id=strategy_template_id,
                    family_id=family_id,
                    family_key=strategy_spec.family,
                    strategy_version_label=strategy_spec.version,
                    execution_mode=strategy_spec.execution_mode,
                    parameter_values=dict(parameter_values),
                    manifest_hash=manifest_hash,
                )
            )

    if not strategy_instances:
        raise ValueError("ephemeral strategy space resolved to 0 strategy instances")

    strategy_space_id = "sspace_" + _stable_hash(
        "|".join(instance.strategy_instance_id for instance in strategy_instances)
    )
    return EphemeralStrategySpace(
        strategy_space_id=strategy_space_id,
        strategy_instances=tuple(strategy_instances),
    )


def run_backtest_batch(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    derived_indicator_output_dir: Path,
    output_dir: Path,
    request: BacktestBatchRequest,
    engine_config: BacktestEngineConfig | None = None,
    strategy_registry: StrategyRegistry | None = None,
    cache: ResearchFrameCache | None = None,
) -> dict[str, object]:
    registry = strategy_registry or build_strategy_registry()
    engine_config = engine_config or BacktestEngineConfig()
    dataset_manifest = load_materialized_research_dataset(
        output_dir=dataset_output_dir,
        dataset_version=request.dataset_version,
    )["dataset_manifest"]
    split_windows = _manifest_split_windows(dataset_manifest)
    series_frames, cache_id, cache_hit = load_backtest_frames(
        dataset_output_dir=dataset_output_dir,
        indicator_output_dir=indicator_output_dir,
        derived_indicator_output_dir=derived_indicator_output_dir,
        request=ResearchSliceRequest(
            dataset_version=request.dataset_version,
            indicator_set_version=request.indicator_set_version,
            derived_indicator_set_version=request.derived_indicator_set_version,
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
    total_combinations = len(request.strategy_instances)

    for instance_chunk in _chunked(list(request.strategy_instances), request.param_batch_size):
        if not instance_chunk:
            continue
        for series_chunk in _chunked(selected_series, request.series_batch_size):
            if not series_chunk:
                continue
            for series in series_chunk:
                for strategy_instance in instance_chunk:
                    strategy_spec = registry.get(strategy_instance.strategy_version_label)
                    result = run_backtest_series(
                        series=series,
                        strategy_spec=strategy_spec,
                        strategy_instance=strategy_instance,
                        config=engine_config,
                        backtest_batch_id=batch_id,
                        campaign_run_id=request.campaign_run_id,
                        strategy_space_id=request.strategy_space_id,
                        dataset_version=request.dataset_version,
                        indicator_set_version=request.indicator_set_version,
                        derived_indicator_set_version=request.derived_indicator_set_version,
                        split_windows=split_windows,
                    )
                    all_run_rows.extend(result["run_rows"])
                    all_stat_rows.extend(result["stat_rows"])
                    all_trade_rows.extend(result["trade_rows"])
                    all_order_rows.extend(result["order_rows"])
                    all_drawdown_rows.extend(result["drawdown_rows"])

    batch_row = {
        "backtest_batch_id": batch_id,
        "campaign_run_id": request.campaign_run_id,
        "strategy_space_id": request.strategy_space_id,
        "dataset_version": request.dataset_version,
        "indicator_set_version": request.indicator_set_version,
        "derived_indicator_set_version": request.derived_indicator_set_version,
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
        "delta_manifest": backtest_store_contract(),
        "output_paths": output_paths,
    }
