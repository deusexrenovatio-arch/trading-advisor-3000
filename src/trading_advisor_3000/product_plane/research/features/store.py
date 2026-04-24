from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows

from .snapshot import GOLD_FEATURE_NUMERIC_COLUMNS, GOLD_FEATURE_STRING_COLUMNS


def _gold_feature_typed_columns() -> dict[str, str]:
    return {
        **{column: "double" for column in GOLD_FEATURE_NUMERIC_COLUMNS},
        **{column: "string" for column in GOLD_FEATURE_STRING_COLUMNS},
    }


def _technical_indicator_columns() -> dict[str, str]:
    return {
        "indicator_snapshot_id": "string",
        "contract_id": "string",
        "instrument_id": "string",
        "timeframe": "string",
        "ts": "timestamp",
        "indicator_set_version": "string",
        "source_bar_fingerprint": "string",
        "close": "double",
        "volume": "double",
        "atr_14": "double",
        "natr_14": "double",
        "ema_12": "double",
        "ema_26": "double",
        "sma_20": "double",
        "sma_50": "double",
        "rsi_14": "double",
        "macd": "double",
        "macd_signal": "double",
        "macd_hist": "double",
        "bb_lower": "double",
        "bb_mid": "double",
        "bb_upper": "double",
        "bb_width": "double",
        "bb_percent_b": "double",
        "adx_14": "double",
        "dmp_14": "double",
        "dmn_14": "double",
        "stoch_k": "double",
        "stoch_d": "double",
        "cci_20": "double",
        "willr_14": "double",
        "mom_10": "double",
        "roc_10": "double",
        "obv": "double",
        "mfi_14": "double",
        "donchian_low_20": "double",
        "donchian_mid_20": "double",
        "donchian_high_20": "double",
        "supertrend": "double",
        "supertrend_direction": "double",
        "rvol_20": "double",
        "computed_at_utc": "timestamp",
        "indicator_values_json": "json",
    }


def _feature_frame_columns() -> dict[str, str]:
    return {
        "dataset_version": "string",
        "indicator_set_version": "string",
        "feature_set_version": "string",
        "profile_version": "string",
        "contract_id": "string",
        "instrument_id": "string",
        "timeframe": "string",
        "ts": "timestamp",
        "trend_state_fast_slow_code": "int",
        "trend_strength": "double",
        "ma_stack_state_code": "int",
        "regime_state_code": "int",
        "rolling_high_20": "double",
        "rolling_low_20": "double",
        "opening_range_high": "double",
        "opening_range_low": "double",
        "swing_high_10": "double",
        "swing_low_10": "double",
        "session_vwap": "double",
        "distance_to_session_vwap": "double",
        "distance_to_rolling_high_20": "double",
        "distance_to_rolling_low_20": "double",
        "bb_width_20_2": "double",
        "kc_width_20_1_5": "double",
        "squeeze_on_code": "int",
        "breakout_ready_state_code": "int",
        "breakout_ready_flag": "int",
        "rvol_20": "double",
        "volume_zscore_20": "double",
        "above_below_vwma_code": "int",
        "session_volume_state_code": "int",
        "reversion_ready_flag": "int",
        "atr_stop_ref_1x": "double",
        "atr_target_ref_2x": "double",
        "htf_ma_relation_code": "int",
        "htf_trend_state_code": "int",
        "htf_adx_14": "double",
        "htf_rsi_14": "double",
        "source_bars_hash": "string",
        "source_indicators_hash": "string",
        "row_count": "int",
        "warmup_span": "int",
        "null_warmup_span": "int",
        "created_at": "timestamp",
    }


@dataclass(frozen=True)
class FeatureFramePartitionKey:
    dataset_version: str
    indicator_set_version: str
    feature_set_version: str
    timeframe: str
    instrument_id: str
    contract_id: str | None = None

    def partition_path(self) -> str:
        contract_token = self.contract_id or "continuous-front"
        return (
            f"dataset_version={self.dataset_version}/"
            f"indicator_set_version={self.indicator_set_version}/"
            f"feature_set_version={self.feature_set_version}/"
            f"instrument_id={self.instrument_id}/"
            f"contract_id={contract_token}/"
            f"timeframe={self.timeframe}"
        )

    def matches_row(self, row: dict[str, object]) -> bool:
        if str(row.get("dataset_version")) != self.dataset_version:
            return False
        if str(row.get("indicator_set_version")) != self.indicator_set_version:
            return False
        if str(row.get("feature_set_version")) != self.feature_set_version:
            return False
        if str(row.get("instrument_id")) != self.instrument_id:
            return False
        if str(row.get("timeframe")) != self.timeframe:
            return False
        if self.contract_id is None:
            return True
        return str(row.get("contract_id")) == self.contract_id


@dataclass(frozen=True)
class FeatureFrameRow:
    dataset_version: str
    indicator_set_version: str
    feature_set_version: str
    profile_version: str
    contract_id: str
    instrument_id: str
    timeframe: str
    ts: str
    values: dict[str, float | int | None]
    source_bars_hash: str
    source_indicators_hash: str
    row_count: int
    warmup_span: int
    null_warmup_span: int
    created_at: str

    def partition_key(self, *, series_mode: str) -> FeatureFramePartitionKey:
        return FeatureFramePartitionKey(
            dataset_version=self.dataset_version,
            indicator_set_version=self.indicator_set_version,
            feature_set_version=self.feature_set_version,
            timeframe=self.timeframe,
            instrument_id=self.instrument_id,
            contract_id=None if series_mode == "continuous_front" else self.contract_id,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset_version": self.dataset_version,
            "indicator_set_version": self.indicator_set_version,
            "feature_set_version": self.feature_set_version,
            "profile_version": self.profile_version,
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "ts": self.ts,
            **self.values,
            "source_bars_hash": self.source_bars_hash,
            "source_indicators_hash": self.source_indicators_hash,
            "row_count": self.row_count,
            "warmup_span": self.warmup_span,
            "null_warmup_span": self.null_warmup_span,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "FeatureFrameRow":
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
        value_types = _feature_frame_columns()
        values: dict[str, float | int | None] = {}
        for key, value in payload.items():
            if key in reserved:
                continue
            if value is None:
                values[key] = None
                continue
            if value_types.get(key) == "int":
                values[key] = int(value)
                continue
            values[key] = float(value)
        return cls(
            dataset_version=str(payload["dataset_version"]),
            indicator_set_version=str(payload["indicator_set_version"]),
            feature_set_version=str(payload["feature_set_version"]),
            profile_version=str(payload["profile_version"]),
            contract_id=str(payload["contract_id"]),
            instrument_id=str(payload["instrument_id"]),
            timeframe=str(payload["timeframe"]),
            ts=str(payload["ts"]),
            values=values,
            source_bars_hash=str(payload["source_bars_hash"]),
            source_indicators_hash=str(payload["source_indicators_hash"]),
            row_count=int(payload["row_count"]),
            warmup_span=int(payload["warmup_span"]),
            null_warmup_span=int(payload["null_warmup_span"]),
            created_at=str(payload["created_at"]),
        )


def research_feature_store_contract() -> dict[str, dict[str, object]]:
    gold_feature_columns = {
        "snapshot_id": "string",
        "contract_id": "string",
        "instrument_id": "string",
        "timeframe": "string",
        "ts": "timestamp",
        "feature_set_version": "string",
        "regime": "string",
        "atr": "double",
        "ema_fast": "double",
        "ema_slow": "double",
        "donchian_high": "double",
        "donchian_low": "double",
        "rvol": "double",
        **_gold_feature_typed_columns(),
        "features_json": "json",
    }
    return {
        "technical_indicator_snapshot": {
            "format": "delta",
            "partition_by": ["contract_id", "timeframe", "indicator_set_version"],
            "constraints": ["unique(contract_id, timeframe, ts, indicator_set_version)"],
            "columns": _technical_indicator_columns(),
        },
        "research_feature_frames": {
            "format": "delta",
            "partition_by": [
                "dataset_version",
                "indicator_set_version",
                "feature_set_version",
                "instrument_id",
                "timeframe",
            ],
            "constraints": [
                "unique(dataset_version, indicator_set_version, feature_set_version, contract_id, timeframe, ts)"
            ],
            "columns": _feature_frame_columns(),
        },
        "feature_snapshots": {
            "format": "delta",
            "partition_by": ["contract_id", "timeframe", "feature_set_version"],
            "constraints": ["unique(contract_id, timeframe, ts, feature_set_version)"],
            "columns": dict(gold_feature_columns),
        },
        "gold_feature_snapshot": {
            "format": "delta",
            "partition_by": ["contract_id", "timeframe", "feature_set_version"],
            "constraints": ["unique(contract_id, timeframe, ts, feature_set_version)"],
            "columns": dict(gold_feature_columns),
        },
        "research_backtest_runs": {
            "format": "delta",
            "partition_by": ["strategy_version_id", "dataset_version"],
            "columns": {
                "backtest_run_id": "string",
                "strategy_version_id": "string",
                "dataset_version": "string",
                "started_at": "timestamp",
                "finished_at": "timestamp",
                "status": "string",
                "params_hash": "string",
                "candidate_count": "bigint",
                "walk_forward_windows": "int",
                "commission_total": "double",
                "slippage_total": "double",
                "session_filtered_out": "bigint",
            },
        },
        "research_runtime_candidate_projection": {
            "format": "delta",
            "partition_by": ["strategy_version_id", "contract_id", "timeframe"],
            "columns": {
                "candidate_projection_id": "string",
                "candidate_id": "string",
                "backtest_run_id": "string",
                "strategy_version_id": "string",
                "strategy_family": "string",
                "contract_id": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "ts_signal": "timestamp",
                "side": "string",
                "entry_ref": "double",
                "stop_ref": "double",
                "target_ref": "double",
                "score": "double",
                "window_id": "string",
                "estimated_commission": "double",
                "estimated_slippage": "double",
                "capital_rub": "bigint",
                "reproducibility_fingerprint": "string",
            },
        },
        "research_signal_candidates": {
            "format": "delta",
            "alias_of": "research_runtime_candidate_projection",
            "partition_by": ["strategy_version_id", "contract_id", "timeframe"],
            "columns": {
                "candidate_id": "string",
                "backtest_run_id": "string",
                "strategy_version_id": "string",
                "contract_id": "string",
                "timeframe": "string",
                "ts_signal": "timestamp",
                "side": "string",
                "entry_ref": "double",
                "stop_ref": "double",
                "target_ref": "double",
                "score": "double",
                "window_id": "string",
                "estimated_commission": "double",
                "estimated_slippage": "double",
            },
        },
        "research_strategy_metrics": {
            "format": "delta",
            "partition_by": ["walk_forward_windows"],
            "columns": {
                "walk_forward_windows": "int",
                "session_hours_utc": "array<int>",
                "session_filtered_out": "bigint",
                "candidate_count": "bigint",
                "long_count": "bigint",
                "short_count": "bigint",
                "avg_score": "double",
                "avg_risk_reward": "double",
                "commission_total": "double",
                "slippage_total": "double",
            },
        },
        "strategy_scorecard": {
            "format": "delta",
            "partition_by": ["strategy_version_id", "profile_version"],
            "columns": {
                "scorecard_id": "string",
                "strategy_version_id": "string",
                "strategy_family": "string",
                "profile_version": "string",
                "capital_rub": "bigint",
                "annual_return_pct": "double",
                "sharpe_ratio": "double",
                "max_drawdown_pct": "double",
                "negative_months_4y": "int",
                "signals_per_week": "double",
                "active_assets_count": "int",
                "repeatable": "string",
                "criteria_json": "json",
                "verdict": "string",
                "blocked_reasons_json": "json",
                "generated_at": "timestamp",
            },
        },
        "strategy_promotion_decision": {
            "format": "delta",
            "partition_by": ["strategy_version_id", "profile_version"],
            "columns": {
                "decision_id": "string",
                "scorecard_id": "string",
                "strategy_version_id": "string",
                "profile_version": "string",
                "verdict": "string",
                "promotion_state": "string",
                "blocked_reasons_json": "json",
                "effective_from": "timestamp",
                "capital_rub": "bigint",
                "reproducibility_fingerprint": "string",
            },
        },
        "promoted_strategy_registry": {
            "format": "delta",
            "partition_by": ["strategy_version_id", "profile_version"],
            "columns": {
                "strategy_version_id": "string",
                "strategy_family": "string",
                "profile_version": "string",
                "capital_rub": "bigint",
                "promoted_at": "timestamp",
                "reproducibility_fingerprint": "string",
            },
        },
    }


def write_feature_frames(
    *,
    output_dir: Path,
    rows: list[FeatureFrameRow],
    replace_partitions: tuple[FeatureFramePartitionKey, ...],
) -> dict[str, str]:
    contract = research_feature_store_contract()
    path = output_dir / "research_feature_frames.delta"
    existing_rows = read_delta_table_rows(path) if (path / "_delta_log").exists() else []
    preserved_rows = [
        row
        for row in existing_rows
        if not any(partition.matches_row(row) for partition in replace_partitions)
    ]
    write_delta_table_rows(
        table_path=path,
        rows=[*preserved_rows, *[row.to_dict() for row in rows]],
        columns=contract["research_feature_frames"]["columns"],
    )
    return {"research_feature_frames": path.as_posix()}


def load_feature_frames(
    *,
    output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    feature_set_version: str,
) -> list[FeatureFrameRow]:
    path = output_dir / "research_feature_frames.delta"
    rows = read_delta_table_rows(path)
    return [
        FeatureFrameRow.from_dict(row)
        for row in rows
        if row.get("dataset_version") == dataset_version
        and row.get("indicator_set_version") == indicator_set_version
        and row.get("feature_set_version") == feature_set_version
    ]
