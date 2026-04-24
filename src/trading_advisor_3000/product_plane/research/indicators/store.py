from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows


@dataclass(frozen=True)
class IndicatorFramePartitionKey:
    dataset_version: str
    indicator_set_version: str
    timeframe: str
    instrument_id: str
    contract_id: str | None = None

    def partition_path(self) -> str:
        contract_token = self.contract_id or "continuous-front"
        return (
            f"dataset_version={self.dataset_version}/"
            f"indicator_set_version={self.indicator_set_version}/"
            f"instrument_id={self.instrument_id}/"
            f"contract_id={contract_token}/"
            f"timeframe={self.timeframe}"
        )

    def matches_row(self, row: dict[str, object]) -> bool:
        if str(row.get("dataset_version")) != self.dataset_version:
            return False
        if str(row.get("indicator_set_version")) != self.indicator_set_version:
            return False
        if str(row.get("instrument_id")) != self.instrument_id:
            return False
        if str(row.get("timeframe")) != self.timeframe:
            return False
        if self.contract_id is None:
            return True
        return str(row.get("contract_id")) == self.contract_id


@dataclass(frozen=True)
class IndicatorFrameRow:
    dataset_version: str
    indicator_set_version: str
    profile_version: str
    contract_id: str
    instrument_id: str
    timeframe: str
    ts: str
    values: dict[str, float | int | None]
    source_bars_hash: str
    row_count: int
    warmup_span: int
    null_warmup_span: int
    created_at: str

    def partition_key(self, *, series_mode: str) -> IndicatorFramePartitionKey:
        return IndicatorFramePartitionKey(
            dataset_version=self.dataset_version,
            indicator_set_version=self.indicator_set_version,
            timeframe=self.timeframe,
            instrument_id=self.instrument_id,
            contract_id=None if series_mode == "continuous_front" else self.contract_id,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset_version": self.dataset_version,
            "indicator_set_version": self.indicator_set_version,
            "profile_version": self.profile_version,
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "ts": self.ts,
            **self.values,
            "source_bars_hash": self.source_bars_hash,
            "row_count": self.row_count,
            "warmup_span": self.warmup_span,
            "null_warmup_span": self.null_warmup_span,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "IndicatorFrameRow":
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
        values = {
            key: (None if payload[key] is None else float(payload[key]))
            for key in payload
            if key not in reserved
        }
        return cls(
            dataset_version=str(payload["dataset_version"]),
            indicator_set_version=str(payload["indicator_set_version"]),
            profile_version=str(payload["profile_version"]),
            contract_id=str(payload["contract_id"]),
            instrument_id=str(payload["instrument_id"]),
            timeframe=str(payload["timeframe"]),
            ts=str(payload["ts"]),
            values=values,
            source_bars_hash=str(payload["source_bars_hash"]),
            row_count=int(payload["row_count"]),
            warmup_span=int(payload["warmup_span"]),
            null_warmup_span=int(payload["null_warmup_span"]),
            created_at=str(payload["created_at"]),
        )


def indicator_store_contract() -> dict[str, dict[str, object]]:
    columns = {
        "dataset_version": "string",
        "indicator_set_version": "string",
        "profile_version": "string",
        "contract_id": "string",
        "instrument_id": "string",
        "timeframe": "string",
        "ts": "timestamp",
        "sma_10": "double",
        "sma_20": "double",
        "sma_50": "double",
        "ema_10": "double",
        "ema_20": "double",
        "ema_50": "double",
        "hma_20": "double",
        "atr_14": "double",
        "natr_14": "double",
        "rsi_14": "double",
        "stoch_k_14_3_3": "double",
        "stoch_d_14_3_3": "double",
        "macd_12_26_9": "double",
        "macd_signal_12_26_9": "double",
        "macd_hist_12_26_9": "double",
        "adx_14": "double",
        "dmp_14": "double",
        "dmn_14": "double",
        "aroon_up_25": "double",
        "aroon_down_25": "double",
        "donchian_high_20": "double",
        "donchian_low_20": "double",
        "bb_upper_20_2": "double",
        "bb_mid_20_2": "double",
        "bb_lower_20_2": "double",
        "bb_width_20_2": "double",
        "kc_upper_20_1_5": "double",
        "kc_mid_20_1_5": "double",
        "kc_lower_20_1_5": "double",
        "obv": "double",
        "mfi_14": "double",
        "cmf_20": "double",
        "vwma_20": "double",
        "roc_10": "double",
        "ppo_12_26_9": "double",
        "cci_20": "double",
        "willr_14": "double",
        "tsi_25_13": "double",
        "rvol_20": "double",
        "volume_z_20": "double",
        "source_bars_hash": "string",
        "row_count": "int",
        "warmup_span": "int",
        "null_warmup_span": "int",
        "created_at": "timestamp",
    }
    return {
        "research_indicator_frames": {
            "format": "delta",
            "partition_by": ["dataset_version", "indicator_set_version", "instrument_id", "timeframe"],
            "constraints": ["unique(dataset_version, indicator_set_version, contract_id, timeframe, ts)"],
            "columns": columns,
        }
    }


def write_indicator_frames(
    *,
    output_dir: Path,
    rows: list[IndicatorFrameRow],
    replace_partitions: tuple[IndicatorFramePartitionKey, ...],
) -> dict[str, str]:
    contract = indicator_store_contract()
    path = output_dir / "research_indicator_frames.delta"
    existing_rows = read_delta_table_rows(path) if (path / "_delta_log").exists() else []
    preserved_rows = [
        row
        for row in existing_rows
        if not any(partition.matches_row(row) for partition in replace_partitions)
    ]
    write_delta_table_rows(
        table_path=path,
        rows=[*preserved_rows, *[row.to_dict() for row in rows]],
        columns=contract["research_indicator_frames"]["columns"],
    )
    return {"research_indicator_frames": path.as_posix()}


def load_indicator_frames(*, output_dir: Path, dataset_version: str, indicator_set_version: str) -> list[IndicatorFrameRow]:
    path = output_dir / "research_indicator_frames.delta"
    rows = read_delta_table_rows(path)
    return [
        IndicatorFrameRow.from_dict(row)
        for row in rows
        if row.get("dataset_version") == dataset_version and row.get("indicator_set_version") == indicator_set_version
    ]
