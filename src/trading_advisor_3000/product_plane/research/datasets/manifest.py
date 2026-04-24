from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Literal

from .continuous import ContinuousFrontPolicy


DatasetSeriesMode = Literal["contract", "continuous_front"]
DatasetSplitMethod = Literal["full", "holdout", "walk_forward"]

_DEFAULT_SOURCES = (
    "canonical_bars",
    "canonical_session_calendar",
    "canonical_roll_map",
)


@dataclass(frozen=True)
class ResearchDatasetManifest:
    dataset_version: str
    universe_id: str
    timeframes: tuple[str, ...]
    dataset_name: str | None = None
    source_table: str = "canonical_bars"
    base_timeframe: str | None = None
    start_ts: str | None = None
    end_ts: str | None = None
    series_mode: DatasetSeriesMode = "contract"
    split_method: DatasetSplitMethod = "holdout"
    warmup_bars: int = 200
    source_tables: tuple[str, ...] = _DEFAULT_SOURCES
    continuous_front_policy: ContinuousFrontPolicy | None = None
    split_params: dict[str, object] = field(default_factory=dict)
    bars_hash: str | None = None
    created_at: str | None = None
    code_version: str = "working-tree"
    notes: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.dataset_version.strip():
            raise ValueError("dataset_version must be non-empty")
        if not self.universe_id.strip():
            raise ValueError("universe_id must be non-empty")
        if not self.timeframes:
            raise ValueError("timeframes must not be empty")
        if self.warmup_bars < 0:
            raise ValueError("warmup_bars must be non-negative")
        if self.series_mode == "continuous_front" and self.continuous_front_policy is None:
            raise ValueError("continuous_front datasets require a ContinuousFrontPolicy")
        if not self.source_table.strip():
            raise ValueError("source_table must be non-empty")
        if not self.code_version.strip():
            raise ValueError("code_version must be non-empty")

    def lineage_key(self) -> str:
        parts = [
            self.dataset_version,
            self.universe_id,
            self.source_table,
            self.series_mode,
            self.split_method,
            str(self.warmup_bars),
            *self.timeframes,
            *self.source_tables,
        ]
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16].upper()

    def resolved_dataset_name(self) -> str:
        return self.dataset_name or self.dataset_version

    def resolved_base_timeframe(self) -> str:
        return self.base_timeframe or self.timeframes[0]

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "dataset_version": self.dataset_version,
            "dataset_name": self.resolved_dataset_name(),
            "source_table": self.source_table,
            "series_mode": self.series_mode,
            "universe_id": self.universe_id,
            "timeframes_json": list(self.timeframes),
            "base_timeframe": self.resolved_base_timeframe(),
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "split_method": self.split_method,
            "warmup_bars": self.warmup_bars,
            "split_params_json": self.split_params,
            "bars_hash": self.bars_hash,
            "created_at": self.created_at,
            "code_version": self.code_version,
            "notes_json": self.notes,
            "source_tables": list(self.source_tables),
            "lineage_key": self.lineage_key(),
        }
        if self.continuous_front_policy is not None:
            payload["continuous_front_policy"] = {
                "roll_source": self.continuous_front_policy.roll_source,
                "active_contract_field": self.continuous_front_policy.active_contract_field,
                "require_point_in_time_alignment": self.continuous_front_policy.require_point_in_time_alignment,
                "preserve_roll_gap_columns": self.continuous_front_policy.preserve_roll_gap_columns,
            }
        return payload

    def manifest_hash(self) -> str:
        normalized = json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()
