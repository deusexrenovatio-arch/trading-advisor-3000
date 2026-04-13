from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class BacktestBatchRequest:
    dataset_version: str
    indicator_set_version: str
    feature_set_version: str
    strategy_versions: tuple[str, ...]
    combination_count: int

    def __post_init__(self) -> None:
        if not self.strategy_versions:
            raise ValueError("strategy_versions must not be empty")
        if self.combination_count <= 0:
            raise ValueError("combination_count must be positive")

    def batch_id(self) -> str:
        payload = "|".join(
            (
                self.dataset_version,
                self.indicator_set_version,
                self.feature_set_version,
                *self.strategy_versions,
                str(self.combination_count),
            )
        )
        return "BTBATCH-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12].upper()

