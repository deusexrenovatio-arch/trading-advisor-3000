from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RankingPolicy:
    policy_id: str
    metric_order: tuple[str, ...]
    require_out_of_sample_pass: bool = True

    def __post_init__(self) -> None:
        if not self.metric_order:
            raise ValueError("metric_order must not be empty")

