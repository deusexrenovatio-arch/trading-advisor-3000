from __future__ import annotations

from dataclasses import dataclass

from trading_advisor_3000.product_plane.research.datasets import ResearchDatasetPartitionKey


@dataclass(frozen=True)
class IndicatorMaterializationRequest:
    dataset_version: str
    indicator_set_version: str
    partition: ResearchDatasetPartitionKey
    profile_version: str = "core_v1"
    point_in_time_safe: bool = True
    affected_partitions: tuple[str, ...] = ()
