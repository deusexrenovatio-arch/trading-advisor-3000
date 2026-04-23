from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResearchDatasetPartitionKey:
    dataset_version: str
    timeframe: str
    instrument_id: str
    contract_id: str | None = None

    def partition_path(self) -> str:
        contract_token = self.contract_id or "continuous-front"
        return (
            f"dataset_version={self.dataset_version}/"
            f"instrument_id={self.instrument_id}/"
            f"contract_id={contract_token}/"
            f"timeframe={self.timeframe}"
        )

