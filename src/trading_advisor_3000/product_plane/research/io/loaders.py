from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResearchSliceRequest:
    dataset_version: str
    timeframe: str
    contract_ids: tuple[str, ...] = ()
    instrument_ids: tuple[str, ...] = ()
    warmup_bars: int = 0

