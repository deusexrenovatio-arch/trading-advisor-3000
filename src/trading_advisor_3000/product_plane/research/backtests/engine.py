from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BacktestEngineConfig:
    engine_name: str = "vectorbt"
    order_execution_mode: str = "from_signals"
    fees_bps: float = 0.0
    slippage_bps: float = 0.0

    def __post_init__(self) -> None:
        if self.fees_bps < 0:
            raise ValueError("fees_bps must be non-negative")
        if self.slippage_bps < 0:
            raise ValueError("slippage_bps must be non-negative")

