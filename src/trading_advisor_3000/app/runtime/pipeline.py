from __future__ import annotations

from dataclasses import dataclass

from .config import StrategyRegistry
from .decision import SignalRuntimeEngine
from .publishing import TelegramPublicationEngine
from .signal_store import InMemorySignalStore


@dataclass(frozen=True)
class RuntimeStack:
    strategy_registry: StrategyRegistry
    signal_store: InMemorySignalStore
    publisher: TelegramPublicationEngine
    runtime_engine: SignalRuntimeEngine


def build_runtime_stack(*, telegram_channel: str) -> RuntimeStack:
    registry = StrategyRegistry()
    store = InMemorySignalStore()
    publisher = TelegramPublicationEngine(channel=telegram_channel)
    engine = SignalRuntimeEngine(
        strategy_registry=registry,
        signal_store=store,
        publisher=publisher,
    )
    return RuntimeStack(
        strategy_registry=registry,
        signal_store=store,
        publisher=publisher,
        runtime_engine=engine,
    )
