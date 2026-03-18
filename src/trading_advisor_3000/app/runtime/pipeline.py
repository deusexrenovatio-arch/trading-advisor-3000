from __future__ import annotations

from dataclasses import dataclass

from .config import StrategyRegistry
from .context import ContextProviderRegistry
from .decision import SignalRuntimeEngine
from .publishing import TelegramPublicationEngine
from .signal_store import InMemorySignalStore


@dataclass(frozen=True)
class RuntimeStack:
    strategy_registry: StrategyRegistry
    context_provider_registry: ContextProviderRegistry
    signal_store: InMemorySignalStore
    publisher: TelegramPublicationEngine
    runtime_engine: SignalRuntimeEngine


def build_runtime_stack(*, telegram_channel: str) -> RuntimeStack:
    return build_runtime_stack_with_policies(telegram_channel=telegram_channel)


def build_runtime_stack_with_policies(
    *,
    telegram_channel: str,
    validity_minutes_by_timeframe: dict[str, int] | None = None,
    cooldown_seconds: int = 0,
    blackout_windows_by_contract: dict[str, list[tuple[str, str]]] | None = None,
) -> RuntimeStack:
    registry = StrategyRegistry()
    context_registry = ContextProviderRegistry()
    store = InMemorySignalStore()
    publisher = TelegramPublicationEngine(channel=telegram_channel)
    engine = SignalRuntimeEngine(
        strategy_registry=registry,
        context_provider_registry=context_registry,
        signal_store=store,
        publisher=publisher,
        validity_minutes_by_timeframe=validity_minutes_by_timeframe,
        cooldown_seconds=cooldown_seconds,
        blackout_windows_by_contract=blackout_windows_by_contract,
    )
    return RuntimeStack(
        strategy_registry=registry,
        context_provider_registry=context_registry,
        signal_store=store,
        publisher=publisher,
        runtime_engine=engine,
    )
