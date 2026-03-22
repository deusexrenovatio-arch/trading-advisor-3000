from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .config import StrategyRegistry, require_phase9_battle_run_config
from .context import ContextProviderRegistry
from .decision import SignalRuntimeEngine
from .publishing import TelegramBotTransport, TelegramBotTransportConfig, TelegramPublicationEngine
from .signal_store import InMemorySignalStore, PostgresSignalStore, SignalStore


@dataclass(frozen=True)
class RuntimeStack:
    strategy_registry: StrategyRegistry
    context_provider_registry: ContextProviderRegistry
    signal_store: SignalStore
    publisher: TelegramPublicationEngine
    runtime_engine: SignalRuntimeEngine


def build_runtime_stack(*, telegram_channel: str, signal_store: SignalStore | None = None) -> RuntimeStack:
    return build_runtime_stack_with_policies(telegram_channel=telegram_channel, signal_store=signal_store)


def build_runtime_stack_with_policies(
    *,
    telegram_channel: str,
    validity_minutes_by_timeframe: dict[str, int] | None = None,
    cooldown_seconds: int = 0,
    blackout_windows_by_contract: dict[str, list[tuple[str, str]]] | None = None,
    signal_store: SignalStore | None = None,
    publisher_transport: TelegramBotTransport | None = None,
) -> RuntimeStack:
    registry = StrategyRegistry()
    context_registry = ContextProviderRegistry()
    store = signal_store or InMemorySignalStore()
    publisher = TelegramPublicationEngine(channel=telegram_channel, transport=publisher_transport)
    publisher.restore(publications=store.list_publication_events())
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


def build_phase9_battle_run_stack(
    *,
    env: Mapping[str, str] | None = None,
    validity_minutes_by_timeframe: dict[str, int] | None = None,
    cooldown_seconds: int = 0,
    blackout_windows_by_contract: dict[str, list[tuple[str, str]]] | None = None,
    telegram_channel_override: str | None = None,
) -> RuntimeStack:
    config = require_phase9_battle_run_config(env)
    source = dict(env or {})
    publisher_transport = None
    if config.telegram_transport == "bot-api":
        publisher_transport = TelegramBotTransport(
            config=TelegramBotTransportConfig(
                bot_token=str(source.get("TA3000_TELEGRAM_BOT_TOKEN", "")).strip(),
                api_base_url=str(
                    source.get("TA3000_TELEGRAM_API_BASE_URL", "https://api.telegram.org")
                ).strip()
                or "https://api.telegram.org",
            )
        )
    return build_runtime_stack_with_policies(
        telegram_channel=telegram_channel_override or config.telegram_shadow_channel,
        validity_minutes_by_timeframe=validity_minutes_by_timeframe,
        cooldown_seconds=cooldown_seconds,
        blackout_windows_by_contract=blackout_windows_by_contract,
        signal_store=PostgresSignalStore(
            dsn=config.app_dsn,
            schema_name=config.signal_store_schema,
        ),
        publisher_transport=publisher_transport,
    )
