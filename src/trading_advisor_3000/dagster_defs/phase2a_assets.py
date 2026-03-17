from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AssetSpec:
    key: str
    description: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


def phase2a_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="raw_market_backfill",
            description="Ingest raw backfill rows for whitelist contracts.",
            inputs=("raw_backfill_source",),
            outputs=("raw_market_backfill_delta",),
        ),
        AssetSpec(
            key="canonical_bars",
            description="Build canonical OHLCV bars from raw backfill.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_bars_delta",),
        ),
        AssetSpec(
            key="canonical_instruments",
            description="Build canonical instruments table from raw backfill.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_instruments_delta",),
        ),
        AssetSpec(
            key="canonical_contracts",
            description="Build canonical contracts table from raw backfill.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_contracts_delta",),
        ),
        AssetSpec(
            key="canonical_session_calendar",
            description="Build canonical session calendar from raw backfill.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_session_calendar_delta",),
        ),
        AssetSpec(
            key="canonical_roll_map",
            description="Build canonical roll map by instrument/session.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_roll_map_delta",),
        ),
    ]
