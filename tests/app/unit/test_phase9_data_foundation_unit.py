from __future__ import annotations

from trading_advisor_3000.app.data_plane import (
    Phase9LiveFeedObservation,
    build_phase9_dataset_version,
    default_phase9_pilot_universe,
    default_phase9_provider_contracts,
    evaluate_phase9_live_smoke,
    phase9_data_provider_registry,
)


def test_phase9_provider_contracts_freeze_moex_history_and_quik_live() -> None:
    contracts = default_phase9_provider_contracts()
    registry = phase9_data_provider_registry()

    assert set(contracts) == {"moex-history", "quik-live"}
    assert contracts["moex-history"].external_system == "MOEX"
    assert contracts["moex-history"].role == "historical_source"
    assert contracts["quik-live"].external_system == "QUIK"
    assert contracts["quik-live"].role == "live_feed"
    assert contracts["quik-live"].freshness_window_seconds == 90
    assert [item.provider_id for item in registry.list_providers(provider_kind="market")] == [
        "moex-history",
        "quik-live",
    ]


def test_phase9_dataset_version_is_deterministic_for_watermarks() -> None:
    pilot_universe = default_phase9_pilot_universe()
    watermark_by_key = {
        "BR-6.26|15m": "2026-03-16T10:15:00Z",
        "Si-6.26|15m": "2026-03-16T10:15:00Z",
    }

    first = build_phase9_dataset_version(
        provider_id="moex-history",
        pilot_universe=pilot_universe,
        watermark_by_key=watermark_by_key,
    )
    second = build_phase9_dataset_version(
        provider_id="moex-history",
        pilot_universe=pilot_universe,
        watermark_by_key=dict(watermark_by_key),
    )

    assert first == second
    assert first.startswith("phase9-moex-futures-pilot-moex-history-20260316T101500Z-")


def test_phase9_live_smoke_is_ok_when_quik_snapshot_is_fresh_and_complete() -> None:
    report = evaluate_phase9_live_smoke(
        provider_id="quik-live",
        snapshot_rows=[
            Phase9LiveFeedObservation(
                contract_id="BR-6.26",
                event_ts="2026-03-20T07:00:10Z",
                session_state="open",
                last_price=82.6,
            ),
            Phase9LiveFeedObservation(
                contract_id="Si-6.26",
                event_ts="2026-03-20T07:00:15Z",
                session_state="open",
                last_price=104330.0,
            ),
        ],
        as_of_ts="2026-03-20T07:01:00Z",
        max_lag_seconds=60,
    )

    assert report["status"] == "ok"
    assert report["missing_contract_ids"] == []
    assert report["stale_contract_ids"] == []
    assert report["invalid_session_contract_ids"] == []
    assert {row["contract_id"] for row in report["rows"]} == {"BR-6.26", "Si-6.26"}


def test_phase9_live_smoke_is_degraded_when_snapshot_is_stale_or_incomplete() -> None:
    report = evaluate_phase9_live_smoke(
        provider_id="quik-live",
        snapshot_rows=[
            Phase9LiveFeedObservation(
                contract_id="BR-6.26",
                event_ts="2026-03-20T06:55:00Z",
                session_state="closed",
                last_price=82.6,
            ),
        ],
        as_of_ts="2026-03-20T07:01:00Z",
        max_lag_seconds=60,
    )

    assert report["status"] == "degraded"
    assert report["missing_contract_ids"] == ["Si-6.26"]
    assert report["stale_contract_ids"] == ["BR-6.26"]
    assert report["invalid_session_contract_ids"] == ["BR-6.26"]
