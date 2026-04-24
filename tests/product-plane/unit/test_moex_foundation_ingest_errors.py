from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.foundation import DiscoveryRecord, ingest_moex_bootstrap_window


class _FailingClient:
    def iter_candles(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        secid: str,
        interval: int,
        date_from,
        date_till,
    ):
        del engine, market, board, secid, interval, date_from, date_till
        raise TimeoutError("simulated moex timeout")
        yield  # pragma: no cover


def test_ingest_writes_item_level_error_artifact(tmp_path: Path) -> None:
    coverage = [
        DiscoveryRecord(
            internal_id="FUT_BR",
            finam_symbol="BRQ6",
            moex_engine="futures",
            moex_market="forts",
            moex_board="RFUD",
            moex_secid="BRQ6",
            asset_group="commodity",
            requested_target_timeframes="5m,15m",
            source_interval=1,
            source_timeframe="1m",
            coverage_begin_utc="2026-04-01T00:00:00Z",
            coverage_end_utc="2026-04-02T00:00:00Z",
            discovered_at_utc="2026-04-02T00:00:00Z",
            discovery_url="https://iss.moex.com/example",
        )
    ]
    error_latest = tmp_path / "raw-ingest-error.latest.json"

    with pytest.raises(TimeoutError, match="simulated moex timeout"):
        ingest_moex_bootstrap_window(
            client=_FailingClient(),
            coverage=coverage,
            table_path=tmp_path / "delta" / "raw_moex_history.delta",
            run_id="raw-ingest-error",
            ingest_till_utc="2026-04-02T00:00:00Z",
            bootstrap_window_days=5,
            stability_lag_minutes=0,
            refresh_overlap_minutes=0,
            error_latest_path=error_latest,
        )

    payload = json.loads(error_latest.read_text(encoding="utf-8"))
    assert payload["internal_id"] == "FUT_BR"
    assert payload["moex_secid"] == "BRQ6"
    assert payload["source_timeframe"] == "1m"
    assert payload["error_type"] == "TimeoutError"
    assert "simulated moex timeout" in payload["error"]
