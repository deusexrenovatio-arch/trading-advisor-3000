from __future__ import annotations

import json
from datetime import date

import pytest

from trading_advisor_3000.product_plane.data_plane.moex import quote_pilot as pilot
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import HistorySecurity


class Source:
    """Small source double; expected identities and UTC values are independent."""

    def fetch_security_snapshot(self, **scope):
        return {"SECID": scope["secid"], "BOARDID": scope["board"]}

    def fetch_history_board_securities(self, **scope):
        return [
            HistorySecurity(
                boardid="RFUD",
                tradedate=scope["trade_date"].isoformat(),
                secid="SRU6",
                shortname="SBRF-9.26",
                assetcode="SBRF",
                volume=1,
                numtrades=1,
                payload={"SECID": "SRU6", "ASSETCODE": "SBRF", "BOARDID": "RFUD"},
            )
        ]

    def fetch_futures_contract_securities(self):
        return [{"SECID": "SRZ6", "ASSETCODE": "SBRF", "BOARDID": "RFUD"}]

    def iter_candle_source_rows(self, **scope):
        return iter(
            [
                {
                    "open": 100,
                    "high": 103,
                    "low": 99,
                    "close": 101,
                    "volume": 0 if scope["market"] == "index" else 2.5,
                    "value": 205,
                    "begin": "2026-09-01 00:00:00",
                    "end": "2026-09-01 23:59:59",
                }
            ]
        )


@pytest.fixture
def namespace(tmp_path, monkeypatch):
    monkeypatch.setattr(pilot, "PILOT_ROOT", tmp_path / "pilot")
    return pilot.PILOT_ROOT


def test_acquisition_retains_expired_chain_raw_index_values_and_immutable_evidence(namespace):
    root = namespace / "source"
    metadata = pilot.acquire_quote_pilot(
        acquisition_dir=root,
        date_from=date(2026, 9, 1),
        date_till=date(2026, 9, 2),
        run_id="source-1",
        client=Source(),
    )
    assert metadata["source_row_count"] == 5
    series = [json.loads(line) for line in (root / "series.jsonl").read_text("utf-8").splitlines()]
    assert {row["secid"] for row in series} == {"SBER", "SRU6", "SRZ6", "IMOEX", "IMOEX2"}
    futures = [row for row in series if row["asset_class"] == "futures"]
    assert all(row["underlying_security_id"] == "MOEX:stock:shares:SBER" for row in futures)
    records = [
        json.loads(line) for line in (root / "candles.jsonl").read_text("utf-8").splitlines()
    ]
    index = next(row for row in records if ":IMOEX:SNDX:" in row["series_id"])
    assert json.loads(index["payload_json"])["volume"] == 0
    assert json.loads(index["payload_json"])["value"] == 205
    assert ":IMOEX2:SNDX:" in next(
        row["series_id"] for row in records if ":IMOEX2:" in row["series_id"]
    )
    original = (root / "acquisition.json").read_bytes()
    with pytest.raises(ValueError, match="immutable"):
        pilot.acquire_quote_pilot(
            acquisition_dir=root,
            date_from=date(2026, 9, 1),
            date_till=date(2026, 9, 2),
            run_id="source-1",
            client=Source(),
        )
    assert (root / "acquisition.json").read_bytes() == original
    (root / "candles.jsonl").write_text("tampered", "utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        pilot.load_acquisition(root)


def test_pilot_rejects_escape_current_day_and_unsafe_run_id(namespace):
    with pytest.raises(ValueError, match="inside candidate"):
        pilot.validate_pilot_path(namespace.parent / "production")
    with pytest.raises(ValueError, match="identifier"):
        pilot.acquire_quote_pilot(
            acquisition_dir=namespace / "unsafe",
            date_from=date(2026, 9, 1),
            date_till=date(2026, 9, 2),
            run_id="../current",
            client=Source(),
        )
    today = pilot.datetime.now(pilot.ZoneInfo("Europe/Moscow")).date()
    with pytest.raises(ValueError, match="completed"):
        pilot.acquire_quote_pilot(
            acquisition_dir=namespace / "today",
            date_from=today,
            date_till=today,
            run_id="current-day",
            client=Source(),
        )
    assert not namespace.exists()


def test_pilot_rejects_symlink_escape(namespace):
    namespace.mkdir()
    target = namespace.parent / "outside"
    target.mkdir()
    link = namespace / "escaped"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks unavailable on this host; Docker proof executes this case")
    with pytest.raises(ValueError, match="inside candidate"):
        pilot.validate_pilot_path(link / "quotes")


def test_acquisition_rejects_missing_futures_metadata_identity_before_writes(namespace):
    class BrokenSource(Source):
        def fetch_futures_contract_securities(self):
            return [{"SECID": None, "ASSETCODE": "SBRF", "BOARDID": "RFUD"}]

    with pytest.raises(ValueError, match="SECID"):
        pilot.acquire_quote_pilot(
            acquisition_dir=namespace / "broken",
            date_from=date(2026, 9, 1),
            date_till=date(2026, 9, 2),
            run_id="broken-source",
            client=BrokenSource(),
        )
    assert not namespace.exists()
