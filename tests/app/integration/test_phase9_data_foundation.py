from __future__ import annotations

import json
import subprocess
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator


ROOT = Path(__file__).resolve().parents[3]
BACKFILL_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"
FRESH_SNAPSHOT_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "quik_live_snapshot_fresh.json"
STALE_SNAPSHOT_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "quik_live_snapshot_stale.json"


@contextmanager
def _phase9_data_server() -> Iterator[str]:
    candle_columns = ["open", "close", "high", "low", "value", "volume", "begin", "end"]
    history_columns = [
        "BOARDID",
        "TRADEDATE",
        "SECID",
        "OPEN",
        "LOW",
        "HIGH",
        "CLOSE",
        "OPENPOSITIONVALUE",
        "VALUE",
        "VOLUME",
        "OPENPOSITION",
        "SETTLEPRICE",
        "SWAPRATE",
        "WAPRICE",
        "SETTLEPRICEDAY",
        "CHANGE",
        "QTY",
        "NUMTRADES",
        "SHORTNAME",
        "ASSETCODE",
    ]
    live_snapshot = json.loads(FRESH_SNAPSHOT_FIXTURE.read_text(encoding="utf-8"))

    class Handler(BaseHTTPRequestHandler):
        def _json(self, payload: dict[str, object]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            path = self.path
            if path.startswith("/iss/history/engines/futures/markets/forts/securities/"):
                secid = path.split("/securities/")[1].split(".json")[0]
                shortname = "BR-6.26" if secid == "BRM6" else "Si-6.26"
                asset = "BR" if secid == "BRM6" else "Si"
                self._json(
                    {
                        "history": {
                            "columns": history_columns,
                            "data": [
                                ["RFUD", "2026-03-16", secid, 82.1, 81.9, 82.8, 82.4, 0.0, 100.0, 1500, 21000, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 10, shortname, asset]
                            ],
                        },
                        "history.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"], "data": [[0, 1, 100]]},
                    }
                )
                return
            if path.startswith("/iss/engines/futures/markets/forts/securities/"):
                secid = path.split("/securities/")[1].split("/candles.json")[0]
                open_price = 82.1 if secid == "BRM6" else 104210.0
                close_price = 82.4 if secid == "BRM6" else 104330.0
                high_price = 82.8 if secid == "BRM6" else 104350.0
                low_price = 81.9 if secid == "BRM6" else 104100.0
                volume = 1500 if secid == "BRM6" else 900
                begin = "2026-03-16 10:00:00"
                end = "2026-03-16 10:15:00"
                self._json(
                    {
                        "candles": {
                            "columns": candle_columns,
                            "data": [[open_price, close_price, high_price, low_price, 0.0, volume, begin, end]],
                        }
                    }
                )
                return
            if path == "/quik/live-snapshot":
                self._json(live_snapshot)
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield base_url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_phase9_provider_bootstrap_script_emits_moex_report(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase9_provider_bootstrap.py",
            "--provider",
            "moex-history",
            "--source-path",
            str(BACKFILL_FIXTURE),
            "--output-dir",
            str(tmp_path / "bootstrap"),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["provider"]["external_system"] == "MOEX"
    assert payload["pilot_universe"]["live_provider_id"] == "quik-live"
    assert payload["canonical_rows"] == 2
    assert payload["materialization_mode"] == "manifest_only_jsonl_samples"
    assert payload["materialized_delta_tables"] == []
    assert payload["dataset_version"].startswith("phase9-moex-futures-pilot-moex-history-")
    assert Path(str(payload["output_paths"]["canonical_bars"])).exists()


def test_phase9_provider_bootstrap_script_can_fetch_real_moex_style_http_payload(tmp_path: Path) -> None:
    with _phase9_data_server() as base_url:
        result = subprocess.run(
            [
                sys.executable,
                "scripts/run_phase9_provider_bootstrap.py",
                "--provider",
                "moex-history",
                "--output-dir",
                str(tmp_path / "bootstrap-http"),
                "--from-date",
                "2026-03-16",
                "--till-date",
                "2026-03-16",
                "--timeframe",
                "15m",
                "--base-url",
                f"{base_url}/iss",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stdout + "\n" + result.stderr
        payload = json.loads(result.stdout)
        assert payload["source_kind"] == "moex-iss"
        assert payload["resolved_secids"] == {"BR-6.26": "BRM6", "Si-6.26": "SiM6"}
        assert payload["canonical_rows"] == 2
        assert payload["materialization_mode"] == "manifest_only_jsonl_samples"
        assert len(payload["source_urls"]) == 2


def test_phase9_live_smoke_script_succeeds_for_fresh_quik_snapshot() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase9_real_data_smoke.py",
            "--provider",
            "quik-live",
            "--snapshot-path",
            str(FRESH_SNAPSHOT_FIXTURE),
            "--as-of-ts",
            "2026-03-20T07:01:00Z",
            "--max-lag-seconds",
            "60",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["provider"]["external_system"] == "QUIK"
    assert payload["missing_contract_ids"] == []
    assert payload["source_kind"] == "file"


def test_phase9_live_smoke_script_fails_for_stale_or_incomplete_snapshot() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase9_real_data_smoke.py",
            "--provider",
            "quik-live",
            "--snapshot-path",
            str(STALE_SNAPSHOT_FIXTURE),
            "--as-of-ts",
            "2026-03-20T07:01:00Z",
            "--max-lag-seconds",
            "60",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "degraded"
    assert payload["missing_contract_ids"] == ["Si-6.26"]
    assert payload["stale_contract_ids"] == ["BR-6.26"]


def test_phase9_live_smoke_script_supports_external_quik_snapshot_url() -> None:
    with _phase9_data_server() as base_url:
        result = subprocess.run(
            [
                sys.executable,
                "scripts/run_phase9_real_data_smoke.py",
                "--provider",
                "quik-live",
                "--snapshot-url",
                f"{base_url}/quik/live-snapshot",
                "--as-of-ts",
                "2026-03-20T07:01:00Z",
                "--max-lag-seconds",
                "60",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stdout + "\n" + result.stderr
        payload = json.loads(result.stdout)
        assert payload["status"] == "ok"
        assert payload["source_kind"] == "url"
        assert payload["snapshot_url"] == f"{base_url}/quik/live-snapshot"
