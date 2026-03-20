from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_build_phase9_quik_connector_script_writes_lua_and_config_bundle(tmp_path: Path) -> None:
    export_path = tmp_path / "runtime" / "quik_live_snapshot.json"
    lua_path = tmp_path / "bundle" / "phase9_quik_live_export.lua"
    config_path = tmp_path / "bundle" / "phase9_quik_live_export.config.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/build_phase9_quik_connector.py",
            "--export-path",
            str(export_path),
            "--output-script",
            str(lua_path),
            "--output-config",
            str(config_path),
            "--poll-interval-ms",
            "1500",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert lua_path.exists()
    assert config_path.exists()
    assert export_path.parent.exists()

    lua_script = lua_path.read_text(encoding="utf-8")
    config = json.loads(config_path.read_text(encoding="utf-8"))

    assert config["provider_id"] == "quik-live"
    assert config["poll_interval_ms"] == 1500
    assert [item["sec_code"] for item in config["bindings"]] == ["BRM6", "SiM6"]
    assert 'contract_id = "BR-6.26"' in lua_script
    assert 'sec_code = "BRM6"' in lua_script
    assert 'contract_id = "Si-6.26"' in lua_script
    assert 'sec_code = "SiM6"' in lua_script
    assert str(export_path).replace("\\", "\\\\") in lua_script
