from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, cast


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from run_f1d_sidecar_immutable_evidence import (  # noqa: E402
    run_broken_binary_disprover,
    run_hash_mismatch_disprover,
    run_kill_switch_disprover,
    validate_smoke_payload,
    verify_hash_manifest,
    write_hash_manifest,
)


def _valid_smoke_payload() -> dict[str, object]:
    return {
        "smoke": {
            "metrics": {
                "while_kill_switch_enabled": "ta3000_sidecar_gateway_kill_switch 1",
                "after_kill_switch_restore": "ta3000_sidecar_gateway_kill_switch 0",
            },
            "kill_switch": {
                "ready_under_kill_switch": {"ready": False, "reason": "kill_switch_active"},
                "blocked_submit": {"error_code": "kill_switch_active", "status_code": 503},
                "submit_after_restore": {"accepted": True},
            },
        }
    }


def test_validate_smoke_payload_accepts_kill_switch_fail_closed_contract() -> None:
    validate_smoke_payload(_valid_smoke_payload())


def test_validate_smoke_payload_rejects_ready_true_under_kill_switch() -> None:
    payload = _valid_smoke_payload()
    smoke = cast(dict[str, Any], payload["smoke"])
    kill_switch = cast(dict[str, Any], smoke["kill_switch"])
    ready = cast(dict[str, Any], kill_switch["ready_under_kill_switch"])
    ready["ready"] = True
    ready["reason"] = "wrong_reason"

    try:
        validate_smoke_payload(payload)
    except ValueError as exc:
        assert "readiness under kill-switch" in str(exc)
    else:
        raise AssertionError("expected kill-switch readiness validation failure")


def test_hash_manifest_and_disprover_detect_hash_mismatch(tmp_path: Path) -> None:
    build_artifact = tmp_path / "build.json"
    build_artifact.write_text('{"step":"build"}\n', encoding="utf-8")
    smoke_artifact = tmp_path / "smoke.json"
    smoke_artifact.write_text(json.dumps(_valid_smoke_payload(), ensure_ascii=False, indent=2), encoding="utf-8")

    entries = write_hash_manifest(
        base_dir=tmp_path,
        targets=[build_artifact, smoke_artifact],
        output_json=tmp_path / "hashes.json",
        output_sha256=tmp_path / "hashes.sha256",
    )
    verify_hash_manifest(base_dir=tmp_path, entries=entries)

    disprover = run_hash_mismatch_disprover(
        base_dir=tmp_path,
        verified_entries=entries,
        disprover_dir=tmp_path / "disprovers",
    )
    assert disprover["status"] == "expected_failure_observed"
    assert "hash mismatch" in str(disprover["detail"])

    # After disprover restore, the original hash set remains valid.
    reloaded = json.loads((tmp_path / "hashes.json").read_text(encoding="utf-8"))
    verify_hash_manifest(base_dir=tmp_path, entries=reloaded["entries"])


def test_broken_binary_disprover_fails_closed(tmp_path: Path) -> None:
    result = run_broken_binary_disprover(
        smoke_script=(ROOT / "scripts" / "smoke_stocksharp_sidecar_binary.py").resolve(),
        python_executable=sys.executable,
        dotnet_executable="dotnet",
        missing_binary=tmp_path / "not-compiled-stub.dll",
        host="127.0.0.1",
        port=18091,
        logs_dir=tmp_path / "logs",
    )

    assert result["status"] == "expected_failure_observed"
    assert int(result["return_code"]) != 0
