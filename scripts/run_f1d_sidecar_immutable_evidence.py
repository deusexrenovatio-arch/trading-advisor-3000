from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = ROOT / "artifacts" / "f1" / "phase04" / "sidecar-immutable"
SIDECAR_SCRIPT_DIR = ROOT / "deployment" / "stocksharp-sidecar" / "scripts"
SMOKE_SCRIPT = ROOT / "scripts" / "smoke_stocksharp_sidecar_binary.py"
PHASE_BRIEF = "docs/codex/modules/f1-full-closure.phase-04.md"
PHASE_NAME = "F1-D - Sidecar Immutable Evidence Hardening"


@dataclass(frozen=True)
class StepResult:
    step_id: str
    command: tuple[str, ...]
    return_code: int
    started_at: str
    finished_at: str
    duration_sec: float
    stdout_path: str
    stderr_path: str

    def as_json(self) -> dict[str, object]:
        return {
            "step_id": self.step_id,
            "command": render_command(self.command),
            "return_code": self.return_code,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_sec": self.duration_sec,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
        }


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def render_command(command: Sequence[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(list(command))
    return shlex.join(command)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path.as_posix()}")
    return payload


def rel(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def git_head_sha() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"failed to resolve git HEAD SHA: {detail}")
    value = completed.stdout.strip()
    if len(value) < 12:
        raise RuntimeError(f"unexpected git SHA output: {value!r}")
    return value


def resolve_dotnet_executable(explicit: str) -> str:
    if explicit.strip():
        return explicit.strip()
    env_dotnet = os.environ.get("TA3000_DOTNET_BIN", "").strip()
    if env_dotnet:
        return env_dotnet
    return "dotnet"


def run_command(
    *,
    step_id: str,
    command: Sequence[str],
    logs_dir: Path,
    cwd: Path = ROOT,
    env: dict[str, str] | None = None,
    require_success: bool = True,
) -> StepResult:
    started_at = utc_now()
    started_clock = time.perf_counter()
    completed = subprocess.run(
        list(command),
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    finished_at = utc_now()
    duration_sec = round(time.perf_counter() - started_clock, 3)

    logs_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = logs_dir / f"{step_id}.stdout.log"
    stderr_path = logs_dir / f"{step_id}.stderr.log"
    stdout_path.write_text(completed.stdout, encoding="utf-8")
    stderr_path.write_text(completed.stderr, encoding="utf-8")

    result = StepResult(
        step_id=step_id,
        command=tuple(command),
        return_code=int(completed.returncode),
        started_at=started_at,
        finished_at=finished_at,
        duration_sec=duration_sec,
        stdout_path=rel(stdout_path),
        stderr_path=rel(stderr_path),
    )
    if require_success and completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"{step_id} failed ({completed.returncode}): {detail}")
    return result


def validate_smoke_payload(payload: dict[str, Any]) -> None:
    smoke = payload.get("smoke")
    if not isinstance(smoke, dict):
        raise ValueError("smoke payload must contain `smoke` object")

    kill_switch = smoke.get("kill_switch")
    if not isinstance(kill_switch, dict):
        raise ValueError("smoke payload missing `smoke.kill_switch` object")

    ready_under_kill = kill_switch.get("ready_under_kill_switch")
    if not isinstance(ready_under_kill, dict):
        raise ValueError("smoke payload missing `smoke.kill_switch.ready_under_kill_switch` object")
    if ready_under_kill.get("ready") is not False:
        raise ValueError("kill-switch disprover: readiness under kill-switch must be ready=false")
    if str(ready_under_kill.get("reason", "")).strip() != "kill_switch_active":
        raise ValueError("kill-switch disprover: readiness under kill-switch must expose reason=kill_switch_active")

    blocked_submit = kill_switch.get("blocked_submit")
    if not isinstance(blocked_submit, dict):
        raise ValueError("smoke payload missing `smoke.kill_switch.blocked_submit` object")
    if str(blocked_submit.get("error_code", "")).strip() != "kill_switch_active":
        raise ValueError("kill-switch disprover: blocked submit must expose error_code=kill_switch_active")
    if int(blocked_submit.get("status_code", 0)) != 503:
        raise ValueError("kill-switch disprover: blocked submit must expose status_code=503")

    submit_after_restore = kill_switch.get("submit_after_restore")
    if not isinstance(submit_after_restore, dict):
        raise ValueError("smoke payload missing `smoke.kill_switch.submit_after_restore` object")
    if submit_after_restore.get("accepted") is not True:
        raise ValueError("kill-switch restore check failed: submit_after_restore.accepted must be true")

    metrics = smoke.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("smoke payload missing `smoke.metrics` object")
    metrics_on = str(metrics.get("while_kill_switch_enabled", ""))
    metrics_restore = str(metrics.get("after_kill_switch_restore", ""))
    if "ta3000_sidecar_gateway_kill_switch 1" not in metrics_on:
        raise ValueError("metrics disprover: kill switch enable marker missing from metrics snapshot")
    if "ta3000_sidecar_gateway_kill_switch 0" not in metrics_restore:
        raise ValueError("metrics disprover: kill switch restore marker missing from metrics snapshot")


def collect_hash_entries(*, base_dir: Path, targets: Sequence[Path]) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for path in sorted({item.resolve() for item in targets}):
        if not path.exists():
            raise FileNotFoundError(f"hash target is missing: {path.as_posix()}")
        if not path.is_file():
            raise RuntimeError(f"hash target is not a file: {path.as_posix()}")
        entries.append(
            {
                "path": path.relative_to(base_dir).as_posix(),
                "sha256": sha256_file(path),
                "size_bytes": int(path.stat().st_size),
            }
        )
    return entries


def write_hash_manifest(
    *,
    base_dir: Path,
    targets: Sequence[Path],
    output_json: Path,
    output_sha256: Path,
) -> list[dict[str, object]]:
    entries = collect_hash_entries(base_dir=base_dir, targets=targets)
    write_json(
        output_json,
        {
            "schema_version": 1,
            "generated_at": utc_now(),
            "entries": entries,
        },
    )
    sha_lines = [f"{entry['sha256']} *{entry['path']}" for entry in entries]
    output_sha256.write_text("\n".join(sha_lines) + "\n", encoding="utf-8")
    return entries


def verify_hash_manifest(*, base_dir: Path, entries: Sequence[dict[str, object]]) -> None:
    for entry in entries:
        path_text = str(entry.get("path", "")).strip()
        expected_hash = str(entry.get("sha256", "")).strip().lower()
        if not path_text or not expected_hash:
            raise ValueError("hash manifest entry must include path and sha256")
        target = (base_dir / path_text).resolve()
        if not target.exists():
            raise FileNotFoundError(f"hash verification target is missing: {path_text}")
        actual = sha256_file(target).lower()
        if actual != expected_hash:
            raise ValueError(
                "hash mismatch for "
                f"{path_text}: expected {expected_hash}, got {actual}"
            )


def run_broken_binary_disprover(
    *,
    smoke_script: Path,
    python_executable: str,
    dotnet_executable: str,
    missing_binary: Path,
    host: str,
    port: int,
    logs_dir: Path,
) -> dict[str, object]:
    command = [
        python_executable,
        str(smoke_script),
        "--sidecar-binary",
        str(missing_binary),
        "--dotnet",
        dotnet_executable,
        "--host",
        host,
        "--port",
        str(port),
    ]
    result = run_command(
        step_id="negative-broken-binary-path",
        command=command,
        logs_dir=logs_dir,
        require_success=False,
    )
    if result.return_code == 0:
        raise RuntimeError("broken-binary disprover failed: smoke unexpectedly succeeded on missing binary")
    return {
        "name": "broken_binary_path",
        "status": "expected_failure_observed",
        "return_code": result.return_code,
        "command": render_command(command),
        "stdout_path": result.stdout_path,
        "stderr_path": result.stderr_path,
    }


def run_kill_switch_disprover(*, smoke_payload: dict[str, Any], disprover_dir: Path) -> dict[str, object]:
    mutated = json.loads(json.dumps(smoke_payload))
    kill = mutated.setdefault("smoke", {}).setdefault("kill_switch", {})
    ready = kill.setdefault("ready_under_kill_switch", {})
    ready["ready"] = True
    ready["reason"] = "unexpected_ready_state"
    mutated_path = disprover_dir / "kill-switch-readiness-failure.payload.json"
    write_json(mutated_path, mutated)

    try:
        validate_smoke_payload(mutated)
    except ValueError as exc:
        return {
            "name": "kill_switch_readiness_failure",
            "status": "expected_failure_observed",
            "detail": str(exc),
            "mutated_payload_path": rel(mutated_path),
        }
    raise RuntimeError("kill-switch disprover failed: mutated payload passed validation")


def run_hash_mismatch_disprover(
    *,
    base_dir: Path,
    verified_entries: Sequence[dict[str, object]],
    disprover_dir: Path,
) -> dict[str, object]:
    if not verified_entries:
        raise RuntimeError("hash-mismatch disprover requires at least one hash entry")
    first_target = base_dir / str(verified_entries[0]["path"])
    original_bytes = first_target.read_bytes()
    try:
        first_target.write_bytes(original_bytes + b"\nintentional-hash-mismatch\n")
        try:
            verify_hash_manifest(base_dir=base_dir, entries=verified_entries)
        except ValueError as exc:
            detail = str(exc)
        else:
            raise RuntimeError("hash-mismatch disprover failed: hash verification unexpectedly passed")
    finally:
        first_target.write_bytes(original_bytes)

    post_restore = collect_hash_entries(
        base_dir=base_dir,
        targets=[base_dir / str(entry["path"]) for entry in verified_entries],
    )
    verify_hash_manifest(base_dir=base_dir, entries=post_restore)
    disprover_payload = {
        "name": "artifact_hash_mismatch",
        "status": "expected_failure_observed",
        "detail": detail,
        "tampered_target": rel(first_target),
        "post_restore_verification": "ok",
    }
    write_json(disprover_dir / "hash-mismatch.result.json", disprover_payload)
    return disprover_payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run F1-D immutable sidecar evidence chain with commit-linked artifacts, hash proof, and disprovers."
    )
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT.as_posix())
    parser.add_argument("--run-id", default="")
    parser.add_argument("--configuration", default="Release")
    parser.add_argument("--dotnet", default="")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18091)
    parser.add_argument("--python-executable", default=sys.executable)
    parser.add_argument("--skip-disprovers", action="store_true")
    args = parser.parse_args()

    git_sha = git_head_sha()
    dotnet_executable = resolve_dotnet_executable(args.dotnet)
    run_stamp = args.run_id.strip() or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{git_sha[:12]}"
    output_root = Path(args.output_root)
    if not output_root.is_absolute():
        output_root = (ROOT / output_root).resolve()
    run_dir = (output_root / run_stamp).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    logs_dir = run_dir / "logs"
    publish_dir = run_dir / "publish"
    smoke_output_path = run_dir / "smoke.json"

    dotnet_info = run_command(
        step_id="dotnet-info",
        command=[dotnet_executable, "--info"],
        logs_dir=logs_dir,
        require_success=False,
    )
    if dotnet_info.return_code != 0:
        raise RuntimeError(
            "dotnet --info failed; install .NET SDK or set TA3000_DOTNET_BIN to SDK-backed dotnet executable"
        )

    environment_payload = {
        "schema_version": 1,
        "phase": PHASE_NAME,
        "phase_brief": PHASE_BRIEF,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "generated_at": utc_now(),
        "git_sha": git_sha,
        "python": {
            "executable": sys.executable,
            "version": sys.version,
            "platform": platform.platform(),
        },
        "dotnet": {
            "executable": dotnet_executable,
            "info_return_code": dotnet_info.return_code,
            "stdout_log": dotnet_info.stdout_path,
            "stderr_log": dotnet_info.stderr_path,
        },
    }
    environment_path = run_dir / "environment.json"
    write_json(environment_path, environment_payload)

    step_records: list[StepResult] = []

    build_step = run_command(
        step_id="build",
        command=[
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str((SIDECAR_SCRIPT_DIR / "build.ps1").resolve()),
            "-Configuration",
            args.configuration,
            "-DotnetExecutable",
            dotnet_executable,
        ],
        logs_dir=logs_dir,
    )
    step_records.append(build_step)
    write_json(run_dir / "build.json", build_step.as_json())

    test_step = run_command(
        step_id="test",
        command=[
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str((SIDECAR_SCRIPT_DIR / "test.ps1").resolve()),
            "-Configuration",
            args.configuration,
            "-DotnetExecutable",
            dotnet_executable,
        ],
        logs_dir=logs_dir,
    )
    step_records.append(test_step)
    write_json(run_dir / "test.json", test_step.as_json())

    publish_step = run_command(
        step_id="publish",
        command=[
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str((SIDECAR_SCRIPT_DIR / "publish.ps1").resolve()),
            "-Configuration",
            args.configuration,
            "-OutputDir",
            str(publish_dir),
            "-DotnetExecutable",
            dotnet_executable,
        ],
        logs_dir=logs_dir,
    )
    step_records.append(publish_step)
    write_json(run_dir / "publish.json", publish_step.as_json())

    sidecar_binary = publish_dir / "TradingAdvisor3000.StockSharpSidecar.dll"
    if not sidecar_binary.exists():
        raise RuntimeError(f"publish step did not produce compiled binary: {sidecar_binary.as_posix()}")

    smoke_step = run_command(
        step_id="smoke",
        command=[
            args.python_executable,
            str(SMOKE_SCRIPT.resolve()),
            "--sidecar-binary",
            str(sidecar_binary),
            "--dotnet",
            dotnet_executable,
            "--host",
            args.host,
            "--port",
            str(args.port),
            "--output",
            str(smoke_output_path),
        ],
        logs_dir=logs_dir,
    )
    step_records.append(smoke_step)
    smoke_payload = read_json(smoke_output_path)
    validate_smoke_payload(smoke_payload)
    write_json(
        run_dir / "smoke-validation.json",
        {
            "status": "ok",
            "validated_at": utc_now(),
            "checks": [
                "kill_switch_readiness_fail_closed",
                "kill_switch_blocked_submit_503",
                "kill_switch_metric_toggle",
                "submit_restore_acceptance",
            ],
        },
    )

    hash_targets = [
        run_dir / "environment.json",
        run_dir / "build.json",
        run_dir / "test.json",
        run_dir / "publish.json",
        run_dir / "smoke.json",
        run_dir / "smoke-validation.json",
    ]
    hash_targets.extend(path for path in publish_dir.rglob("*") if path.is_file())
    hashes_json = run_dir / "hashes.json"
    hashes_sha256 = run_dir / "hashes.sha256"
    hash_entries = write_hash_manifest(
        base_dir=run_dir,
        targets=hash_targets,
        output_json=hashes_json,
        output_sha256=hashes_sha256,
    )
    verify_hash_manifest(base_dir=run_dir, entries=hash_entries)
    hash_validation_payload = {
        "status": "ok",
        "validated_at": utc_now(),
        "entries_count": len(hash_entries),
    }
    write_json(run_dir / "hash-validation.json", hash_validation_payload)

    negative_tests: list[dict[str, object]] = []
    if not args.skip_disprovers:
        disprover_dir = run_dir / "disprovers"
        disprover_dir.mkdir(parents=True, exist_ok=True)
        negative_tests.append(
            run_broken_binary_disprover(
                smoke_script=SMOKE_SCRIPT.resolve(),
                python_executable=args.python_executable,
                dotnet_executable=dotnet_executable,
                missing_binary=run_dir / "publish" / "not-compiled-stub.dll",
                host=args.host,
                port=args.port,
                logs_dir=logs_dir,
            )
        )
        negative_tests.append(
            run_kill_switch_disprover(
                smoke_payload=smoke_payload,
                disprover_dir=disprover_dir,
            )
        )
        negative_tests.append(
            run_hash_mismatch_disprover(
                base_dir=run_dir,
                verified_entries=hash_entries,
                disprover_dir=disprover_dir,
            )
        )
    write_json(
        run_dir / "negative-tests.json",
        {
            "generated_at": utc_now(),
            "tests": negative_tests,
        },
    )

    manifest_payload = {
        "schema_version": 1,
        "phase": PHASE_NAME,
        "phase_brief": PHASE_BRIEF,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "generated_at": utc_now(),
        "run_id": run_stamp,
        "git_sha": git_sha,
        "commands": [item.as_json() for item in step_records],
        "artifacts": {
            "run_dir": rel(run_dir),
            "environment": rel(run_dir / "environment.json"),
            "build": rel(run_dir / "build.json"),
            "test": rel(run_dir / "test.json"),
            "publish": rel(run_dir / "publish.json"),
            "smoke": rel(run_dir / "smoke.json"),
            "smoke_validation": rel(run_dir / "smoke-validation.json"),
            "negative_tests": rel(run_dir / "negative-tests.json"),
            "hashes_json": rel(run_dir / "hashes.json"),
            "hashes_sha256": rel(run_dir / "hashes.sha256"),
            "hash_validation": rel(run_dir / "hash-validation.json"),
            "publish_dir": rel(publish_dir),
        },
        "real_bindings": [
            "compiled sidecar artifact set",
            "governed replay environment",
            "immutable artifact hashes",
        ],
    }
    manifest_path = run_dir / "manifest.json"
    write_json(manifest_path, manifest_payload)
    print(json.dumps(manifest_payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
