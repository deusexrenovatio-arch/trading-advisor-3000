from __future__ import annotations

# ruff: noqa: E402, E501
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

try:
    from scripts.proof_runtime_contract import (
        container_to_host_path,
        docker_host_owner,
        docker_subprocess_timeout_seconds,
        ensure_output_directory_writable,
        ensure_output_file_writable,
        host_to_container_path,
        normalize_runtime_root,
        resolve_repo_path,
        spark_docker_env_flags,
        wrap_with_owner_normalization,
    )
except ImportError:  # pragma: no cover - script execution fallback
    from proof_runtime_contract import (
        container_to_host_path,
        docker_host_owner,
        docker_subprocess_timeout_seconds,
        ensure_output_directory_writable,
        ensure_output_file_writable,
        host_to_container_path,
        normalize_runtime_root,
        resolve_repo_path,
        spark_docker_env_flags,
        wrap_with_owner_normalization,
    )


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.spark_jobs import DEFAULT_SPARK_MASTER
from trading_advisor_3000.spark_jobs.moex_canonicalization_job import (
    run_moex_canonicalization_spark_delta_job,
    run_moex_canonicalization_spark_job,
)

DEFAULT_DOCKER_IMAGE = "ta3000-phase-proof:latest"
DEFAULT_DOCKERFILE = Path("deployment/docker/phase-proofs/Dockerfile")
DEFAULT_DOCKER_RUNTIME_ROOT = "/tmp/ta3000-phase-proof"
DEFAULT_DOCKER_DATA_ROOT = "/ta3000-data/moex-historical"
SPARK_DOCKER_SUBPROCESS_TIMEOUT_SECONDS = 3600
SPARK_DOCKER_SUBPROCESS_TIMEOUT_ENV = "TA3000_SPARK_DOCKER_SUBPROCESS_TIMEOUT_SECONDS"
MOEX_HISTORICAL_DATA_ROOT_ENV = "TA3000_MOEX_HISTORICAL_DATA_ROOT"


def _repo_root() -> Path:
    return ROOT


def _resolve_repo_path(path: Path) -> Path:
    return resolve_repo_path(path, repo_root=_repo_root())


def _external_mount_roots() -> list[tuple[Path, str]]:
    raw_root = os.environ.get(MOEX_HISTORICAL_DATA_ROOT_ENV, "").strip()
    if not raw_root:
        return []
    return [(Path(raw_root).expanduser().resolve(), DEFAULT_DOCKER_DATA_ROOT)]


def _container_path(path: Path) -> str:
    return host_to_container_path(
        path,
        repo_root=_repo_root().resolve(),
        extra_roots=_external_mount_roots(),
    )


def _hostify_container_path(value: str) -> str:
    return container_to_host_path(
        value,
        repo_root=_repo_root().resolve(),
        extra_roots=_external_mount_roots(),
    )


def _ensure_docker_image(image: str, dockerfile: Path) -> None:
    inspect = subprocess.run(
        ["docker", "image", "inspect", image],
        cwd=_repo_root(),
        capture_output=True,
        text=True,
        check=False,
    )
    if inspect.returncode == 0:
        return
    completed = subprocess.run(
        ["docker", "build", "-f", str(dockerfile), "-t", image, "."],
        cwd=_repo_root(),
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"docker build failed for Spark canonicalization image `{image}`")


def _docker_python_command(
    *,
    normalized_source_jsonl: Path | None,
    raw_table_path: Path | None,
    changed_windows_jsonl: Path | None,
    selected_source_intervals_jsonl: Path,
    session_intervals_path: Path | None,
    output_dir: Path,
    run_id: str,
    built_at_utc: str,
    spark_master: str,
    output_json: Path | None,
) -> list[str]:
    command = [
        "python",
        "scripts/run_moex_historical_canonical_route_spark.py",
        "--profile",
        "local",
        "--selected-source-intervals-jsonl",
        _container_path(selected_source_intervals_jsonl),
        "--output-dir",
        _container_path(output_dir),
        "--run-id",
        run_id,
        "--built-at-utc",
        built_at_utc,
        "--spark-master",
        spark_master,
    ]
    if session_intervals_path is not None:
        command.extend(["--session-intervals-path", _container_path(session_intervals_path)])
    if raw_table_path is not None:
        if changed_windows_jsonl is None:
            raise RuntimeError("raw Delta canonicalization requires changed_windows_jsonl")
        command.extend(
            [
                "--raw-table-path",
                _container_path(raw_table_path),
                "--changed-windows-jsonl",
                _container_path(changed_windows_jsonl),
            ]
        )
    else:
        if normalized_source_jsonl is None:
            raise RuntimeError("JSONL canonicalization requires normalized_source_jsonl")
        command.extend(["--normalized-source-jsonl", _container_path(normalized_source_jsonl)])
    if output_json is not None:
        command.extend(["--output-json", _container_path(output_json)])
    return command


def _docker_exec_args(
    *,
    normalized_source_jsonl: Path | None,
    raw_table_path: Path | None,
    changed_windows_jsonl: Path | None,
    selected_source_intervals_jsonl: Path,
    session_intervals_path: Path | None,
    output_dir: Path,
    run_id: str,
    built_at_utc: str,
    spark_master: str,
    output_json: Path | None,
) -> list[str]:
    python_command = _docker_python_command(
        normalized_source_jsonl=normalized_source_jsonl,
        raw_table_path=raw_table_path,
        changed_windows_jsonl=changed_windows_jsonl,
        selected_source_intervals_jsonl=selected_source_intervals_jsonl,
        session_intervals_path=session_intervals_path,
        output_dir=output_dir,
        run_id=run_id,
        built_at_utc=built_at_utc,
        spark_master=spark_master,
        output_json=output_json,
    )
    owner = docker_host_owner()
    chown_targets = [_container_path(output_dir)]
    if output_json is not None:
        chown_targets.append(_container_path(output_json))
    return wrap_with_owner_normalization(
        command=python_command,
        owner=owner,
        targets=chown_targets,
    )


def _run_in_docker(
    *,
    normalized_source_jsonl: Path | None,
    raw_table_path: Path | None,
    changed_windows_jsonl: Path | None,
    selected_source_intervals_jsonl: Path,
    session_intervals_path: Path | None,
    output_dir: Path,
    run_id: str,
    built_at_utc: str,
    spark_master: str,
    output_json: Path | None,
    image: str,
    dockerfile: Path,
    runtime_root: str,
) -> dict[str, object]:
    _ensure_docker_image(image, dockerfile)

    repo_root = _repo_root().resolve()
    ensure_output_directory_writable(output_dir)
    if output_json is not None:
        ensure_output_file_writable(output_json)

    command = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{repo_root}:/workspace",
        "-w",
        "/workspace",
        "-e",
        "PYTHONPATH=/workspace/src",
        "-e",
        f"HOME={runtime_root}",
        "-e",
        f"TA3000_SPARK_RUNTIME_ROOT={runtime_root}",
        *spark_docker_env_flags(),
    ]
    for host_root, container_root in _external_mount_roots():
        command.extend(
            [
                "-v",
                f"{host_root}:{container_root}",
                "-e",
                f"{MOEX_HISTORICAL_DATA_ROOT_ENV}={container_root}",
            ]
        )
    command.extend(
        [
            image,
            *_docker_exec_args(
                normalized_source_jsonl=normalized_source_jsonl,
                raw_table_path=raw_table_path,
                changed_windows_jsonl=changed_windows_jsonl,
                selected_source_intervals_jsonl=selected_source_intervals_jsonl,
                session_intervals_path=session_intervals_path,
                output_dir=output_dir,
                run_id=run_id,
                built_at_utc=built_at_utc,
                spark_master=spark_master,
                output_json=output_json,
            ),
        ]
    )

    completed = subprocess.run(
        command,
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
        timeout=docker_subprocess_timeout_seconds(
            env_name=SPARK_DOCKER_SUBPROCESS_TIMEOUT_ENV,
            default_seconds=SPARK_DOCKER_SUBPROCESS_TIMEOUT_SECONDS,
        ),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"docker Spark canonicalization failed: {detail}")

    if output_json is None:
        raise RuntimeError(
            "docker Spark canonicalization requires output_json for deterministic capture"
        )

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("docker Spark canonicalization report must be a JSON object")
    output_paths = payload.get("output_paths")
    if isinstance(output_paths, dict):
        payload["output_paths"] = {
            str(key): _hostify_container_path(str(value)) for key, value in output_paths.items()
        }
    payload["proof_profile"] = "docker-linux"
    output_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Execute Spark-backed MOEX canonicalization for scoped normalized raw rows. "
            "This is the canonical Spark resampling/provenance engine for the historical route."
        )
    )
    parser.add_argument("--normalized-source-jsonl", default="")
    parser.add_argument("--raw-table-path", default="")
    parser.add_argument("--changed-windows-jsonl", default="")
    parser.add_argument("--selected-source-intervals-jsonl", required=True)
    parser.add_argument("--session-intervals-path", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--built-at-utc", required=True)
    parser.add_argument("--profile", choices=("docker", "local"), default="local")
    parser.add_argument("--spark-master", default=DEFAULT_SPARK_MASTER)
    parser.add_argument("--output-json", default="")
    parser.add_argument("--docker-image", default=DEFAULT_DOCKER_IMAGE)
    parser.add_argument("--dockerfile", default=DEFAULT_DOCKERFILE.as_posix())
    parser.add_argument("--docker-runtime-root", default=DEFAULT_DOCKER_RUNTIME_ROOT)
    args = parser.parse_args()

    normalized_source_jsonl = (
        _resolve_repo_path(Path(args.normalized_source_jsonl))
        if str(args.normalized_source_jsonl).strip()
        else None
    )
    raw_table_path = (
        _resolve_repo_path(Path(args.raw_table_path)) if str(args.raw_table_path).strip() else None
    )
    changed_windows_jsonl = (
        _resolve_repo_path(Path(args.changed_windows_jsonl))
        if str(args.changed_windows_jsonl).strip()
        else None
    )
    selected_source_intervals_jsonl = _resolve_repo_path(Path(args.selected_source_intervals_jsonl))
    session_intervals_path = (
        _resolve_repo_path(Path(args.session_intervals_path))
        if str(args.session_intervals_path).strip()
        else None
    )
    output_dir = _resolve_repo_path(Path(args.output_dir))
    output_json = _resolve_repo_path(Path(args.output_json)) if args.output_json else None
    if raw_table_path is not None and changed_windows_jsonl is None:
        raise SystemExit("--changed-windows-jsonl is required with --raw-table-path")
    if raw_table_path is None and normalized_source_jsonl is None:
        raise SystemExit("provide either --raw-table-path or --normalized-source-jsonl")

    if args.profile == "docker":
        runtime_root = normalize_runtime_root(
            args.docker_runtime_root, field_name="docker runtime root"
        )
        report = _run_in_docker(
            normalized_source_jsonl=normalized_source_jsonl,
            raw_table_path=raw_table_path,
            changed_windows_jsonl=changed_windows_jsonl,
            selected_source_intervals_jsonl=selected_source_intervals_jsonl,
            session_intervals_path=session_intervals_path,
            output_dir=output_dir,
            run_id=args.run_id,
            built_at_utc=args.built_at_utc,
            spark_master=args.spark_master,
            output_json=output_json,
            image=args.docker_image,
            dockerfile=_resolve_repo_path(Path(args.dockerfile)),
            runtime_root=runtime_root,
        )
    elif raw_table_path is not None:
        report = run_moex_canonicalization_spark_delta_job(
            raw_table_path=raw_table_path,
            changed_windows_path=changed_windows_jsonl,
            selected_source_intervals_path=selected_source_intervals_jsonl,
            session_intervals_path=session_intervals_path,
            output_dir=output_dir,
            build_run_id=args.run_id,
            built_at_utc=args.built_at_utc,
            spark_master=args.spark_master,
        )
        report["proof_profile"] = "local-spark"
    else:
        report = run_moex_canonicalization_spark_job(
            normalized_source_path=normalized_source_jsonl,
            selected_source_intervals_path=selected_source_intervals_jsonl,
            session_intervals_path=session_intervals_path,
            output_dir=output_dir,
            build_run_id=args.run_id,
            built_at_utc=args.built_at_utc,
            spark_master=args.spark_master,
        )
        report["proof_profile"] = "local-spark"

    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(payload + "\n", encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()
