from __future__ import annotations

# ruff: noqa: E402
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
        resolve_repo_path,
        spark_docker_env_flags,
        wrap_with_owner_normalization,
    )


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.spark_jobs import DEFAULT_SPARK_MASTER
from trading_advisor_3000.spark_jobs.moex_canonical_publish_job import (
    run_moex_canonical_publish_spark_delta_job,
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
    data_root = os.environ.get(MOEX_HISTORICAL_DATA_ROOT_ENV, "").strip()
    if not data_root:
        return []
    return [(Path(data_root).expanduser().resolve(), DEFAULT_DOCKER_DATA_ROOT)]


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


_PATH_FIELD_NAMES = {"path", "publish_scope_path", "recovery_manifest_path"}
_PATH_CONTAINER_FIELD_NAMES = {"output_paths", "target_paths", "staged_paths"}


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
        raise RuntimeError(f"docker build failed for Spark publish image `{image}`")


def _hostify_report(payload: dict[str, object]) -> dict[str, object]:
    output_paths = payload.get("output_paths")
    if isinstance(output_paths, dict):
        payload["output_paths"] = {
            str(key): _hostify_container_path(str(value)) for key, value in output_paths.items()
        }
    delta_log = payload.get("delta_log")
    if isinstance(delta_log, dict):
        for item in delta_log.values():
            if isinstance(item, dict) and isinstance(item.get("path"), str):
                item["path"] = _hostify_container_path(str(item["path"]))
    publish_protocol = payload.get("publish_protocol")
    if isinstance(publish_protocol, dict):
        payload["publish_protocol"] = _hostify_publish_protocol_paths(publish_protocol)
    return payload


def _hostify_publish_protocol_paths(value: object, *, path_context: bool = False) -> object:
    if isinstance(value, dict):
        hostified: dict[str, object] = {}
        for raw_key, raw_item in value.items():
            key = str(raw_key)
            child_path_context = path_context or key in _PATH_CONTAINER_FIELD_NAMES
            if isinstance(raw_item, str) and (
                child_path_context or key in _PATH_FIELD_NAMES or key.endswith("_path")
            ):
                hostified[key] = _hostify_container_path(raw_item) if raw_item.strip() else raw_item
            else:
                hostified[key] = _hostify_publish_protocol_paths(
                    raw_item,
                    path_context=child_path_context,
                )
        return hostified
    if isinstance(value, list):
        return [_hostify_publish_protocol_paths(item, path_context=path_context) for item in value]
    return value


def _write_report(path: Path | None, report: dict[str, object]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _run_local(args: argparse.Namespace) -> dict[str, object]:
    report = run_moex_canonical_publish_spark_delta_job(
        staged_bars_path=Path(args.staged_bars_path).resolve(),
        staged_provenance_path=Path(args.staged_provenance_path).resolve(),
        publish_scope_path=Path(args.publish_scope_path).resolve()
        if args.publish_scope_path
        else None,
        target_bars_path=Path(args.target_bars_path).resolve(),
        target_provenance_path=Path(args.target_provenance_path).resolve(),
        session_calendar_path=Path(args.session_calendar_path).resolve(),
        session_intervals_path=Path(args.session_intervals_path).resolve()
        if str(args.session_intervals_path).strip()
        else None,
        roll_map_path=Path(args.roll_map_path).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        run_id=args.run_id,
        spark_master=args.spark_master,
    )
    output_json = Path(args.output_json).resolve() if args.output_json else None
    _write_report(output_json, report)
    return report


def _docker_exec_args(args: argparse.Namespace) -> list[str]:
    python_command = [
        "python",
        "scripts/run_moex_canonical_publish_spark.py",
        "--profile",
        "local",
        "--staged-bars-path",
        _container_path(Path(args.staged_bars_path).resolve()),
        "--staged-provenance-path",
        _container_path(Path(args.staged_provenance_path).resolve()),
        "--publish-scope-path",
        _container_path(Path(args.publish_scope_path).resolve()) if args.publish_scope_path else "",
        "--target-bars-path",
        _container_path(Path(args.target_bars_path).resolve()),
        "--target-provenance-path",
        _container_path(Path(args.target_provenance_path).resolve()),
        "--session-calendar-path",
        _container_path(Path(args.session_calendar_path).resolve()),
        "--roll-map-path",
        _container_path(Path(args.roll_map_path).resolve()),
        "--output-dir",
        _container_path(Path(args.output_dir).resolve()),
        "--run-id",
        args.run_id,
        "--spark-master",
        args.spark_master,
    ]
    if str(args.session_intervals_path).strip():
        python_command.extend(
            [
                "--session-intervals-path",
                _container_path(Path(args.session_intervals_path).resolve()),
            ]
        )
    if args.output_json:
        python_command.extend(["--output-json", _container_path(Path(args.output_json).resolve())])
    owner = docker_host_owner()
    chown_targets = [_container_path(Path(args.output_dir).resolve())]
    if args.output_json:
        chown_targets.append(_container_path(Path(args.output_json).resolve()))
    return wrap_with_owner_normalization(
        command=python_command,
        owner=owner,
        targets=chown_targets,
    )


def _run_docker(args: argparse.Namespace) -> dict[str, object]:
    if not str(args.output_json).strip():
        raise RuntimeError("docker Spark canonical publish requires --output-json")

    image = str(args.docker_image).strip()
    dockerfile = _resolve_repo_path(Path(args.dockerfile))
    _ensure_docker_image(image, dockerfile)

    output_dir = Path(args.output_dir).resolve()
    output_json = Path(args.output_json).resolve()
    ensure_output_directory_writable(output_dir)
    ensure_output_file_writable(output_json)

    repo_root = _repo_root().resolve()
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
        f"HOME={args.docker_runtime_root}",
        "-e",
        f"TA3000_SPARK_RUNTIME_ROOT={args.docker_runtime_root}",
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
    command.extend([image, *_docker_exec_args(args)])

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
        raise RuntimeError(f"docker Spark canonical publish failed: {detail}")
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("docker Spark canonical publish report must be a JSON object")
    payload = _hostify_report(payload)
    payload["proof_profile"] = "docker-linux"
    _write_report(output_json, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MOEX Spark/Delta canonical publish.")
    parser.add_argument("--profile", choices=("local", "docker"), default="local")
    parser.add_argument("--staged-bars-path", required=True)
    parser.add_argument("--staged-provenance-path", required=True)
    parser.add_argument("--publish-scope-path", default="")
    parser.add_argument("--target-bars-path", required=True)
    parser.add_argument("--target-provenance-path", required=True)
    parser.add_argument("--session-calendar-path", required=True)
    parser.add_argument("--session-intervals-path", default="")
    parser.add_argument("--roll-map-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--spark-master", default=DEFAULT_SPARK_MASTER)
    parser.add_argument("--output-json", default="")
    parser.add_argument("--docker-image", default=DEFAULT_DOCKER_IMAGE)
    parser.add_argument("--dockerfile", default=str(DEFAULT_DOCKERFILE))
    parser.add_argument("--docker-runtime-root", default=DEFAULT_DOCKER_RUNTIME_ROOT)
    args = parser.parse_args()

    report = _run_docker(args) if args.profile == "docker" else _run_local(args)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
