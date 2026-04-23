from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from scripts.proof_runtime_contract import (
        container_to_host_path,
        docker_host_owner,
        ensure_output_directory_writable,
        ensure_output_file_writable,
        host_to_container_path,
        normalize_runtime_root,
        resolve_repo_path,
        wrap_with_owner_normalization,
    )
except ImportError:  # pragma: no cover - script execution fallback
    from proof_runtime_contract import (
        container_to_host_path,
        docker_host_owner,
        ensure_output_directory_writable,
        ensure_output_file_writable,
        host_to_container_path,
        normalize_runtime_root,
        resolve_repo_path,
        wrap_with_owner_normalization,
    )
from trading_advisor_3000.spark_jobs import DEFAULT_SPARK_MASTER, run_canonical_bars_spark_job


DEFAULT_FIXTURE = Path("tests/product-plane/fixtures/data_plane/raw_backfill_sample.jsonl")
DEFAULT_OUTPUT_DIR = Path(".tmp/phase2a-spark-proof")
DEFAULT_DOCKER_IMAGE = "ta3000-phase-proof:latest"
DEFAULT_DOCKERFILE = Path("deployment/docker/phase-proofs/Dockerfile")
DEFAULT_DOCKER_RUNTIME_ROOT = "/tmp/ta3000-phase-proof"


def _parse_contracts(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def _repo_root() -> Path:
    return ROOT


def _resolve_repo_path(path: Path) -> Path:
    return resolve_repo_path(path, repo_root=_repo_root())


def _container_path(path: Path) -> str:
    return host_to_container_path(path, repo_root=_repo_root().resolve())


def _hostify_container_path(value: str) -> str:
    return container_to_host_path(value, repo_root=_repo_root().resolve())


def _ensure_docker_image(image: str, dockerfile: Path) -> None:
    inspect = subprocess.run(
        ["docker", "image", "inspect", image],
        cwd=_repo_root(),
        capture_output=True,
        text=True,
        check=False,
    )
    if inspect.returncode == 0 and _docker_image_healthcheck(image):
        return
    completed = subprocess.run(
        ["docker", "build", "-f", str(dockerfile), "-t", image, "."],
        cwd=_repo_root(),
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"docker build failed for Spark proof image `{image}`")


def _docker_image_healthcheck(image: str) -> bool:
    completed = subprocess.run(
        ["docker", "run", "--rm", image, "python", "-c", "import yaml"],
        cwd=_repo_root(),
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode == 0


def _docker_host_owner() -> str:
    return docker_host_owner()


def _docker_runtime_root(raw_runtime_root: str) -> str:
    return normalize_runtime_root(raw_runtime_root, field_name="docker runtime root")


def _docker_python_command(
    *,
    source: Path,
    output_dir: Path,
    contracts: str,
    spark_master: str,
    output_json: Path | None,
) -> list[str]:
    command = [
        "python",
        "scripts/run_phase2a_spark_proof.py",
        "--profile",
        "local",
        "--source",
        _container_path(source),
        "--output-dir",
        _container_path(output_dir),
        "--contracts",
        contracts,
        "--spark-master",
        spark_master,
    ]
    if output_json is not None:
        command.extend(["--output-json", _container_path(output_json)])
    return command


def _docker_exec_args(
    *,
    source: Path,
    output_dir: Path,
    contracts: str,
    spark_master: str,
    output_json: Path | None,
) -> list[str]:
    python_command = _docker_python_command(
        source=source,
        output_dir=output_dir,
        contracts=contracts,
        spark_master=spark_master,
        output_json=output_json,
    )
    host_owner = _docker_host_owner()
    chown_targets = [_container_path(output_dir)]
    if output_json is not None:
        chown_targets.append(_container_path(output_json))
    return wrap_with_owner_normalization(
        command=python_command,
        owner=host_owner,
        targets=chown_targets,
    )


def _run_in_docker(
    *,
    source: Path,
    output_dir: Path,
    contracts: str,
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
        image,
        *_docker_exec_args(
            source=source,
            output_dir=output_dir,
            contracts=contracts,
            spark_master=spark_master,
            output_json=output_json,
        ),
    ]

    completed = subprocess.run(
        command,
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"docker Spark proof failed: {detail}")

    if output_json is None:
        raise RuntimeError("docker Spark proof requires output_json for deterministic evidence capture")

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("docker Spark proof report must be a JSON object")
    output_paths = payload.get("output_paths")
    if isinstance(output_paths, dict):
        payload["output_paths"] = {
            str(key): _hostify_container_path(str(path_text))
            for key, path_text in output_paths.items()
        }
    payload["proof_profile"] = "docker-linux"
    output_json.unlink(missing_ok=True)
    output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute the phase-04 Spark proof profile for canonical Delta outputs.")
    parser.add_argument("--source", default=DEFAULT_FIXTURE.as_posix(), help="Path to JSONL source fixture")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR.as_posix(), help="Directory for Delta outputs")
    parser.add_argument("--profile", choices=("docker", "local"), default="docker")
    parser.add_argument(
        "--contracts",
        default="BR-6.26,Si-6.26",
        help="Comma-separated contract allowlist for the proof profile",
    )
    parser.add_argument("--spark-master", default=DEFAULT_SPARK_MASTER, help="Spark master, e.g. local[2]")
    parser.add_argument("--output-json", default="", help="Optional JSON report output path")
    parser.add_argument("--docker-image", default=DEFAULT_DOCKER_IMAGE, help="Docker image tag for the Linux proof profile")
    parser.add_argument("--dockerfile", default=DEFAULT_DOCKERFILE.as_posix(), help="Dockerfile for the Linux proof profile")
    parser.add_argument(
        "--docker-runtime-root",
        default=DEFAULT_DOCKER_RUNTIME_ROOT,
        help="Absolute runtime root path inside docker container",
    )
    args = parser.parse_args()

    source_path = _resolve_repo_path(Path(args.source))
    output_dir = _resolve_repo_path(Path(args.output_dir))
    output_json = _resolve_repo_path(Path(args.output_json)) if args.output_json else None

    if args.profile == "docker":
        if output_json is None:
            raise RuntimeError("docker Spark proof requires output_json for deterministic evidence capture")
        runtime_root = _docker_runtime_root(args.docker_runtime_root)
        report = _run_in_docker(
            source=source_path,
            output_dir=output_dir,
            contracts=args.contracts,
            spark_master=args.spark_master,
            output_json=output_json,
            image=args.docker_image,
            dockerfile=_resolve_repo_path(Path(args.dockerfile)),
            runtime_root=runtime_root,
        )
    else:
        report = run_canonical_bars_spark_job(
            source_path=source_path,
            output_dir=output_dir,
            whitelist_contracts=_parse_contracts(args.contracts),
            spark_master=args.spark_master,
        )
        report["proof_profile"] = "local-spark"

    report_json = json.dumps(report, ensure_ascii=False, indent=2)
    if output_json is not None:
        output_path = output_json
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report_json + "\n", encoding="utf-8")
    print(report_json)


if __name__ == "__main__":
    main()
