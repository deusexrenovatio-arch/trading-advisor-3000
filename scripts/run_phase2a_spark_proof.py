from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

from trading_advisor_3000.spark_jobs import DEFAULT_SPARK_MASTER, run_canonical_bars_spark_job


DEFAULT_FIXTURE = Path("tests/app/fixtures/data_plane/raw_backfill_sample.jsonl")
DEFAULT_OUTPUT_DIR = Path(".tmp/phase2a-spark-proof")
DEFAULT_DOCKER_IMAGE = "ta3000-phase-proof:latest"
DEFAULT_DOCKERFILE = Path("deployment/docker/phase-proofs/Dockerfile")


def _parse_contracts(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _resolve_repo_path(path: Path) -> Path:
    return path if path.is_absolute() else (_repo_root() / path).resolve()


def _container_path(path: Path) -> str:
    repo_root = _repo_root().resolve()
    resolved = _resolve_repo_path(path)
    try:
        relative = resolved.relative_to(repo_root)
    except ValueError as exc:
        raise RuntimeError(
            f"path must stay inside repo for docker proof: {resolved.as_posix()}"
        ) from exc
    return (Path("/workspace") / relative).as_posix()


def _hostify_container_path(value: str) -> str:
    prefix = "/workspace/"
    if not value.startswith(prefix):
        return value
    relative = value[len(prefix) :].replace("/", "\\")
    return str((_repo_root() / relative).resolve())


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
        raise RuntimeError(f"docker build failed for Spark proof image `{image}`")


def _docker_user_args() -> list[str]:
    getuid = getattr(os, "getuid", None)
    getgid = getattr(os, "getgid", None)
    if not callable(getuid) or not callable(getgid):
        return []
    return ["--user", f"{getuid()}:{getgid()}"]


def _run_in_docker(
    *,
    source: Path,
    output_dir: Path,
    contracts: str,
    spark_master: str,
    output_json: Path | None,
    image: str,
    dockerfile: Path,
) -> dict[str, object]:
    _ensure_docker_image(image, dockerfile)

    repo_root = _repo_root().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)

    command = [
        "docker",
        "run",
        "--rm",
        *_docker_user_args(),
        "-v",
        f"{repo_root}:/workspace",
        "-w",
        "/workspace",
        "-e",
        "PYTHONPATH=/workspace/src",
        image,
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
    args = parser.parse_args()

    source_path = _resolve_repo_path(Path(args.source))
    output_dir = _resolve_repo_path(Path(args.output_dir))
    output_json = _resolve_repo_path(Path(args.output_json)) if args.output_json else None

    if args.profile == "docker":
        report = _run_in_docker(
            source=source_path,
            output_dir=output_dir,
            contracts=args.contracts,
            spark_master=args.spark_master,
            output_json=output_json,
            image=args.docker_image,
            dockerfile=_resolve_repo_path(Path(args.dockerfile)),
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
