"""Bounded branch/native acceptance launcher. No builds, retries or quota setup."""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import tarfile
import time
import uuid
from pathlib import Path

from staging_support import CleanupUncertain, budget_remaining, reserve, worker_lock
from validate_staging_acceptance import git, validate_result
from watch_task_status import observe

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = "deployment/docker/dagster-staging/docker-compose.proof.yml"


def settings(root=ROOT):
    path = Path(os.environ["TA3000_STAGING_CONFIG"]).resolve(strict=True)
    config = json.loads(path.read_text(encoding="utf-8"))
    keys = ("budget_home", "work_root", "acceptance_data", "fixture_root")
    paths = [Path(config[k]).resolve(strict=True) for k in keys]
    if not all(p.is_dir() for p in paths):
        raise ValueError("Worker roots must already exist")
    protected = [*paths, root.resolve(), path]
    if any(
        a == b or a in b.parents or b in a.parents
        for i, a in enumerate(protected)
        for b in protected[i + 1 :]
    ):
        raise ValueError("Worker roots/config/checkout must be separate")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", config["image_digest"]):
        raise ValueError("One locally available pinned image required")
    if not config.get("task_id") or not all(
        config.get(f"{k}_consumer") for k in ("branch", "acceptance")
    ):
        raise ValueError("Stable task and normal consumer request required")
    if not (paths[0] / "dagster.yaml").is_file():
        raise ValueError("Persistent native budget must be configured first")
    if (paths[2] / ".ta3000-acceptance-stage").read_text().strip() != "TA3000_ACCEPTANCE_STAGE":
        raise ValueError("Acceptance data is not a configured staging root")
    return {**config, **dict(zip(keys, paths, strict=True))}


def freeze(root, base, head, stage):
    if git(root, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError("Commit the candidate before staging")
    binding = dict(
        base_sha=git(root, "rev-parse", base),
        head_sha=git(root, "rev-parse", head),
        tree_sha=git(root, "rev-parse", "HEAD^{tree}"),
    )
    expected = (
        git(root, "merge-tree", "--write-tree", base, head).splitlines()[0]
        if stage == "acceptance"
        else git(root, "rev-parse", f"{head}^{{tree}}")
    )
    if expected != binding["tree_sha"]:
        raise ValueError("Checkout is not the frozen candidate/merge tree")
    return binding


def execute(plan, config, code, session, data):
    """One isolated native session; container TTL also survives host interruption."""
    control, home = session / "control", session / "dagster"
    control.mkdir()
    home.mkdir()
    plan_path = control / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    (home / "dagster.yaml").write_text(
        "telemetry:\n  enabled: false\nrun_retries:\n  enabled: false\n"
        "run_launcher:\n  module: trading_advisor_3000.product_plane.common.staging_execution\n"
        "  class: StagingRunLauncher\n",
        encoding="utf-8",
    )
    initial_bytes = tree_bytes(data)
    seconds = 600 if plan["stage"] == "branch" else 1800
    deadline = time.monotonic() + seconds
    env = {
        **os.environ,
        "STAGING_IMAGE": config["image_digest"],
        "STAGING_CODE": str(code),
        "STAGING_CONTROL": str(control),
        "STAGING_HOME": str(home),
        "STAGING_DATA": str(data),
        "STAGING_FIXTURE": str(config["fixture_root"]),
        "STAGING_SECONDS": str(seconds),
        "STAGING_INTERNAL": str(plan["stage"] == "branch").lower(),
    }
    compose = ["docker", "compose", "-f", str(code / COMPOSE), "-p", "proof-" + plan["root_run_id"]]

    def call(*args, cleanup=False):
        remaining = deadline - time.monotonic()
        if remaining <= 0 and not cleanup:
            raise TimeoutError("Staging elapsed-time limit exhausted")
        return subprocess.run(
            [*compose, *args],
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=60 if cleanup else min(120, remaining),
        ).stdout

    def read(*args):
        return json.loads(call("exec", "-T", "worker", "python", *args).splitlines()[-1])

    try:
        raw = call(
            "run",
            "--rm",
            "--no-deps",
            "worker",
            "python",
            "scripts/prepare_staging_plan.py",
            "/staging-control/plan.json",
        )
        plan = json.loads(raw.splitlines()[-1])
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        # Fixed host-only instance: never mounted into candidate containers.
        from dagster import DagsterInstance

        with DagsterInstance.from_config(str(config["budget_home"])) as instance:
            plan["attempt"] = reserve(
                instance, config["task_id"], plan["stage"], plan["root_run_id"], plan
            )
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        call("up", "-d", "--no-build", "--pull", "never", "worker")
        query = """query { repositoriesOrError { __typename ... on RepositoryConnection {
          nodes { name location { name } } } } }"""
        # Only server readiness is polled. Mutation is issued exactly once.
        while True:
            info = read("-c", graphql_code(query, {}, readiness=True))
            if info.get("data", {}).get("repositoriesOrError", {}).get("nodes"):
                break
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        repo = info["data"]["repositoriesOrError"]["nodes"][0]
        mutation = """mutation Launch($p: ExecutionParams!) { launchRun(executionParams: $p) {
          __typename ... on LaunchRunSuccess { run { runId } } } }"""
        params = dict(
            selector=dict(
                repositoryLocationName=repo["location"]["name"],
                repositoryName=repo["name"],
                jobName=plan["jobs"][0],
            ),
            runConfigData=plan["run_config"],
            executionMetadata=dict(runId=plan["root_run_id"]),
        )
        launched = read("-c", graphql_code(mutation, {"p": params}))
        result = launched.get("data", {}).get("launchRun", {})
        if result.get("run", {}).get("runId") != plan["root_run_id"]:
            raise ValueError("Native launch failed; no retry")

        def status():
            if (
                tree_bytes(data) + tree_bytes(home) - initial_bytes
                > (4 if plan["stage"] == "branch" else 8) * 1024**3
            ):
                raise ValueError("Staging output growth limit exceeded")
            return read(
                "scripts/collect_staging_acceptance.py", "status", "/staging-control/plan.json"
            )

        outcome = observe(
            status,
            fields=["status", "runs"],
            terminal={"success", "failed"},
            success={"success"},
            interval=5,
            timeout=max(0, deadline - time.monotonic()),
            emit=lambda value: print(json.dumps(value), flush=True),
            clock=time.monotonic,
            sleep=time.sleep,
        )
        if outcome:
            raise ValueError("Native job failed, incomplete or timed out; no retry")
        result = read(
            "scripts/collect_staging_acceptance.py", "collect", "/staging-control/plan.json"
        )
        if plan["stage"] == "acceptance":
            validate_result(
                result,
                base=plan["base_sha"],
                head=plan["head_sha"],
                tree=plan["tree_sha"],
                ci_run=plan["ci_run_id"],
                ci_attempt=plan["ci_attempt"],
            )
        return result
    finally:
        # down kills the entire JVM/native graph; files and spent attempts remain.
        try:
            call("down", "--timeout", "5", cleanup=True)
        except (OSError, subprocess.SubprocessError) as exc:
            raise CleanupUncertain(
                "Session cleanup unconfirmed; retain worker lock for recovery"
            ) from exc


def tree_bytes(root):
    return sum(p.stat().st_size for p in Path(root).rglob("*") if p.is_file())


def graphql_code(query, variables, readiness=False):
    payload = json.dumps(dict(query=query, variables=variables))
    code = (
        "import json,urllib.request; "
        f"r=urllib.request.Request('http://127.0.0.1:3000/graphql',data={payload.encode()!r},"
        "headers={'Content-Type':'application/json'}); "
        "print(urllib.request.urlopen(r,timeout=10).read().decode())"
    )
    if readiness:
        code = (
            "import urllib.error\ntry:\n " + code + '\nexcept urllib.error.URLError:\n print("{}")'
        )
    return code


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("stage", choices=("branch", "acceptance"))
    p.add_argument("--target", choices=("data", "research"), default="research")
    p.add_argument("--base-ref", default="origin/main")
    p.add_argument("--head-ref", default="HEAD")
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args(argv)
    if a.stage == "acceptance" and a.target != "research":
        p.error("Full acceptance requires Research and the normal consumer")
    if a.output.exists():
        p.error("Output already exists; evidence cannot be overwritten")
    try:
        config = settings()
        binding = freeze(ROOT, a.base_ref, a.head_ref, a.stage)
        from dagster import DagsterInstance

        with worker_lock(config["budget_home"]):
            with DagsterInstance.from_config(str(config["budget_home"])) as instance:
                budget_remaining(instance, config["task_id"], a.stage)
            session = config["work_root"] / str(uuid.uuid4())
            code, data = session / "code", config["acceptance_data"]
            code.mkdir(parents=True)
            if a.stage == "branch":
                data = session / "data"
                data.mkdir()
            archive = subprocess.check_output(
                [
                    "git",
                    "archive",
                    binding["tree_sha"],
                    "src",
                    "scripts",
                    "configs",
                    "deployment",
                    "pyproject.toml",
                ],
                cwd=ROOT,
            )
            with tarfile.open(fileobj=io.BytesIO(archive)) as source:
                source.extractall(code, filter="data")
            plan = {
                **binding,
                "stage": a.stage,
                "target": a.target,
                "root_run_id": session.name,
                "image_digest": config["image_digest"],
                "fixture_sha256": config["fixture_sha256"],
                "consumer": config[f"{a.stage}_consumer"],
                "ci_run_id": os.getenv("GITHUB_RUN_ID", "local"),
                "ci_attempt": os.getenv("GITHUB_RUN_ATTEMPT", "1"),
            }
            result = execute(plan, config, code, session, data)
            a.output.parent.mkdir(parents=True, exist_ok=True)
            with a.output.open("x", encoding="utf-8") as handle:
                json.dump(result, handle, indent=2)
            print(f"Staging PASS: {a.output}")
            return 0
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as exc:
        print(f"Staging STOP: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
