from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as cli
from staging_support import LEGACY_TASK, initialize_budget
from validate_pr_evidence import check_pr_evidence, validate_result

JOBS = ("branch_job",)


@pytest.fixture
def native_budget(tmp_path, monkeypatch):
    DagsterInstance = pytest.importorskip("dagster").DagsterInstance

    budget = tmp_path / "budget"
    budget.mkdir()
    with DagsterInstance.local_temp(str(budget)) as instance:
        initialize_budget(
            instance, "test-only", dict(branch=2, acceptance=1), dict(branch=0, acceptance=0)
        )
        monkeypatch.setattr(DagsterInstance, "from_config", lambda *a, **k: nullcontext(instance))
        yield instance


@pytest.fixture
def session(tmp_path):
    for name in ("code", "session", "data", "fixture"):
        (tmp_path / name).mkdir()
    return tmp_path


def run_mock_session(monkeypatch, session, instance, outcome="SUCCESS"):
    from dagster import RunsFilter

    calls = []
    clock = [0]
    monkeypatch.setattr(cli.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(cli.time, "sleep", lambda _: None)
    plan = dict(stage="branch", target="data", root_run_id="native-root")
    prepared = dict(plan, jobs=[JOBS[0]], run_config={})
    config = dict(
        budget_home=session / "budget",
        task_id="test-only",
        fixture_root=session / "fixture",
        image_digest="sha256:" + "a" * 64,
    )

    def fake_run(command, **kwargs):
        assert command[:2] == ["docker", "compose"]  # intercepted; never executed
        calls.append(command)
        if "down" in command and outcome == "cleanup-error":
            raise subprocess.CalledProcessError(1, command)
        if "run" in command:
            answer = prepared
        elif "up" in command:
            assert instance.get_runs_count(filters=RunsFilter(job_name="staging_attempt")) == 1
            answer = {}
        elif "-c" in command:
            if "mutation Launch" in command[-1]:
                answer = {"data": {"launchRun": {"run": {"runId": "native-root"}}}}
                if outcome == "launch-error":
                    answer = {"data": {"launchRun": {"__typename": "PythonError"}}}
            else:
                answer = {
                    "data": {
                        "repositoriesOrError": {
                            "nodes": [dict(name="repo", location={"name": "location"})]
                        }
                    }
                }
        elif "status" in command:
            answer = {
                "status": "SUCCESS" if outcome in {"SUCCESS", "cleanup-error"} else "FAILED",
                "runs": {},
            }
            if outcome == "timeout":
                answer["status"] = "PENDING"
                clock[0] = 601
        elif "collect" in command:
            answer = dict(status="PASS", stage="branch")
        else:
            answer = {}
        assert kwargs["timeout"] <= 120
        return SimpleNamespace(stdout=json.dumps(answer))

    monkeypatch.setattr(cli.subprocess, "run", fake_run)
    args = (plan, config, session / "code", session / "session", session / "data")
    if outcome == "SUCCESS":
        assert cli.execute(*args)["status"] == "PASS"
    else:
        with pytest.raises((ValueError, OSError)):
            cli.execute(*args)
    assert sum("mutation Launch" in x[-1] for x in calls) == 1
    assert "down" in calls[-1]
    assert instance.get_runs_count(filters=RunsFilter(job_name="staging_attempt")) == 1
    if outcome in {"FAILED", "launch-error", "timeout"}:
        assert not any("collect" in c for c in calls)
    return calls


@pytest.mark.parametrize(
    "outcome", ["SUCCESS", "FAILED", "launch-error", "timeout", "cleanup-error"]
)
def test_one_launch_after_reservation_and_terminal_cleanup(
    monkeypatch, session, native_budget, outcome
):
    run_mock_session(monkeypatch, session, native_budget, outcome)


def test_exhausted_legacy_budget_stops_cli_before_any_docker(tmp_path, monkeypatch):
    DagsterInstance = pytest.importorskip("dagster").DagsterInstance

    home = tmp_path / "budget"
    home.mkdir()
    with DagsterInstance.local_temp(str(home)) as instance:
        initialize_budget(
            instance, LEGACY_TASK, dict(branch=3, acceptance=1), dict(branch=3, acceptance=0)
        )
        monkeypatch.setattr(DagsterInstance, "from_config", lambda *a, **k: nullcontext(instance))
        monkeypatch.setattr(cli, "settings", lambda: dict(budget_home=home, task_id=LEGACY_TASK))
        monkeypatch.setattr(cli, "freeze", lambda *a: {})
        monkeypatch.setattr(cli.subprocess, "run", lambda *a, **k: pytest.fail("Docker invoked"))
        assert cli.main(["branch", "--output", str(tmp_path / "proof.json")]) == 2
        assert not (tmp_path / "proof.json").exists()


def test_freeze_rejects_another_merge_tree(monkeypatch):
    monkeypatch.setattr(
        cli,
        "git",
        lambda root, *a: "" if a[0] == "status" else "wrong" if a[0] == "merge-tree" else "tree",
    )
    with pytest.raises(ValueError, match="frozen"):
        cli.freeze(Path.cwd(), "base", "head", "acceptance")


def test_ready_poll_only_catches_connection_failure(monkeypatch):
    import urllib.error
    import urllib.request

    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *a, **k: (_ for _ in ()).throw(urllib.error.URLError("not ready")),
    )
    exec(cli.graphql_code("query { repositoriesOrError { __typename } }", {}, readiness=True), {})
    with pytest.raises(urllib.error.URLError):
        exec(cli.graphql_code("mutation Launch", {}), {})


def test_pr_gate_git_failure_is_not_empty_scope(monkeypatch):
    import validate_pr_evidence as gate

    monkeypatch.setattr(
        gate, "git", lambda *a: (_ for _ in ()).throw(subprocess.CalledProcessError(1, "git"))
    )
    assert check_pr_evidence([], base_ref="missing", head_ref="head") == 2


def test_receipt_from_prior_ci_attempt_rejected():
    # Rejected before route inspection; rerunning a CI gate cannot resurrect an old artifact.
    value = dict(
        version=1,
        evidence_level="native",
        stage="acceptance",
        status="PASS",
        base_sha="a",
        head_sha="b",
        tree_sha="c",
        ci_run_id="42",
        ci_attempt="1",
    )
    with pytest.raises(ValueError, match="another CI attempt"):
        validate_result(value, base="a", head="b", tree="c", ci_run="42", ci_attempt="2")


def test_compose_bounds_whole_container_and_hides_host_counter():
    source = Path(__file__).resolve().parents[2] / cli.COMPOSE
    config = yaml.safe_load(source.read_text(encoding="utf-8-sig"))
    worker = config["services"]["worker"]
    assert worker["entrypoint"] == [
        "timeout",
        "--signal=KILL",
        "${STAGING_SECONDS:?time limit required}",
    ]
    assert worker["restart"] == "no" and worker["mem_limit"] == "9g"
    assert worker["pull_policy"] == "never" and "build" not in worker
    assert "product_plane.common.staging_execution" in worker["command"][-1]
    assert config["networks"]["proof"]["internal"] == "${STAGING_INTERNAL:?}"
    volumes = worker["volumes"]
    assert all(
        v.get("read_only")
        for v in volumes
        if v["target"] in {"/workspace", "/staging-control", "/staging-control/fixture"}
    )
    assert not any("budget" in str(v) or "docker.sock" in str(v) for v in volumes)


def test_ci_consumes_only_current_successful_native_job(tmp_path):
    path = Path(__file__).resolve().parents[2] / ".github/workflows/ci.yml"
    jobs = yaml.safe_load(path.read_text())["jobs"]
    native, lane = jobs["native-acceptance"], jobs["pr-lane"]
    assert "ta3000-staging" in native["runs-on"]
    assert "needs.code-checks.result == 'success'" in native["if"]
    assert "needs.code-checks.outputs.evidence_level == 'native'" in native["if"]
    assert (
        jobs["pr-lane"]["steps"][0]["env"]["EVIDENCE_LEVEL"]
        == "${{ needs.code-checks.outputs.evidence_level }}"
    )
    assert any(
        "scripts/run_boring_checks.py" in step.get("run", "")
        for step in jobs["code-checks"]["steps"]
    )
    install = next(
        s
        for s in jobs["code-checks"]["steps"]
        if s.get("name") == "Install code-check dependencies"
    )
    assert '"dagster>=1.8,<2"' in install["run"]
    selector = next(s for s in jobs["code-checks"]["steps"] if s.get("id") == "scope")
    assert jobs["code-checks"]["steps"].index(install) < jobs["code-checks"]["steps"].index(
        selector
    )
    producer_test = next(
        s
        for s in jobs["code-checks"]["steps"]
        if s.get("name") == "Check the configured native producer contract"
    )
    assert "if" not in producer_test
    assert "test_staging_acceptance.py" in producer_test["run"]
    assert native["concurrency"]["cancel-in-progress"] is False
    assert set(lane["needs"]) == {"code-checks", "native-acceptance"}
    guard = lane["steps"][0]
    assert guard["env"]["NATIVE_RESULT"] == "${{ needs.native-acceptance.result }}"
    # Execute the real guard for every terminal job conclusion; no Actions/network/runtime.
    bash = next(
        (p for p in (Path("C:/Program Files/Git/bin/bash.exe"), Path("/bin/bash")) if p.is_file()),
        None,
    )
    if bash is None:
        pytest.fail("Git Bash required for actual CI guard test")
    script = tmp_path / "guard.sh"
    script.write_text("set -e\n" + guard["run"])
    for required, result, expected in [
        ("native", "success", 0),
        ("code", "skipped", 0),
        ("native", "skipped", 1),
        ("native", "failure", 1),
        ("native", "cancelled", 1),
        ("native", "", 1),
    ]:
        actual = subprocess.run(
            [str(bash), str(script)],
            env={
                **os.environ,
                "SCOPE_RESULT": "success",
                "EVIDENCE_LEVEL": required,
                "NATIVE_RESULT": result,
            },
            capture_output=True,
        )
        assert (actual.returncode == 0) is (expected == 0)
    download = next(
        s for s in lane["steps"] if s.get("uses", "").startswith("actions/download-artifact@")
    )
    upload = next(
        s for s in native["steps"] if s.get("uses", "").startswith("actions/upload-artifact@")
    )
    assert (
        download["with"]["name"]
        == upload["with"]["name"]
        == "staging-acceptance-${{ github.run_attempt }}"
    )
    assert not any(k in download["with"] for k in ("run-id", "github-token", "repository"))
    launch = next(s for s in native["steps"] if "run" in s)
    assert "GITHUB_RUN_ATTEMPT -ne '1'" in launch["run"]
    gate = next(s for s in lane["steps"] if s.get("name") == "PR Gate")
    assert "--evidence-receipt artifacts/ci/native/staging-acceptance.json" in gate["run"]


def test_unconfirmed_cleanup_keeps_host_lock(tmp_path):
    from staging_support import CleanupUncertain, worker_lock

    with pytest.raises(CleanupUncertain):
        with worker_lock(tmp_path):
            raise CleanupUncertain("container termination unknown")
    assert (tmp_path / "staging-worker.lock").is_file()
    with pytest.raises(FileExistsError):
        with worker_lock(tmp_path):
            pytest.fail("another proof entered an uncertain session")


def test_archive_uses_verified_tree_even_if_head_moves(monkeypatch, session, native_budget):
    import io
    import tarfile

    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w"):
        pass
    monkeypatch.setattr(
        cli, "freeze", lambda *a: dict(base_sha="base", head_sha="pr-head", tree_sha="frozen-tree")
    )
    monkeypatch.setattr(
        cli,
        "settings",
        lambda: dict(
            budget_home=session / "budget",
            task_id="test-only",
            work_root=session,
            acceptance_data=session / "data",
            fixture_sha256="hash",
            branch_consumer={},
            image_digest="sha256:" + "a" * 64,
        ),
    )
    commands = []

    def capture(command, **kwargs):
        commands.append(command)
        assert command[2] == "frozen-tree"  # mutable HEAD could now be any other commit
        return archive.getvalue()

    monkeypatch.setattr(cli.subprocess, "check_output", capture)
    monkeypatch.setattr(cli, "execute", lambda *a: dict(status="PASS"))
    assert cli.main(["branch", "--output", str(session / "result.json")]) == 0
    assert len(commands) == 1


@pytest.mark.parametrize("bad", ["missing-marker", "overlap", "image"])
def test_worker_config_rejects_unmarked_or_overlapping_data_roots(tmp_path, monkeypatch, bad):
    config = dict(
        task_id="fixed",
        image_digest="sha256:" + "a" * 64,
        branch_consumer={"dataset_version": "fixture"},
        acceptance_consumer={"dataset_version": "current"},
    )
    for key in ("budget_home", "work_root", "acceptance_data", "fixture_root"):
        value = tmp_path / key
        value.mkdir()
        config[key] = str(value)
    (Path(config["budget_home"]) / "dagster.yaml").write_text("{}")
    if bad != "missing-marker":
        (Path(config["acceptance_data"]) / ".ta3000-acceptance-stage").write_text(
            "TA3000_ACCEPTANCE_STAGE"
        )
    if bad == "overlap":
        config["acceptance_data"] = config["budget_home"]
    if bad == "image":
        config["image_digest"] = "latest"
    path = tmp_path / "external-config.json"
    path.write_text(json.dumps(config))
    monkeypatch.setenv("TA3000_STAGING_CONFIG", str(path))
    with pytest.raises((ValueError, OSError)):
        cli.settings()


def test_draft_pr_cannot_spend_native_acceptance_budget():
    path = Path(__file__).resolve().parents[2] / ".github/workflows/ci.yml"
    condition = yaml.safe_load(path.read_text())["jobs"]["native-acceptance"]["if"]
    terms = condition.removeprefix("${{ ").removesuffix(" }}").split(" && ")
    assert "github.event.pull_request.draft == false" in terms


def test_proof_definitions_preserve_native_graph_and_disable_executor_retries():
    for dependency in ("dagster", "pyarrow", "deltalake"):
        pytest.importorskip(dependency)
    from trading_advisor_3000.dagster_defs.product_plane_definitions import (
        product_plane_definitions as native,
    )
    from trading_advisor_3000.product_plane.common.staging_execution import (
        product_plane_definitions as proof,
    )

    for field in ("assets", "jobs", "sensors", "schedules"):
        assert all(
            a is b for a, b in zip(getattr(proof, field), getattr(native, field), strict=True)
        )
    assert native.executor is None
    assert proof.resources == (native.resources or {})
    expected = {"max_concurrent": 1, "retries": {"disabled": {}}}
    for name in (
        "moex_data_rebuild_job",
        "moex_baseline_update_job",
        "moex_cf_catch_up_job",
        "research_data_prep_job",
    ):
        executor = proof.get_job_def(name).executor_def
        result = executor.apply_config_mapping({"config": {}})
        assert result.success and result.value == {"config": expected}
