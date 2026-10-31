from __future__ import annotations

import copy
import io
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from gate_common import collect_changed_files  # noqa: E402
from validate_architecture_review import (  # noqa: E402
    decision_digest,
    load_review_body,
    read_decision_record,
    read_standard,
    scope_digest,
    validate_review,
)

RECORD_PATH = "docs/architecture/adr/0003-durable-decision-records.md"


def body(record: dict) -> str:
    return "Existing task discussion\n```architecture-review\n" + json.dumps(record) + "\n```\n"


def record(binding: str = "current-scope", *, standard=None) -> dict:
    standard = standard or read_standard(ROOT)
    decision = {
        "owner": "shell",
        "accepted_contract": "Existing PR gate is the only merge lane",
        "change": "Consume architecture review from the existing PR body",
        "reuse": "Reuse gate runners; no additional status registry",
        "cost": "One parse and scoped Git content hashes",
        "compatibility": "Diagnostic loop stays usable without admission evidence",
        "validation": "Real Git scope -> body -> existing gate consumer; reject stale input",
        "standard": standard.pin,
        "record": read_decision_record(ROOT, RECORD_PATH),
    }
    review = {
        "reviewer": "independent-reviewer",
        "verdict": "PASS",
        "findings": [],
        "evidence": ["Actual scoped diff and producer readback"],
        "criteria": [
            {
                "id": identifier,
                "status": "satisfied",
                "evidence": "Unit fixture; review evidence input",
            }
            for identifier in standard.criteria
        ],
    }
    return {
        "impact": "contract-change",
        "author": "implementation-author",
        "reason": "Changes the delivery admission contract",
        "scope_digest": binding,
        "decision": decision,
        "design": {**copy.deepcopy(review), "decision_digest": decision_digest(decision)},
        "implementation": {
            **copy.deepcopy(review),
            "decision_digest": decision_digest(decision),
            "producer_proof": {
                "kind": "real",
                "producer": "git diff and exported task discussion",
                "consumer": "PR gate architecture validator",
                "evidence": "Actual scoped invocation and negative readback",
            },
        },
    }


def errors(value: dict | None, phase: str = "implementation", binding="current-scope", repo=ROOT):
    return validate_review(
        None if value is None else body(value), phase=phase, expected_scope=binding, repo_root=repo
    )


def test_ordinary_accepted_implementation_needs_no_spec_or_reviewer():
    value = {
        "impact": "none",
        "author": "author",
        "reason": "Reuse the accepted owner and contract; change formatting only",
        "scope_digest": "current-scope",
    }
    assert errors(value) == []


@pytest.mark.parametrize("impact", ["within-contract", "contract-change"])
@pytest.mark.parametrize("phase", ["design", "implementation"])
def test_admission_requires_a_durable_decision_record(impact, phase):
    value = record()
    value["impact"] = impact
    value["decision"].pop("record", None)
    assert any("decision.record" in item for item in errors(value, phase=phase))


def test_none_does_not_read_an_optional_record_or_standard(tmp_path, monkeypatch):
    import validate_architecture_review as validator

    def forbidden(*args, **kwargs):
        pytest.fail("ordinary implementation must not depend on architecture documents")

    monkeypatch.setattr(validator, "read_decision_record", forbidden)
    monkeypatch.setattr(validator, "read_standard", forbidden)
    value = {
        "impact": "none",
        "author": "author",
        "reason": "Accepted formatting rule reused",
        "scope_digest": "current-scope",
    }
    assert errors(value, repo=tmp_path) == []
    assert errors(value, phase="design", repo=tmp_path) == []


@pytest.mark.parametrize("phase", ["design", "implementation"])
def test_record_pin_participates_in_both_review_bindings(phase):
    value = record()
    original = decision_digest(value["decision"])
    value["decision"]["record"]["digest"] = "another-revision"
    assert decision_digest(value["decision"]) != original
    assert any(f"{phase}.decision_digest" in item for item in errors(value, phase=phase))


def test_code_only_boundary_change_requires_both_independent_reviews():
    value = record()
    del value["design"]
    assert any("design" in item for item in errors(value))
    value = record()
    del value["implementation"]
    assert any("implementation" in item for item in errors(value))


def test_design_admission_does_not_require_a_future_producer_artifact():
    value = record()
    del value["implementation"]
    del value["scope_digest"]
    assert errors(value, phase="design") == []


@pytest.mark.parametrize("phase", ["design", "implementation"])
def test_self_review_cannot_supply_independent_acceptance(phase):
    value = record()
    value[phase]["reviewer"] = " Implementation-Author "
    assert any("independent" in item for item in errors(value))


def test_changed_decision_invalidates_old_design_acceptance():
    value = record()
    value["decision"]["change"] = "Introduce a second canonical status database"
    assert any("decision_digest" in item for item in errors(value))


def test_new_design_cannot_reuse_implementation_review_of_an_old_decision():
    value = record()
    value["decision"]["validation"] = "New acceptance criteria and recovery requirement"
    value["design"]["decision_digest"] = decision_digest(value["decision"])
    assert any("implementation.decision_digest" in item for item in errors(value))


@pytest.mark.parametrize("verdict", ["CHANGES_REQUIRED", "BLOCKED", "UNKNOWN"])
def test_non_pass_does_not_close_review(verdict):
    value = record()
    value["implementation"]["verdict"] = verdict
    assert any("verdict" in item for item in errors(value))


def test_every_open_applicable_finding_blocks_even_with_pass_verdict():
    value = record()
    value["design"]["findings"] = [{"id": "F1", "status": "open", "evidence": "Named caller"}]
    assert any("open" in item for item in errors(value))
    value["design"]["findings"][0]["status"] = "closed"
    assert errors(value) == []


def test_fixtures_cannot_prove_real_contract_boundary_closure():
    value = record()
    value["implementation"]["producer_proof"]["kind"] = "synthetic"
    assert any("real" in item for item in errors(value))


def test_within_contract_decision_does_not_require_a_changed_boundary_example():
    value = record()
    value["impact"] = "within-contract"
    del value["implementation"]["producer_proof"]
    assert errors(value) == []


@pytest.mark.parametrize(
    "text",
    [None, "No classification", "```architecture-review\nnot-json\n```", body(record()) * 2],
)
def test_missing_malformed_or_ambiguous_evidence_fails_closed(text):
    assert validate_review(text, phase="implementation", expected_scope="current-scope")


def test_scope_change_invalidates_implementation_pass():
    assert any("scope_digest" in item for item in errors(record(), binding="changed-scope"))


def test_pr_event_body_is_data_not_a_command(tmp_path):
    event = tmp_path / "event.json"
    text = body(record()) + "\n$(do-not-execute)"
    event.write_text(json.dumps({"pull_request": {"body": text}}), encoding="utf-8")
    assert load_review_body(environ={"GITHUB_EVENT_PATH": str(event)}) == text


def test_local_export_overrides_inherited_pr_event(tmp_path):
    export = tmp_path / "body.md"
    export.write_text(body(record()), encoding="utf-8")
    assert load_review_body(str(export), environ={"GITHUB_EVENT_PATH": "missing"}) == body(record())


def git(repo, *args):
    result = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True)
    return result.stdout.strip()


@pytest.fixture
def repository(tmp_path):
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.name", "Test Producer")
    git(tmp_path, "config", "user.email", "producer@example.invalid")
    git(tmp_path, "config", "core.autocrlf", "false")
    target = tmp_path / "boundary.py"
    target.write_text("VERSION = 1\n", encoding="utf-8")
    standard_path = tmp_path / "docs/architecture/product-plane/architecture-standard.md"
    standard_path.parent.mkdir(parents=True)
    shutil.copy2(ROOT / "docs/architecture/product-plane/architecture-standard.md", standard_path)
    record_path = tmp_path / RECORD_PATH
    record_path.parent.mkdir(parents=True)
    shutil.copy2(ROOT / RECORD_PATH, record_path)
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "initial producer")
    return tmp_path


def test_real_git_producer_binding_rejects_stale_worktree_and_scope(repository):
    binding = scope_digest(repository, ["boundary.py"])
    assert errors(record(binding), binding=binding) == []
    (repository / "boundary.py").write_text("VERSION = 2\n", encoding="utf-8")
    assert errors(record(binding), binding=scope_digest(repository, ["boundary.py"]))
    assert binding != scope_digest(repository, ["boundary.py", "removed.py"])


def test_immutable_pr_binding_reads_head_not_merge_checkout(repository):
    base = git(repository, "rev-parse", "HEAD")
    (repository / "boundary.py").write_text("VERSION = 2\n", encoding="utf-8")
    git(repository, "commit", "-qam", "reviewed head")
    head = git(repository, "rev-parse", "HEAD")
    expected = scope_digest(repository, ["boundary.py"], base_ref=base, head_ref=head)
    (repository / "boundary.py").write_text("UNRELATED_MERGE_RESULT = True\n", encoding="utf-8")
    assert scope_digest(repository, ["boundary.py"], base_ref=base, head_ref=head) == expected
    assert scope_digest(repository, ["boundary.py"], base_ref=base, head_ref=base) != expected


def test_binding_handles_added_deleted_and_line_ending_normalization(repository):
    before = scope_digest(repository, ["boundary.py", "new.py"])
    (repository / "new.py").write_text("NEW = True\n", encoding="utf-8")
    assert scope_digest(repository, ["boundary.py", "new.py"]) != before
    (repository / "boundary.py").unlink()
    assert scope_digest(repository, ["boundary.py"]) != before
    git(repository, "config", "core.autocrlf", "true")
    (repository / "boundary.py").write_bytes(b"VERSION = 1\r\n")
    crlf = scope_digest(repository, ["boundary.py"])
    (repository / "boundary.py").write_bytes(b"VERSION = 1\n")
    assert crlf == scope_digest(repository, ["boundary.py"])


@pytest.mark.parametrize("path", ["../outside.py", "/absolute.py", "C:/outside.py"])
def test_binding_rejects_paths_outside_repository(repository, path):
    with pytest.raises(ValueError):
        scope_digest(repository, [path])


def test_binding_rejects_unknown_git_ref(repository):
    with pytest.raises(ValueError):
        scope_digest(repository, ["boundary.py"], base_ref="missing-ref", head_ref="HEAD")


def test_empty_scope_cannot_hide_invalid_or_partial_ref_pair(repository):
    with pytest.raises(ValueError):
        scope_digest(repository, [], base_ref="missing-ref", head_ref="HEAD")
    with pytest.raises(ValueError):
        scope_digest(repository, [], base_ref="HEAD")


def test_cli_real_producer_to_validator_roundtrip(repository):
    target = repository / "body.md"
    digest = scope_digest(repository, ["boundary.py"])
    target.write_text(body(record(digest)), encoding="utf-8")
    command = [
        sys.executable,
        str(ROOT / "scripts/validate_architecture_review.py"),
        "--changed-files",
        "boundary.py",
        "--architecture-review-source",
        str(target),
    ]
    result = subprocess.run(command, cwd=repository, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    (repository / "boundary.py").write_text("VERSION = 3\n", encoding="utf-8")
    result = subprocess.run(command, cwd=repository, capture_output=True, text=True)
    assert result.returncode != 0
    assert "scope_digest" in result.stdout


def test_existing_pr_gate_rejects_code_only_change_without_classification():
    env = os.environ.copy()
    env["AI_SHELL_BORING_SKIP_FAST_TESTS"] = "1"
    env.pop("AI_SHELL_ARCHITECTURE_REVIEW_SOURCE", None)
    env.pop("GITHUB_EVENT_PATH", None)
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_pr_gate.py",
            "--changed-files",
            "scripts/run_loop_gate.py",
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "none",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 2
    assert "architecture review" in result.stdout
    assert "classification" in result.stdout


@pytest.fixture
def gate_repository(repository):
    scripts = repository / "scripts"
    scripts.mkdir()
    for name in (
        "compute_change_surface.py",
        "gate_common.py",
        "gate_marker_contract.py",
        "gate_reuse.py",
        "run_loop_gate.py",
        "run_pr_gate.py",
        "run_nightly_gate.py",
        "validate_architecture_review.py",
        "validate_pr_evidence.py",
    ):
        shutil.copy2(ROOT / "scripts" / name, scripts / name)
    (repository / "configs").mkdir()
    (repository / "configs/change_surface_mapping.yaml").write_text(
        "surface_priority: [governance]\n"
        "surfaces:\n  governance:\n    prefixes: [boundary.py]\n"
        "command_profiles:\n  loop:\n    default:\n"
        '      - "{python} -c \\"print(\'loop-check-executed\')\\""\n'
        "  pr:\n    default:\n"
        '      - "{python} -c \\"print(\'pr-check-executed\')\\""\n'
        "  nightly:\n    default:\n"
        '      - "{python} -c \\"print(\'nightly-check-executed\')\\""\n',
        encoding="utf-8",
    )
    (repository / ".gitignore").write_text("__pycache__/\n.tmp/\n", encoding="utf-8")
    git(repository, "add", ".")
    git(repository, "commit", "-qm", "gate runtime")
    base = git(repository, "rev-parse", "HEAD")
    (repository / "boundary.py").write_text("VERSION = 2\n", encoding="utf-8")
    git(repository, "commit", "-qam", "actual producer revision")
    return repository, base, git(repository, "rev-parse", "HEAD")


def run_gate(repo, name, *args, env=None, stdin=None):
    environment = os.environ.copy()
    for key in (
        "PYTHONPATH",
        "AI_SHELL_ARCHITECTURE_REVIEW_SOURCE",
        "GITHUB_EVENT_PATH",
        "GITHUB_EVENT_NAME",
        "GITHUB_ACTIONS",
        "GITHUB_REF",
    ):
        environment.pop(key, None)
    environment.update(env or {})
    return subprocess.run(
        [
            sys.executable,
            f"scripts/{name}.py",
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "none",
            *args,
        ],
        cwd=repo,
        capture_output=True,
        text=True,
        input=stdin,
        env=environment,
    )


def test_existing_nested_gates_consume_real_review_export_and_immutable_scope(gate_repository):
    repo, base, head = gate_repository
    digest = scope_digest(repo, ["boundary.py"], base_ref=base, head_ref=head)
    export = repo / ".tmp" / "review with spaces.md"
    export.parent.mkdir()
    export.write_text(body(record(digest)), encoding="utf-8")
    result = run_gate(
        repo,
        "run_pr_gate",
        "--base-ref",
        base,
        "--head-ref",
        head,
        "--stdin",
        "--architecture-review-source",
        str(export),
        stdin="boundary.py\n",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "phase=implementation" in result.stdout
    assert "phase=design" in result.stdout
    assert "loop-check-executed" in result.stdout
    assert "pr-check-executed" in result.stdout
    assert "pr gate: OK" in result.stdout


def test_hosted_pr_event_is_consumed_by_actual_pr_gate(gate_repository):
    repo, base, head = gate_repository
    digest = scope_digest(repo, ["boundary.py"], base_ref=base, head_ref=head)
    event = repo / ".tmp" / "event.json"
    event.parent.mkdir()
    event.write_text(json.dumps({"pull_request": {"body": body(record(digest))}}), encoding="utf-8")
    result = run_gate(
        repo,
        "run_pr_gate",
        "--base-ref",
        base,
        "--head-ref",
        head,
        env={"GITHUB_EVENT_PATH": str(event)},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "phase=implementation" in result.stdout


def test_clean_hosted_nightly_reuses_checks_as_post_merge_hygiene(gate_repository):
    repo, base, head = gate_repository
    summary = repo / ".tmp" / "nightly.md"
    result = run_gate(
        repo,
        "run_nightly_gate",
        "--base-ref",
        base,
        "--head-ref",
        head,
        "--summary-file",
        str(summary),
        env={
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "schedule",
            "GITHUB_REF": "refs/heads/main",
        },
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "post-merge hygiene; architecture admission not evaluated" in result.stdout
    assert "architecture admission not evaluated" in summary.read_text(encoding="utf-8")
    for name in ("loop", "pr", "nightly"):
        assert f"{name}-check-executed" in result.stdout


@pytest.mark.parametrize(
    "mode", ["local", "event-local", "false-marker", "dirty-hosted", "explicit-hosted", "pr-cli"]
)
def test_post_merge_hygiene_cannot_bypass_admission_for_other_contexts(gate_repository, mode):
    repo, base, head = gate_repository
    extra = []
    env = {
        "GITHUB_ACTIONS": "true",
        "GITHUB_EVENT_NAME": "schedule",
        "GITHUB_REF": "refs/heads/main",
    }
    if mode == "local":
        env = {}
    if mode == "event-local":
        env.pop("GITHUB_ACTIONS")
    if mode == "false-marker":
        env["GITHUB_ACTIONS"] = "false"
    if mode == "dirty-hosted":
        (repo / "boundary.py").write_text("VERSION = 3\n", encoding="utf-8")
    if mode == "explicit-hosted":
        extra = ["--changed-files", "boundary.py"]
    name = "run_pr_gate" if mode == "pr-cli" else "run_nightly_gate"
    result = run_gate(repo, name, "--base-ref", base, "--head-ref", head, *extra, env=env)
    assert result.returncode == 2, result.stdout + result.stderr
    assert "classification" in result.stdout
    assert "post-merge hygiene" not in result.stdout


def test_local_nightly_forwards_explicit_review_source(gate_repository):
    repo, _, _ = gate_repository
    export = repo / ".tmp" / "body.md"
    export.parent.mkdir()
    digest = scope_digest(repo, ["boundary.py"])
    export.write_text(body(record(digest)), encoding="utf-8")
    result = run_gate(
        repo,
        "run_nightly_gate",
        "--changed-files",
        "boundary.py",
        "--architecture-review-source",
        str(export),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "phase=implementation" in result.stdout
    assert "nightly-check-executed" in result.stdout


@pytest.mark.parametrize("name", ["run_loop_gate", "run_pr_gate", "run_nightly_gate"])
def test_gate_git_collection_error_is_not_an_empty_change(gate_repository, name):
    repo, _, head = gate_repository
    result = run_gate(repo, name, "--base-ref", "missing-ref", "--head-ref", head)
    assert result.returncode == 2
    assert "Git diff collection failed" in result.stdout


@pytest.mark.parametrize("name", ["run_pr_gate", "run_nightly_gate"])
def test_default_scope_cannot_hide_working_tree_change(gate_repository, name):
    repo, _, _ = gate_repository
    (repo / "boundary.py").write_text("VERSION = 3\n", encoding="utf-8")
    result = run_gate(repo, name)
    assert result.returncode == 2, result.stdout + result.stderr
    assert "classification" in result.stdout


def test_git_collection_preserves_unicode_names_for_freshness(repository, monkeypatch):
    target = repository / "граница.py"
    target.write_text("VERSION = 1\n", encoding="utf-8")
    git(repository, "add", ".")
    git(repository, "commit", "-qm", "unicode boundary")
    target.write_text("VERSION = 2\n", encoding="utf-8")
    monkeypatch.chdir(repository)
    paths = collect_changed_files(
        base_ref=None,
        head_ref=None,
        git_ref="HEAD",
        from_git=True,
        changed_files=[],
        from_stdin=False,
    )
    assert paths == ["граница.py"]
    before = scope_digest(repository, paths)
    target.write_text("VERSION = 3\n", encoding="utf-8")
    assert before != scope_digest(repository, paths)


def test_pr_body_with_windows_line_endings_is_valid():
    assert (
        validate_review(
            body(record()).replace("\n", "\r\n"),
            phase="implementation",
            expected_scope="current-scope",
        )
        == []
    )


def test_git_collection_preserves_spaces_and_case(repository, monkeypatch):
    (repository / " Boundary.py").write_text("A = 1\n", encoding="utf-8")
    (repository / "Boundary.py").write_text("A = 1\n", encoding="utf-8")
    monkeypatch.chdir(repository)
    paths = collect_changed_files(
        base_ref=None,
        head_ref=None,
        git_ref="HEAD",
        from_git=True,
        changed_files=["boundary.py", "Boundary.py"],
        from_stdin=False,
    )
    assert " Boundary.py" in paths
    assert "Boundary.py" in paths
    assert "boundary.py" in paths


def test_stdin_scope_preserves_exact_path_identity(repository, monkeypatch):
    (repository / " leading.py").write_text("A = 1\n", encoding="utf-8")
    monkeypatch.setattr(sys, "stdin", io.StringIO(" leading.py\r\nboundary.py\n"))
    received = collect_changed_files(
        base_ref=None,
        head_ref=None,
        git_ref="HEAD",
        from_git=False,
        changed_files=[],
        from_stdin=True,
    )
    expected = [" leading.py", "boundary.py"]
    assert received == expected
    assert scope_digest(repository, received) == scope_digest(repository, expected)


def test_architecture_pass_without_standard_binding_is_rejected():
    value = record()
    value["decision"].pop("standard", None)
    digest = decision_digest(value["decision"])
    value["design"]["decision_digest"] = digest
    value["implementation"]["decision_digest"] = digest
    assert any("standard" in item for item in errors(value))


def test_review_cannot_choose_an_arbitrary_standard_source():
    value = record()
    value["decision"]["standard"]["path"] = "../../untrusted-standard.md"
    digest = decision_digest(value["decision"])
    value["design"]["decision_digest"] = digest
    value["implementation"]["decision_digest"] = digest
    assert any("decision.standard.path" in item for item in errors(value))


@pytest.mark.parametrize("phase", ["design", "implementation"])
def test_pass_without_criterion_assessment_is_rejected(phase):
    value = record()
    value[phase]["criteria"] = []
    assert any("criteria" in item for item in errors(value))


@pytest.mark.parametrize("phase", ["design", "implementation"])
def test_known_architectural_gap_cannot_be_closed_by_green_tests(phase):
    value = record()
    value[phase]["criteria"] = [
        {"id": "AS-03", "status": "gap", "evidence": "New consumer bypasses its state owner"}
    ]
    assert any("gap" in item for item in errors(value))


def test_not_applicable_requires_a_scope_reason():
    value = record()
    value["design"]["criteria"] = [{"id": "AS-06", "status": "not-applicable"}]
    assert any("reason" in item for item in errors(value))


def test_scoped_non_applicability_is_accepted():
    value = record()
    for phase in ("design", "implementation"):
        value[phase]["criteria"][0] = {
            "id": value[phase]["criteria"][0]["id"],
            "status": "not-applicable",
            "reason": "Fixture scope reason; actual applicability is reviewed independently",
        }
    assert errors(value) == []


@pytest.mark.parametrize(
    "mutation", ["unknown", "duplicate", "missing", "empty-evidence", "invalid-row"]
)
def test_incomplete_or_ambiguous_criterion_coverage_cannot_pass(mutation):
    value = record()
    assessments = value["implementation"]["criteria"]
    if mutation == "unknown":
        assessments[0]["id"] = "UNKNOWN-999"
    elif mutation == "duplicate":
        assessments.append(copy.deepcopy(assessments[0]))
    elif mutation == "missing":
        assessments.pop()
    elif mutation == "empty-evidence":
        assessments[0]["evidence"] = " "
    else:
        assessments[0] = {"id": [], "status": "satisfied"}
    assert any("criteria" in item for item in errors(value))


@pytest.mark.parametrize(
    "content", ["", "# No criterion headings\n", "### AS-01 rule\n### AS-01 duplicate\n"]
)
def test_standard_without_a_unique_criterion_set_fails_closed(repository, content):
    path = repository / "docs/architecture/product-plane/architecture-standard.md"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError, match="criterion headings"):
        read_standard(repository)


def test_standard_digest_is_stable_across_windows_line_endings(repository):
    path = repository / "docs/architecture/product-plane/architecture-standard.md"
    text = path.read_text(encoding="utf-8")
    before = read_standard(repository)
    path.write_bytes(text.replace("\n", "\r\n").encode("utf-8"))
    assert read_standard(repository) == before


@pytest.mark.parametrize("name", ["run_loop_gate", "run_pr_gate"])
def test_standard_change_outside_selected_scope_invalidates_old_pass(gate_repository, name):
    repo, _, _ = gate_repository
    export = repo / ".tmp" / "body.md"
    export.parent.mkdir()
    digest = scope_digest(repo, ["boundary.py"])
    export.write_text(body(record(digest)), encoding="utf-8")
    path = repo / "docs/architecture/product-plane/architecture-standard.md"
    with path.open("a", encoding="utf-8") as output:
        output.write("\nClarified standard; previous admission must be reconsidered.\n")
    assert scope_digest(repo, ["boundary.py"]) == digest
    result = run_gate(
        repo, name, "--changed-files", "boundary.py", "--architecture-review-source", str(export)
    )
    assert result.returncode == 2, result.stdout + result.stderr
    assert "decision.standard.digest" in result.stdout


def test_immutable_review_uses_updated_standard_from_pr_head(gate_repository):
    repo, base, _ = gate_repository
    path = repo / "docs/architecture/product-plane/architecture-standard.md"
    with path.open("a", encoding="utf-8") as output:
        output.write("\n### AS-11 Fixture-only future criterion\nProof for a versioned standard.\n")
    git(repo, "commit", "-qam", "reviewed standard revision")
    head = git(repo, "rev-parse", "HEAD")
    standard = read_standard(repo, head_ref=head)
    assert "AS-11" in standard.criteria
    binding = scope_digest(
        repo,
        ["boundary.py", "docs/architecture/product-plane/architecture-standard.md"],
        base_ref=base,
        head_ref=head,
    )
    export = repo / ".tmp" / "body.md"
    export.parent.mkdir()
    export.write_text(body(record(binding, standard=standard)), encoding="utf-8")
    path.write_text("# Different merge checkout; not the reviewed head\n", encoding="utf-8")
    result = run_gate(
        repo,
        "run_pr_gate",
        "--base-ref",
        base,
        "--head-ref",
        head,
        "--architecture-review-source",
        str(export),
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("name", ["run_loop_gate", "run_pr_gate"])
def test_missing_standard_is_not_accepted_by_triggered_gate(gate_repository, name):
    repo, _, _ = gate_repository
    export = repo / ".tmp" / "body.md"
    export.parent.mkdir()
    export.write_text(body(record(scope_digest(repo, ["boundary.py"]))), encoding="utf-8")
    (repo / "docs/architecture/product-plane/architecture-standard.md").unlink()
    result = run_gate(
        repo, name, "--changed-files", "boundary.py", "--architecture-review-source", str(export)
    )
    assert result.returncode == 2
    assert "architecture" in result.stdout


def test_print_binding_exposes_standard_pin_and_current_ids(gate_repository, monkeypatch):
    # This fixture requests only a standard binding, without any enclosing PR review.
    monkeypatch.delenv("AI_SHELL_ARCHITECTURE_REVIEW_SOURCE", raising=False)
    monkeypatch.delenv("GITHUB_EVENT_PATH", raising=False)
    repo, _, _ = gate_repository
    result = subprocess.run(
        [
            sys.executable,
            "scripts/validate_architecture_review.py",
            "--changed-files",
            "boundary.py",
            "--print-binding",
        ],
        cwd=repo,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    binding = json.loads(result.stdout)
    expected = read_standard(repo)
    assert binding["standard"] == {**expected.pin, "criteria": list(expected.criteria)}


@pytest.mark.parametrize(
    "path",
    [
        "../outside.md",
        "/outside.md",
        "C:/outside.md",
        "docs/architecture/../outside.md",
        "docs/other.md",
        "docs/architecture/record.json",
        "docs/architecture/record.md:hidden.md",
    ],
)
def test_record_rejects_unsafe_or_non_architecture_paths(repository, path):
    with pytest.raises(ValueError, match="repository-relative"):
        read_decision_record(repository, path)


@pytest.mark.parametrize("content", [b"", b" \r\n\t", b"\xef\xbb\xbf\r\n"])
def test_empty_record_cannot_authorize_triggered_work(repository, content):
    (repository / RECORD_PATH).write_bytes(content)
    assert any("record must not be empty" in item for item in errors(record(), repo=repository))


def test_record_digest_normalizes_bom_and_windows_newlines(repository):
    path = repository / RECORD_PATH
    text = path.read_text(encoding="utf-8")
    expected = read_decision_record(repository, RECORD_PATH)
    path.write_bytes(b"\xef\xbb\xbf" + text.replace("\n", "\r\n").encode("utf-8"))
    assert read_decision_record(repository, RECORD_PATH) == expected


@pytest.mark.parametrize("ancestor", [False, True])
def test_record_rejects_symlink_and_symlink_ancestor(repository, ancestor):
    target = repository / "original"
    target.mkdir()
    (target / "decision.md").write_text("# Decision\n", encoding="utf-8")
    link = repository / "docs/architecture/linked"
    try:
        if ancestor:
            link.symlink_to(target, target_is_directory=True)
            path = "docs/architecture/linked/decision.md"
        else:
            link = link.with_suffix(".md")
            link.symlink_to(target / "decision.md")
            path = "docs/architecture/linked.md"
    except OSError as exc:
        pytest.skip(f"Host cannot create test symlink: {exc}")
    with pytest.raises(ValueError, match="symlinks/reparse"):
        read_decision_record(repository, path)


@pytest.mark.skipif(os.name != "nt", reason="Windows junction guard")
def test_record_rejects_real_windows_junction_ancestor(repository):
    target = repository / "junction-target"
    target.mkdir()
    (target / "decision.md").write_text("# Decision\n", encoding="utf-8")
    junction = repository / "docs/architecture/junction"
    result = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(target)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stdout + result.stderr
    with pytest.raises(ValueError, match="symlinks/reparse"):
        read_decision_record(repository, "docs/architecture/junction/decision.md")


def test_immutable_git_symlink_is_not_a_record(repository):
    blob = git(repository, "hash-object", "-w", "--stdin")
    # Git mode is authoritative even when this host cannot create a native symlink.
    path = "docs/architecture/link.md"
    git(repository, "update-index", "--add", "--cacheinfo", f"120000,{blob},{path}")
    git(repository, "commit", "-qm", "symlink producer")
    with pytest.raises(ValueError, match="regular file in reviewed head"):
        read_decision_record(repository, path, head_ref="HEAD")


@pytest.mark.skipif(os.name != "nt", reason="NTFS alternate stream guard")
def test_windows_alternate_stream_is_not_a_trackable_record(repository):
    path = repository / "docs/architecture/record.md"
    path.write_text("# Ordinary tracked file\n", encoding="utf-8")
    stream = Path(str(path) + ":hidden.md")
    stream.write_text("# Hidden alternate stream\n", encoding="utf-8")
    with pytest.raises(ValueError, match="repository-relative"):
        read_decision_record(repository, "docs/architecture/record.md:hidden.md")


@pytest.mark.parametrize("name", ["run_loop_gate", "run_pr_gate"])
@pytest.mark.parametrize("mutation", ["changed", "missing"])
def test_record_outside_selected_scope_is_checked_by_nested_gates(gate_repository, name, mutation):
    repo, _, _ = gate_repository
    export = repo / ".tmp/record-review.md"
    export.parent.mkdir()
    digest = scope_digest(repo, ["boundary.py"])
    export.write_text(body(record(digest)), encoding="utf-8")
    accepted = run_gate(
        repo, name, "--changed-files", "boundary.py", "--architecture-review-source", str(export)
    )
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    path = repo / RECORD_PATH
    if mutation == "changed":
        path.write_text(
            path.read_text(encoding="utf-8") + "\nRevised ownership.\n", encoding="utf-8"
        )
    else:
        path.unlink()
    assert scope_digest(repo, ["boundary.py"]) == digest
    rejected = run_gate(
        repo, name, "--changed-files", "boundary.py", "--architecture-review-source", str(export)
    )
    assert rejected.returncode == 2, rejected.stdout + rejected.stderr
    assert "decision.record" in rejected.stdout


def test_immutable_record_is_read_from_reviewed_head_by_actual_pr_gate(gate_repository):
    repo, base, _ = gate_repository
    path = repo / RECORD_PATH
    path.write_text(
        path.read_text(encoding="utf-8") + "\nReviewed decision revision.\n", encoding="utf-8"
    )
    git(repo, "commit", "-qam", "actual owning decision producer")
    head = git(repo, "rev-parse", "HEAD")
    binding = scope_digest(repo, ["boundary.py", RECORD_PATH], base_ref=base, head_ref=head)
    value = record(binding)
    value["decision"]["record"] = read_decision_record(repo, RECORD_PATH, head_ref=head)
    for phase in ("design", "implementation"):
        value[phase]["decision_digest"] = decision_digest(value["decision"])
    export = repo / ".tmp/body.md"
    export.parent.mkdir()
    export.write_text(body(value), encoding="utf-8")
    path.write_text("# Divergent merge checkout\n", encoding="utf-8")
    accepted = run_gate(
        repo,
        "run_pr_gate",
        "--base-ref",
        base,
        "--head-ref",
        head,
        "--architecture-review-source",
        str(export),
    )
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    value["decision"]["record"] = read_decision_record(repo, RECORD_PATH)
    for phase in ("design", "implementation"):
        value[phase]["decision_digest"] = decision_digest(value["decision"])
    export.write_text(body(value), encoding="utf-8")
    rejected = run_gate(
        repo,
        "run_pr_gate",
        "--base-ref",
        base,
        "--head-ref",
        head,
        "--architecture-review-source",
        str(export),
    )
    assert rejected.returncode == 2
    assert "decision.record.digest" in rejected.stdout


@pytest.mark.parametrize("mode", ["explicit", "from-body"])
def test_print_binding_exposes_current_decision_record(gate_repository, mode):
    repo, _, _ = gate_repository
    args = ["--record-path", RECORD_PATH]
    if mode == "from-body":
        export = repo / "body.md"
        export.write_text(body(record()), encoding="utf-8")
        args = ["--architecture-review-source", str(export)]
    result = subprocess.run(
        [
            sys.executable,
            "scripts/validate_architecture_review.py",
            "--changed-files",
            "boundary.py",
            "--print-binding",
            *args,
        ],
        cwd=repo,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["decision_record"] == read_decision_record(repo, RECORD_PATH)
