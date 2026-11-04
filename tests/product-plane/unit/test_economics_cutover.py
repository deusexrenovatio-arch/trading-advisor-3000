import json
from dataclasses import replace

import pyarrow as pa
import pytest
from deltalake import CommitProperties, write_deltalake
from test_moex_contract_parameter_continuation import _metadata_fixture

from trading_advisor_3000.product_plane.common.data_snapshots import (
    SnapshotUnavailableError,
    _hash,
    capture_table,
)
from trading_advisor_3000.product_plane.data_plane.moex import (
    contract_parameter_continuation as continuation,
)
from trading_advisor_3000.product_plane.data_plane.moex.economics_cutover import calculation_policy


def _clone_binding(tmp_path, monkeypatch):
    rules, evidence = _metadata_fixture(tmp_path, monkeypatch)
    source = tmp_path / "old.delta"
    write_deltalake(
        str(source),
        pa.table({"moex_secid": ["TESTU6"], "trade_date": ["2026-06-26"], "min_step": [0.25]}),
    )
    original = capture_table(source)
    monkeypatch.setattr(
        continuation,
        "_ADMITTED_BUNDLE",
        replace(continuation._ADMITTED_BUNDLE, prepared_table_id=original["table_id"]),
    )
    canonical = tmp_path / "canonical_contract_parameters.delta"
    owner = {
        "kind": "economics-canonical-cutover.v1",
        "intent_id": "test-intent",
        "role": "parameters",
    }
    origin = {
        "kind": "accepted_production_baseline",
        "source_table": original,
        "key": {"moex_secid": "TESTU6", "trade_date": "2026-06-26"},
    }
    write_deltalake(
        str(canonical),
        pa.table(
            {
                "moex_secid": ["TESTU6"],
                "trade_date": ["2026-06-26"],
                "min_step": [0.25],
                "calculation_origin_json": [json.dumps(origin)],
                "source_validation_status": ["ACCEPTED_PRODUCTION_BASELINE"],
                "execution_admissible": [True],
            }
        ),
        commit_properties=CommitProperties(custom_metadata={"userMetadata": json.dumps(owner)}),
    )
    base = continuation.validate_contract_parameter_evidence(rules, evidence)
    retained = tmp_path / ".production-cutover/owner-acceptance.json"
    retained.parent.mkdir(exist_ok=True)
    acceptance = {
        "status": "ACCEPTED_PRODUCTION_BASELINE",
        "production_tables": {"calculated_parameters": original},
    }
    retained.write_text(json.dumps(acceptance))
    body = {
        "status": "ACCEPTED_PRODUCTION_BASELINE",
        "acceptance_path": str(retained),
        "acceptance_sha256": _hash(acceptance),
        "calculation_policy": calculation_policy(),
        "owner": owner,
        "source": original,
        "canonical": capture_table(canonical),
        "business_columns": ["moex_secid", "trade_date", "min_step"],
        "regimes_sha256": base["regimes_sha256"],
        "dated_official_tc_evidence_sha256": base["dated_official_tc_evidence_sha256"],
        "checks": {"production_parity": "PASS", "origins": "PASS"},
    }
    path = tmp_path / "accepted-production-baseline.json"
    path.write_text(json.dumps({**body, "binding_sha256": _hash(body)}))
    return rules, replace(evidence, canonical_binding_path=path), original, body, path


def test_canonical_clone_gets_new_identity_without_relaxing_legacy_binder(tmp_path, monkeypatch):
    rules, evidence, original, body, _ = _clone_binding(tmp_path, monkeypatch)
    legacy = continuation.validate_contract_parameter_evidence(
        rules, replace(evidence, canonical_binding_path=None)
    )
    with pytest.raises(ValueError, match="identity/version mismatch"):
        continuation.bind_contract_parameter_input(
            tmp_path / "canonical_contract_parameters.delta", legacy
        )
    admitted = continuation.validate_contract_parameter_evidence(rules, evidence)
    assert admitted["prepared_input_table_id"] != original["table_id"]
    assert admitted["prepared_input_table_id"] == body["canonical"]["table_id"]
    assert (
        continuation.bind_contract_parameter_input(
            tmp_path / "canonical_contract_parameters.delta", admitted
        )
        == 0
    )
    assert admitted["canonical_binding"]["source"] == original


@pytest.mark.parametrize(
    "change", ["digest", "old_id", "target", "qc", "acceptance_missing", "acceptance_changed"]
)
def test_canonical_clone_rejects_changed_admission(tmp_path, monkeypatch, change):
    rules, evidence, _, body, path = _clone_binding(tmp_path, monkeypatch)
    if change == "old_id":
        body["source"]["table_id"] = "foreign"
    elif change == "target":
        body["canonical"]["table_id"] = "foreign"
    elif change == "qc":
        body["checks"]["production_parity"] = "NOT_REVALIDATED"
    elif change == "acceptance_missing":
        (tmp_path / ".production-cutover/owner-acceptance.json").unlink()
    elif change == "acceptance_changed":
        (tmp_path / ".production-cutover/owner-acceptance.json").write_text("{}")
    path.write_text(
        json.dumps({**body, "binding_sha256": "bad" if change == "digest" else _hash(body)})
    )
    with pytest.raises((ValueError, SnapshotUnavailableError)):
        continuation.validate_contract_parameter_evidence(rules, evidence)


def test_release_record_rejects_untracked_executable_source(tmp_path, monkeypatch):
    import importlib.util
    import subprocess
    import sys
    from pathlib import Path

    script = Path(__file__).parents[3] / "scripts/run_moex_economics_cutover.py"
    spec = importlib.util.spec_from_file_location("cutover_tool", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    (tmp_path / "scripts").mkdir()
    (tmp_path / "src").mkdir()
    (tmp_path / "src/tracked.py").write_text("value = 1")

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=tmp_path, capture_output=True, text=True, check=True
        ).stdout.strip()

    git("init")
    git("add", "src/tracked.py")
    git(
        "-c",
        "user.name=Cutover Test",
        "-c",
        "user.email=cutover-test@localhost",
        "commit",
        "-m",
        "fixture",
    )
    sha = git("rev-parse", "HEAD")
    monkeypatch.setattr(module, "__file__", str(tmp_path / "scripts/tool.py"))
    monkeypatch.setattr(module, "runtime_provenance", lambda: {"git_commit": sha})
    output = tmp_path.parent / (tmp_path.name + "-release.json")
    monkeypatch.setattr(
        sys, "argv", ["tool", "release-record", "--release-sha", sha, "--output", str(output)]
    )
    module.main()
    (tmp_path / "src/untracked.py").write_text("value = 2")
    with pytest.raises(ValueError, match="clean release"):
        module.main()
