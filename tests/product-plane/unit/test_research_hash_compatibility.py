from __future__ import annotations

from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.research import hash_compatibility as owner


def test_reviewed_current_package_is_exact():
    assert owner.reviewed_current_source()


def test_reviewed_digest_only_normalizes_one_anchor_and_line_endings(tmp_path):
    path = tmp_path / "product_plane/research/hash_compatibility.py"
    path.parent.mkdir(parents=True)
    content = '_REVIEWED_CURRENT_SOURCE_SHA256 = "' + "a" * 64 + '"\nVALUE = 1\n'
    path.write_bytes(content.encode())
    expected = owner.reviewed_source_digest(tmp_path)
    path.write_bytes(content.replace("a" * 64, "b" * 64).replace("\n", "\r\n").encode())
    assert owner.reviewed_source_digest(tmp_path) == expected
    path.write_bytes(content.replace("VALUE = 1", "VALUE = 2").encode())
    assert owner.reviewed_source_digest(tmp_path) != expected
    path.write_bytes(content.encode())
    (tmp_path / "extra.py").write_text("VALUE = 3\n")
    assert owner.reviewed_source_digest(tmp_path) != expected


@pytest.mark.parametrize("kind", ["missing", "duplicate", "malformed"])
def test_reviewed_digest_rejects_invalid_anchor(tmp_path, kind):
    path = tmp_path / "product_plane/research/hash_compatibility.py"
    path.parent.mkdir(parents=True)
    anchor = '_REVIEWED_CURRENT_SOURCE_SHA256 = "' + "a" * 64 + '"\n'
    path.write_text(
        "VALUE = 1\n"
        if kind == "missing"
        else anchor * 2
        if kind == "duplicate"
        else anchor.replace("a" * 64, "bad")
    )
    with pytest.raises(ValueError):
        owner.reviewed_source_digest(tmp_path)


@pytest.mark.parametrize(
    "change", ["source", "dependency", "contract", "extra_key", "current_code"]
)
def test_compatibility_never_ignores_unknown_semantics(monkeypatch, change):
    old = {
        "source_sha256": next(iter(owner.LEGACY_SOURCES)),
        "dependencies": {"numpy": "same"},
        "contracts_sha256": "same",
    }
    current = {**old, "source_sha256": "new"}
    monkeypatch.setattr(owner, "reviewed_current_source", lambda: change != "current_code")
    if change == "source":
        old["source_sha256"] = "unknown"
    if change == "dependency":
        current["dependencies"] = {"numpy": "other"}
    if change == "contract":
        current["contracts_sha256"] = "other"
    if change == "extra_key":
        current["extra"] = True
    assert not owner.compatible_provenance(old, current)


def test_known_source_transition_is_finite_and_scope_is_revoked(monkeypatch):
    monkeypatch.setattr(owner, "reviewed_current_source", lambda: True)
    for source in owner.LEGACY_SOURCES:
        assert owner.compatible_provenance({"source_sha256": source}, {"source_sha256": "new"})
        with owner._verified_migration_scope(source):
            assert owner.migration_source() == source
        assert owner.migration_source() is None
    with pytest.raises(ValueError):
        with owner._verified_migration_scope("unknown"):
            pass


@pytest.fixture
def accepted_chain(tmp_path, monkeypatch):
    from types import SimpleNamespace

    import pyarrow as pa
    from deltalake import write_deltalake

    from trading_advisor_3000.product_plane.common import data_snapshots as snapshots
    from trading_advisor_3000.product_plane.research import data_snapshots as checkpoint

    monkeypatch.setattr(owner, "reviewed_current_source", lambda: True)
    canonical = tmp_path / "canonical"
    output = tmp_path / "research"
    source = canonical / "canonical_bars.delta"
    write_deltalake(str(source), pa.table({"id": [1]}))
    tables = {"canonical_bars": snapshots.capture_table(source)}
    data = snapshots.create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version="data",
        run_id="data",
        code_version="old",
        quality_checks={"data": "PASS"},
        parameters={"downstream_scope": {"version": 1, "full_refresh": False}},
    )
    registry = canonical / ".data_snapshots"
    data_path = snapshots.save_snapshot(registry, data)
    snapshots.publish_snapshot(registry, data_path, expected_current_id=None)
    config = {
        "canonical_output_dir": str(canonical),
        "materialized_output_dir": str(output),
        "dataset_version": "nightly",
        "series_mode": "continuous_front",
        "timeframes": ["15m"],
        "refresh_scope_version": 1,
        "force_full_refresh": False,
        "changed_windows": [],
        "data_snapshot_manifest_path": str(data_path),
    }
    old = {"source_sha256": next(iter(owner.LEGACY_SOURCES)), "dependencies": {"numpy": "same"}}
    current = {**old, "source_sha256": "new"}
    monkeypatch.setattr(checkpoint, "runtime_provenance", lambda: current)
    stages = owner.REQUIRED_QUALITY - {"continuous_front_acceptance"}
    status = SimpleNamespace(value="SUCCESS")
    instance = SimpleNamespace(
        get_run_by_id=lambda _: SimpleNamespace(status=status),
        get_run_step_stats=lambda _: [
            SimpleNamespace(step_key=stage, status=status) for stage in stages
        ],
    )

    def publish(*, quality=None, owners=None, parameters=None):
        research = snapshots.create_snapshot(
            tables=tables,
            required_tables=list(tables),
            dataset_version="nightly",
            run_id="research",
            code_version="old",
            quality_checks=quality
            if quality is not None
            else {stage: "PASS" for stage in owner.REQUIRED_QUALITY},
            parameters={
                **checkpoint._parameters(config),
                "runtime_provenance": old,
                "quality_owners": owners
                if owners is not None
                else {stage: "native" for stage in stages},
                **(parameters or {}),
            },
            parents=[data["snapshot_id"]],
        )
        research_registry = checkpoint._registry(config)
        prior = snapshots.current_snapshot(research_registry)
        snapshots.publish_snapshot(
            research_registry,
            snapshots.save_snapshot(research_registry, research),
            expected_current_id=prior["snapshot_id"] if prior else None,
        )
        config["hash_compatibility"] = {
            "version": 1,
            "snapshot_id": research["snapshot_id"],
            "manifest_sha256": research["manifest_sha256"],
            "source_sha256": old["source_sha256"],
        }
        return research

    accepted = publish()
    pending = {
        "tables": dict(tables),
        "parents": [data["snapshot_id"]],
        "parent_manifest_sha256": data["manifest_sha256"],
        "runtime_provenance": current,
        "quality_checks": {},
        "quality_owners": {},
        "quality_evidence": {},
    }
    return config, pending, instance, accepted, publish, source, checkpoint


def test_accepted_state_admits_exact_receipt_and_refresh_scope(accepted_chain, monkeypatch):
    from trading_advisor_3000.product_plane.research import refresh_scope

    config, pending, instance, accepted, _, _, checkpoint = accepted_chain
    assert checkpoint._validate_hash_compatibility(config, pending, instance) == accepted
    monkeypatch.setattr(refresh_scope, "runtime_provenance", lambda: pending["runtime_provenance"])
    resolved = refresh_scope.resolve_refresh_scope(
        data_manifest_path=Path(config["data_snapshot_manifest_path"]),
        materialized_output_dir=Path(config["materialized_output_dir"]),
        dataset_version=config["dataset_version"],
        timeframes=config["timeframes"],
        configuration=config,
    )
    assert resolved["windows"] == [] and resolved["force_full_refresh"] is False
    assert resolved["hash_compatibility"] == config["hash_compatibility"]


@pytest.mark.parametrize(
    "change",
    [
        "snapshot",
        "digest",
        "source",
        "profile",
        "dependency",
        "owner",
        "owner_run",
        "owner_step",
        "qc",
        "output",
        "data_parent",
    ],
)
def test_accepted_migration_rejects_forgery_or_changed_evidence(accepted_chain, change):
    from types import SimpleNamespace

    import pyarrow as pa
    from deltalake import write_deltalake

    config, pending, instance, accepted, publish, source, checkpoint = accepted_chain
    if change in ("snapshot", "digest", "source"):
        key = {"snapshot": "snapshot_id", "digest": "manifest_sha256", "source": "source_sha256"}[
            change
        ]
        config["hash_compatibility"][key] = "forged"
    elif change == "profile":
        config["indicator_profile_version"] = "other"
    elif change == "dependency":
        pending["runtime_provenance"]["dependencies"] = {"numpy": "other"}
    elif change == "owner":
        publish(owners={})
    elif change == "owner_run":
        instance.get_run_by_id = lambda _: SimpleNamespace(status=SimpleNamespace(value="CANCELED"))
    elif change == "owner_step":
        instance.get_run_step_stats = lambda _: []
    elif change == "qc":
        publish(quality={"fixture": "PASS"})
    elif change == "output":
        write_deltalake(str(source), pa.table({"id": [2]}), mode="overwrite")
    elif change == "data_parent":
        pending["parents"] = ["different"]
    with pytest.raises((ValueError, checkpoint.SnapshotUnavailableError)):
        checkpoint._validate_hash_compatibility(config, pending, instance)


def test_pending_resume_stays_strict_despite_compatible_transition(accepted_chain, tmp_path):
    from types import SimpleNamespace

    config, pending, _, _, _, _, checkpoint = accepted_chain
    config["code_version"] = "new"
    context = SimpleNamespace(run_id="attempt")
    pending.update(parameters=checkpoint._parameters(config), code_version="new")
    pending["runtime_provenance"] = {
        **pending["runtime_provenance"],
        "source_sha256": next(iter(owner.LEGACY_SOURCES)),
    }
    checkpoint.atomic_json(checkpoint._pending_path(context, config), pending)
    with pytest.raises(ValueError, match="same parameters and code"):
        checkpoint._initial(context, config)


@pytest.mark.parametrize("bad_qc", [False, True])
def test_accepted_cf_revalidation_preserves_qc_and_never_calls_numeric_owner(
    accepted_chain, bad_qc
):
    from types import SimpleNamespace

    import pyarrow as pa
    from deltalake import write_deltalake

    from trading_advisor_3000.product_plane.common import data_snapshots as snapshots

    config, pending, instance, accepted, _, _, checkpoint = accepted_chain
    names = (
        "continuous_front_bars",
        "continuous_front_roll_events",
        "continuous_front_adjustment_ladder",
        "continuous_front_qc_report",
    )
    tables = dict(accepted["tables"])
    for name in names:
        path = Path(config["materialized_output_dir"]) / f"{name}.delta"
        from trading_advisor_3000.product_plane.research.operations import _store_contracts

        contract = _store_contracts()[name]
        fields = set(contract["columns"]) | set(contract.get("physical_columns", ()))
        write_deltalake(str(path), pa.table({field: ["fixture"] for field in fields}))
        tables[name] = snapshots.capture_table(path)
    result = {
        "status": "PASS",
        "dataset_version": "nightly",
        "output_paths": {name: tables[name]["table_path"] for name in names},
        "rows_by_table": {name: 1 for name in names},
        "qc_rows": [{"status": "FAIL" if bad_qc else "PASS"}],
    }
    research = snapshots.create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version="nightly",
        run_id="old-research",
        code_version="old",
        quality_checks=accepted["quality_checks"],
        parameters={
            **accepted["parameters"],
            "quality_evidence": {"continuous_front_bars": {"status": "PASS", "cf_result": result}},
        },
        parents=accepted["parents"],
    )
    registry = checkpoint._registry(config)
    snapshots.publish_snapshot(
        registry,
        snapshots.save_snapshot(registry, research),
        expected_current_id=accepted["snapshot_id"],
    )
    config["hash_compatibility"].update(
        snapshot_id=research["snapshot_id"], manifest_sha256=research["manifest_sha256"]
    )
    config.update(data_snapshot_enabled=True, code_version="new")
    context = SimpleNamespace(run_id="current-cf", op_config=config, instance=instance)

    @checkpoint.snapshot_materialization(names)
    def continuous_front_bars(_):
        pytest.fail("compatible accepted CF started numerical work")

    if bad_qc:
        with pytest.raises(ValueError, match="CF evidence mismatch"):
            continuous_front_bars(context)
    else:
        reused = continuous_front_bars(context)
        assert reused["status"] == "PASS-NOOP"
        assert reused["qc_rows"] == result["qc_rows"]
        stored = __import__("json").loads(checkpoint._pending_path(context, config).read_text())
        assert stored["quality_checks"]["continuous_front_bars"] == "PASS"
        assert stored["quality_evidence"]["continuous_front_bars"]["cf_result"]["status"] == "PASS"
