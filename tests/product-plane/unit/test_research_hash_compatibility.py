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
def accepted_chain(tmp_path, monkeypatch, request):
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
    if getattr(request, "param", None) == "schedule":
        for name in ("raw_moex_session_schedule", "canonical_session_calendar"):
            path = canonical / f"{name}.delta"
            write_deltalake(str(path), pa.table({"id": [1]}))
            tables[name] = snapshots.capture_table(path)
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
    from trading_advisor_3000.product_plane.research import refresh_scope

    monkeypatch.setattr(refresh_scope, "runtime_provenance", lambda: current)
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


@pytest.mark.parametrize("bad_qc,change_data", [(False, False), (True, False), (False, True)])
def test_accepted_cf_revalidation_preserves_qc_and_never_calls_numeric_owner(
    accepted_chain, bad_qc, change_data
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
        write_deltalake(
            str(path),
            pa.table(
                {
                    field: [
                        "2026-01-10T00:00:00Z"
                        if field in {"ts", "ts_close", "bar_end_ts", "end_utc"}
                        else "fixture"
                    ]
                    for field in fields
                }
            ),
        )
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
    if change_data:
        _advance_data(accepted_chain)
        _apply_scope(accepted_chain, _resolved(accepted_chain))
    context = SimpleNamespace(run_id="current-cf", op_config=config, instance=instance)

    @checkpoint.snapshot_materialization(names)
    def continuous_front_bars(_):
        if change_data:
            assert _.run_id == "current-cf", "native CF handoff incorrectly recomputed"
            for name in names:
                reference = tables[name]
                from deltalake import DeltaTable

                fields = [
                    field.name for field in DeltaTable(reference["table_path"]).schema().fields
                ]
                write_deltalake(
                    reference["table_path"],
                    pa.table(
                        {
                            field: [
                                "2026-01-11T00:00:00Z"
                                if field in {"ts", "ts_close", "bar_end_ts", "end_utc"}
                                else "advanced"
                            ]
                            for field in fields
                        }
                    ),
                    mode="overwrite",
                )
            return {**result, "numerical_owner_called": True}
        pytest.fail("compatible accepted CF started numerical work")

    if bad_qc:
        with pytest.raises(ValueError, match="CF evidence mismatch"):
            continuous_front_bars(context)
    else:
        reused = continuous_front_bars(context)
        assert reused["status"] == ("PASS" if change_data else "PASS-NOOP")
        assert bool(reused.get("numerical_owner_called")) == change_data
        assert reused["qc_rows"] == result["qc_rows"]
        stored = __import__("json").loads(checkpoint._pending_path(context, config).read_text())
        assert stored["quality_checks"]["continuous_front_bars"] == "PASS"
        assert stored["quality_evidence"]["continuous_front_bars"]["cf_result"]["status"] == "PASS"
        if change_data:
            old_get = instance.get_run_by_id
            instance.get_run_by_id = lambda run_id: (
                SimpleNamespace(
                    status=SimpleNamespace(value="SUCCESS"), job_name="moex_cf_catch_up_job"
                )
                if run_id == "current-cf"
                else old_get(run_id)
            )
            consumer = SimpleNamespace(
                run_id="current-research",
                op_config=config,
                instance=instance,
                dagster_run=SimpleNamespace(
                    tags={
                        "ta3000/upstream_job": "moex_cf_catch_up_job",
                        "ta3000/upstream_run_id": "current-cf",
                    }
                ),
            )
            imported = continuous_front_bars(consumer)
            assert imported["reused_from_run_id"] == "current-cf"
            assert imported["status"] == "PASS-NOOP"
            # Even a legitimate native import cannot bypass a forged receipt.
            from copy import deepcopy

            forged = deepcopy(config)
            forged["hash_compatibility"]["manifest_sha256"] = "forged"
            consumer.run_id = "forged-research"
            consumer.op_config = forged
            with pytest.raises(ValueError, match="forged"):
                continuous_front_bars(consumer)


def _advance_data(
    accepted_chain,
    *,
    full=False,
    covered=True,
    extra=None,
    schema=False,
    remove=(),
    update_names=(),
):
    import pyarrow as pa
    from deltalake import write_deltalake

    from trading_advisor_3000.product_plane.common import data_snapshots as snapshots

    config, pending, _, _, _, source, _ = accepted_chain
    root = Path(config["canonical_output_dir"]) / ".data_snapshots"
    previous = snapshots.current_snapshot(root)
    values = {"id": [previous["tables"]["canonical_bars"]["delta_version"] + 2]}
    if schema:
        values["extra"] = [1]
    write_deltalake(str(source), pa.table(values), mode="overwrite", schema_mode="overwrite")
    tables = {
        **{name: ref for name, ref in previous["tables"].items() if name not in remove},
        "canonical_bars": snapshots.capture_table(source),
        **(extra or {}),
    }
    for name in update_names:
        path = Path(tables[name]["table_path"])
        write_deltalake(
            str(path), pa.table({"id": [tables[name]["delta_version"] + 2]}), mode="overwrite"
        )
        tables[name] = snapshots.capture_table(path)
    windows = [
        {
            "instrument_id": "BR",
            "timeframe": "15m",
            "start_ts": "2026-01-09T12:00:00Z",
            "end_ts": "2026-01-10T00:00:00Z",
        }
    ]
    from trading_advisor_3000.product_plane.data_plane.data_snapshots import _downstream_scope

    scope = _downstream_scope(
        report={
            "cf_catch_up": {"windows": windows, "target_timeframes": ["15m"]},
            "canonical_report": {
                "sidecar_refresh": {
                    "dependency_scope": {
                        "version": 1,
                        "full_refresh": False,
                        "output_delta_versions": tables,
                    }
                }
            },
        },
        tables=tables,
    )
    assert set(scope["covered_tables"]["canonical_bars"]) == {"table_id", "delta_version"}
    scope["full_refresh"] = full
    if not covered:
        scope["covered_tables"] = {}
    data = snapshots.create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version="data",
        run_id=f"changed-{tables['canonical_bars']['delta_version']}",
        code_version="current",
        quality_checks={"data": "PASS"},
        parents=[previous["snapshot_id"]],
        parameters={"downstream_scope": scope},
    )
    data_path = snapshots.save_snapshot(root, data)
    snapshots.publish_snapshot(root, data_path, expected_current_id=previous["snapshot_id"])
    config["data_snapshot_manifest_path"] = str(data_path)
    pending.update(
        tables=tables, parents=[data["snapshot_id"]], parent_manifest_sha256=data["manifest_sha256"]
    )
    return data


def _resolved(accepted_chain):
    from trading_advisor_3000.product_plane.research.refresh_scope import resolve_refresh_scope

    config = accepted_chain[0]
    return resolve_refresh_scope(
        data_manifest_path=Path(config["data_snapshot_manifest_path"]),
        materialized_output_dir=Path(config["materialized_output_dir"]),
        dataset_version=config["dataset_version"],
        timeframes=config["timeframes"],
        configuration=config,
    )


def _apply_scope(accepted_chain, scope):
    config = accepted_chain[0]
    config.update(
        changed_windows=scope["windows"],
        force_full_refresh=scope["force_full_refresh"],
        refresh_scope_version=scope["refresh_scope_version"],
    )
    if scope.get("hash_compatibility"):
        config["hash_compatibility"] = scope["hash_compatibility"]


def test_bounded_changed_data_keeps_finite_migration(accepted_chain):
    config, pending, instance, accepted, _, _, checkpoint = accepted_chain
    _advance_data(accepted_chain)
    scope = _resolved(accepted_chain)
    assert scope["force_full_refresh"] is False
    assert scope["hash_compatibility"] == config["hash_compatibility"]
    assert scope["windows"] == [
        {
            "instrument_id": "BR",
            "timeframe": "15m",
            "start_ts": "2026-01-02T00:00:00Z",
            "end_ts": "2026-01-10T00:00:00Z",
        }
    ]
    _apply_scope(accepted_chain, scope)
    assert checkpoint._validate_hash_compatibility(config, pending, instance) == accepted


@pytest.mark.parametrize(
    "change", ["unbounded", "uncovered", "schema_roundtrip", "added_roundtrip", "missing_pin"]
)
def test_changed_data_migration_rejects_unproven_ancestry_edges(accepted_chain, change):
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    from trading_advisor_3000.product_plane.common import data_snapshots as snapshots

    config, pending, instance, accepted, _, source, checkpoint = accepted_chain
    old_files = DeltaTable(str(source)).file_uris()
    if change == "added_roundtrip":
        extra = source.parent / "canonical_extra.delta"
        write_deltalake(str(extra), pa.table({"id": [1]}))
        _advance_data(accepted_chain, extra={"canonical_extra": snapshots.capture_table(extra)})
        _advance_data(accepted_chain, remove=("canonical_extra",))
    elif change == "schema_roundtrip":
        _advance_data(accepted_chain, schema=True)
        _advance_data(accepted_chain)
    else:
        _advance_data(accepted_chain, full=change == "unbounded", covered=change != "uncovered")
    if change == "missing_pin":
        from urllib.parse import unquote, urlparse

        for uri in old_files:
            parsed = urlparse(uri)
            path = unquote(parsed.path) if parsed.scheme == "file" else unquote(uri)
            if len(path) > 2 and path[0] == "/" and path[2] == ":":
                path = path[1:]
            Path(path).unlink()
    scope = _resolved(accepted_chain)
    assert scope["force_full_refresh"] is True
    with pytest.raises((ValueError, checkpoint.SnapshotUnavailableError)):
        checkpoint._validate_hash_compatibility(config, pending, instance)


@pytest.mark.parametrize(
    "change", ["empty", "narrow_start", "narrow_end", "contract", "instrument", "full"]
)
def test_migration_runtime_rejects_forged_frozen_scope(accepted_chain, change):
    config, pending, instance, _, _, _, checkpoint = accepted_chain
    _advance_data(accepted_chain)
    scope = _resolved(accepted_chain)
    assert scope["force_full_refresh"] is False
    _apply_scope(accepted_chain, scope)
    if change == "empty":
        config["changed_windows"] = []
    elif change == "full":
        config["force_full_refresh"] = True
    else:
        window = config["changed_windows"][0]
        key, value = {
            "narrow_start": ("start_ts", "2026-01-09T00:00:00Z"),
            "narrow_end": ("end_ts", "2026-01-09T00:00:00Z"),
            "contract": ("contract_id", "BRH6"),
            "instrument": ("instrument_id", "SI"),
        }[change]
        window[key] = value
    with pytest.raises(ValueError):
        checkpoint._validate_hash_compatibility(config, pending, instance)


def test_migration_scope_ignores_only_successful_stage_tail_advance(accepted_chain, monkeypatch):
    from datetime import UTC, datetime

    import pyarrow as pa
    from deltalake import write_deltalake

    from trading_advisor_3000.product_plane.common import data_snapshots as snapshots

    config, pending, instance, _, _, source, checkpoint = accepted_chain
    # Add an actual owned output to the accepted manifest before advancing Data.
    output = Path(config["materialized_output_dir"]) / "continuous_front_bars.delta"
    write_deltalake(str(output), pa.table({"ts": [datetime(2026, 1, 10, tzinfo=UTC)]}))
    registry = checkpoint._registry(config)
    before = snapshots.current_snapshot(registry)
    tables = {**before["tables"], output.stem: snapshots.capture_table(output)}
    accepted = snapshots.create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version=before["dataset_version"],
        run_id="with-output",
        code_version="old",
        quality_checks=before["quality_checks"],
        parameters=before["parameters"],
        parents=before["parents"],
    )
    snapshots.publish_snapshot(
        registry,
        snapshots.save_snapshot(registry, accepted),
        expected_current_id=before["snapshot_id"],
    )
    config["hash_compatibility"].update(
        snapshot_id=accepted["snapshot_id"], manifest_sha256=accepted["manifest_sha256"]
    )
    _advance_data(accepted_chain)
    scope = _resolved(accepted_chain)
    _apply_scope(accepted_chain, scope)
    write_deltalake(
        str(output), pa.table({"ts": [datetime(2026, 1, 20, tzinfo=UTC)]}), mode="overwrite"
    )
    with pytest.raises((ValueError, checkpoint.SnapshotUnavailableError)):
        checkpoint._validate_hash_compatibility(config, pending, instance)
    monkeypatch.setitem(checkpoint._STAGE_OUTPUTS, "continuous_front_bars", (output.stem,))
    pending["quality_checks"]["continuous_front_bars"] = "PASS"
    pending["quality_owners"]["continuous_front_bars"] = "native"
    pending["tables"][output.stem] = snapshots.capture_table(output)
    assert checkpoint._validate_hash_compatibility(config, pending, instance) == accepted


@pytest.mark.parametrize("accepted_chain", ["schedule"], indirect=True)
def test_native_canonical_calendar_scope_covers_raw_schedule_provenance_advance(accepted_chain):
    config, pending, instance, accepted, _, _, checkpoint = accepted_chain
    data = _advance_data(
        accepted_chain, update_names=("raw_moex_session_schedule", "canonical_session_calendar")
    )
    covered = data["parameters"]["downstream_scope"]["covered_tables"]
    assert "raw_moex_session_schedule" not in covered
    assert set(covered["canonical_session_calendar"]) == {"table_id", "delta_version"}
    scope = _resolved(accepted_chain)
    assert scope["force_full_refresh"] is False
    _apply_scope(accepted_chain, scope)
    assert checkpoint._validate_hash_compatibility(config, pending, instance) == accepted


@pytest.mark.parametrize(
    "status,hops", [("FAILURE", 1), ("CANCELED", 1), ("FAILURE", 2), ("CANCELED", 2)]
)
@pytest.mark.parametrize(
    "change",
    [
        None,
        "unrelated",
        "forged_lineage",
        "missing_step",
        "missing_owner",
        "missing_ancestor",
        "owner_started",
        "cycle",
    ],
)
def test_migration_completed_stage_requires_verified_native_ancestor(
    accepted_chain, monkeypatch, status, hops, change
):
    from types import SimpleNamespace

    config, pending, instance, accepted, _, _, checkpoint = accepted_chain
    pending.update(run_id="child", recovery_lineage=["ancestor", "parent"])
    pending["quality_checks"]["completed"] = "PASS"
    pending["quality_owners"]["completed"] = "ancestor"
    parents = {"child": "ancestor" if hops == 1 else "parent", "parent": "ancestor"}
    if change == "unrelated":
        parents = {"child": "other", "other": None}
    if change == "forged_lineage":
        pending["recovery_lineage"] = ["parent"]
    if change == "cycle":
        parents = {"child": "parent", "parent": "ancestor", "ancestor": "child"}
    if change == "missing_ancestor":
        parents = {"child": "parent", "parent": "ancestor"}
    original_run = instance.get_run_by_id
    original_steps = instance.get_run_step_stats
    monkeypatch.setattr(
        instance,
        "get_run_by_id",
        lambda run_id: (
            original_run(run_id)
            if run_id == "native"
            else None
            if (change == "missing_owner" and run_id == "ancestor")
            or (change == "missing_ancestor" and run_id == "parent")
            else SimpleNamespace(
                status=SimpleNamespace(
                    value=status
                    if run_id == "ancestor" and change != "owner_started"
                    else "STARTED"
                ),
                parent_run_id=parents.get(run_id),
            )
        ),
    )
    monkeypatch.setattr(
        instance,
        "get_run_step_stats",
        lambda run_id: (
            original_steps(run_id)
            if run_id == "native"
            else []
            if change == "missing_step"
            else [SimpleNamespace(step_key="completed", status=SimpleNamespace(value="SUCCESS"))]
        ),
    )
    if change:
        with pytest.raises(ValueError, match="completed native stage proof"):
            checkpoint._validate_hash_compatibility(config, pending, instance)
    else:
        assert checkpoint._validate_hash_compatibility(config, pending, instance) == accepted


def reconstruct_hash_seed_job():
    from dagster import in_process_executor, job, op

    @op
    def proof():
        return None

    @job(executor_def=in_process_executor)
    def seed():
        for stage in sorted(owner.REQUIRED_QUALITY - {"continuous_front_acceptance"}):
            proof.alias(stage)()

    return seed


def reconstruct_hash_resume_job():
    from dagster import in_process_executor, job, op

    from trading_advisor_3000.product_plane.research import data_snapshots as checkpoint

    @op(config_schema=dict)
    @checkpoint.snapshot_materialization(("research_indicator_frames",))
    def hash_resume_l1(context):
        import pyarrow as pa
        from deltalake import write_deltalake

        root = Path(context.op_config["materialized_output_dir"])
        count = root / "l1-calls.txt"
        count.write_text(str(int(count.read_text()) + 1) if count.exists() else "1")
        write_deltalake(
            str(root / "research_indicator_frames.delta"),
            pa.table(
                {
                    "ts_close_year": [2026],
                    "value": [1.0],
                    "dataset_version": ["nightly"],
                    "contour_id": ["pit_active_front"],
                    "series_mode": ["continuous_front"],
                    "series_id": ["BR"],
                    "indicator_set_version": ["indicators-v1"],
                    "timeframe": ["15m"],
                    "ts": ["2026-10-12T10:00:00Z"],
                }
            ),
            partition_by=["ts_close_year"],
        )
        return {"status": "PASS"}

    @op(config_schema=dict)
    @checkpoint.snapshot_materialization(())
    def hash_resume_r2(context, hash_resume_l1):
        root = Path(context.op_config["materialized_output_dir"])
        count = root / "r2-calls.txt"
        attempts = int(count.read_text()) if count.exists() else 0
        count.write_text(str(attempts + 1))
        if attempts < context.op_config["test_failures"]:
            raise RuntimeError("simulated downstream failure after verified migration L1")
        pending = checkpoint._initial(context, context.op_config)
        assert pending["quality_owners"]["hash_resume_l1"] in pending["recovery_lineage"]
        return {"status": "PASS"}

    @job(executor_def=in_process_executor)
    def resume():
        hash_resume_r2(hash_resume_l1())

    return resume


@pytest.mark.parametrize("failures", [1, 2])
def test_native_migration_from_failure_preserves_completed_delta_checkpoint(
    accepted_chain, tmp_path, failures
):
    import json
    from types import SimpleNamespace

    from dagster import DagsterInstance, ReexecutionOptions, execute_job, reconstructable
    from deltalake import DeltaTable

    config, _, _, _, publish, _, checkpoint = accepted_chain
    output = Path(config["materialized_output_dir"])
    output.mkdir(exist_ok=True)
    config.update(code_version="fixture", data_snapshot_enabled=True, test_failures=failures)
    stages = owner.REQUIRED_QUALITY - {"continuous_front_acceptance"}
    instance_root = tmp_path / "instance"
    instance_root.mkdir()
    with DagsterInstance.local_temp(str(instance_root)) as instance:
        with execute_job(reconstructable(reconstruct_hash_seed_job), instance=instance) as seeded:
            assert seeded.success
            publish(owners={stage: seeded.run_id for stage in stages})
        run_config = {
            "ops": {name: {"config": config} for name in ("hash_resume_l1", "hash_resume_r2")}
        }
        lineage = []
        for attempt in range(failures + 1):
            options = ReexecutionOptions.from_failure(lineage[-1], instance) if lineage else None
            with execute_job(
                reconstructable(reconstruct_hash_resume_job),
                instance=instance,
                run_config=run_config,
                reexecution_options=options,
                raise_on_error=False,
            ) as result:
                assert result.success == (attempt == failures)
                assert (output / "l1-calls.txt").read_text() == "1"
                pending = json.loads(
                    checkpoint._pending_path(
                        SimpleNamespace(run_id=result.run_id), config
                    ).read_text()
                )
                assert pending.get("recovery_lineage", []) == lineage
                lineage.append(result.run_id)
        assert (output / "r2-calls.txt").read_text() == str(failures + 1)
        assert DeltaTable(str(output / "research_indicator_frames.delta")).version() == 0
        assert pending["quality_owners"]["hash_resume_l1"] == lineage[0]
        assert pending["quality_owners"]["hash_resume_r2"] == lineage[-1]
