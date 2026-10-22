from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from trading_advisor_3000.product_plane.common import data_snapshots as storage
from trading_advisor_3000.product_plane.research import admission as owner
from trading_advisor_3000.product_plane.research.data_snapshots import (
    _parameters,
    snapshot_registry,
)


@pytest.fixture
def admitted_chain(tmp_path):
    canonical, research = tmp_path / "canonical", tmp_path / "research"
    research.mkdir()
    source = canonical / "canonical_bars.delta"
    write_deltalake(
        str(source), pa.table({"ts": [datetime(2026, 1, 10, tzinfo=UTC)], "value": [1]})
    )
    root = canonical / ".data_snapshots"
    config = dict(
        canonical_output_dir=canonical.resolve().as_posix(),
        materialized_output_dir=research.resolve().as_posix(),
        dataset_version="test",
        series_mode="continuous_front",
        timeframes=["15m", "1d"],
        dataset_instrument_ids=[],
        refresh_scope_version=1,
        force_full_refresh=False,
        hash_compatibility={},
        data_snapshot_enabled=True,
        operation="daily",
    )
    current = None

    def data(windows=()):
        nonlocal current
        tables = {"canonical_bars": storage.capture_table(source)}
        manifest = storage.create_snapshot(
            tables=tables,
            required_tables=list(tables),
            dataset_version="data",
            run_id="producer",
            code_version="fixture",
            quality_checks={"fixture": "PASS"},
            parents=[current["snapshot_id"]] if current else [],
            parameters={
                "downstream_scope": {
                    "version": 2,
                    "full_refresh": False,
                    "windows": list(windows),
                    "effective_intervals": [],
                    "covered_tables": tables,
                }
            },
        )
        path = storage.save_snapshot(root, manifest)
        storage.publish_snapshot(
            root, path, expected_current_id=current["snapshot_id"] if current else None
        )
        current = manifest
        return path

    path = data()
    accepted = storage.create_snapshot(
        tables=current["tables"],
        required_tables=list(current["tables"]),
        dataset_version="test",
        run_id="accepted",
        code_version="fixture",
        quality_checks={"fixture": "PASS"},
        parents=[current["snapshot_id"]],
        parameters={**_parameters(config), "runtime_provenance": storage.runtime_provenance()},
    )
    registry = snapshot_registry(research, "test", "pit_active_front")
    storage.publish_snapshot(
        registry, storage.save_snapshot(registry, accepted), expected_current_id=None
    )
    config["data_snapshot_manifest_path"] = path.resolve().as_posix()
    return config, data, source


def window(instrument="A"):
    return dict(
        instrument_id=instrument,
        timeframe="15m",
        start_ts="2026-01-04T12:00:00Z",
        end_ts="2026-01-05T00:00:00Z",
    )


def scoped(admitted_chain):
    config, data, _ = admitted_chain
    config["data_snapshot_manifest_path"] = data([window()]).resolve().as_posix()
    value = owner.issue_admission(config, operation="daily")
    assert value["outcome"] == "SCOPED"
    config["changed_windows"] = value["effective_windows"]
    config["refresh_admission"] = value
    return config, value


def test_actual_native_manifests_issue_and_validate_daily_admission(admitted_chain):
    config, value = scoped(admitted_chain)
    assert owner.validate_admission(value, config=config) == value
    assert value["data_parent"]["manifest_sha256"]
    assert value["research_parent"]["manifest_sha256"]


@pytest.mark.parametrize(
    "key,new",
    [
        ("operation", "rebuild"),
        ("refresh_scope_version", 0),
        ("refresh_scope_version", True),
        ("force_full_refresh", True),
        ("hash_compatibility", {"forged": True}),
        ("changed_windows", []),
        ("data_snapshot_enabled", False),
        ("data_snapshot_manifest_path", "other.json"),
        ("timeframes", ["1h"]),
    ],
)
def test_execution_field_mutations_cannot_keep_admission(admitted_chain, key, new):
    config, value = scoped(admitted_chain)
    config[key] = new
    with pytest.raises((ValueError, storage.SnapshotUnavailableError)):
        owner.validate_admission(value, config=config)


def test_rehashed_scope_still_requires_authoritative_derivation(admitted_chain):
    config, value = scoped(admitted_chain)
    payload = {key: item for key, item in deepcopy(value).items() if key != "admission_id"}
    payload["effective_windows"][0]["instrument_id"] = "B"
    forged = owner._seal(payload)
    config["changed_windows"] = forged["effective_windows"]
    with pytest.raises(ValueError, match="proof changed"):
        owner.validate_admission(forged, config=config)


def test_original_scope_retains_all_unaccepted_data_ancestry(admitted_chain):
    config, data, _ = admitted_chain
    data([window("A")])
    config["data_snapshot_manifest_path"] = data([window("B")]).resolve().as_posix()
    value = owner.issue_admission(config, operation="daily")
    assert {item["instrument_id"] for item in value["original_windows"]} == {"A", "B"}


def test_native_version_drift_a_b_a_does_not_reauthorize_old_receipt(admitted_chain):
    config, value = scoped(admitted_chain)
    _, _, source = admitted_chain
    for number in (2, 1):
        write_deltalake(
            str(source),
            pa.table({"ts": [datetime(2026, 1, 10, tzinfo=UTC)], "value": [number]}),
            mode="overwrite",
        )
    with pytest.raises(storage.SnapshotUnavailableError):
        owner.validate_admission(value, config=config)


def test_direct_daily_native_requires_actual_dagster_owner(admitted_chain):
    config, value = scoped(admitted_chain)
    calls = []

    @owner.admitted_compute(output="output_dir")
    def native(*, output_dir, dataset_version):
        calls.append(True)

    with pytest.raises(ValueError, match="Dagster snapshot owner"):
        native(
            output_dir=Path(config["materialized_output_dir"]),
            dataset_version="test",
            refresh_admission=value,
        )
    assert not calls


def test_explicit_rebuild_receipt_cannot_change_native_sources_or_selectors(tmp_path):
    source = tmp_path / "input.delta"
    write_deltalake(str(source), pa.table({"id": [1]}))
    calls = []

    @owner.admitted_compute(output="output_dir", sources=("source_path",))
    def native(*, source_path, output_dir, dataset_version, instrument_ids=(), timeframes=()):
        calls.append(True)
        return owner.active_admission()

    options = dict(
        source_path=source,
        output_dir=tmp_path / "result",
        dataset_version="test",
        instrument_ids=("A",),
        timeframes=("1h",),
    )
    value = native(**options, operation="rebuild")
    assert native(**options, refresh_admission=value) == value
    assert len(calls) == 2
    for mutation in (
        dict(source_path=tmp_path / "other.delta"),
        dict(instrument_ids=("B",)),
        dict(timeframes=("5m",)),
    ):
        with pytest.raises(ValueError, match="admission"):
            native(**{**options, **mutation}, refresh_admission=value)
    assert len(calls) == 2


@pytest.mark.parametrize("value", [None, {}, {"schema_version": 0}])
def test_missing_or_legacy_admission_never_grants_work(value):
    with pytest.raises(ValueError, match="admission"):
        owner.validate_admission(value)


def test_legacy_python_input_writer_retired_before_loading_rows(tmp_path, monkeypatch):
    from trading_advisor_3000.product_plane.research.continuous_front_indicators import (
        input_projection,
    )

    monkeypatch.setattr(
        input_projection,
        "load_research_bar_views",
        lambda **kw: pytest.fail("retired writer read rows"),
    )
    with pytest.raises(ValueError, match="retired"):
        input_projection.materialize_cf_indicator_input_frame(
            dataset_output_dir=tmp_path, output_dir=tmp_path, dataset_version="test"
        )


def test_nested_explicit_rebuild_sources_bind_declared_native_roots(tmp_path):
    path = tmp_path / "input" / "research_bar_views.delta"
    write_deltalake(str(path), pa.table({"value": [1]}))
    calls = []

    @owner.admitted_compute(output="output_dir", sources=("bar_views_path",))
    def child(*, bar_views_path, output_dir, dataset_version):
        calls.append(bar_views_path)

    @owner.admitted_compute(
        output="output_dir", input_roots=(("dataset_output_dir", ("research_bar_views",)),)
    )
    def parent(*, dataset_output_dir, output_dir, dataset_version):
        child(
            bar_views_path=dataset_output_dir / "research_bar_views.delta",
            output_dir=output_dir,
            dataset_version=dataset_version,
        )

    parent(
        dataset_output_dir=path.parent,
        output_dir=tmp_path / "output",
        dataset_version="fixture",
        operation="rebuild",
    )
    assert calls == [path]


def test_rebuild_optional_volume_source_omission_preserves_native_binding(admitted_chain):
    config, _, _ = admitted_chain
    path = Path(config["materialized_output_dir"]).parent / "optional" / "vp.delta"
    config = {**config, "operation": "rebuild", "volume_profile_raw_1m_table_path": path.as_posix()}
    value = owner.issue_admission(config, operation="rebuild")
    calls = []

    @owner.admitted_compute(output="output_dir", sources=("volume_profile_raw_1m_table_path",))
    def native(*, output_dir, dataset_version, volume_profile_raw_1m_table_path=None):
        calls.append(volume_profile_raw_1m_table_path)

    options = dict(output_dir=Path(config["materialized_output_dir"]), dataset_version="test")
    with owner._owner_scope(value):
        native(**options)
    assert calls == [None]
    write_deltalake(str(path), pa.table({"id": [1]}))
    with owner._owner_scope(value):
        for actual in (None, path):
            with pytest.raises(ValueError, match="input appeared"):
                native(**options, volume_profile_raw_1m_table_path=actual)
    value = owner.issue_admission(config, operation="rebuild")
    with owner._owner_scope(value):
        native(**options, volume_profile_raw_1m_table_path=path)
        with pytest.raises(ValueError, match="input path changed"):
            native(**options)
    assert calls == [None, path]
