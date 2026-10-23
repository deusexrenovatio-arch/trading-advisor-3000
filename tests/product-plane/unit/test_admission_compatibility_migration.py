import copy
from pathlib import Path

import pytest
from test_research_hash_compatibility import accepted_chain as accepted_chain

from trading_advisor_3000.product_plane.research import compute_contract as c
from trading_advisor_3000.product_plane.research import hash_compatibility as h
from trading_advisor_3000.product_plane.research.semantic_fingerprint import digest


def old_contract(current):
    old = c.legacy_source_schema_contract(current)
    old["engine"] = h.BOOTSTRAP_ENGINE
    old["kernels"] = {**old["kernels"], "sidecar": h.BOOTSTRAP_KERNELS["sidecar"]}
    old["digest"] = digest({k: v for k, v in old.items() if k != "digest"})
    return old


def test_operational_admission_transition_requires_exact_old_and_new_boundaries():
    current = c.semantic_compute_contract()
    old = old_contract(current)
    mapped, proof = h.admission_contract_transition(old, current)
    assert proof["old_compute_digest"] == old["digest"]
    assert proof["compute_digest"] == current["digest"]
    assert mapped == c.legacy_source_schema_contract(current)
    for section, key in [
        ("kernels", "l1"),
        ("kernels", "sidecar"),
        ("numeric_dependencies", "numpy"),
    ]:
        changed = copy.deepcopy(current)
        changed[section][key] = "0" * 64
        changed["digest"] = digest({k: v for k, v in changed.items() if k != "digest"})
        with pytest.raises(ValueError):
            h.admission_contract_transition(old, changed)


def test_legacy_parameters_accept_new_daily_receipt_but_not_changed_domain(accepted_chain):
    from trading_advisor_3000.product_plane.common import data_snapshots as storage
    from trading_advisor_3000.product_plane.research.admission import issue_admission
    from trading_advisor_3000.product_plane.research.refresh_scope import resolve_refresh_scope

    config, pending, instance, _, publish, _, checkpoint = accepted_chain
    current = c.semantic_compute_contract(config)
    accepted = publish(parameters={"semantic_compute_contract": old_contract(current)})
    root = Path(config["canonical_output_dir"]) / ".data_snapshots"
    previous = storage.current_snapshot(root)
    tables = previous["tables"]
    child = storage.create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version="data",
        run_id="native-v2",
        code_version="test",
        quality_checks={"producer": "PASS"},
        parents=[previous["snapshot_id"]],
        parameters={
            "downstream_scope": {
                "version": 2,
                "full_refresh": False,
                "windows": [
                    {
                        "instrument_id": "FUT_BR",
                        "timeframe": "15m",
                        "start_ts": "2026-01-09T00:00:00Z",
                        "end_ts": "2026-01-10T00:00:00Z",
                    }
                ],
                "effective_intervals": [],
                "covered_tables": tables,
            }
        },
    )
    path = storage.save_snapshot(root, child)
    storage.publish_snapshot(root, path, expected_current_id=previous["snapshot_id"])
    config["data_snapshot_manifest_path"] = str(path)
    scope = resolve_refresh_scope(
        data_manifest_path=path,
        materialized_output_dir=Path(config["materialized_output_dir"]),
        dataset_version=config["dataset_version"],
        timeframes=config["timeframes"],
        configuration=config,
    )
    assert scope["outcome"] == "SCOPED" and scope["hash_compatibility"]["version"] == 4
    config.update(
        operation="daily",
        changed_windows=scope["windows"],
        hash_compatibility=scope["hash_compatibility"],
    )
    config["refresh_admission"] = issue_admission(config, operation="daily")
    pending.update(parents=[child["snapshot_id"]], parent_manifest_sha256=child["manifest_sha256"])
    assert checkpoint._validate_hash_compatibility(config, pending, instance) == accepted
    changed = {**config, "continuous_front_policy": {"unreviewed": True}}
    with pytest.raises(ValueError, match="semantics or QC"):
        checkpoint._validate_hash_compatibility(changed, pending, instance)


def _numeric_hashes():
    from test_indicator_scoped_writer import _profile

    from trading_advisor_3000.product_plane.research.derived_indicators.materialize import (
        _derived_compute_contract_hash,
    )
    from trading_advisor_3000.product_plane.research.indicators.materialize import (
        _indicator_compute_contract_hash,
    )

    return _indicator_compute_contract_hash(_profile()), _derived_compute_contract_hash(())


def test_audited_engine_keeps_stored_l1_l2_hashes_and_other_changes_invalidate():
    identity = c._current_numeric_identity()
    assert identity["engine"] == h.ADMISSION_ENGINE
    old = {**identity, "engine": h.BOOTSTRAP_ENGINE}
    token = c._active_numeric_identity.set(old)
    try:
        stored = _numeric_hashes()
    finally:
        c._active_numeric_identity.reset(token)
    assert _numeric_hashes() == stored
    for section, key in [("engine", None), ("kernels", "l1"), ("dependencies", "numpy")]:
        changed = copy.deepcopy(identity)
        if key is None:
            changed[section] = "unknown-engine"
        else:
            changed[section][key] = "changed"
        token = c._active_numeric_identity.set(changed)
        try:
            assert all(a != b for a, b in zip(_numeric_hashes(), stored, strict=True))
        finally:
            c._active_numeric_identity.reset(token)


def test_equivalent_alias_does_not_mask_raw_engine_drift(monkeypatch):
    identity = c._current_numeric_identity()
    calls = iter([identity, {**identity, "engine": h.BOOTSTRAP_ENGINE}])
    monkeypatch.setattr(c, "_current_numeric_identity", lambda: next(calls))

    @c.native_compute_scope
    def operation():
        assert c.numerical_identity()["engine"] == h.BOOTSTRAP_ENGINE

    with pytest.raises(ValueError, match="changed during native operation"):
        operation()


def test_old_engine_carrier_changed_suffix_preserves_native_prefix(tmp_path, monkeypatch):
    from dataclasses import replace

    from deltalake import DeltaTable
    from test_indicator_cache_identity_contract import _window_kwargs
    from test_research_indicator_layer import _view

    from trading_advisor_3000.product_plane.research.indicators import materialize as owner
    from trading_advisor_3000.product_plane.research.indicators.batch_writer import (
        write_indicator_frame_scoped_batches,
    )

    kwargs = _window_kwargs(tmp_path)
    key = next(iter(kwargs["partition_counts"]))
    bars = [_view(ts_index=i, close=80.0 + i) for i in range(5)]
    identity = c._current_numeric_identity()
    token = c._active_numeric_identity.set({**identity, "engine": h.BOOTSTRAP_ENGINE})
    try:
        rows = owner._build_partition_rows(
            dataset_version=key.dataset_version,
            indicator_set_version=key.indicator_set_version,
            profile=kwargs["profile"],
            series=bars,
            source_snapshot_hash="OLD",
            native_full_compute=True,
        )
    finally:
        c._active_numeric_identity.reset(token)
    write_indicator_frame_scoped_batches(
        output_dir=tmp_path,
        profile=kwargs["profile"],
        partition_row_batches=((key, rows),),
        partition_start_ts={key: rows[0].ts},
    )
    table = tmp_path / "research_indicator_frames.delta"
    before = DeltaTable(str(table)).to_pyarrow_table().to_pylist()
    kwargs["partition_counts"] = {key: len(bars)}
    kwargs["existing_by_partition"] = {key: rows[-1].to_dict()}
    kwargs["source_snapshot_hash"] = "NEW"
    kwargs["windows"] = owner._normalize_indicator_windows(
        [
            {
                "instrument_id": "BR",
                "timeframe": "15m",
                "start_ts": bars[-1].ts,
                "end_ts": bars[-1].ts,
            }
        ]
    )
    corrected = [*bars[:-1], replace(bars[-1], close=100.0)]
    monkeypatch.setattr(owner, "_load_bar_partition_rows", lambda **_: corrected)
    report = owner._materialize_indicator_window_tails(**kwargs)
    after = DeltaTable(str(table)).to_pyarrow_table().to_pylist()
    assert report["refreshed_row_count"] == 1
    assert sorted(
        (r for r in after if owner._utc_timestamp(r["ts"]) < owner._utc_timestamp(bars[-1].ts)),
        key=lambda r: r["ts"],
    ) == sorted(
        (r for r in before if owner._utc_timestamp(r["ts"]) < owner._utc_timestamp(bars[-1].ts)),
        key=lambda r: r["ts"],
    )
    assert (
        next(
            r for r in after if owner._utc_timestamp(r["ts"]) == owner._utc_timestamp(bars[-1].ts)
        )["sma_2"]
        != rows[-1].values["sma_2"]
    )
