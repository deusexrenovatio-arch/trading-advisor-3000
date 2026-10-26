from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from trading_advisor_3000.product_plane.common import data_snapshots as snapshots
from trading_advisor_3000.product_plane.research.data_snapshots import snapshot_registry
from trading_advisor_3000.product_plane.research.refresh_scope import resolve_refresh_scope


@pytest.fixture
def chain(tmp_path):
    canonical = tmp_path / "canonical"
    materialized = tmp_path / "research"
    materialized.mkdir()
    source = canonical / "canonical_bars.delta"
    write_deltalake(
        str(source), pa.table({"ts": [datetime(2026, 1, 10, tzinfo=UTC)], "value": [1]})
    )
    root = canonical / ".data_snapshots"
    current = None

    def data(run_id, windows=(), intervals=(), extra=None, covered=True, legacy=False):
        nonlocal current
        tables = {"canonical_bars": snapshots.capture_table(source), **(extra or {})}
        scope = {
            "version": 2,
            "full_refresh": False,
            "windows": list(windows),
            "effective_intervals": list(intervals),
            "covered_tables": tables if covered else {},
        }
        manifest = snapshots.create_snapshot(
            tables=tables,
            required_tables=list(tables),
            dataset_version="data",
            run_id=run_id,
            code_version="fixture",
            quality_checks={"fixture": "PASS"},
            parameters={} if legacy else {"downstream_scope": scope},
            parents=[current["snapshot_id"]] if current else [],
        )
        path = snapshots.save_snapshot(root, manifest)
        snapshots.publish_snapshot(
            root, path, expected_current_id=current["snapshot_id"] if current else None
        )
        current = manifest
        return path

    data("accepted-data")
    research = snapshots.create_snapshot(
        tables=current["tables"],
        required_tables=list(current["tables"]),
        dataset_version="nightly",
        run_id="accepted-research",
        code_version="fixture",
        quality_checks={"fixture": "PASS"},
        parameters={"refresh_scope_version": 1, "timeframes": ["15m", "1d"]},
        parents=[current["snapshot_id"]],
    )
    registry = snapshot_registry(materialized, "nightly", "pit_active_front")
    snapshots.publish_snapshot(
        registry, snapshots.save_snapshot(registry, research), expected_current_id=None
    )

    def resolve(path):
        return resolve_refresh_scope(
            data_manifest_path=path,
            materialized_output_dir=materialized,
            dataset_version="nightly",
            timeframes=("15m", "1d"),
        )

    return data, resolve, source, materialized, root


def _window(*, instrument="BR", end="2026-01-05T00:00:00Z"):
    return {
        "instrument_id": instrument,
        "timeframe": "15m",
        "start_ts": "2026-01-04T12:00:00Z",
        "end_ts": end,
    }


def test_failed_research_backlog_survives_raw_noop_and_a_b_a(chain):
    data, resolve, source, _, _ = chain
    first = data("changed", [_window()])
    noop = data("raw-noop")
    scope = resolve(noop)
    assert scope["force_full_refresh"] is False
    assert scope["windows"][0]["start_ts"] == "2025-12-28T00:00:00Z"
    assert scope["windows"][0]["end_ts"] == "2026-01-10T00:00:00Z"
    for value in (2, 1):
        write_deltalake(
            str(source),
            pa.table({"ts": [datetime(2026, 1, 10, tzinfo=UTC)], "value": [value]}),
            mode="overwrite",
        )
        path = data(f"value-{value}", [_window()])
    assert resolve(path)["windows"] == scope["windows"]
    with pytest.raises(ValueError, match="not accepted current"):
        resolve(first)


def test_tail_includes_failed_l2_sidecar_and_original_dirty_end(chain):
    data, resolve, _, materialized, _ = chain
    write_deltalake(
        str(materialized / "cf_indicator_state.delta"),
        pa.table({"ts": [datetime(2026, 1, 20, tzinfo=UTC)], "value": [1]}),
    )
    assert (
        resolve(data("late-sidecar", [_window()]))["windows"][0]["end_ts"] == "2026-01-20T00:00:00Z"
    )
    assert (
        resolve(data("declared-later", [_window(end="2026-01-30T00:00:00Z")]))["windows"][0][
            "end_ts"
        ]
        == "2026-01-30T00:00:00Z"
    )


def test_incomplete_stats_in_one_owned_file_force_full(chain):
    data, resolve, _, materialized, _ = chain
    path = materialized / "research_indicator_frames.delta"
    for day, mode in ((15, "overwrite"), (20, "append")):
        write_deltalake(
            str(path), pa.table({"ts": [datetime(2026, 1, day, tzinfo=UTC)]}), mode=mode
        )
    log = path / "_delta_log/00000000000000000001.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "add" in action:
            stats = json.loads(action["add"]["stats"])
            stats["maxValues"].pop("ts")
            action["add"]["stats"] = json.dumps(stats)
    log.write_text("\n".join(json.dumps(action) for action in actions) + "\n")
    assert resolve(data("missing-one-file-stat", [_window()]))["force_full_refresh"] is False


@pytest.mark.parametrize("name", ["raw_daily_open_interest", "raw_moex_history"])
def test_unscoped_dependency_change_forces_full_despite_raw_scope(chain, name):
    data, resolve, source, _, _ = chain
    dependency = source.parent / f"{name}.delta"
    write_deltalake(str(dependency), pa.table({"id": [1]}))
    initial = snapshots.capture_table(dependency)
    data("dependency-known", extra={name: initial})
    write_deltalake(str(dependency), pa.table({"id": [2]}), mode="overwrite")
    scope = resolve(
        data(
            "dependency-moved",
            [_window()],
            extra={name: snapshots.capture_table(dependency)},
            covered=False,
        )
    )
    assert scope["force_full_refresh"] is False
    assert scope["reason"] == f"unbounded_dependency:{name}"


def test_missing_ancestry_and_unmapped_or_ambiguous_economics_are_full(chain):
    data, resolve, _, _, root = chain
    intermediate = data("middle", [_window()])
    latest = data("latest")
    saved = intermediate.read_text()
    intermediate.unlink()
    assert resolve(latest)["force_full_refresh"] is False
    intermediate.write_text(saved)
    interval = {
        "instrument_id": "FUT_BR",
        "effective_from_ts": "2025-01-01T00:00:00Z",
        "effective_to_ts": None,
    }
    assert (
        resolve(data("ambiguous", [_window(), _window(instrument="FUT_BR")], [interval]))[
            "force_full_refresh"
        ]
        is False
    )


def test_economics_interval_reaches_all_timeframes_and_old_owned_rows(chain):
    data, resolve, _, _, _ = chain
    interval = {
        "instrument_id": "FUT_BR",
        "effective_from_ts": "2025-01-01T00:00:00Z",
        "effective_to_ts": None,
    }
    scope = resolve(data("economics-only", [_window()], [interval]))
    assert scope["force_full_refresh"] is False
    assert {window["timeframe"] for window in scope["windows"]} == {"15m", "1d"}
    assert all(window["start_ts"] == "2024-12-25T00:00:00Z" for window in scope["windows"])


def test_data_publisher_keeps_parent_and_checks_per_table_scope_versions(tmp_path, monkeypatch):
    from trading_advisor_3000.product_plane.data_plane import data_snapshots as owner

    raw = tmp_path / "raw" / "raw_moex_history.delta"
    bars = tmp_path / "canonical" / "canonical_bars.delta"
    for path in (raw, bars):
        write_deltalake(str(path), pa.table({"ts": [datetime(2026, 1, 10, tzinfo=UTC)]}))
    reference = snapshots.capture_table(bars)
    provenance = {"fixture": "stable"}
    monkeypatch.setattr(owner, "runtime_provenance", lambda: provenance)
    report = {
        "status": "PASS",
        "publish_decision": "publish",
        "run_id": "first",
        "refresh_window_days": 3,
        "economics_mode": "refresh",
        "economics_refresh": {"status": "PASS"},
        "canonical_report": {
            "status": "PASS",
            "sidecar_refresh": {
                "dependency_scope": {
                    "version": 2,
                    "full_refresh": False,
                    "windows": [],
                    "output_delta_versions": {"canonical_bars": reference},
                }
            },
        },
        "cf_catch_up": {
            "source_changed_windows": [],
            "windows": [_window()],
            "target_timeframes": ["15m"],
        },
    }
    first = owner.publish_data_snapshot(
        inputs={"raw_moex_history": snapshots.capture_table(raw)},
        canonical_paths=[bars],
        canonical_roots=[],
        report=report,
        expected_current_id=None,
        provenance=provenance,
    )
    report["run_id"] = "second"
    second = owner.publish_data_snapshot(
        inputs={"raw_moex_history": snapshots.capture_table(raw)},
        canonical_paths=[bars],
        canonical_roots=[],
        report=report,
        expected_current_id=first["snapshot_id"],
        provenance=provenance,
    )
    manifest = snapshots.load_snapshot(Path(second["snapshot_manifest_path"]))
    assert manifest["parents"] == [first["snapshot_id"]]
    assert "canonical_bars" in manifest["parameters"]["downstream_scope"]["covered_tables"]
    report["canonical_report"]["sidecar_refresh"]["dependency_scope"]["output_delta_versions"] = {
        "canonical_bars": {**reference, "delta_version": reference["delta_version"] + 1}
    }
    assert (
        "canonical_bars"
        not in owner._downstream_scope(report=report, tables=manifest["tables"])["covered_tables"]
    )


def test_bound_legacy_data_and_removed_dependency_never_become_noop(chain):
    data, resolve, source, _, _ = chain
    result = resolve(data("bound-legacy-raw-noop", legacy=True))
    assert result["force_full_refresh"] is False
    assert result["reason"] == "legacy_bound_data_scope"
    dependency = source.parent / "canonical_closed_dates.delta"
    write_deltalake(str(dependency), pa.table({"id": [1]}))
    data(
        "dependency-present", extra={"canonical_closed_dates": snapshots.capture_table(dependency)}
    )
    result = resolve(data("dependency-removed"))
    assert result["force_full_refresh"] is False
    assert result["reason"] == "unbounded_dependency:canonical_closed_dates"


@pytest.mark.parametrize(
    "key,value",
    [
        ("continuous_front_policy", {"changed": True}),
        ("indicator_profile_version", "different"),
        ("volume_profile_tick_size_by_instrument", {"BR": 0.1}),
        ("volume_profile_raw_1m_table_path", "/different/raw.delta"),
    ],
)
def test_changed_research_semantics_force_full_on_data_noop(chain, key, value):
    data, _, _, materialized, _ = chain
    result = resolve_refresh_scope(
        data_manifest_path=data("raw-noop"),
        materialized_output_dir=materialized,
        dataset_version="nightly",
        timeframes=("15m", "1d"),
        configuration={key: value},
    )
    assert result["force_full_refresh"] is False
    assert result["reason"] == "research_semantics_changed"


def test_external_volume_profile_version_is_checked_before_noop(chain):
    data, _, source, materialized, _ = chain
    vp = source.parent.parent / "external" / "minute_feed.delta"
    write_deltalake(str(vp), pa.table({"id": [1]}))
    registry = snapshot_registry(materialized, "nightly", "pit_active_front")
    old = snapshots.current_snapshot(registry)
    accepted = snapshots.create_snapshot(
        tables={**old["tables"], vp.stem: snapshots.capture_table(vp)},
        required_tables=[*old["tables"], vp.stem],
        dataset_version="nightly",
        run_id="external-vp",
        code_version="fixture",
        quality_checks={"fixture": "PASS"},
        parents=old["parents"],
        parameters={
            **old["parameters"],
            "volume_profile_raw_1m_table_path": str(vp),
            "runtime_provenance": snapshots.runtime_provenance(),
        },
    )
    snapshots.publish_snapshot(
        registry,
        snapshots.save_snapshot(registry, accepted),
        expected_current_id=old["snapshot_id"],
    )
    path = data("raw-noop-with-external-vp")
    options = dict(
        data_manifest_path=path,
        materialized_output_dir=materialized,
        dataset_version="nightly",
        timeframes=("15m", "1d"),
        configuration={"volume_profile_raw_1m_table_path": str(vp)},
    )
    assert resolve_refresh_scope(**options)["force_full_refresh"] is False
    write_deltalake(str(vp), pa.table({"id": [2]}), mode="append")
    result = resolve_refresh_scope(**options)
    assert result["force_full_refresh"] is False
    assert result["reason"] == "unbounded_external_volume_profile"


def test_daily_only_change_reaches_weekly_usage_dependency(chain):
    data, _, _, materialized, _ = chain
    registry = snapshot_registry(materialized, "nightly", "pit_active_front")
    old = snapshots.current_snapshot(registry)
    accepted = snapshots.create_snapshot(
        tables=old["tables"],
        required_tables=list(old["tables"]),
        dataset_version="nightly",
        run_id="daily-weekly",
        code_version="fixture",
        quality_checks={"fixture": "PASS"},
        parents=old["parents"],
        parameters={"refresh_scope_version": 1, "timeframes": ["1d", "1w"]},
    )
    snapshots.publish_snapshot(
        registry,
        snapshots.save_snapshot(registry, accepted),
        expected_current_id=old["snapshot_id"],
    )
    result = resolve_refresh_scope(
        data_manifest_path=data("daily-only", [{**_window(), "timeframe": "1d"}]),
        materialized_output_dir=materialized,
        dataset_version="nightly",
        timeframes=("1d", "1w"),
    )
    assert result["force_full_refresh"] is False
    assert {window["timeframe"] for window in result["windows"]} == {"1d", "1w"}


def test_daily_unknown_scope_is_observably_blocked_not_full_or_noop(chain):
    data, resolve, _, _, _ = chain
    result = resolve(data("legacy-admission", legacy=True))
    assert result["outcome"] == "BLOCKED"
    assert result["reason"] == "legacy_bound_data_scope"
    assert result["force_full_refresh"] is False
    assert result["windows"] == []


def test_daily_no_changes_has_explicit_noop_outcome(chain):
    data, resolve, _, _, _ = chain
    assert resolve(data("proven-noop"))["outcome"] == "NOOP"


def test_daily_proven_change_has_explicit_scoped_outcome(chain):
    data, resolve, _, _, _ = chain
    result = resolve(data("proven-change", [_window()]))
    assert result["outcome"] == "SCOPED"
    assert result["windows"]


def test_direct_l1_missing_admission_rejected_before_writer_lock(tmp_path, monkeypatch):
    from trading_advisor_3000.product_plane.research.indicators import materialize as owner

    def forbidden(**kwargs):
        pytest.fail("missing admission reached the writer lock")

    monkeypatch.setattr(owner, "_acquire_indicator_frame_materialization_lock", forbidden)
    with pytest.raises(ValueError, match="admission"):
        owner.materialize_indicator_frames(
            dataset_output_dir=tmp_path,
            indicator_output_dir=tmp_path,
            dataset_version="daily",
            indicator_set_version="indicators-v1",
            changed_windows=None,
            force_recompute=True,
        )


@pytest.mark.parametrize(
    "identities,expected", [(["FUT_BR"], "SCOPED"), (["FUT_BR", "BR"], "BLOCKED")]
)
def test_economics_only_scope_uses_actual_pinned_instrument_identity(chain, identities, expected):
    data, resolve, source, _, _ = chain
    write_deltalake(
        str(source),
        pa.table(
            {
                "ts": [datetime(2026, 1, 10, tzinfo=UTC)] * len(identities),
                "instrument_id": identities,
                "value": [1] * len(identities),
            }
        ),
        mode="overwrite",
        schema_mode="overwrite",
    )
    scope = resolve(
        data(
            "economics-only",
            intervals=[{"instrument_id": "BR", "effective_from_ts": "2026-01-05T00:00:00Z"}],
        )
    )
    assert scope["outcome"] == expected
    if expected == "SCOPED":
        assert {w["instrument_id"] for w in scope["windows"]} == {"FUT_BR"}
    else:
        assert scope["reason"] == "unproven_economics_identity"


def test_legacy_v1_producer_scope_cannot_authorize_daily(chain):
    data, resolve, _, _, _ = chain
    path = data("old-v1", [_window()])
    payload = json.loads(path.read_text())
    payload["parameters"]["downstream_scope"]["version"] = 1
    payload.pop("manifest_sha256")
    payload["manifest_sha256"] = snapshots._hash(payload)
    path.write_text(json.dumps(payload))
    # The current pointer binds the manifest digest and must be republished.
    root = path.parent.parent
    pointer = json.loads((root / "published_current.json").read_text())
    pointer["manifest_sha256"] = payload["manifest_sha256"]
    (root / "published_current.json").write_text(json.dumps(pointer))
    scope = resolve(path)
    assert scope["outcome"] == "BLOCKED" and scope["reason"] == "legacy_bound_data_scope"
