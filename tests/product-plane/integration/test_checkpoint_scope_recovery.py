"""A superseded daily CF can seed the remaining bounded Data changes."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow as pa
import pytest
from dagster._core.remote_origin import (
    LoadableTargetOrigin,
    ManagedGrpcPythonEnvCodeLocationOrigin,
    RemoteRepositoryOrigin,
)
from deltalake import write_deltalake
from test_native_scope_recovery import chain as daily_chain  # noqa: F401
from test_native_scope_recovery import pytestmark, spark_factory  # noqa: F401

from trading_advisor_3000.product_plane.common import cf_checkpoints as owner
from trading_advisor_3000.product_plane.common import data_snapshots as storage
from trading_advisor_3000.product_plane.data_plane.data_snapshots import recover_data_scope
from trading_advisor_3000.product_plane.research import admission
from trading_advisor_3000.product_plane.research.continuous_front import CONTINUOUS_FRONT_TABLES
from trading_advisor_3000.product_plane.research.refresh_scope import resolve_refresh_scope
from trading_advisor_3000.spark_jobs.snapshot_diff_job import verify_cf_retained_rows


@pytest.mark.parametrize("daily_chain", ["daily"], indirect=True)
@pytest.mark.usefixtures("daily_chain")
def test_certified_cf_and_retained_prefix_then_foreign_write(request):
    canonical, output, registry, first, intermediate, previous, bars = request.getfixturevalue(
        "daily_chain"
    )
    data_registry = canonical / ".data_snapshots"
    frames = {}
    old_tables = dict(previous["tables"])
    for name in CONTINUOUS_FRONT_TABLES:
        row = {
            "dataset_version": "nightly",
            "instrument_id": "FUT_BR",
            "timeframe": "15m",
            "value": 1,
        }
        dates = (
            ("ts",)
            if name.endswith("bars")
            else ("effective_ts", "decision_ts", "last_old_bar_ts", "first_new_bar_ts")
        )
        for col in dates:
            row[col] = datetime(2026, 7, 30, tzinfo=UTC)
        frame = pa.Table.from_pylist([row])
        path = output / (name + ".delta")
        write_deltalake(str(path), frame)
        old_tables[name] = storage.capture_table(path)
        frames[name] = frame
    contract = {"digest": "isolated-consumer"}
    accepted = storage.create_snapshot(
        tables=old_tables,
        required_tables=list(old_tables),
        dataset_version="nightly",
        run_id="research-owner",
        code_version="test",
        parents=[first["snapshot_id"]],
        quality_checks={"native": "PASS"},
        parameters={
            **previous["parameters"],
            "materialized_output_dir": str(output),
            "semantic_compute_contract": contract,
        },
    )
    storage.publish_snapshot(
        registry,
        storage.save_snapshot(registry, accepted),
        expected_current_id=previous["snapshot_id"],
    )
    outputs = {}
    for name, frame in frames.items():
        path = output / (name + ".delta")
        write_deltalake(
            str(path),
            frame.set_column(frame.schema.get_field_index("value"), "value", pa.array([2])),
            mode="overwrite",
        )
        outputs[name] = storage.capture_table(path)
    window = {
        "instrument_id": "FUT_BR",
        "timeframe": "15m",
        "start_ts": "2026-07-23T00:00:00Z",
        "end_ts": "2026-07-31T00:00:00Z",
    }
    mid = admission._parent(data_registry, intermediate)
    receipt = admission._seal(
        dict(
            operation="daily",
            outcome="SCOPED",
            inputs=intermediate["tables"],
            research_parent=admission._parent(registry, accepted),
            data_parent=mid,
            effective_windows=[window],
        )
    )
    stage = "continuous_front_bars"
    config = dict(
        materialized_output_dir=str(output),
        series_mode="continuous_front",
        dataset_version="nightly",
        operation="daily",
        force_full_refresh=False,
        refresh_admission=receipt,
        changed_windows=[window],
        data_snapshot_manifest_path=mid["manifest_path"],
    )
    status = SimpleNamespace(value="SUCCESS")
    location = ManagedGrpcPythonEnvCodeLocationOrigin(
        LoadableTargetOrigin(
            module_name="trading_advisor_3000.dagster_defs", attribute="product_plane_definitions"
        )
    )
    repo = RemoteRepositoryOrigin(code_location_origin=location, repository_name="__repository__")
    origin = SimpleNamespace(repository_origin=repo)
    baseline = SimpleNamespace(
        run_id="baseline",
        job_name="moex_baseline_update_job",
        status=status,
        remote_job_origin=origin,
        tags={"dagster/schedule_name": "moex_baseline_daily_update_schedule"},
        run_config={
            "ops": {"moex_baseline_update": {"config": {"run_id": intermediate["run_id"]}}}
        },
    )
    cf = SimpleNamespace(
        run_id="cf",
        job_name="moex_cf_catch_up_job",
        status=status,
        remote_job_origin=origin,
        run_config={"ops": {stage: {"config": config}}},
        tags={
            "ta3000/upstream_run_id": "baseline",
            "ta3000/upstream_job": baseline.job_name,
            "dagster/sensor_name": "moex_cf_catch_up_after_moex_baseline_sensor",
            "ta3000/refresh_admission_id": receipt["admission_id"],
        },
    )
    storage.atomic_json(
        owner._pending_path(cf, config),
        dict(
            run_id="cf",
            parameters={"refresh_admission": receipt},
            parents=[intermediate["snapshot_id"]],
            parent_manifest_sha256=intermediate["manifest_sha256"],
            expected_current_id=accepted["snapshot_id"],
            quality_owners={stage: "cf"},
            quality_checks={stage: "PASS"},
            semantic_compute_contract=contract,
            tables={**old_tables, **intermediate["tables"], **outputs},
            quality_evidence={
                stage: {
                    "cf_result": dict(
                        status="PASS",
                        dataset_version="nightly",
                        rows_by_table={name: 1 for name in outputs},
                        qc_rows=[{"status": "PASS"}],
                        output_paths={k: v["table_path"] for k, v in outputs.items()},
                    )
                }
            },
        ),
    )
    instance = SimpleNamespace(
        get_run_by_id={"cf": cf, "baseline": baseline}.get,
        get_run_step_stats=lambda _: [SimpleNamespace(step_key=stage, status=status)],
        get_run_record_by_id=lambda _: SimpleNamespace(
            create_timestamp=datetime(2026, 11, 18, tzinfo=UTC)
        ),
    )

    def checkpoint():
        return owner.recovery_cf_checkpoint(
            instance, "cf", research=accepted, data_root=data_registry, research_root=registry
        )

    origin.repository_origin = RemoteRepositoryOrigin(
        code_location_origin=ManagedGrpcPythonEnvCodeLocationOrigin(
            location.loadable_target_origin, location_name="foreign"
        ),
        repository_name="__repository__",
    )
    with pytest.raises(ValueError, match="code location"):
        checkpoint()
    origin.repository_origin = repo
    events = {
        repo.get_instigator_origin(obj.tags[tag]).get_id(): obj.run_id
        for obj, tag in ((baseline, "dagster/schedule_name"), (cf, "dagster/sensor_name"))
    }
    instance.get_ticks = lambda *_args, **_kwargs: []
    with pytest.raises(ValueError, match="successful owner tick"):
        checkpoint()
    instance.get_ticks = lambda key, *args, **kwargs: [
        SimpleNamespace(instigator_origin_id=key, run_ids=[events[key]], tick_id=1)
    ]
    write_deltalake(
        str(bars),
        pa.Table.from_pylist(
            [
                dict(
                    instrument_id="FUT_BR",
                    timeframe="15m",
                    ts=datetime(2026, 7, 30, tzinfo=UTC),
                    close=4.0,
                    ts_close_year=2026,
                )
            ]
        ),
        mode="overwrite",
        predicate="ts_close_year = 2026",
    )
    current = storage.create_snapshot(
        tables={"canonical_bars": storage.capture_table(bars)},
        required_tables=["canonical_bars"],
        dataset_version="data",
        run_id="next",
        code_version="test",
        quality_checks={"baseline": "PASS"},
        parents=[intermediate["snapshot_id"]],
        parameters={"downstream_scope": {"version": 2, "full_refresh": True}},
    )
    storage.publish_snapshot(
        data_registry,
        storage.save_snapshot(data_registry, current),
        expected_current_id=intermediate["snapshot_id"],
    )
    value = recover_data_scope(
        canonical_root=canonical,
        research_root=registry,
        not_before="2026-07-01T00:00:00Z",
        expected_data_id=current["snapshot_id"],
        expected_research_id=accepted["snapshot_id"],
        prior_cf_run_id="cf",
        instance=instance,
        spark_session_factory=spark_factory,
        publish=True,
    )
    bridge = value["manifest"]
    assert bridge["parents"] == [current["snapshot_id"]]
    assert bridge["parameters"]["downstream_scope"]["prior_cf"]["recovery"][
        "source"
    ] == storage._recovery_identity(intermediate)

    def decision():
        return resolve_refresh_scope(
            data_manifest_path=data_registry / "manifests" / (bridge["snapshot_id"] + ".json"),
            materialized_output_dir=output,
            dataset_version="nightly",
            timeframes=["15m"],
        )

    assert decision()["windows"] == [window]
    for name, ref in outputs.items():
        assert storage.capture_table(output / (name + ".delta")) == ref
    name = "continuous_front_adjustment_ladder"
    write_deltalake(str(output / (name + ".delta")), frames[name], mode="append")
    blocked = decision()
    assert blocked["outcome"] == "BLOCKED" and name in blocked["reason"]
    name = "continuous_front_bars"
    old_row = {**frames[name].to_pylist()[0], "ts": datetime(2024, 1, 1, tzinfo=UTC)}
    path = output / (name + ".delta")
    write_deltalake(str(path), pa.Table.from_pylist([old_row]), mode="append")
    with pytest.raises(ValueError, match="changed retained Research inputs: continuous_front_bars"):
        verify_cf_retained_rows(
            accepted,
            {**outputs, name: storage.capture_table(path)},
            value["report"]["windows"],
            spark_session_factory=spark_factory,
        )
