"""Isolated native daily admission proof, using physical Delta and real Dagster assets."""

from __future__ import annotations

import importlib.util
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from dagster import (
    DagsterInstance,
    ReexecutionOptions,
    build_sensor_context,
    execute_job,
    in_process_executor,
    reconstructable,
)
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.dagster_defs import research_assets as assets
from trading_advisor_3000.dagster_defs.product_plane_definitions import (
    build_product_plane_definitions,
)
from trading_advisor_3000.product_plane.common import data_snapshots as storage
from trading_advisor_3000.product_plane.research import admission
from trading_advisor_3000.product_plane.research.data_snapshots import snapshot_registry


def native_daily_prep_job():
    repository = build_product_plane_definitions().get_repository_def()
    return (
        repository.get_job(assets.RESEARCH_DATA_PREP_JOB_NAME)
        .get_subset(
            asset_selection={assets.AssetKey(name) for name in assets.RESEARCH_DATA_PREP_JOB_ASSETS}
        )
        .with_executor_def(in_process_executor)
    )


@pytest.mark.skipif(
    os.name == "nt", reason="native Spark/Delta proof executes in isolated Docker/Linux"
)
def test_native_rebuild_then_sensor_daily_publish_and_readback(tmp_path, monkeypatch):
    _native_handoff(tmp_path, monkeypatch, foreign_write=False)


@pytest.mark.skipif(
    os.name == "nt", reason="native Spark/Delta proof executes in isolated Docker/Linux"
)
def test_foreign_cf_commit_and_replacement_block_scheduler_and_consumer(tmp_path, monkeypatch):
    _native_handoff(tmp_path, monkeypatch, foreign_write=True)


@pytest.mark.parametrize("foreign_write", [False, True, "initial"])
@pytest.mark.skipif(os.name == "nt", reason="native Spark/Delta proof runs on Linux")
def test_accepted_input_pins_through_native_daily_chain(tmp_path, monkeypatch, foreign_write):
    _native_handoff(tmp_path, monkeypatch, foreign_write=foreign_write, pinned_recovery=True)


def _native_handoff(tmp_path, monkeypatch, *, foreign_write, pinned_recovery=False):
    source = Path(__file__).with_name("test_research_dagster_jobs.py")
    spec = importlib.util.spec_from_file_location("native_daily_fixtures", source)
    fixtures = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixtures)
    canonical, output, raw = tmp_path / "canonical", tmp_path / "research", tmp_path / "raw"
    fixtures._write_rich_stage7_canonical_context(canonical)
    if pinned_recovery:
        monkeypatch.setattr(
            assets, "DEFAULT_VOLUME_PROFILE_TICK_SIZE_BY_INSTRUMENT", {"BR": 0.01, "Si": 1.0}
        )
    # This intraday fixture has no exchange expiry packet. Use the native OI
    # policy and cross two contracts so all four CF outputs have physical rows.
    monkeypatch.setenv(
        assets.RESEARCH_DATA_PREP_CONTINUOUS_FRONT_POLICY_ENV,
        json.dumps(
            dict(
                roll_policy_version="front_liquidity_oi_v1",
                roll_policy_mode="liquidity_oi_v1",
                primary_metric="open_interest",
                secondary_metric="volume",
                switch_timing="next_tradable_bar_after_decision_watermark",
                tie_breaker="higher_open_interest_then_volume_then_contract_id",
                reference_price_policy="decision_bar_close",
            )
        ),
    )
    for name in ("canonical_bars", "canonical_bar_provenance", "canonical_contract_economics"):
        path = canonical / f"{name}.delta"
        original = DeltaTable(str(path)).to_pyarrow_table()
        later = [
            dict(row, contract_id="BR-9.26")
            for row in original.to_pylist()
            if row["contract_id"] == "BR-6.26"
        ]
        if name == "canonical_bars":
            for i, row in enumerate(sorted(later, key=lambda r: r["ts"])):
                row["open_interest"] = 100 if i < 15 else 50000
        write_deltalake(
            str(path), pa.Table.from_pylist(later, schema=original.schema), mode="append"
        )
    if pinned_recovery:
        path = canonical / "canonical_bars.delta"
        original = DeltaTable(str(path)).to_pyarrow_table()
        prefix = dict(original.to_pylist()[0], instrument_id="PREFIX", timeframe="1m")
        for name in ("ts", "ts_open", "ts_close"):
            if name in prefix:
                prefix[name] = (
                    datetime(2018, 1, 1, tzinfo=UTC)
                    if isinstance(prefix[name], datetime)
                    else datetime(2018, 1, 1, tzinfo=UTC).isoformat()
                )
        write_deltalake(
            str(path), pa.Table.from_pylist([prefix], schema=original.schema), mode="append"
        )
    raw.mkdir()
    rows = DeltaTable(str(canonical / "canonical_bars.delta")).to_pyarrow_table().to_pylist()
    raw_rows = [{**r, "ts_open": r["ts"], "ts_close": r["ts"]} for r in rows]
    if pinned_recovery:
        raw_rows = [dict(row, timeframe="1m") for row in raw_rows]
    write_deltalake(str(raw / "raw_moex_history.delta"), pa.Table.from_pylist(raw_rows))
    monkeypatch.setenv(assets.RESEARCH_DATA_PREP_MATERIALIZED_OUTPUT_DIR_ENV, str(output))
    monkeypatch.setenv(assets.RESEARCH_DATA_PREP_RESULTS_OUTPUT_DIR_ENV, str(tmp_path / "results"))
    monkeypatch.setenv(assets.RESEARCH_DATA_PREP_TIMEFRAMES_ENV, "15m")
    monkeypatch.setenv(assets.RESEARCH_DATA_PREP_DATASET_VERSION_ENV, "native-daily-proof")
    monkeypatch.setenv(
        assets.RESEARCH_VOLUME_PROFILE_RAW_1M_TABLE_PATH_ENV, str(raw / "raw_moex_history.delta")
    )
    parent = None

    def publish_data(windows=()):
        nonlocal parent
        tables = {
            p.stem: storage.capture_table(p)
            for root in (canonical, raw)
            for p in root.glob("*.delta")
        }
        manifest = storage.create_snapshot(
            tables=tables,
            required_tables=list(tables),
            dataset_version="isolated-data",
            run_id="native-producer",
            code_version="fixture",
            quality_checks={"producer": "PASS"},
            parents=[parent["snapshot_id"]] if parent else [],
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
        path = storage.save_snapshot(canonical / ".data_snapshots", manifest)
        storage.publish_snapshot(
            canonical / ".data_snapshots",
            path,
            expected_current_id=parent["snapshot_id"] if parent else None,
        )
        parent = manifest
        return path

    data_path = publish_data()
    config = assets._build_moex_cf_catch_up_run_config(
        canonical_output_dir=canonical,
        dataset_version="native-daily-proof",
        campaign_run_id="seed-rebuild",
        data_snapshot_manifest_path=str(data_path),
        data_snapshot_raw_output_dir=str(raw),
        refresh_scope_version=1,
        _admission_probe=True,
    )["ops"]["continuous_front_bars"]["config"]
    config["spark_master"] = "local[2]"
    config = admission.configure_admission(config, operation="rebuild")
    repository = build_product_plane_definitions().get_repository_def()
    prep = repository.get_job(assets.RESEARCH_DATA_PREP_JOB_NAME).get_subset(
        asset_selection={assets.AssetKey(name) for name in assets.RESEARCH_DATA_PREP_JOB_ASSETS}
    )
    run_config = assets._research_data_prep_run_config_from_research_config(config)
    instance_dir = tmp_path / "dagster-instance"
    instance_dir.mkdir()
    with DagsterInstance.local_temp(str(instance_dir)) as instance:
        seed = prep.execute_in_process(
            run_config=run_config, instance=instance, raise_on_error=True
        )
        assert seed.success
        root = snapshot_registry(output, "native-daily-proof", "pit_active_front")
        accepted = storage.current_snapshot(root)
        assert accepted and accepted["parameters"]["operation"] == "rebuild"
        before = {
            p.stem: DeltaTable(str(p)).to_pyarrow_table().to_pylist()
            for p in output.glob("*.delta")
        }
        changed = [
            dict(r, close=r["close"] + 0.01) if r["instrument_id"] == "BR" else r for r in rows
        ]
        write_deltalake(
            str(canonical / "canonical_bars.delta"), pa.Table.from_pylist(changed), mode="overwrite"
        )
        window = {
            "instrument_id": "BR",
            "timeframe": "15m",
            "start_ts": "2026-03-16T09:00:00Z",
            "end_ts": "2026-03-16T23:45:00Z",
        }
        latest = publish_data([window])
        pinned_hash = None
        consumed_raw = []
        if pinned_recovery:
            indicator_module = importlib.import_module(
                "trading_advisor_3000.product_plane.research.indicators.materialize"
            )
            native_load_raw = indicator_module._load_volume_profile_source_rows

            def observe_raw_read(**kwargs):
                result = native_load_raw(**kwargs)
                consumed_raw.extend(row for rows in result.values() for row in rows)
                return result

            monkeypatch.setattr(
                indicator_module, "_load_volume_profile_source_rows", observe_raw_read
            )

            from test_native_scope_recovery import spark_factory

            from trading_advisor_3000.product_plane.data_plane.data_snapshots import (
                recover_data_scope,
            )
            from trading_advisor_3000.spark_jobs.research_bar_views_job import _delta_log_hash

            path = canonical / "canonical_session_intervals.delta"
            pinned_hash = _delta_log_hash(path)
            intervals = DeltaTable(str(path)).to_pyarrow_table()
            changed_intervals = [
                dict(
                    row, interval_id="foreign-" + row["interval_id"], session_class="partial_or_gap"
                )
                for row in intervals.to_pylist()
            ]
            write_deltalake(
                str(path),
                pa.Table.from_pylist(changed_intervals, schema=intervals.schema),
                mode="overwrite",
            )
            raw_path = raw / "raw_moex_history.delta"
            raw_payload = DeltaTable(str(raw_path)).to_pyarrow_table()
            write_deltalake(
                str(raw_path),
                pa.Table.from_pylist(
                    [
                        dict(
                            row,
                            low=row["low"] * 100,
                            high=row["high"] * 100,
                            volume=row["volume"] * 10,
                        )
                        for row in raw_payload.to_pylist()
                    ],
                    schema=raw_payload.schema,
                ),
                mode="overwrite",
            )
            version_before = {
                name: DeltaTable(ref["table_path"]).version()
                for name, ref in parent["tables"].items()
            }
            options = dict(
                canonical_root=canonical,
                research_root=root,
                expected_data_id=parent["snapshot_id"],
                expected_research_id=accepted["snapshot_id"],
                not_before="2026-03-01T00:00:00Z",
                spark_session_factory=spark_factory,
            )
            with pytest.raises(
                storage.SnapshotUnavailableError, match="changed during calculation"
            ):
                recover_data_scope(**options)
            recovered_scope = recover_data_scope(**options, accepted_input_pins=True, publish=True)
            parent = recovered_scope["manifest"]
            assert set(storage.recovery_input_controls(parent)) == {
                "canonical_session_intervals",
                "raw_moex_history",
            }
            assert version_before == {
                name: DeltaTable(ref["table_path"]).version()
                for name, ref in parent["tables"].items()
            }
            latest = canonical / ".data_snapshots/manifests" / f"{parent['snapshot_id']}.json"
        context = fixtures._baseline_update_sensor_context(
            tmp_path=tmp_path,
            run_id="daily-data",
            report={
                "status": "PASS",
                "publish_decision": "publish",
                "raw_table_path": str(raw / "raw_moex_history.delta"),
                "cf_catch_up": {"status": "NOOP", "windows": []},
                "data_snapshot": {"snapshot_manifest_path": str(latest)},
            },
        )
        request = fixtures._single_sensor_run_request(
            assets.moex_cf_catch_up_after_moex_baseline_sensor, context
        )
        assert hasattr(request, "run_config"), request
        if pinned_recovery:
            import copy

            from dagster import validate_run_config

            from trading_advisor_3000.product_plane.research import data_snapshots as snapshot_owner

            cf_job = repository.get_job(assets.MOEX_CF_CATCH_UP_JOB_NAME)
            effective = validate_run_config(cf_job, request.run_config)["ops"][
                "continuous_front_bars"
            ]["config"]
            assert admission.recovery_inputs_for_config(effective)
            alias = dict(
                effective, canonical_contract_economics_path=str(raw / "raw_moex_history.delta")
            )
            with pytest.raises(ValueError, match="aliases an unpinned CF reader"):
                admission.recovery_inputs_for_config(alias)
            tampered = copy.deepcopy(parent)
            tampered["parameters"]["downstream_scope"]["recovery"]["input_read_consistency"][
                "inputs"
            ]["raw_moex_history"]["observed_head"]["delta_version"] += 1
            with pytest.raises(storage.SnapshotUnavailableError, match="receipt"):
                storage.recovery_input_controls(tampered)
            original_current = admission.current_snapshot
            with monkeypatch.context() as stale:
                stale.setattr(
                    admission,
                    "current_snapshot",
                    lambda path: (
                        dict(accepted, snapshot_id="f" * 32)
                        if Path(path) == root
                        else original_current(path)
                    ),
                )
                with pytest.raises(admission.AdmissionParentSuperseded, match="Research changed"):
                    admission.recovery_inputs_for_config(effective)
            if foreign_write == "initial":
                original_inputs = snapshot_owner._input_tables
                path = output / "research_bar_views.delta"
                table = DeltaTable(str(path))
                table_id = table.metadata().id

                def append_after_input_validation(*args):
                    result = original_inputs(*args)
                    write_deltalake(str(path), table.to_pyarrow_table(), mode="append")
                    return result

                context = SimpleNamespace(
                    run_id="initial-race",
                    op_config=effective,
                    instance=instance,
                    dagster_run=SimpleNamespace(tags=request.tags),
                )
                with monkeypatch.context() as race:
                    race.setattr(snapshot_owner, "_input_tables", append_after_input_validation)
                    with pytest.raises(
                        storage.SnapshotUnavailableError, match="research_bar_views.delta"
                    ):
                        snapshot_owner._initial(context, effective)
                assert DeltaTable(str(path)).metadata().id == table_id
                assert not snapshot_owner._pending_path(context, effective).exists()
                assert storage.current_snapshot(root)["snapshot_id"] == accepted["snapshot_id"]
                assert all(
                    DeltaTable(str(output / f"{name}.delta")).version()
                    == accepted["tables"][name]["delta_version"]
                    for name in assets.CONTINUOUS_FRONT_TABLES
                )
                return
        sensor = assets.research_data_prep_after_moex_cf_catch_up_sensor

        def tick(cursor=None):
            with build_sensor_context(
                instance=instance, repository_def=repository, cursor=cursor
            ) as sensor_context:
                return sensor.evaluate_tick(sensor_context)

        initial_cursor = tick().cursor
        # A successful explicit rebuild of the same job does not own a daily handoff.
        from dagster import Permissive, job, op

        @op(name="continuous_front_bars", config_schema=Permissive())
        def noop_step():
            pass

        @job(name=assets.MOEX_CF_CATCH_UP_JOB_NAME)
        def unrelated_rebuild():
            noop_step()

        unrelated_rebuild.execute_in_process(
            run_config={
                "ops": {
                    "continuous_front_bars": {
                        "config": {
                            "operation": "rebuild",
                            "refresh_admission": {"operation": "rebuild"},
                        }
                    }
                }
            },
            instance=instance,
        )
        skipped = tick(initial_cursor)
        assert not skipped.run_requests and "only delivers daily" in skipped.skip_message
        assert skipped.cursor != initial_cursor
        initial_cursor = skipped.cursor
        cf = repository.get_job(assets.MOEX_CF_CATCH_UP_JOB_NAME).execute_in_process(
            run_config=request.run_config, tags=request.tags, instance=instance, raise_on_error=True
        )
        assert cf.success
        cf_versions = {
            name: DeltaTable(str(output / f"{name}.delta")).version()
            for name in assets.CONTINUOUS_FRONT_TABLES
        }
        for name, version in cf_versions.items():
            assert version > accepted["tables"][name]["delta_version"], name
        if foreign_write:
            first = tick(initial_cursor)
            assert first.run_requests, first.skip_message
            (in_flight,) = first.run_requests
            path = output / "continuous_front_adjustment_ladder.delta"
            original_table = DeltaTable(str(path))
            payload = original_table.to_pyarrow_table()
            assert payload.num_rows
            write_deltalake(str(path), payload, mode="append")
            blocked = tick(initial_cursor)
            assert blocked.cursor == initial_cursor and not blocked.run_requests
            assert (
                path.name in blocked.skip_message
                and "changed during calculation" in blocked.skip_message
            )
            failed = prep.execute_in_process(
                run_config=in_flight.run_config,
                tags=in_flight.tags,
                instance=instance,
                raise_on_error=False,
            )
            assert not failed.success
            assert DeltaTable(str(path)).version() == cf_versions[path.stem] + 1
            path.rename(tmp_path / "foreign-table.delta")
            write_deltalake(str(path), payload)
            assert DeltaTable(str(path)).metadata().id != original_table.metadata().id
            blocked = tick(initial_cursor)
            assert blocked.cursor == initial_cursor and not blocked.run_requests
            assert "identity changed" in blocked.skip_message and path.name in blocked.skip_message
            assert storage.current_snapshot(root)["snapshot_id"] == accepted["snapshot_id"]
            next_data = publish_data()
            superseded = tick(initial_cursor)
            assert not superseded.run_requests and superseded.cursor != initial_cursor
            assert "parent changed: data" in superseded.skip_message
            next_baseline = fixtures._baseline_update_sensor_context(
                tmp_path=tmp_path,
                run_id="next-data",
                report={
                    "status": "PASS",
                    "publish_decision": "publish",
                    "raw_table_path": str(raw / "raw_moex_history.delta"),
                    "data_snapshot": {"snapshot_manifest_path": str(next_data)},
                },
            )
            next_cf = assets.moex_cf_catch_up_after_moex_baseline_sensor._run_status_sensor_fn(
                next_baseline
            )
            assert "accepted_research_outputs_changed" in next_cf.skip_message
            return
        calendar_file = storage._file_path(
            storage.open_pinned_table(parent["tables"]["canonical_session_calendar"]).file_uris()[0]
        )
        # The immutable pin still exists. Reproduce the failed active-file stat.
        original = Path.is_file

        def fail_calendar_stat(path):
            if path == calendar_file:
                raise OSError(5, "Input/output error", str(path))
            return original(path)

        build_prep_config = assets.build_research_data_prep_run_config

        def fail_during_request_construction(**kwargs):
            with monkeypatch.context() as io_fault:
                io_fault.setattr(Path, "is_file", fail_calendar_stat)
                return build_prep_config(**kwargs)

        with monkeypatch.context() as fault:
            fault.setattr(
                assets, "build_research_data_prep_run_config", fail_during_request_construction
            )
            blocked = tick(initial_cursor)
        assert not blocked.run_requests and blocked.cursor == initial_cursor
        assert "canonical_session_calendar.delta" in blocked.skip_message
        assert "Input/output error" in blocked.skip_message
        assert storage.current_snapshot(root)["snapshot_id"] == accepted["snapshot_id"]
        delivered = tick(blocked.cursor)
        assert delivered.run_requests, delivered.skip_message
        (handoff,) = delivered.run_requests
        assert handoff.tags["ta3000/upstream_run_id"] == cf.run_id
        assert tick(blocked.cursor).run_requests[0].run_key == handoff.run_key
        assert not tick(delivered.cursor).run_requests
        receipt = request.run_config["ops"]["continuous_front_bars"]["config"]["refresh_admission"]
        assert receipt["operation"] == "daily" and receipt["outcome"] == "SCOPED"
        assert {(w["instrument_id"], w["timeframe"]) for w in receipt["effective_windows"]} == (
            {("BR", "15m"), ("Si", "15m")} if pinned_recovery else {("BR", "15m")}
        )
        for step_config in handoff.run_config["ops"].values():
            assert step_config["config"]["refresh_admission"] == receipt
        native_l2 = assets.materialize_derived_indicator_frames

        def fail_after_native_l2(**kwargs):
            native_l2(**kwargs)
            raise RuntimeError("injected worker failure after native L2 commit")

        monkeypatch.setattr(assets, "materialize_derived_indicator_frames", fail_after_native_l2)
        reconstructable_job = reconstructable(native_daily_prep_job)
        with execute_job(
            reconstructable_job,
            run_config=handoff.run_config,
            tags=handoff.tags,
            instance=instance,
            raise_on_error=False,
        ) as failed:
            assert not failed.success
            failed_id = failed.run_id
        assert storage.current_snapshot(root)["snapshot_id"] == accepted["snapshot_id"]
        l1_version = DeltaTable(str(output / "research_indicator_frames.delta")).version()
        monkeypatch.setattr(assets, "materialize_derived_indicator_frames", native_l2)
        with execute_job(
            reconstructable_job,
            run_config=handoff.run_config,
            tags=handoff.tags,
            reexecution_options=ReexecutionOptions.from_failure(failed_id, instance),
            instance=instance,
            raise_on_error=True,
        ) as recovered:
            assert recovered.success
            daily = SimpleNamespace(run_id=recovered.run_id, success=True)
        assert DeltaTable(str(output / "research_indicator_frames.delta")).version() == l1_version
        after = storage.current_snapshot(root)
        assert after["snapshot_id"] != accepted["snapshot_id"]
        assert after["parents"] == [parent["snapshot_id"]]
        pit = storage.open_pinned_table(after["tables"]["research_datasets"]).to_pyarrow_table(
            filters=[("contour_id", "=", "pit_active_front")], columns=["continuous_front_policy"]
        )
        assert json.loads(pit.column(0)[0].as_py()) == receipt["domain"]["continuous_front_policy"]
        assert after["parameters"]["quality_owners"]["continuous_front_bars"] == cf.run_id
        assert not tick(delivered.cursor).run_requests
        for name, version in cf_versions.items():
            assert after["tables"][name]["delta_version"] == version
            assert DeltaTable(str(output / f"{name}.delta")).version() == version
        assert after["parameters"]["refresh_admission"] == receipt
        if pinned_recovery:

            def raw_values(row):
                timestamp = datetime.fromisoformat(str(row["ts_open"]).replace("Z", "+00:00"))
                return (
                    str(row["instrument_id"]),
                    str(row["contract_id"]),
                    timestamp.astimezone(UTC).timestamp(),
                    *(float(row[key]) for key in ("low", "high", "volume")),
                )

            expected_raw = {raw_values(row) for row in raw_rows}
            assert consumed_raw
            unexpected = [
                raw_values(row) for row in consumed_raw if raw_values(row) not in expected_raw
            ]
            assert not unexpected, (unexpected[:2], sorted(expected_raw)[:2])
            assert all(
                float(row["high"]) < 100000 for row in consumed_raw if row["instrument_id"] == "BR"
            )
            bar_rows = (
                storage.open_pinned_table(after["tables"]["research_bar_views"])
                .to_pyarrow_table()
                .to_pylist()
            )
            active_rows = [row for row in bar_rows if row["contour_id"] == "pit_active_front"]
            assert active_rows
            assert all(row["session_class"] == "regular" for row in active_rows)
            assert all(not row["session_interval_id"].startswith("foreign-") for row in active_rows)
            for name in ("canonical_session_intervals", "raw_moex_history"):
                assert after["tables"][name] == parent["tables"][name]
                assert (
                    DeltaTable(after["tables"][name]["table_path"]).version()
                    > after["tables"][name]["delta_version"]
                )
            manifests = (
                storage.open_pinned_table(after["tables"]["research_datasets"])
                .to_pyarrow_table()
                .to_pylist()
            )
            for row in manifests:
                versions = json.loads(row["source_delta_versions_json"])
                hashes = json.loads(row["source_delta_hashes_json"])
                assert (
                    versions["canonical_session_intervals"]
                    == parent["tables"]["canonical_session_intervals"]["delta_version"]
                )
                assert hashes["canonical_session_intervals"] == pinned_hash
        for name in (
            "continuous_front_bars",
            "research_bar_views",
            "research_indicator_frames",
            "research_derived_indicator_frames",
            "continuous_front_indicator_frames",
            "continuous_front_derived_indicator_frames",
        ):
            pinned = storage.open_pinned_table(after["tables"][name]).to_pyarrow_table().to_pylist()
            old = [r for r in before[name] if r.get("instrument_id") == "Si"]
            if not pinned_recovery:
                assert [r for r in pinned if r.get("instrument_id") == "Si"] == old, name
        # Same-attempt publication is idempotent only with the unchanged native
        # checkpoint/config/QC. This exercises the real publisher after CAS.
        from dagster import validate_run_config

        from trading_advisor_3000.product_plane.research.data_snapshots import (
            publish_research_snapshot,
        )

        effective = validate_run_config(prep, handoff.run_config)["ops"]["research_data_snapshot"][
            "config"
        ]
        publisher_context = SimpleNamespace(
            run_id=daily.run_id,
            op_execution_context=SimpleNamespace(
                op_config=effective,
                instance=instance,
                dagster_run=instance.get_run_by_id(daily.run_id),
            ),
        )
        repeated = publish_research_snapshot(
            publisher_context,
            acceptance={
                "sidecar_result": {
                    "status": after["parameters"]["quality_evidence"][
                        "continuous_front_indicator_acceptance_report"
                    ]["status"]
                }
            },
        )
        assert repeated["snapshot_id"] == after["snapshot_id"]
        # Fresh native consumer process reads the accepted manifest's frozen pins.
        import subprocess
        import sys

        manifest_path = root / "manifests" / f"{after['snapshot_id']}.json"
        expected_rows = sum(
            row["contour_id"] == "pit_active_front"
            for row in storage.open_pinned_table(after["tables"]["research_bar_views"])
            .to_pyarrow_table(columns=["contour_id"])
            .to_pylist()
        )
        consumer = """
from pathlib import Path
import sys
import os
import pyarrow as pa
pa.set_cpu_count(1)
pa.set_io_thread_count(1)
from trading_advisor_3000.product_plane.common.data_snapshots import (
    load_snapshot, snapshot_read_scope,
)
from trading_advisor_3000.product_plane.research.datasets import load_materialized_research_dataset
def read():
    with snapshot_read_scope(load_snapshot(Path(sys.argv[1]))):
        dataset=load_materialized_research_dataset(output_dir=Path(sys.argv[2]),
            dataset_version='native-daily-proof',contour_id='pit_active_front')
        rows = len(dataset['bar_views'])
    assert rows == int(sys.argv[3]), (rows, sys.argv[3])
    print(f'PINNED_READBACK={rows}', flush=True)
read()
# This proof checks the fully materialized pinned DTO, not native-library
# shutdown. A prior successful read aborted during interpreter teardown.
os._exit(0)
"""

        readback = subprocess.check_output(
            [sys.executable, "-c", consumer, str(manifest_path), str(output), str(expected_rows)],
            text=True,
            timeout=120,
        ).strip()
        assert readback == f"PINNED_READBACK={expected_rows}"
        proof = {
            "rebuild_run": seed.run_id,
            "cf_run": cf.run_id,
            "daily_run": daily.run_id,
            "admission_id": receipt["admission_id"],
            "snapshot_id": after["snapshot_id"],
            "tables": after["tables"],
            "unaffected_pair": "Si/15m",
            "fresh_readback": {
                "marker": readback,
                "native_teardown": "excluded",
                "shutdown_stability": "unresolved",
            },
        }
        (tmp_path / "native-proof.json").write_text(json.dumps(proof, indent=2))
