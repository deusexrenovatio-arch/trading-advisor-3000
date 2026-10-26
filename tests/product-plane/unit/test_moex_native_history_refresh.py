from datetime import date

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.iss_client import MoexISSClient


def test_history_payload_retained_for_native_support(monkeypatch):
    client = MoexISSClient()
    official = {
        "SECID": "BRF7",
        "BOARDID": "RFUD",
        "TRADEDATE": "2026-10-09",
        "ASSETCODE": "BR",
        "SHORTNAME": "BR-1.27",
        "VOLUME": 7,
        "NUMTRADES": 3,
        "SETTLEPRICE": 100.0,
        "OPENPOSITION": 5806,
    }
    pages = iter(
        [
            {"history": {"columns": list(official), "data": [list(official.values())]}},
            {"history": {"columns": list(official), "data": []}},
        ]
    )
    monkeypatch.setattr(client, "_get_json", lambda *args, **kwargs: next(pages))
    rows = client.fetch_history_board_securities(
        engine="futures", market="forts", board="RFUD", trade_date=date(2026, 10, 9)
    )
    assert rows[0].payload == official
    assert rows[0].volume == 7 and rows[0].numtrades == 3


def test_support_refresh_native_idempotence_correction_and_reject(tmp_path, monkeypatch):
    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    path = tmp_path / "raw_moex_contract_securities.delta"
    row = {
        "source_id": refresh.HISTORY_SOURCE,
        "board": "RFUD",
        "moex_secid": "BRF7",
        "trade_date": "2026-10-09",
        "source_document_hash": "original",
        "fetched_at_utc": "2026-10-12T00:00:00Z",
        "last_settle_price": 100.0,
    }
    columns = {key: "string" for key in row}
    columns.update(trade_date="date", fetched_at_utc="timestamp", last_settle_price="double")
    first = refresh.write_changed_support_rows(path, [row], columns=columns)
    identity = DeltaTable(str(path)).metadata().id
    assert first["changed_rows"] == 1
    old_version = first["delta_version"]
    assert (
        refresh.write_changed_support_rows(
            path, [dict(row, fetched_at_utc="2026-10-13T00:00:00Z")], columns=columns
        )["changed_rows"]
        == 0
    )
    assert DeltaTable(str(path)).version() == old_version
    corrected = dict(row, source_document_hash="corrected", last_settle_price=101.0)
    result = refresh.write_changed_support_rows(path, [corrected], columns=columns)
    assert result["changed_rows"] == 1 and result["delta_version"] == old_version + 1
    assert DeltaTable(str(path)).metadata().id == identity
    active = read_delta_table_rows(path, limit=2)
    assert len(active) == 1 and active[0]["last_settle_price"] == 101.0
    assert (
        DeltaTable(str(path), version=old_version)
        .to_pyarrow_table()
        .to_pylist()[0]["last_settle_price"]
        == 100.0
    )
    for value in (True, -1, 1.25, float("inf"), 2**63):
        with pytest.raises(ValueError):
            refresh.project_open_interest(value)
    assert refresh.project_open_interest(None) == (None, "MISSING_IN_SOURCE", None)
    assert refresh.project_open_interest(0) == (0, "OBSERVED_ZERO", "0")
    assert refresh.project_open_interest(5806) == (5806, "OBSERVED", "5806")


def test_registered_nightly_uses_native_history_and_actual_next_day(tmp_path, monkeypatch):
    from datetime import UTC, datetime

    from dagster import build_schedule_context

    from trading_advisor_3000.dagster_defs import moex_historical_assets as assets

    kwargs = {
        "economics_history_refresh": True,
        "economics_source_paths": {
            "raw_moex_contract_specs": tmp_path / "prepared_contract_specs.delta",
        },
        "economics_parameter_evidence": {"bundle_id": "bounded-tail-20261010-v1"},
        "allow_calibrated_latest_margin": True,
        "margin_calibration_as_of_date": date(2026, 8, 14),
        "daily_open_interest_path": tmp_path / "raw_moex_daily_open_interest.delta",
        "daily_open_interest_version": 3,
    }
    seen = []

    def accepted_bindings(root):
        seen.append(root)
        return kwargs

    monkeypatch.setattr(assets, "_route_artifact_root", lambda: tmp_path)
    monkeypatch.setattr(assets, "registered_history_refresh_kwargs", accepted_bindings)
    # Source trust/admission is covered by the helper's owning native cases.
    monkeypatch.setattr(assets, "_baseline_economics_options", lambda config: {})
    context = build_schedule_context(
        scheduled_execution_time=datetime(2026, 10, 13, 23, tzinfo=UTC)
    )
    tick = assets.moex_baseline_daily_update_schedule.evaluate_tick(context)
    assert len(tick.run_requests) == 1
    config = tick.run_requests[0].run_config["ops"]["moex_baseline_update"]["config"]
    assert seen == [tmp_path]
    assert config["ingest_till_utc"] == "2026-10-13T21:00:00Z"
    assert config["run_id"] == "20261013T230000Z"
    assert config["economics_history_refresh"] is True
    assert config["economics_source_paths"] == {
        "raw_moex_contract_specs": (tmp_path / "prepared_contract_specs.delta").as_posix()
    }
    assert config["economics_parameter_evidence"] == kwargs["economics_parameter_evidence"]
    assert config["daily_open_interest_version"] == 3
    assert config["daily_open_interest_path"] == kwargs["daily_open_interest_path"].as_posix()
    assert config["allow_calibrated_latest_margin"] is True
    assert config["margin_calibration_as_of_date"] == "2026-08-14"

    def rejected_bindings(root):
        raise ValueError("accepted history admission missing")

    monkeypatch.setattr(assets, "registered_history_refresh_kwargs", rejected_bindings)
    with pytest.raises(ValueError, match="accepted history admission missing"):
        assets._build_baseline_daily_schedule_run_config(context)


def _current_br_specification(*, imtime="2026-10-12 07:00:04", step_price=8.49048):
    return {
        "SECID": "BRX6",
        "ASSETCODE": "BR",
        "MINSTEP": 0.01,
        "LOTVOLUME": 10,
        "STEPPRICE": step_price,
        "IMTIME": imtime,
    }


def _br_rule():
    from types import SimpleNamespace

    return SimpleNamespace(
        assetcode="BR", min_step=0.01, lot_volume=10, tick_value_quote=0.1, quote_currency="USD"
    )


def _actual_mc_rows():
    return [
        {
            "secid": "USD/RUB",
            "tradedate": "2026-10-09",
            "tradetime": "19:00:00",
            "clearing": "mc",
            "rate": 84.9048,
        },
        {
            "secid": "USD/RUB",
            "tradedate": "2026-10-12",
            "tradetime": "19:00:00",
            "clearing": "mc",
            "rate": 83.9153,
        },
    ]


def test_current_crosscheck_uses_mc_available_at_specification_time():
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = _actual_mc_rows()
    descriptions, report = refresh._check_current(
        [_current_br_specification()], [_br_rule()], refresh._fx_records(rows), dated_fx_rows=rows
    )
    assert set(descriptions) == {"BRX6"}
    assert report["status"] == "PASS"
    assert report["dated_mc_date"] == "2026-10-09"
    assert report["dated_mc_rate"] == 84.9048


def test_current_crosscheck_uses_same_day_mc_after_its_fixing():
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = _actual_mc_rows()
    _, report = refresh._check_current(
        [_current_br_specification(imtime="2026-10-12 20:00:00", step_price=8.39153)],
        [_br_rule()],
        refresh._fx_records(rows),
        dated_fx_rows=rows,
    )
    assert report["dated_mc_date"] == "2026-10-12"
    assert report["dated_mc_rate"] == 83.9153


@pytest.mark.parametrize("step_price", [8.39153, 8.51])
def test_current_crosscheck_does_not_pick_a_rate_to_fit_step_price(step_price):
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = _actual_mc_rows()
    with pytest.raises(ValueError, match="current datedMC crosscheck failed: BR"):
        refresh._check_current(
            [_current_br_specification(step_price=step_price)],
            [_br_rule()],
            refresh._fx_records(rows),
            dated_fx_rows=rows,
        )


@pytest.mark.parametrize("imtime", [None, "", "2026-10-12", "invalid"])
def test_current_crosscheck_rejects_missing_or_invalid_specification_time(imtime):
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = _actual_mc_rows()
    with pytest.raises(ValueError, match="IMTIME"):
        refresh._check_current(
            [_current_br_specification(imtime=imtime)],
            [_br_rule()],
            refresh._fx_records(rows),
            dated_fx_rows=rows,
        )


def test_current_crosscheck_rejects_missing_preceding_mc():
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = _actual_mc_rows()[1:]
    with pytest.raises(ValueError, match="dated MC.*IMTIME"):
        refresh._check_current(
            [_current_br_specification()],
            [_br_rule()],
            refresh._fx_records(rows),
            dated_fx_rows=rows,
        )


def test_current_crosscheck_rejects_missing_fixing_time():
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = _actual_mc_rows()
    rows[0].pop("tradetime")
    with pytest.raises(ValueError, match="dated MC.*time"):
        refresh._check_current(
            [_current_br_specification()],
            [_br_rule()],
            refresh._fx_records(rows),
            dated_fx_rows=rows,
        )


def test_oi_write_ahead_retains_old_and_new_keys_across_failed_commit(tmp_path, monkeypatch):
    import json

    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    path = tmp_path / "raw_daily_oi.delta"
    pending = tmp_path / "pending-changed-windows.json"
    row = {
        "source_id": refresh.HISTORY_SOURCE,
        "board": "RFUD",
        "moex_secid": "BRF7",
        "trade_date": "2026-10-09",
        "internal_id": "FUT_BR",
        "open_interest": 5,
    }
    columns = {key: "string" for key in row}
    columns.update(trade_date="date", open_interest="long")
    first = refresh.write_changed_support_rows(path, [row], columns=columns)
    changed = {**row, "internal_id": "BR", "open_interest": 6}
    writer = refresh.write_delta_table_rows

    def fail(**kwargs):
        evidence = json.loads(pending.read_text())
        assert {key["internal_id"] for key in evidence["support_changed_keys"]} == {"BR", "FUT_BR"}
        raise RuntimeError("worker died before commit")

    monkeypatch.setattr(refresh, "write_delta_table_rows", fail)
    with pytest.raises(RuntimeError, match="worker died"):
        refresh.write_changed_support_rows(
            path, [changed], columns=columns, pending_scope_path=pending
        )
    monkeypatch.setattr(refresh, "write_delta_table_rows", writer)
    result = refresh.write_changed_support_rows(
        path, [changed], columns=columns, pending_scope_path=pending
    )
    evidence = json.loads(pending.read_text())
    assert evidence["support_input_reference"]["delta_version"] == first["delta_version"]
    assert evidence["support_output_reference"]["delta_version"] == result["delta_version"]
    assert not evidence.get("support_scope_unproven")
    assert {key["internal_id"] for key in result["changed_keys"]} == {"BR", "FUT_BR"}


def test_raw_producer_chain_rejects_interleaved_unknown_commit(tmp_path):
    import pyarrow as pa
    from deltalake import write_deltalake

    from trading_advisor_3000.product_plane.common.data_snapshots import capture_table
    from trading_advisor_3000.spark_jobs.moex_raw_ingest_job import _raw_change_scope

    path = tmp_path / "raw_moex_history.delta"
    write_deltalake(str(path), pa.table({"value": [0]}))
    before = capture_table(path)
    write_deltalake(str(path), pa.table({"value": [1]}), mode="overwrite")
    assert _raw_change_scope(path, before, changed=True)["proven"]
    write_deltalake(str(path), pa.table({"value": [2]}), mode="overwrite")
    assert not _raw_change_scope(path, before, changed=True)["proven"]
    assert not _raw_change_scope(path, before, changed=False)["proven"]
