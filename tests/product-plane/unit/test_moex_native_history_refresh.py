from datetime import date

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.iss_client import (
    FuturesContractSnapshot,
    MoexISSClient,
)


def _snapshot(rows, day="2026-10-12", *, seqnum=1):
    return FuturesContractSnapshot(
        tuple(rows),
        date.fromisoformat(day),
        ({"dataversion": {"trade_date": day, "data_version": 1, "seqnum": seqnum}},),
    )


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


def _ngv6_archive(tmp_path, *, overrides=None, duplicate=False):
    import hashlib
    import json

    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    provenance = json.dumps(
        {
            "description_hash": "a" * 64,
            "description_lot": 100.0,
            "rule_min_step": 0.001,
            "rule_lot_volume": 100.0,
            "original_source_id": refresh.HISTORY_SOURCE,
            "original_source_document_id": "RFUD:NGV6:2026-10-05",
            "parameter_validation_evidence_sha256": "b" * 64,
        }
    )
    digest = hashlib.sha256(provenance.encode()).hexdigest()
    row = {
        "source_id": refresh.PREPARED_SOURCE,
        "board": "RFUD",
        "moex_secid": "NGV6",
        "assetcode": "NG",
        "trade_date": "2026-10-05",
        "last_trade_date": "2026-10-28",
        "last_del_date": "2026-10-28",
        "min_step": 0.001,
        "lot_volume": 100.0,
        "source_document_id": f"prepared-specification:{digest}",
        "source_document_hash": digest,
        "enrichment_provenance_json": provenance,
        **(overrides or {}),
    }
    columns = {key: "string" for key in row}
    columns.update(trade_date="date", min_step="double", lot_volume="double")
    path = tmp_path / "prepared.delta"
    write_delta_table_rows(
        table_path=path, rows=[row] * (2 if duplicate else 1), columns=columns, compact=False
    )
    verified = {
        "prepared_input_table_id": DeltaTable(str(path)).metadata().id,
        "prepared_input_version": 0,
        "dated_official_tc_evidence_sha256": "c" * 64,
    }
    return path, verified, row, columns


def _ng_rule():
    from types import SimpleNamespace

    return SimpleNamespace(
        assetcode="NG",
        min_step=0.001,
        lot_volume=100.0,
        tick_value_quote=0.1,
        quote_currency="USD",
    )


@pytest.mark.parametrize(
    "failure",
    [
        None,
        "provenance",
        "min_step",
        "lot_volume",
        "missing",
        "future",
        "post_expiry",
        "active",
        "duplicate",
        "current_crosscheck",
    ],
)
def test_ngv6_expired_history_survives_current_delisting(tmp_path, monkeypatch, failure):
    """Real archived Delta; replace only trust/production-ID and output-write boundaries."""
    import json
    from datetime import UTC, datetime
    from pathlib import Path
    from types import SimpleNamespace

    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    overrides = {
        "provenance": {"enrichment_provenance_json": "{}"},
        "min_step": {"min_step": 0.01},
        "lot_volume": {"lot_volume": 10.0},
        "missing": {"moex_secid": "NGX6"},
        "future": {"trade_date": "2026-10-24"},
        "post_expiry": {"last_trade_date": "2026-10-22"},
        "active": {"last_trade_date": "2026-11-01", "last_del_date": "2026-11-01"},
    }.get(failure)
    path, verified, archived, _ = _ngv6_archive(
        tmp_path, overrides=overrides, duplicate=failure == "duplicate"
    )

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 30, 22, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr(refresh, "datetime", FixedDateTime)
    raw_path, oi_path = tmp_path / "raw.delta", tmp_path / "oi.delta"
    real_delta = DeltaTable
    production_ids = {
        raw_path: "ae9e49f9-9e8f-405d-9e07-0a2079eee691",
        oi_path: "026176bf-9d3b-4192-adcf-559cd33f7c93",
    }
    monkeypatch.setattr(refresh, "validate_contract_parameter_evidence", lambda *args: verified)
    monkeypatch.setattr(refresh, "bind_contract_parameter_input", lambda *args: 0)
    monkeypatch.setattr(
        refresh,
        "DeltaTable",
        lambda p, **kw: (
            real_delta(p, **kw)
            if str(p) == str(path)
            else SimpleNamespace(metadata=lambda: SimpleNamespace(id=production_ids[Path(p)]))
        ),
    )
    raw_columns = dict.fromkeys(
        (
            "source_id source_url source_document_id source_document_hash fetched_at_utc engine "
            "market board moex_secid trade_date assetcode contract_shortname last_trade_date "
            "last_del_date min_step lot_volume official_step_price official_initial_margin "
            "last_settle_price raw_payload_json enrichment_provenance_json"
        ).split(),
        "string",
    )
    oi_columns = dict.fromkeys(
        (
            "source_id board moex_secid trade_date source_document_hash raw_payload_json "
            "internal_id source_open_interest_text open_interest observation_status "
            "observation_frequency available_at_utc availability_status source_delta_path "
            "source_delta_version parent_raw_snapshot_sha256 materialization_run_id "
            "materialized_at_utc"
        ).split(),
        "string",
    )
    monkeypatch.setattr(refresh, "_columns", lambda p: oi_columns if p == oi_path else raw_columns)
    written = {}

    def capture(p, rows, **kwargs):
        written[p] = rows
        return {
            "table_path": str(p),
            "table_id": verified["prepared_input_table_id"],
            "delta_version": 1,
        }

    monkeypatch.setattr(refresh, "write_changed_support_rows", capture)
    official = {
        "SECID": "NGV6",
        "ASSETCODE": "NG",
        "BOARDID": "RFUD",
        "TRADEDATE": "2026-10-23",
        "SHORTNAME": "NG-10.26",
        "OPENPOSITION": 991522,
        "SETTLEPRICE": 3.125,
    }
    client = SimpleNamespace(
        fetch_history_board_securities=lambda **kwargs: [SimpleNamespace(payload=official)]
    )
    current = [
        {
            "SECID": "NGX6",
            "ASSETCODE": "NG",
            "MINSTEP": 0.001,
            "LOTVOLUME": 100.0,
            "STEPPRICE": 8.5 if failure == "current_crosscheck" else 8.0,
            "IMTIME": "2026-10-23 07:00:04",
        }
    ]
    fx = [
        {
            "secid": "USD/RUB",
            "tradedate": "2026-10-22",
            "tradetime": "19:00:00",
            "clearing": "mc",
            "rate": 80.0,
        },
        {
            "secid": "USD/RUB",
            "tradedate": "2026-10-23",
            "tradetime": "19:00:00",
            "clearing": "tc",
            "rate": 81.0,
        },
    ]
    kwargs = dict(
        client=client,
        raw_contract_specs_path=raw_path,
        prepared_path=path,
        daily_open_interest_path=oi_path,
        trade_dates=[date(2026, 10, 23)],
        base_rules=(_ng_rule(),),
        base_evidence=object(),
        current_snapshot=_snapshot(current, "2026-10-23"),
        dated_fx_rows=fx,
        run_id="ngv6-regression",
    )
    if failure:
        with pytest.raises(ValueError):
            refresh.refresh_native_history_support(**kwargs)
        assert written == {}
        return
    renewal = refresh.refresh_native_history_support(**kwargs)
    assert renewal.admitted_date_end == "2026-10-23"
    assert written[oi_path][0]["open_interest"] == 991522
    assert json.loads(written[raw_path][0]["raw_payload_json"]) == official
    assert written[raw_path][0]["last_settle_price"] == 3.125
    prepared = written[path][0]
    assert prepared["last_trade_date"] == "2026-10-28"
    provenance = json.loads(prepared["enrichment_provenance_json"])
    assert provenance["description_hash"] == "a" * 64
    ref = provenance["archived_description"]
    assert ref["delta_version"] == 0
    assert ref["source_document_hash"] == archived["source_document_hash"]
    assert ref["parameter_validation_evidence_sha256"] == "b" * 64
    assert provenance["current_snapshot_crosscheck_sha256"] == refresh._current_crosscheck_hash(
        renewal.current_crosscheck
    )


def test_ngv6_description_pins_admitted_version_and_replays(tmp_path):
    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    path, verified, row, columns = _ngv6_archive(tmp_path)
    write_delta_table_rows(
        table_path=path,
        rows=[dict(row, trade_date="2026-10-21", min_step=99.0)],
        columns=columns,
        compact=False,
    )
    assert DeltaTable(str(path)).version() == 1
    kwargs = dict(
        secid="NGV6", rule=_ng_rule(), trade_date=date(2026, 10, 23), as_of_date=date(2026, 10, 31)
    )
    first = refresh._expired_contract_description(path, verified, **kwargs)
    assert first == refresh._expired_contract_description(path, verified, **kwargs)
    assert first[0] == {"LASTTRADEDATE": "2026-10-28", "LASTDELDATE": "2026-10-28"}
    assert first[1]["delta_version"] == 0
    assert first[1]["trade_date"] == "2026-10-05"
    with pytest.raises(ValueError, match="units"):
        refresh._expired_contract_description(
            path, dict(verified, prepared_input_version=1), **kwargs
        )
    with pytest.raises(ValueError, match="identity"):
        refresh._expired_contract_description(
            path, dict(verified, prepared_input_table_id="wrong"), **kwargs
        )


@pytest.mark.parametrize(
    "day,as_of,allowed",
    [
        (date(2026, 10, 28), date(2026, 10, 31), True),
        (date(2026, 10, 29), date(2026, 10, 31), False),
        (date(2026, 10, 23), date(2026, 10, 28), False),
    ],
)
def test_ngv6_description_expiry_boundaries(tmp_path, day, as_of, allowed):
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    path, verified, _, _ = _ngv6_archive(tmp_path)
    kwargs = dict(secid="NGV6", rule=_ng_rule(), trade_date=day, as_of_date=as_of)
    if allowed:
        assert (
            refresh._expired_contract_description(path, verified, **kwargs)[0]["LASTTRADEDATE"]
            == "2026-10-28"
        )
    else:
        with pytest.raises(ValueError, match="date"):
            refresh._expired_contract_description(path, verified, **kwargs)


def test_current_crosscheck_rejects_initial_margin_time_without_source_session():
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = _actual_mc_rows()
    with pytest.raises(ValueError, match="source.*session"):
        refresh._check_current(
            [_current_br_specification()],
            [_br_rule()],
            refresh._fx_records(rows),
            dated_fx_rows=rows,
        )


@pytest.mark.parametrize("imtime", ["2026-10-12 07:00:04", "2026-10-12 21:21:03", None, "invalid"])
def test_current_crosscheck_uses_source_session_independently_of_margin_time(imtime):
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = _actual_mc_rows()
    descriptions, report = refresh._check_current(
        _snapshot([_current_br_specification(imtime=imtime)]),
        [_br_rule()],
        refresh._fx_records(rows),
        dated_fx_rows=rows,
    )
    assert set(descriptions) == {"BRX6"}
    assert report["status"] == "PASS"
    assert report["dated_mc_date"] == "2026-10-09"
    assert report["dated_mc_rate"] == 84.9048


def test_current_crosscheck_rolls_mc_only_when_source_session_changes():
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = _actual_mc_rows()
    _, report = refresh._check_current(
        _snapshot(
            [_current_br_specification(imtime="2026-10-12 20:00:00", step_price=8.39153)],
            "2026-10-13",
        ),
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
            _snapshot([_current_br_specification(step_price=step_price)]),
            [_br_rule()],
            refresh._fx_records(rows),
            dated_fx_rows=rows,
        )


@pytest.mark.parametrize(
    "day,step_price", [("2026-10-30", 0.80676), ("2026-10-31", 0.80557), ("2026-11-02", 0.80557)]
)
def test_nasd_night_and_weekend_snapshot_bind_to_their_source_session(day, step_price):
    from types import SimpleNamespace

    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = [
        dict(secid="USD/RUB", tradedate=d, tradetime="19:00:00", clearing="mc", rate=r)
        for d, r in [("2026-10-29", 80.6761), ("2026-10-30", 80.5573)]
    ]
    row = dict(
        SECID="NAZ6",
        ASSETCODE="NASD",
        MINSTEP=1.0,
        LOTVOLUME=41,
        STEPPRICE=step_price,
        IMTIME="2026-10-30 21:21:03",
    )
    rule = SimpleNamespace(
        assetcode="NASD", min_step=1.0, lot_volume=41, tick_value_quote=0.01, quote_currency="USD"
    )
    _, report = refresh._check_current(
        _snapshot([row], day), [rule], refresh._fx_records(rows), dated_fx_rows=rows
    )
    assert report["dated_mc_date"] == ("2026-10-29" if day == "2026-10-30" else "2026-10-30")
    assert report["contracts"][0]["step_price"] == step_price


def test_current_crosscheck_rejects_missing_preceding_mc():
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    rows = _actual_mc_rows()[1:]
    with pytest.raises(ValueError, match="source session.*preceding actual MC"):
        refresh._check_current(
            _snapshot([_current_br_specification()]),
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
            _snapshot([_current_br_specification()]),
            [_br_rule()],
            refresh._fx_records(rows),
            dated_fx_rows=rows,
        )


def test_source_sequence_alone_does_not_rewrite_native_support(tmp_path):
    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh as refresh

    fx = _actual_mc_rows()
    reports = [
        refresh._check_current(
            _snapshot([_current_br_specification()], seqnum=seq),
            [_br_rule()],
            refresh._fx_records(fx),
            dated_fx_rows=fx,
        )[1]
        for seq in (1, 2)
    ]
    assert reports[0]["source_audit"] != reports[1]["source_audit"]
    assert refresh._hash(reports[0]) != refresh._hash(reports[1])
    path = tmp_path / "stable.delta"
    columns = dict(
        source_id="string",
        board="string",
        moex_secid="string",
        trade_date="date",
        source_document_hash="string",
    )
    results = []
    for report in reports:
        row = dict(
            source_id="prepared",
            board="RFUD",
            moex_secid="BRX6",
            trade_date="2026-10-09",
            source_document_hash=refresh._current_crosscheck_hash(report),
        )
        results.append(refresh.write_changed_support_rows(path, [row], columns=columns))
    assert results[0]["changed_rows"] == 1
    assert results[1]["changed_rows"] == 0
    assert DeltaTable(str(path)).version() == results[0]["delta_version"]


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
