"""The CF worker must select T-2 from dated futures records in the admitted packet."""

import copy
import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.common.immutable_files import pin
from trading_advisor_3000.spark_jobs import continuous_front_job as cf


def write_json(root, name, value):
    path = root / name
    path.write_text(json.dumps(value), encoding="utf-8")
    return pin(root, path)


def test_rebuild_cli_rejects_ambiguous_calendar_reference(tmp_path):
    from trading_advisor_3000.product_plane.research.jobs.continuous_front_refresh import main

    ref = tmp_path / "calendar-ref.json"
    ref.write_text('{"path":"first","path":"second","sha256":"untrusted"}')
    with pytest.raises(ValueError, match="duplicate key"):
        main(
            [
                "--operation",
                "rebuild",
                "--calendar-roll-schedule",
                str(ref),
                "--dataset-version",
                "proof",
                "--canonical-output-dir",
                str(tmp_path),
                "--output-dir",
                str(tmp_path),
            ]
        )


def source(root, name, body, url):
    body_pin = write_json(root, name, body)
    receipt = write_json(
        root,
        name + ".receipt",
        {
            "status": 200,
            "body_bytes": (root / name).stat().st_size,
            "body_sha256": body_pin["sha256"],
            "final_url": url,
        },
    )
    return {"body": body_pin, "receipt": receipt}


def test_reviewed_archive_requires_both_originals_and_every_planned_date(tmp_path, monkeypatch):
    from datetime import date, timedelta
    from hashlib import sha256

    from trading_advisor_3000.product_plane.research import moex_calendar as calendar

    year = "2020"
    prefix = calendar.ARCHIVE_PREFIX + year
    refs = []
    events = {
        "holidays": {"data": [[1, "2020-01-30", "workday", "/n26438"]]},
        "off_days": {"data": []},
    }
    for role, kind, url in [
        (
            "events",
            "annual_calendar",
            "https://iss.moex.com/iss/apps/calendar/events.json?year=2020&iss.only=holidays,off_days&iss.meta=off",
        ),
        ("n26438", "dated_schedule", "https://www.moex.com/n26438"),
    ]:
        refs.append(
            dict(
                rule_id=prefix + "-" + role,
                kind=kind,
                effective_from="2020-01-01",
                effective_to="2021-01-01",
                precedence=len(refs),
                **source(tmp_path, role, events if role == "events" else {"original": role}, url),
            )
        )
    ids = [r["rule_id"] for r in refs]
    refs[1].update(effective_from="2020-01-30", effective_to="2020-01-31")
    days = [(date(2020, 1, 1) + timedelta(days=i)).isoformat() for i in range(366)]
    opens = {"2020-01-30", "2020-01-31", "2020-02-03"}
    digest = sha256(b"2020-01-30\n2020-01-31\n2020-02-03\n").hexdigest()
    monkeypatch.setitem(
        calendar.REVIEWED_ARCHIVES,
        year,
        (refs[0]["body"]["sha256"], digest),
    )
    monkeypatch.setitem(calendar.ARCHIVE_NOTICE_DIGESTS, "n26438", refs[1]["body"]["sha256"])
    dates = [
        dict(
            source_date=d,
            state="open" if d in opens else "closed",
            governing_rule_ids=ids if d == "2020-01-30" else ids[:1],
        )
        for d in days
    ]
    schedule = {"governing_rules": refs, "groups": [{"components": [], "planned_dates": dates}]}
    cf._verify_official_workdays(tmp_path, schedule)
    dates[32]["governing_rule_ids"] = ids
    with pytest.raises(ValueError, match="wrong governing MOEX archive rules: 2020-02-02"):
        cf._verify_official_workdays(tmp_path, schedule)
    dates[32]["governing_rule_ids"] = ids[:1]
    with pytest.raises(ValueError, match="incomplete.*2020"):
        cf._verify_official_workdays(
            tmp_path, {**schedule, "groups": [{"components": [], "planned_dates": dates[1:]}]}
        )
    dates[29]["state"] = "closed"
    with pytest.raises(ValueError, match="disagree.*2020"):
        cf._verify_official_workdays(tmp_path, schedule)
    with pytest.raises(ValueError, match="missing or ambiguous.*2020"):
        cf._verify_official_workdays(tmp_path, {**schedule, "governing_rules": refs[:1]})
    refs[1].update(
        source(tmp_path, "foreign", {"original": "other notice"}, "https://www.moex.com/n26438")
    )
    with pytest.raises(ValueError, match="differs from reviewed original"):
        cf._verify_official_workdays(tmp_path, schedule)
    refs[0].update(
        effective_from="2022-01-01",
        effective_to="2023-01-01",
        rule_id=calendar.ARCHIVE_PREFIX + "2022-events",
    )
    with pytest.raises(ValueError, match="unreviewed.*2022"):
        cf._verify_official_workdays(tmp_path, schedule)


def test_may_2021_market_notice_is_required_only_for_its_derivatives_dates():
    from trading_advisor_3000.product_plane.research import moex_calendar as calendar

    rows = [[i, f"2021-05-0{i}", None, "/n33785"] for i in (4, 5, 6, 7)]
    rows.append([31, "2021-05-31", None, "/n33651"])
    raw = json.dumps({"holidays": {"data": rows}, "off_days": {"data": []}})
    notices = calendar.archive_notice_dates(raw, "2021")
    assert notices == {f"2021-05-0{i}": "n33785" for i in (4, 5, 6, 7)}
    from datetime import date, timedelta

    prefix = calendar.ARCHIVE_PREFIX + "2021-"
    dates = [
        {
            "source_date": (date(2021, 1, 1) + timedelta(days=i)).isoformat(),
            "state": "closed",
            "governing_rule_ids": [prefix + "events"],
        }
        for i in range(365)
    ]
    with pytest.raises(ValueError, match="wrong governing MOEX archive rules: 2021-05-04"):
        calendar.verify_archive_dates("2021", dates, notices)


def test_compact_calendar_compares_every_futures_date_and_actual_expiry(tmp_path):
    fixture = Path(__file__).parents[1] / "fixtures/contracts/native_cf_inputs.v1.json"
    schedule = json.loads(fixture.read_text())["calendar_roll_schedule"]
    group = schedule["groups"][0]
    schedule["groups"] = [group]
    dates = ["2026-11-12", "2026-11-13", "2026-11-14", "2026-11-15", "2026-11-16"]
    flags = [1, 1, 0, 0, 1]
    rule = "moex-futures-workdays-2026"
    rows = [[day, flag, 1] for day, flag in zip(dates, flags, strict=True)]
    original = {
        "off_days": {"columns": ["tradedate", "futures_workday", "stock_workday"], "data": rows}
    }
    schedule["governing_rules"] = [
        {
            "rule_id": rule,
            "kind": "annual_calendar",
            "precedence": 0,
            "effective_from": "2026-01-01",
            "effective_to": "2027-01-01",
            **source(
                tmp_path,
                "dates",
                original,
                "https://iss.moex.com/iss/calendars.json?show_all_days=1",
            ),
        }
    ]
    group.update(
        coverage_from=dates[0],
        coverage_to="2026-11-17",
        roll_offset_trading_days=2,
        planned_dates=[
            {
                "source_date": day,
                "state": "open" if flag else "closed",
                "governing_rule_ids": [rule],
            }
            for day, flag in zip(dates, flags, strict=True)
        ],
    )
    for component, last in zip(group["components"], [dates[-1], "2026-12-17"], strict=True):
        component["last_trade_date"] = last
        symbol = component["source_symbol"]
        component["component_key"] = symbol + "@MOEX"
        original = {
            "description": {
                "columns": ["name", "value"],
                "data": [["SECID", symbol], ["LSTTRADE", last]],
            }
        }
        component["last_trade_source"].update(
            source(tmp_path, symbol, original, f"https://iss.moex.com/iss/securities/{symbol}.json")
        )
    ref = write_json(tmp_path, "calendar.json", schedule)
    actual = cf.resolve_legacy_calendar(tmp_path, ref, offset=2)
    intervals = cf.calendar_active_intervals(actual)
    assert intervals[0]["active_to"] == "2026-11-12"
    # Two confirmed open days are needed after the last safe date when expiry
    # is beyond the packet. No successor is established at or beyond this bound.
    assert intervals[-1]["active_to"] == "2026-11-13"
    for last, reason in [
        ("2026-11-11", "last-trade date precedes calendar coverage"),
        ("2026-11-14", "last-trade date is closed in pinned calendar"),
        ("2026-11-13", "insufficient planned open dates"),
    ]:
        invalid = copy.deepcopy(actual)
        component = invalid["groups"][0]["components"][0]
        component["last_trade_date"] = last
        with pytest.raises(ValueError) as exc:
            cf.calendar_active_intervals(invalid)
        message = str(exc.value)
        assert all(
            s in message for s in (reason, group["group_key"], component["component_key"], last)
        )
    dated = copy.deepcopy(actual)
    dated_group = dated["groups"][0]
    dated_group["components"][-1]["last_trade_date"] = "2026-11-19"
    for day in ["2026-11-17", "2026-11-18", "2026-11-19", "2026-11-20"]:
        dated_group["planned_dates"].append(
            {"source_date": day, "state": "open", "governing_rule_ids": [rule]}
        )
    dated_group["coverage_to"] = "2026-11-21"
    assert cf.calendar_active_intervals(dated)[-1]["active_to"] == "2026-11-17"
    altered = copy.deepcopy(schedule)
    altered["groups"][0]["planned_dates"][2]["state"] = "open"
    with pytest.raises(ValueError, match="2026-11-14"):
        cf.resolve_legacy_calendar(tmp_path, write_json(tmp_path, "bad.json", altered), offset=2)
    bad_packet = tmp_path / "duplicate.json"
    bad_packet.write_text('{"schema_version":1,"schema_version":2}')
    with pytest.raises(ValueError, match="duplicate key"):
        cf.resolve_legacy_calendar(tmp_path, pin(tmp_path, bad_packet), offset=2)
    forged = copy.deepcopy(schedule)
    expiry = forged["groups"][0]["components"][0]["last_trade_source"]
    original_bytes = (tmp_path / expiry["body"]["path"]).read_bytes()
    duplicate = tmp_path / "ambiguous-expiry.json"
    duplicate.write_bytes(
        original_bytes.replace(b'{"description":', b'{"description": {}, "description":', 1)
    )
    expiry["body"] = pin(tmp_path, duplicate)
    receipt = json.loads((tmp_path / expiry["receipt"]["path"]).read_bytes())
    receipt.update(body_bytes=duplicate.stat().st_size, body_sha256=expiry["body"]["sha256"])
    expiry["receipt"] = write_json(tmp_path, "ambiguous-receipt.json", receipt)
    with pytest.raises(ValueError, match="duplicate key"):
        cf.resolve_legacy_calendar(tmp_path, write_json(tmp_path, "forged.json", forged), offset=2)
    (tmp_path / "dates").write_bytes(b"{}")
    with pytest.raises(ValueError, match="hash mismatch: dates"):
        cf.resolve_legacy_calendar(tmp_path, ref, offset=2)
    (tmp_path / "calendar.json").unlink()
    with pytest.raises(FileNotFoundError, match="calendar.json"):
        cf.resolve_legacy_calendar(tmp_path, ref, offset=2)


@pytest.mark.parametrize("bad", [None, "other_market", "unknown", "duplicate", "missing_day"])
def test_official_calendar_rejects_ambiguous_or_missing_futures_flags(tmp_path, bad):
    from urllib.parse import urlsplit

    from trading_advisor_3000.product_plane.research.moex_calendar import futures_workdays

    date = "2026-11-14"
    row = {"tradedate": date, "is_traded": 0}
    futures = {"2026": {date: row}}
    days = {"futures": futures, "stock": {"2026": {date: {**row, "is_traded": 1}}}}
    if bad == "other_market":
        del days["futures"]
    elif bad == "unknown":
        row["is_traded"] = None
    elif bad == "missing_day":
        row["tradedate"] = "2026-11-15"
    body = {
        "props": {
            "pageProps": {"recursiveComponentContentProps": {"0": {"initData": {"offDays": days}}}}
        }
    }
    payload = json.dumps(body)
    if bad == "duplicate":
        payload = payload.replace('"is_traded": 0', '"is_traded": 0, "is_traded": 1')
    html = ('<script id="__NEXT_DATA__" type="application/json">' + payload + "</script>").encode()
    if bad is None:
        assert futures_workdays(html, urlsplit("https://www.moex.com/ru/tradingcalendar")) == {
            date: "closed"
        }
        return
    with pytest.raises((ValueError, KeyError)):
        futures_workdays(html, urlsplit("https://www.moex.com/ru/tradingcalendar"))


def test_scheduled_rebuild_and_daily_config_bind_identical_calendar_pin(tmp_path, monkeypatch):
    from datetime import UTC, datetime

    import pyarrow as pa
    from deltalake import write_deltalake

    from trading_advisor_3000.dagster_defs import research_assets as a
    from trading_advisor_3000.product_plane.common import data_snapshots as storage
    from trading_advisor_3000.product_plane.research.data_snapshots import (
        _parameters,
        snapshot_registry,
    )

    canonical = tmp_path / "canonical"
    output = tmp_path / "research"
    output.mkdir()
    packet = write_json(output, "scheduled.json", {"staged": True})
    monkeypatch.setenv(a.RESEARCH_CALENDAR_PIN_ENV, json.dumps(packet))
    monkeypatch.setenv(a.RESEARCH_DATA_PREP_MATERIALIZED_OUTPUT_DIR_ENV, str(output))
    monkeypatch.setenv(a.RESEARCH_DATA_PREP_RESULTS_OUTPUT_DIR_ENV, str(tmp_path / "results"))
    monkeypatch.setenv(a.RESEARCH_DATA_PREP_TIMEFRAMES_ENV, "15m")
    monkeypatch.setenv(a.RESEARCH_DATA_PREP_DATASET_VERSION_ENV, "calendar-test")
    table = canonical / "canonical_bars.delta"
    monkeypatch.setenv(a.RESEARCH_VOLUME_PROFILE_RAW_1M_TABLE_PATH_ENV, str(table))
    write_deltalake(str(table), pa.table({"ts": [datetime(2026, 11, 18, tzinfo=UTC)]}))
    tables = {"canonical_bars": storage.capture_table(table)}
    registry = canonical / ".data_snapshots"

    def publish(root, parents, parameters, current=None):
        obj = storage.create_snapshot(
            tables=tables,
            required_tables=list(tables),
            dataset_version="calendar-test",
            run_id="test",
            code_version="test",
            quality_checks={"fixture": "PASS"},
            parents=parents,
            parameters=parameters,
        )
        path = storage.save_snapshot(root, obj)
        storage.publish_snapshot(root, path, expected_current_id=current)
        return obj["snapshot_id"], path

    scope = {
        "version": 2,
        "full_refresh": False,
        "windows": [],
        "effective_intervals": [],
        "covered_tables": tables,
    }
    data_id, path = publish(registry, [], {"downstream_scope": scope})
    base = a.build_research_data_prep_run_config(
        operation="rebuild",
        data_snapshot_manifest_path=path,
        canonical_output_dir=canonical,
        materialized_output_dir=output,
        results_output_dir=tmp_path / "results",
        dataset_name="moex-cf-catch-up",
        dataset_version="calendar-test",
        base_timeframe="15m",
        timeframes=["15m"],
        continuous_front_policy=a.scheduled_continuous_front_policy_config(),
    )
    conf = base["ops"]["continuous_front_bars"]["config"]
    assert conf["refresh_admission"]["domain"]["calendar_roll_schedule"] == packet
    rr = snapshot_registry(output, "calendar-test", "pit_active_front")
    publish(
        rr, [data_id], {**_parameters(conf), "runtime_provenance": storage.runtime_provenance()}
    )
    win = {
        "instrument_id": "FUT_BR",
        "timeframe": "15m",
        "start_ts": "2026-11-18T00:00:00Z",
        "end_ts": "2026-11-18T12:00:00Z",
    }
    _, new_path = publish(
        registry, [data_id], {"downstream_scope": {**scope, "windows": [win]}}, data_id
    )
    report = {
        "status": "PASS",
        "publish_decision": "publish",
        "data_snapshot": {"snapshot_manifest_path": str(new_path)},
    }
    evidence = tmp_path / "evidence"
    (evidence / "next").mkdir(parents=True)
    (evidence / "next" / "baseline-update-report.json").write_text(json.dumps(report))
    from types import SimpleNamespace

    ctx = SimpleNamespace(
        dagster_run=SimpleNamespace(
            run_id="next",
            run_config={
                "ops": {
                    "moex_baseline_update": {
                        "config": {
                            "canonical_bars_path": str(table),
                            "evidence_root": str(evidence),
                            "run_id": "next",
                        }
                    }
                }
            },
        )
    )
    (request,) = a.moex_cf_catch_up_after_moex_baseline_sensor._run_status_sensor_fn(ctx)
    cf_config = request.run_config["ops"]["continuous_front_bars"]["config"]
    assert (
        cf_config["calendar_roll_schedule"]
        == cf_config["refresh_admission"]["domain"]["calendar_roll_schedule"]
        == packet
    )
    # Check descriptor transport separately from the producer-checkpoint tests.
    from trading_advisor_3000.product_plane.research import data_snapshots as checkpoints

    with monkeypatch.context() as handoff:
        handoff.setenv(a.RESEARCH_CALENDAR_PIN_ENV, '{"path":"rotated","sha256":"unadmitted"}')
        handoff.setattr(checkpoints, "verified_cf_checkpoint", lambda *_: None)
        upstream = SimpleNamespace(
            run_id="cf-run", run_config=request.run_config, tags=request.tags
        )
        research = a._research_request_after_cf(None, upstream)
        downstream = research.run_config["ops"]["continuous_front_bars"]["config"]
        assert downstream["calendar_roll_schedule"] == packet
        assert downstream["refresh_admission"] == cf_config["refresh_admission"]
    monkeypatch.delenv(a.RESEARCH_CALENDAR_PIN_ENV)
    blocked = a.moex_cf_catch_up_after_moex_baseline_sensor._run_status_sensor_fn(ctx)
    assert "research_semantics_changed" in blocked.skip_message
    (output / "scheduled.json").write_text("changed")
    from trading_advisor_3000.product_plane.research.admission import validate_admission

    with pytest.raises(ValueError, match="scheduled.json"):
        validate_admission(cf_config["refresh_admission"], config=cf_config)
