from __future__ import annotations

import json
from pathlib import Path

import pytest
from deltalake import DeltaTable

import trading_advisor_3000.dagster_defs.moex_historical_assets as assets
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.schemas import (
    historical_data_delta_schema_manifest,
)


def _raw_schedule(tmp_path: Path, *, secid: str = "BRK6") -> Path:
    path = tmp_path / "raw-schedule.delta"
    row = {
        "source_id": "moex-public-reconstructed-futures-session",
        "source_url": "https://www.moex.com/a7079",
        "source_document_id": "dated-source",
        "source_document_hash": "a" * 64,
        "fetched_at_utc": "2026-09-19T00:00:00Z",
        "engine": "futures",
        "market": "forts",
        "board": "RFUD",
        "moex_secid": secid,
        "trade_date": "2020-01-27",
        "raw_payload_json": json.dumps(
            {
                "time_from": "2020-01-27 10:00:00",
                "time_till": "2020-01-27 18:45:00",
                "type": "main_session",
                "session_class": "regular",
                "policy_id": "reconstructed-policy",
            }
        ),
    }
    columns = historical_data_delta_schema_manifest()["raw_moex_session_schedule"]["columns"]
    write_delta_table_rows(table_path=path, rows=[row], columns=columns)
    row["trade_date"] = "2020-01-28"
    row["raw_payload_json"] = str(row["raw_payload_json"]).replace("2020-01-27", "2020-01-28")
    write_delta_table_rows(table_path=path, rows=[row], columns=columns, mode="append")
    return path


def _config(tmp_path: Path, raw: Path) -> dict[str, object]:
    return assets.build_moex_data_rebuild_op_config(
        profile_name="canonical_from_existing_raw",
        canonical_run_id="frozen-session-regression",
        canonical_output_dir=tmp_path / "staging",
        raw_table_path=tmp_path / "bars.delta",
        raw_ingest_report_path=tmp_path / "raw-report.json",
        raw_session_schedule_path=raw,
        raw_session_schedule_version=0,
        publish_mode="staging_only",
        downstream_mode="none",
    )


def test_frozen_sessions_are_materialized_before_canonical(tmp_path, monkeypatch):
    raw = _raw_schedule(tmp_path)
    before_files = DeltaTable(str(raw)).file_uris()
    calls = []

    def materialize(**kwargs):
        intervals = read_delta_table_rows(Path(kwargs["canonical_session_intervals_path"]), limit=2)
        assert len(intervals) == 1  # pinned v0, not latest v1
        assert intervals[0]["instrument_id"] == "FUT_BR"
        assert str(intervals[0]["session_date"])[:10] == "2020-01-27"
        assert intervals[0]["policy_id"] == "reconstructed-policy"
        assert intervals[0]["source_document_hash"] == "a" * 64
        assert "07:00:00" in str(intervals[0]["expected_open_ts"])
        calls.append(kwargs)
        return {"success": True, "output_paths": {}, "rows_by_table": {}}

    monkeypatch.setattr(assets, "materialize_moex_historical_assets", materialize)
    report = assets.run_moex_data_rebuild_profile(_config(tmp_path, raw))
    assert report["success"] and len(calls) == 1
    assert DeltaTable(str(raw)).version() == 1
    assert DeltaTable(str(raw)).file_uris() == before_files


def test_frozen_sessions_reject_unmapped_instrument(tmp_path, monkeypatch):
    raw = _raw_schedule(tmp_path, secid="UNKNOWN")
    monkeypatch.setattr(
        assets,
        "materialize_moex_historical_assets",
        lambda **kw: pytest.fail("canonical must not start"),
    )
    with pytest.raises(ValueError, match="unmapped"):
        assets.run_moex_data_rebuild_profile(_config(tmp_path, raw))


def test_frozen_sessions_do_not_overwrite_existing_intervals(tmp_path, monkeypatch):
    raw = _raw_schedule(tmp_path)
    output = tmp_path / "staging/delta/canonical_session_intervals.delta"
    output.mkdir(parents=True)
    sentinel = output / "keep.txt"
    sentinel.write_text("existing")
    monkeypatch.setattr(
        assets,
        "materialize_moex_historical_assets",
        lambda **kw: pytest.fail("canonical must not start"),
    )
    with pytest.raises(FileExistsError):
        assets.run_moex_data_rebuild_profile(_config(tmp_path, raw))
    assert sentinel.read_text() == "existing"


def test_frozen_sessions_reject_production_prewrite(tmp_path, monkeypatch):
    raw = _raw_schedule(tmp_path)
    config = _config(tmp_path, raw)
    config.update(publish_mode="promote", canonical_target_output_dir=str(tmp_path / "current"))
    monkeypatch.setattr(
        assets,
        "materialize_moex_historical_assets",
        lambda **kw: pytest.fail("canonical must not start"),
    )
    with pytest.raises(ValueError, match="staging_only"):
        assets.run_moex_data_rebuild_profile(config)
    assert not (tmp_path / "current").exists()


@pytest.mark.parametrize("version", [None, -1, True])
def test_frozen_sessions_require_explicit_version(tmp_path, version):
    with pytest.raises(ValueError, match="pinned"):
        assets.build_moex_data_rebuild_run_config(
            profile_name="canonical_from_existing_raw",
            canonical_run_id="missing-version",
            canonical_output_dir=tmp_path / "staging",
            raw_table_path=tmp_path / "bars.delta",
            raw_ingest_report_path=tmp_path / "raw-report.json",
            raw_session_schedule_path=tmp_path / "schedule.delta",
            raw_session_schedule_version=version,
            publish_mode="staging_only",
        )


def _append_closed(raw: Path, *, day="2020-01-29", **changes):
    row = DeltaTable(str(raw), version=0).to_pyarrow_table().to_pylist()[0]
    row["trade_date"] = day
    payload = {"trading_status": "closed", "policy_id": "reconstructed-policy", **changes}
    row["raw_payload_json"] = json.dumps(payload)
    columns = historical_data_delta_schema_manifest()["raw_moex_session_schedule"]["columns"]
    write_delta_table_rows(table_path=raw, rows=[row], columns=columns, mode="append")


def test_closed_dates_reach_existing_canonical_job(tmp_path, monkeypatch):
    raw = _raw_schedule(tmp_path)
    _append_closed(raw)

    def materialize(**kwargs):
        path = Path(kwargs["canonical_closed_session_dates_path"])
        rows = DeltaTable(str(path)).to_pyarrow_table().to_pylist()
        assert len(rows) == 1
        assert str(rows[0]["session_date"]) == "2020-01-29"
        assert rows[0]["source_document_hash"] == "a" * 64
        assert rows[0]["policy_id"] == "reconstructed-policy"
        assert (
            DeltaTable(str(kwargs["canonical_session_intervals_path"]))
            .to_pyarrow_dataset()
            .count_rows()
            == 2
        )
        return {"success": True, "output_paths": {}, "rows_by_table": {}}

    monkeypatch.setattr(assets, "materialize_moex_historical_assets", materialize)
    config = _config(tmp_path, raw)
    config["raw_session_schedule_version"] = 2
    report = assets.run_moex_data_rebuild_profile(config)
    assert report["success"]
    assert DeltaTable(str(raw)).version() == 2


@pytest.mark.parametrize(
    "day,changes,error",
    [
        ("2020-01-27", {}, "conflicting open and closed"),
        ("2020-01-29", {"time_from": "2020-01-29 10:00:00"}, "must not contain"),
        ("2020-01-29", {"policy_id": ""}, "requires instrument, date and source provenance"),
    ],
)
def test_bad_closed_evidence_fails_before_writes(tmp_path, monkeypatch, day, changes, error):
    raw = _raw_schedule(tmp_path)
    _append_closed(raw, day=day, **changes)
    monkeypatch.setattr(
        assets,
        "materialize_moex_historical_assets",
        lambda **kw: pytest.fail("canonical must not start"),
    )
    config = _config(tmp_path, raw)
    config["raw_session_schedule_version"] = 2
    with pytest.raises(ValueError, match=error):
        assets.run_moex_data_rebuild_profile(config)
    assert not (tmp_path / "staging/delta/canonical_session_intervals.delta").exists()
    assert not (tmp_path / "staging/delta/canonical_closed_session_dates.delta").exists()


def test_existing_closed_target_blocks_both_writes(tmp_path, monkeypatch):
    raw = _raw_schedule(tmp_path)
    target = tmp_path / "staging/delta/canonical_closed_session_dates.delta"
    target.mkdir(parents=True)
    monkeypatch.setattr(
        assets,
        "materialize_moex_historical_assets",
        lambda **kw: pytest.fail("canonical must not start"),
    )
    with pytest.raises(FileExistsError, match="closed session dates"):
        assets.run_moex_data_rebuild_profile(_config(tmp_path, raw))
    assert not (target.parent / "canonical_session_intervals.delta").exists()
