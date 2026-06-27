from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.run_moex_nightly_backfill as nightly
import scripts.run_moex_route_refresh as route_refresh
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.moex.foundation import (
    DiscoveryRecord,
    load_universe,
)


def _coverage_record(
    *, internal_id: str, secid: str, source_interval: int, source_timeframe: str
) -> DiscoveryRecord:
    return DiscoveryRecord(
        internal_id=internal_id,
        finam_symbol=internal_id if secid == "BRQ6" else f"{secid}@MOEX",
        moex_engine="futures",
        moex_market="forts",
        moex_board="RFUD",
        moex_secid=secid,
        asset_group="commodity",
        requested_target_timeframes="5m,15m" if source_interval == 1 else "1h,4h",
        source_interval=source_interval,
        source_timeframe=source_timeframe,
        coverage_begin_utc="2026-03-01T07:00:00Z",
        coverage_end_utc="2026-04-02T12:00:00Z",
        discovered_at_utc="2026-04-02T12:05:00Z",
        discovery_url=(
            "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/"
            f"securities/{secid}/candleborders.json"
        ),
    )


def _raw_row(
    *,
    internal_id: str,
    secid: str,
    timeframe: str = "1m",
    minute: int = 0,
    close: float = 80.0,
) -> dict[str, object]:
    return {
        "internal_id": internal_id,
        "finam_symbol": f"{secid}@MOEX",
        "moex_engine": "futures",
        "moex_market": "forts",
        "moex_board": "RFUD",
        "moex_secid": secid,
        "asset_group": "commodity",
        "timeframe": timeframe,
        "source_interval": 1,
        "ts_open": f"2026-04-01T09:{minute:02d}:00Z",
        "ts_close": f"2026-04-01T09:{minute + 1:02d}:00Z",
        "open": close,
        "high": close + 0.5,
        "low": close - 0.5,
        "close": close,
        "volume": 1000 + minute,
        "open_interest": 20_000 + minute,
        "ingest_run_id": "test-raw-shard",
        "ingested_at_utc": "2026-04-01T10:00:00Z",
        "provenance_json": "{}",
    }


def test_merge_raw_tables_rejects_overlapping_shard_scopes_before_target_rewrite(
    tmp_path: Path,
) -> None:
    shard_one = tmp_path / "shard-01" / "raw.delta"
    shard_two = tmp_path / "shard-02" / "raw.delta"
    target = tmp_path / "target" / "raw.delta"
    write_delta_table_rows(
        table_path=shard_one,
        rows=[_raw_row(internal_id="FUT_BR", secid="BRQ6", minute=0)],
        columns=nightly.RAW_COLUMNS,
    )
    write_delta_table_rows(
        table_path=shard_two,
        rows=[_raw_row(internal_id="FUT_BR", secid="BRQ6", minute=1)],
        columns=nightly.RAW_COLUMNS,
    )
    write_delta_table_rows(
        table_path=target,
        rows=[_raw_row(internal_id="FUT_SI", secid="SIM6", minute=0)],
        columns=nightly.RAW_COLUMNS,
    )
    shard_reports = [
        {"shard_id": "shard-01", "report": {"raw_table_path": shard_one.as_posix()}},
        {"shard_id": "shard-02", "report": {"raw_table_path": shard_two.as_posix()}},
    ]

    with pytest.raises(RuntimeError, match="overlapping shard scopes"):
        nightly._merge_raw_tables(
            shard_reports=shard_reports,
            target_raw_path=target,
            batch_size=10,
        )

    assert read_delta_table_rows(target, limit=10)[0]["internal_id"] == "FUT_SI"


def test_merge_raw_tables_collapses_identical_raw_bar_duplicates(tmp_path: Path) -> None:
    shard = tmp_path / "dedupe-shard" / "raw.delta"
    target = tmp_path / "dedupe-target" / "raw.delta"
    row = _raw_row(internal_id="FUT_BR", secid="BRQ6", minute=0)
    write_delta_table_rows(
        table_path=shard,
        rows=[row, dict(row)],
        columns=nightly.RAW_COLUMNS,
    )

    written_shards = nightly._merge_raw_tables(
        shard_reports=[{"shard_id": "shard-01", "report": {"raw_table_path": shard.as_posix()}}],
        target_raw_path=target,
        batch_size=1,
    )

    rows = read_delta_table_rows(target, limit=10)
    assert written_shards == 1
    assert len(rows) == 1
    assert rows[0]["close"] == 80.0


def test_merge_raw_tables_rejects_conflicting_raw_bar_duplicates_before_target_rewrite(
    tmp_path: Path,
) -> None:
    shard = tmp_path / "conflict-shard" / "raw.delta"
    target = tmp_path / "conflict-target" / "raw.delta"
    write_delta_table_rows(
        table_path=shard,
        rows=[
            _raw_row(internal_id="FUT_BR", secid="BRQ6", minute=0, close=80.0),
            _raw_row(internal_id="FUT_BR", secid="BRQ6", minute=0, close=81.0),
        ],
        columns=nightly.RAW_COLUMNS,
    )
    write_delta_table_rows(
        table_path=target,
        rows=[_raw_row(internal_id="FUT_SI", secid="SIM6", minute=0)],
        columns=nightly.RAW_COLUMNS,
    )

    with pytest.raises(RuntimeError, match="conflicting rows for the same raw bar key"):
        nightly._merge_raw_tables(
            shard_reports=[
                {"shard_id": "shard-01", "report": {"raw_table_path": shard.as_posix()}}
            ],
            target_raw_path=target,
            batch_size=1,
        )

    assert read_delta_table_rows(target, limit=10)[0]["internal_id"] == "FUT_SI"


def test_route_refresh_merge_raw_tables_rejects_overlapping_shard_scopes(
    tmp_path: Path,
) -> None:
    shard_one = tmp_path / "route-shard-01" / "raw.delta"
    shard_two = tmp_path / "route-shard-02" / "raw.delta"
    write_delta_table_rows(
        table_path=shard_one,
        rows=[_raw_row(internal_id="FUT_BR", secid="BRQ6", minute=0)],
        columns=route_refresh.RAW_COLUMNS,
    )
    write_delta_table_rows(
        table_path=shard_two,
        rows=[_raw_row(internal_id="FUT_BR", secid="BRQ6", minute=1)],
        columns=route_refresh.RAW_COLUMNS,
    )

    with pytest.raises(RuntimeError, match="overlapping shard scopes"):
        route_refresh._merge_raw_tables(
            shard_reports=[
                {"shard_id": "shard-01", "report": {"raw_table_path": shard_one.as_posix()}},
                {"shard_id": "shard-02", "report": {"raw_table_path": shard_two.as_posix()}},
            ],
            target_raw_path=tmp_path / "route-target" / "raw.delta",
            batch_size=10,
        )


def test_build_jobs_scopes_shared_discovery_by_internal_id(tmp_path: Path) -> None:
    universe = load_universe(Path("configs/moex_foundation/universe/moex-futures-priority.v1.yaml"))
    active_symbols = [item for item in universe if item.is_active][:2]
    assert len(active_symbols) == 2

    coverage = [
        _coverage_record(
            internal_id=active_symbols[0].internal_id,
            secid=active_symbols[0].moex_secid,
            source_interval=1,
            source_timeframe="1m",
        ),
        _coverage_record(
            internal_id=active_symbols[1].internal_id,
            secid=active_symbols[1].moex_secid,
            source_interval=60,
            source_timeframe="1h",
        ),
    ]

    jobs = nightly._build_jobs(
        run_id="nightly-shared-discovery",
        raw_ingest_run_dir=tmp_path / "phase01",
        shards_dir=tmp_path / "nightly" / "shards",
        shards=[[active_symbols[0]], [active_symbols[1]]],
        coverage=coverage,
        timeframes={"5m", "15m", "1h", "4h"},
        bootstrap_window_days=180,
        ingest_till_utc="2026-04-02T12:00:00Z",
        stability_lag_minutes=20,
        expand_contract_chain=True,
        contract_discovery_step_days=14,
        contract_discovery_lookback_days=180,
        refresh_overlap_minutes=180,
    )

    assert len(jobs) == 2
    assert {row["internal_id"] for row in jobs[0]["coverage"]} == {active_symbols[0].internal_id}
    assert {row["internal_id"] for row in jobs[1]["coverage"]} == {active_symbols[1].internal_id}
    assert (Path(str(jobs[0]["output_dir"])) / "coverage-report.json").exists()
    assert (Path(str(jobs[1]["output_dir"])) / "coverage-report.csv").exists()


def test_run_shard_job_reuses_cached_success(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []

    def _fake_ingest_moex_bootstrap_window(**kwargs):  # noqa: ANN003 - test double keeps loose signature
        run_id = str(kwargs["run_id"])
        calls.append(run_id)

        table_path = Path(str(kwargs["table_path"]))
        (table_path / "_delta_log").mkdir(parents=True, exist_ok=True)

        progress_path = Path(str(kwargs["progress_path"]))
        progress_latest_path = Path(str(kwargs["progress_latest_path"]))
        error_path = Path(str(kwargs["error_path"]))
        error_latest_path = Path(str(kwargs["error_latest_path"]))
        for path in (progress_path, progress_latest_path, error_path, error_latest_path):
            path.parent.mkdir(parents=True, exist_ok=True)

        progress_payload = {
            "run_id": run_id,
            "internal_id": "FUT_BR",
            "moex_secid": "BRQ6",
            "source_timeframe": "1m",
            "source_interval": 1,
            "window_start_utc": "2026-04-01T07:00:00Z",
            "window_end_utc": "2026-04-02T11:40:00Z",
            "source_rows": 10,
            "incremental_rows": 10,
            "deduplicated_rows": 0,
            "stale_rows": 0,
            "processed_at_utc": "2026-04-02T12:06:00Z",
        }
        progress_path.write_text(
            json.dumps(progress_payload, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        progress_latest_path.write_text(
            json.dumps(progress_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        error_path.write_text("", encoding="utf-8")
        error_latest_path.write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "status": "PASS",
                    "reported_at_utc": "2026-04-02T12:06:01Z",
                    "message": "no raw ingest errors recorded",
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        return nightly.build_raw_ingest_run_report_v2(
            run_id=run_id,
            ingest_till_utc="2026-04-02T12:00:00Z",
            source_rows=10,
            incremental_rows=10,
            deduplicated_rows=0,
            stale_rows=0,
            watermark_by_key={"FUT_BR|1m|BRQ6": "2026-04-02T11:39:59Z"},
            raw_table_path=table_path.as_posix(),
            raw_ingest_progress_path=progress_path.as_posix(),
            raw_ingest_error_path=error_path.as_posix(),
            raw_ingest_error_latest_path=error_latest_path.as_posix(),
            changed_windows=[
                {
                    "internal_id": "FUT_BR",
                    "source_timeframe": "1m",
                    "source_interval": 1,
                    "moex_secid": "BRQ6",
                    "window_start_utc": "2026-04-01T07:00:00Z",
                    "window_end_utc": "2026-04-02T11:40:00Z",
                    "incremental_rows": 10,
                }
            ],
            generated_at_utc="2026-04-02T12:06:02Z",
        )

    monkeypatch.setattr(nightly, "ingest_moex_bootstrap_window", _fake_ingest_moex_bootstrap_window)

    job = {
        "shard_id": "shard-01",
        "internal_ids": ["FUT_BR"],
        "coverage": [
            _coverage_record(
                internal_id="FUT_BR",
                secid="BRQ6",
                source_interval=1,
                source_timeframe="1m",
            ).to_dict()
        ],
        "output_dir": (tmp_path / "phase01" / "shards" / "shard-01").as_posix(),
        "run_id": "nightly-shard-01",
        "timeframes": ["5m", "15m"],
        "bootstrap_window_days": 30,
        "ingest_till_utc": "2026-04-02T12:00:00Z",
        "stability_lag_minutes": 20,
        "expand_contract_chain": True,
        "contract_discovery_step_days": 14,
        "contract_discovery_lookback_days": 30,
        "refresh_overlap_minutes": 180,
    }

    first = nightly._run_shard_job(job)
    second = nightly._run_shard_job(job)

    assert calls == ["nightly-shard-01"]
    assert first["status"] == "PASS"
    assert second["status"] == "PASS"
    assert second["resume_mode"] == "reused_success"
    success_payload = json.loads(
        (Path(str(job["output_dir"])) / "shard-success.json").read_text(encoding="utf-8")
    )
    assert success_payload["status"] == "PASS"
