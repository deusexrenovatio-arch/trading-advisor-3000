"""Build synthetic staging inputs through native Market Data schema and Delta owners."""

from __future__ import annotations

import hashlib
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ...common import data_snapshots as snapshots
from ...common import staging_test_fixture as fixture_contract
from ..delta_runtime import write_delta_table_rows
from ..schemas import historical_data_delta_schema_manifest


def _inputs(bars_per_group=fixture_contract.BARS_PER_GROUP):
    """Fixture construction only; no production formula/indicator is reproduced."""
    rows = {name: [] for name in (*fixture_contract.CANONICAL_TABLES, "raw_moex_history")}
    days_by_tf = {}
    for timeframe, minutes in (("15m", 15), ("1h", 60), ("4h", 240), ("1d", 900)):
        slots, days = [], set()
        day = fixture_contract.FINAL_DAY
        while len(slots) < bars_per_group:
            opened, closed = day + timedelta(hours=6), day + timedelta(hours=21)
            starts = []
            cursor = opened
            while cursor < closed:
                starts.append((cursor, min(cursor + timedelta(minutes=minutes), closed)))
                cursor += timedelta(minutes=minutes)
            slots.extend(reversed(starts))
            day -= timedelta(days=1)
        slots = sorted(slots[:bars_per_group])
        for instrument, base, step in (("BR", 82.0, 0.01), ("Si", 91800.0, 1.0)):
            contract = f"{instrument}-6.26"
            for index, (opened, closed) in enumerate(slots):
                date = opened.date().isoformat()
                days.add(date)
                close = base + step * ((index % 31) - 15)
                common = {
                    "instrument_id": instrument,
                    "contract_id": contract,
                    "timeframe": timeframe,
                    "ts": fixture_contract.fixture_time(opened),
                }
                rows["canonical_bars"].append(
                    {
                        **common,
                        "open": close - step,
                        "high": close + 2 * step,
                        "low": close - 2 * step,
                        "close": close,
                        "volume": int((closed - opened).total_seconds() / 60),
                        "open_interest": 20000 + index,
                    }
                )
                rows["canonical_bar_provenance"].append(
                    {
                        **common,
                        "bar_start_ts": fixture_contract.fixture_time(opened),
                        "bar_end_ts": fixture_contract.fixture_time(closed),
                        "session_interval_id": f"synthetic-{instrument}-{date}",
                        "source_provider": fixture_contract.FIXTURE_VERSION,
                        "source_timeframe": timeframe,
                        "source_interval": minutes,
                        "source_run_id": fixture_contract.FIXTURE_VERSION,
                        "source_ingest_run_id": fixture_contract.FIXTURE_VERSION,
                        "source_row_count": 1,
                        "source_ts_open_first": fixture_contract.fixture_time(opened),
                        "source_ts_close_last": fixture_contract.fixture_time(closed),
                        "open_interest_imputed": 0,
                        "build_run_id": fixture_contract.FIXTURE_VERSION,
                        "built_at_utc": fixture_contract.fixture_time(
                            fixture_contract.FINAL_DAY + timedelta(days=1)
                        ),
                    }
                )
            for date in sorted(days):
                rows["canonical_session_calendar"].append(
                    {
                        "instrument_id": instrument,
                        "timeframe": timeframe,
                        "session_date": date,
                        "session_open_ts": f"{date}T06:00:00Z",
                        "session_close_ts": f"{date}T21:00:00Z",
                        "session_class": "regular",
                    }
                )
        days_by_tf[timeframe] = days
    for instrument in fixture_contract.INSTRUMENTS:
        for date in sorted(set.union(*days_by_tf.values())):
            rows["canonical_session_intervals"].append(
                {
                    "instrument_id": instrument,
                    "session_date": date,
                    "interval_id": f"synthetic-{instrument}-{date}",
                    "interval_seq": 1,
                    "expected_open_ts": f"{date}T06:00:00Z",
                    "expected_close_ts": f"{date}T21:00:00Z",
                    "session_class": "regular",
                    "interval_type": "regular_trading",
                    "policy_id": fixture_contract.FIXTURE_VERSION,
                    "source_id": fixture_contract.FIXTURE_VERSION,
                    "source_document_hash": hashlib.sha256(
                        fixture_contract.FIXTURE_VERSION.encode()
                    ).hexdigest(),
                }
            )
            rows["canonical_roll_map"].append(
                {
                    "instrument_id": instrument,
                    "session_date": date,
                    "active_contract_id": f"{instrument}-6.26",
                    "reason": "SYNTHETIC fixed contract",
                }
            )
            rows["canonical_contract_economics"].append(
                {
                    "contract_id": f"{instrument}-6.26",
                    "instrument_id": instrument,
                    "moex_secid": f"{instrument}-6.26",
                    "assetcode": instrument,
                    "economics_session_date": date,
                    "effective_session_date": date,
                    "clearing_type": "tc",
                    "effective_from_ts": f"{date}T00:00:00Z",
                    "effective_to_ts": fixture_contract.fixture_time(
                        datetime.fromisoformat(date).replace(tzinfo=UTC) + timedelta(days=1)
                    ),
                    "min_step": 0.01 if instrument == "BR" else 1.0,
                    "lot_volume": 10.0 if instrument == "BR" else 1.0,
                    "quote_currency": "USD" if instrument == "BR" else "RUB",
                    "fx_rate_to_rub": 95.0 if instrument == "BR" else 1.0,
                    "tick_value_currency": 0.1 if instrument == "BR" else 1.0,
                    "step_price_rub": 9.5 if instrument == "BR" else 1.0,
                    "official_step_price": 9.5 if instrument == "BR" else 1.0,
                    "official_initial_margin": 10000.0,
                    "last_settle_price": 100.0,
                    "mr1": 0.12,
                    "radius_pct": 15.0,
                    "radius_source": fixture_contract.FIXTURE_VERSION,
                    "margin_formula_base": 10000.0,
                    "margin_radius_adjusted": 10000.0,
                    "margin_required_no_buffer": 10000.0,
                    "margin_buffer_pct": 0.05,
                    "margin_required_estimate": 10500.0,
                    "maturity_rank": 1,
                    "days_to_expiry": 90,
                    "expiration_date": "2026-06-15",
                    "model_version": fixture_contract.FIXTURE_VERSION,
                    "buffer_policy_version": "buffer-v1",
                    "model_quality": "estimated",
                    "source_flags_json": {"synthetic": True},
                    "source_document_hashes_json": {},
                    "created_at": f"{date}T00:00:00Z",
                }
            )
        for offset in range(fixture_contract.RAW_SESSIONS):
            day = fixture_contract.FINAL_DAY - timedelta(days=offset)
            for minute in range(900):
                opened = day + timedelta(hours=6, minutes=minute)
                closed = opened + timedelta(minutes=1)
                close = 82.0 if instrument == "BR" else 91800.0
                rows["raw_moex_history"].append(
                    {
                        "internal_id": instrument,
                        "finam_symbol": f"{instrument}-6.26",
                        "moex_engine": "futures",
                        "moex_market": "forts",
                        "moex_board": "RFUD",
                        "moex_secid": f"{instrument}-6.26",
                        "asset_group": "synthetic",
                        "timeframe": "1m",
                        "source_interval": 1,
                        "ts_open": fixture_contract.fixture_time(opened),
                        "ts_close": fixture_contract.fixture_time(closed),
                        "open": close,
                        "high": close + 0.01,
                        "low": close - 0.01,
                        "close": close,
                        "volume": 1,
                        "open_interest": 20000,
                        "ingest_run_id": fixture_contract.FIXTURE_VERSION,
                        "ingested_at_utc": fixture_contract.fixture_time(
                            fixture_contract.FINAL_DAY + timedelta(days=1)
                        ),
                        "provenance_json": {"synthetic": True},
                        "ts_close_year": closed.year,
                    }
                )
    return rows


def prepare_fixture(path, logical_root=fixture_contract.LOGICAL_ROOT):
    """Create once in a sibling temporary directory, then atomically install."""
    destination, logical = (
        fixture_contract.fixture_root(path),
        fixture_contract.fixture_logical_root(logical_root),
    )
    if destination.exists() or not destination.parent.is_dir():
        raise ValueError("fixture destination must be new with an existing parent")
    with tempfile.TemporaryDirectory(prefix=".staging-fixture-", dir=destination.parent) as temp:
        root = Path(temp) / "fixture"
        root.mkdir()
        rows = _inputs()
        if sum(map(len, rows.values())) > fixture_contract.MAX_ROWS:
            raise ValueError("fixture row cap exceeded")
        schemas = historical_data_delta_schema_manifest()
        tables = {}
        for name, values in rows.items():
            table = root / ("raw" if name == "raw_moex_history" else "canonical") / f"{name}.delta"
            write_delta_table_rows(
                table_path=table,
                rows=values,
                columns=schemas[name]["columns"],
                partition_by=tuple(schemas[name]["partition_by"]),
            )
            tables[name] = snapshots.capture_table(table)
        native = snapshots.create_snapshot(
            tables=tables,
            required_tables=list(tables),
            dataset_version=fixture_contract.FIXTURE_VERSION,
            run_id=fixture_contract.FIXTURE_VERSION,
            code_version=fixture_contract.FIXTURE_VERSION,
            quality_checks={"synthetic_fixture_structure": "PASS"},
            parameters={"synthetic": True, "fixture_version": fixture_contract.FIXTURE_VERSION},
        )
        registry = root / "canonical/.data_snapshots"
        native_path = snapshots.save_snapshot(registry, native)
        snapshots.publish_snapshot(registry, native_path, expected_current_id=None)
        for reference in native["tables"].values():
            relative = Path(reference["table_path"]).relative_to(root).as_posix()
            reference["table_path"] = str(logical / relative)
        native["manifest_sha256"] = snapshots._hash(
            {key: value for key, value in native.items() if key != "manifest_sha256"}
        )
        snapshots.atomic_json(native_path, native)
        snapshots.atomic_json(
            registry / "published_current.json",
            {
                "schema_version": "data_snapshot_publication.v1",
                "snapshot_id": native["snapshot_id"],
                "manifest_sha256": native["manifest_sha256"],
            },
        )
        coverage = fixture_contract.raw_coverage()
        snapshots.atomic_json(
            root / "raw-ingest-report.json",
            {
                "status": "PASS",
                "synthetic_input_only": True,
                "source_rows": len(rows["raw_moex_history"]),
                "real_bindings": [f"{i}-6.26" for i in fixture_contract.INSTRUMENTS],
                "changed_windows": [
                    {
                        "instrument_id": i,
                        "timeframe": "1m",
                        "start_ts": coverage["start_utc"],
                        "end_ts": coverage["end_utc"],
                    }
                    for i in fixture_contract.INSTRUMENTS
                ],
            },
        )
        manifest = {
            "schema_version": "staging_test_fixture.v1",
            "fixture_version": fixture_contract.FIXTURE_VERSION,
            "synthetic": True,
            "logical_root": str(logical),
            "instrument_ids": list(fixture_contract.INSTRUMENTS),
            "timeframes": list(fixture_contract.TIMEFRAMES),
            "warmup_bars": fixture_contract.BARS_PER_GROUP,
            "start_utc": min(r["ts"] for r in rows["canonical_bars"]),
            "end_utc": fixture_contract.fixture_time(
                fixture_contract.FINAL_DAY + timedelta(hours=21)
            ),
            "canonical_dir": str(logical / "canonical"),
            "raw_dir": str(logical / "raw"),
            "raw_1m_coverage": fixture_contract.raw_coverage(),
            "dataset_version": fixture_contract.FIXTURE_VERSION,
            "data_snapshot_id": native["snapshot_id"],
            "data_manifest_sha256": native["manifest_sha256"],
            "data_manifest_path": str(logical / native_path.relative_to(root).as_posix()),
            "limits": {
                "rows": fixture_contract.MAX_ROWS,
                "bytes": fixture_contract.MAX_BYTES,
                "files": fixture_contract.MAX_FILES,
            },
            "tables": native["tables"],
            "files": fixture_contract.fixture_inventory(root),
        }
        snapshots.atomic_json(root / "manifest.json", manifest, immutable=True)
        sha = hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest()
        fixture_contract.verify_fixture(root, sha)
        root.rename(destination)
    return fixture_contract.fixture_descriptor(manifest, sha)
