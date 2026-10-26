"""Immutable SYNTHETIC inputs for a bounded native Research branch exercise.

Only input rows are generated here. Delta owns their storage and the snapshot
owner owns their pins/publication. This is neither Research output nor market QC.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

from ..data_plane.delta_runtime import write_delta_table_rows
from ..data_plane.schemas import historical_data_delta_schema_manifest
from . import data_snapshots as snapshots

LOGICAL_ROOT = "/staging-control/fixture"
FIXTURE_VERSION = "synthetic-native-v1"
INSTRUMENTS = ("BR", "Si")
TIMEFRAMES = ("15m", "1h", "4h", "1d")
BARS_PER_GROUP = 512
MAX_ROWS, MAX_BYTES, MAX_FILES = 32_768, 16 * 1024 * 1024, 256
_FINAL_DAY = datetime(2026, 3, 16, tzinfo=UTC)
_RAW_SESSIONS = 8
_CANONICAL = (
    "canonical_bars",
    "canonical_bar_provenance",
    "canonical_session_intervals",
    "canonical_session_calendar",
    "canonical_closed_session_dates",
    "canonical_roll_map",
    "canonical_contract_economics",
)
_NATIVE_CAPS = {
    name: 1
    for name in (
        "run_continuous_front_spark_job",
        "run_research_bar_views_spark_job",
        "materialize_indicator_frames",
        "materialize_derived_indicator_frames",
        "run_research_derived_source_frames_spark_job",
        "run_continuous_front_indicator_pandas_job",
        "run_continuous_front_indicator_sidecar_spark_job",
    )
} | {"materialize_research_dataset": 2}


def _iso(value):
    return value.isoformat().replace("+00:00", "Z")


def _plain(path):
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
        raise ValueError("fixture links/reparse points are forbidden")
    if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
        raise ValueError("fixture requires regular files/directories")
    if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
        raise ValueError("fixture hard links are forbidden")
    return info


def _safe_root(path):
    path = Path(os.path.abspath(path))
    for part in (path, *path.parents):
        if part.exists() or part.is_symlink():
            _plain(part)
    return path


def _logical(root):
    value = PurePosixPath(root)
    if not value.is_absolute() or str(value) != root or ".." in value.parts or "\\" in root:
        raise ValueError("fixture logical root must be a normalized absolute POSIX path")
    if root == "/":
        raise ValueError("fixture logical root cannot be filesystem root")
    return value


def _relative(value):
    if not isinstance(value, str):
        raise ValueError("fixture path must be a string")
    path = PurePosixPath(value)
    if path.is_absolute() or str(path) != value or any(p in {"", ".", ".."} for p in path.parts):
        raise ValueError("fixture relative path is unsafe")
    if not path.parts or "\\" in value or ":" in value:
        raise ValueError("fixture relative path is unsafe")
    return path


def _inventory(root):
    files, directories, total = {}, set(), 0
    for directory, names, filenames in os.walk(root, followlinks=False):
        for name in sorted([*names, *filenames]):
            path = Path(directory) / name
            info = _plain(path)
            if stat.S_ISDIR(info.st_mode):
                directories.add(path.relative_to(root).as_posix())
                if len(directories) > MAX_FILES:
                    raise ValueError("fixture directory cap exceeded")
                continue
            total += info.st_size
            if total > MAX_BYTES or len(files) >= MAX_FILES:
                raise ValueError("fixture file/byte cap exceeded")
            relative = path.relative_to(root).as_posix()
            _relative(relative)
            with path.open("rb") as stream:
                sha = hashlib.file_digest(stream, "sha256").hexdigest()
            files[relative] = {"sha256": sha, "bytes": info.st_size}
    parents = {
        str(parent)
        for file in files
        for parent in PurePosixPath(file).parents
        if str(parent) != "."
    }
    if directories != parents:
        raise ValueError("fixture contains extra empty directories")
    return files


def _raw_coverage():
    return {
        "start_utc": _iso(_FINAL_DAY - timedelta(days=7) + timedelta(hours=6)),
        "end_utc": _iso(_FINAL_DAY + timedelta(hours=21)),
        "session_open_utc": "06:00",
        "session_close_utc": "21:00",
        "session_count": _RAW_SESSIONS,
        "full_history": False,
    }


def _inputs(bars_per_group=BARS_PER_GROUP):
    """Fixture construction only; no production formula/indicator is reproduced."""
    rows = {name: [] for name in (*_CANONICAL, "raw_moex_history")}
    days_by_tf = {}
    for timeframe, minutes in (("15m", 15), ("1h", 60), ("4h", 240), ("1d", 900)):
        slots, days = [], set()
        day = _FINAL_DAY
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
                    "ts": _iso(opened),
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
                        "bar_start_ts": _iso(opened),
                        "bar_end_ts": _iso(closed),
                        "session_interval_id": f"synthetic-{instrument}-{date}",
                        "source_provider": FIXTURE_VERSION,
                        "source_timeframe": timeframe,
                        "source_interval": minutes,
                        "source_run_id": FIXTURE_VERSION,
                        "source_ingest_run_id": FIXTURE_VERSION,
                        "source_row_count": 1,
                        "source_ts_open_first": _iso(opened),
                        "source_ts_close_last": _iso(closed),
                        "open_interest_imputed": 0,
                        "build_run_id": FIXTURE_VERSION,
                        "built_at_utc": _iso(_FINAL_DAY + timedelta(days=1)),
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
    for instrument in INSTRUMENTS:
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
                    "policy_id": FIXTURE_VERSION,
                    "source_id": FIXTURE_VERSION,
                    "source_document_hash": hashlib.sha256(FIXTURE_VERSION.encode()).hexdigest(),
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
                    "effective_to_ts": _iso(
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
                    "radius_source": FIXTURE_VERSION,
                    "margin_formula_base": 10000.0,
                    "margin_radius_adjusted": 10000.0,
                    "margin_required_no_buffer": 10000.0,
                    "margin_buffer_pct": 0.05,
                    "margin_required_estimate": 10500.0,
                    "maturity_rank": 1,
                    "days_to_expiry": 90,
                    "expiration_date": "2026-06-15",
                    "model_version": FIXTURE_VERSION,
                    "buffer_policy_version": "buffer-v1",
                    "model_quality": "estimated",
                    "source_flags_json": {"synthetic": True},
                    "source_document_hashes_json": {},
                    "created_at": f"{date}T00:00:00Z",
                }
            )
        for offset in range(_RAW_SESSIONS):
            day = _FINAL_DAY - timedelta(days=offset)
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
                        "ts_open": _iso(opened),
                        "ts_close": _iso(closed),
                        "open": close,
                        "high": close + 0.01,
                        "low": close - 0.01,
                        "close": close,
                        "volume": 1,
                        "open_interest": 20000,
                        "ingest_run_id": FIXTURE_VERSION,
                        "ingested_at_utc": _iso(_FINAL_DAY + timedelta(days=1)),
                        "provenance_json": {"synthetic": True},
                        "ts_close_year": closed.year,
                    }
                )
    return rows


def _descriptor(manifest, sha):
    fields = (
        "instrument_ids",
        "timeframes",
        "warmup_bars",
        "start_utc",
        "end_utc",
        "data_manifest_path",
        "canonical_dir",
        "raw_dir",
        "dataset_version",
        "data_snapshot_id",
        "data_manifest_sha256",
        "raw_1m_coverage",
    )
    return {"manifest_sha256": sha, **{key: deepcopy(manifest[key]) for key in fields}}


def prepare_fixture(path, logical_root=LOGICAL_ROOT):
    """Create once in a sibling temporary directory, then atomically install."""
    destination, logical = _safe_root(path), _logical(logical_root)
    if destination.exists() or not destination.parent.is_dir():
        raise ValueError("fixture destination must be new with an existing parent")
    with tempfile.TemporaryDirectory(prefix=".staging-fixture-", dir=destination.parent) as temp:
        root = Path(temp) / "fixture"
        root.mkdir()
        rows = _inputs()
        if sum(map(len, rows.values())) > MAX_ROWS:
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
            dataset_version=FIXTURE_VERSION,
            run_id=FIXTURE_VERSION,
            code_version=FIXTURE_VERSION,
            quality_checks={"synthetic_fixture_structure": "PASS"},
            parameters={"synthetic": True, "fixture_version": FIXTURE_VERSION},
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
        manifest = {
            "schema_version": "staging_test_fixture.v1",
            "fixture_version": FIXTURE_VERSION,
            "synthetic": True,
            "logical_root": str(logical),
            "instrument_ids": list(INSTRUMENTS),
            "timeframes": list(TIMEFRAMES),
            "warmup_bars": BARS_PER_GROUP,
            "start_utc": min(r["ts"] for r in rows["canonical_bars"]),
            "end_utc": _iso(_FINAL_DAY + timedelta(hours=21)),
            "canonical_dir": str(logical / "canonical"),
            "raw_dir": str(logical / "raw"),
            "raw_1m_coverage": _raw_coverage(),
            "dataset_version": FIXTURE_VERSION,
            "data_snapshot_id": native["snapshot_id"],
            "data_manifest_sha256": native["manifest_sha256"],
            "data_manifest_path": str(logical / native_path.relative_to(root).as_posix()),
            "limits": {"rows": MAX_ROWS, "bytes": MAX_BYTES, "files": MAX_FILES},
            "tables": native["tables"],
            "files": _inventory(root),
        }
        snapshots.atomic_json(root / "manifest.json", manifest, immutable=True)
        sha = hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest()
        verify_fixture(root, sha)
        root.rename(destination)
    return _descriptor(manifest, sha)


def verify_fixture(path, expected_sha256):
    """Verify bytes first, then native IDs/versions/schema/counts; never repair."""
    if not isinstance(expected_sha256, str) or not re.fullmatch("[0-9a-f]{64}", expected_sha256):
        raise ValueError("fixture requires an exact SHA256")
    root = _safe_root(path)
    files = _inventory(root)
    if files.get("manifest.json", {}).get("sha256") != expected_sha256:
        raise ValueError("fixture manifest hash mismatch")
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    logical = _logical(manifest["logical_root"])
    expected = {
        "schema_version": "staging_test_fixture.v1",
        "fixture_version": FIXTURE_VERSION,
        "synthetic": True,
        "instrument_ids": list(INSTRUMENTS),
        "timeframes": list(TIMEFRAMES),
        "warmup_bars": BARS_PER_GROUP,
        "dataset_version": FIXTURE_VERSION,
        "start_utc": _iso(_FINAL_DAY - timedelta(days=BARS_PER_GROUP - 1) + timedelta(hours=6)),
        "end_utc": _iso(_FINAL_DAY + timedelta(hours=21)),
        "raw_1m_coverage": _raw_coverage(),
        "limits": {"rows": MAX_ROWS, "bytes": MAX_BYTES, "files": MAX_FILES},
    }
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise ValueError("fixture fixed contract/caps changed")
    if {key: value for key, value in files.items() if key != "manifest.json"} != manifest["files"]:
        raise ValueError("fixture files changed or contain extras")

    def local(value):
        relative = PurePosixPath(value).relative_to(logical).as_posix()
        return root / str(_relative(relative))

    if manifest["canonical_dir"] != str(logical / "canonical") or manifest["raw_dir"] != str(
        logical / "raw"
    ):
        raise ValueError("fixture directories changed")
    data_path = local(manifest["data_manifest_path"])
    if (
        data_path
        != root / "canonical/.data_snapshots/manifests" / f"{manifest['data_snapshot_id']}.json"
    ):
        raise ValueError("fixture Data manifest path changed")
    native = json.loads(data_path.read_text(encoding="utf-8"))
    native_payload = {key: value for key, value in native.items() if key != "manifest_sha256"}
    if (
        snapshots._hash(native_payload) != manifest["data_manifest_sha256"]
        or native.get("manifest_sha256") != manifest["data_manifest_sha256"]
        or native.get("snapshot_id") != manifest["data_snapshot_id"]
        or native.get("tables") != manifest["tables"]
        or native.get("parameters", {}).get("synthetic") is not True
    ):
        raise ValueError("fixture Data manifest binding changed")
    if set(native["tables"]) != {*_CANONICAL, "raw_moex_history"}:
        raise ValueError("fixture input table set changed")
    translated = deepcopy(native)
    total = 0
    for name, reference in translated["tables"].items():
        expected_path = (
            logical / ("raw" if name == "raw_moex_history" else "canonical") / f"{name}.delta"
        )
        if reference["table_path"] != str(expected_path):
            raise ValueError("fixture table path changed")
        reference["table_path"] = local(reference["table_path"]).resolve().as_posix()
        snapshots.open_pinned_table(reference)
        if snapshots.capture_table(Path(reference["table_path"])) != reference:
            raise ValueError("fixture Delta head/pin changed")
        total += reference["row_count"]
    translated["manifest_sha256"] = snapshots._hash(
        {key: value for key, value in translated.items() if key != "manifest_sha256"}
    )
    snapshots._validate(translated)
    if total > MAX_ROWS:
        raise ValueError("fixture row cap exceeded")
    for name, count in (
        ("canonical_bars", 2 * 4 * BARS_PER_GROUP),
        ("canonical_bar_provenance", 2 * 4 * BARS_PER_GROUP),
        ("raw_moex_history", 2 * _RAW_SESSIONS * 900),
    ):
        if native["tables"][name]["row_count"] != count:
            raise ValueError("fixture fixed input row count changed")
    pointer = json.loads(
        (data_path.parent.parent / "published_current.json").read_text(encoding="utf-8")
    )
    if pointer != {
        "schema_version": "data_snapshot_publication.v1",
        "snapshot_id": native["snapshot_id"],
        "manifest_sha256": native["manifest_sha256"],
    }:
        raise ValueError("fixture Data publication changed")
    return _descriptor(manifest, expected_sha256)


def validate_branch_fixture(plan, *, fixture_root=None):
    """Validate one branch role; only the host supplies its physical mount root."""
    binding = plan.get("source", {}).get("fixture", {})
    if (
        plan.get("stage") != "branch"
        or plan.get("attempt_kind") != "small"
        or plan.get("root_job") != "research_data_prep_job"
        or plan.get("mode", "candidate") != "candidate"
        or plan.get("require_scheduled") is not False
        or plan.get("jobs") != {"research_data_prep_job": {"upstream_job": None, "max_runs": 1}}
        or type(plan["jobs"]["research_data_prep_job"]["max_runs"]) is not int
        or not isinstance(binding, Mapping)
        or set(binding) != {"manifest_path", "manifest_sha256"}
        or binding.get("manifest_path") != f"{LOGICAL_ROOT}/manifest.json"
    ):
        raise ValueError("branch Research requires the protected synthetic fixture binding")
    steps = plan.get("native_steps", {})
    if (
        not isinstance(steps, Mapping)
        or set(steps) != set(_NATIVE_CAPS)
        or any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 < value <= _NATIVE_CAPS[name]
            for name, value in steps.items()
        )
    ):
        raise ValueError("branch Research native caps exceed the fixed fixture route")
    root = Path(LOGICAL_ROOT) if fixture_root is None else Path(fixture_root)
    descriptor = verify_fixture(root, binding["manifest_sha256"])
    if (
        descriptor.get("canonical_dir") != f"{LOGICAL_ROOT}/canonical"
        or descriptor.get("raw_dir") != f"{LOGICAL_ROOT}/raw"
    ):
        raise ValueError("branch fixture must preserve the fixed logical control root")
    return descriptor
