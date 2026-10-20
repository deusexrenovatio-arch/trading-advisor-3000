"""Adopt proven native metadata after projection retirement; never compute indicators.

The compatibility constants describe one audited legacy release, not a runtime
fallback. Plan/apply require paused writers. No vacuum; receipt supports rollback.
"""

from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
from deltalake import CommitProperties, DeltaTable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    _normalize_loaded_rows,
    read_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.indicators import IndicatorFrameRow

from ..hash_compatibility import BOOTSTRAP_ENGINE, BOOTSTRAP_KERNELS
from . import materialize as m
from .retire_mtf import _check_writer, _fingerprint, rollback_retirement
from .source_frames import (
    NativeRefreshScope,
    derived_source_indicator_columns_hash,
    load_derived_source_frame_partition_metadata,
    load_derived_source_frame_partition_rows,
)
from .store import derived_indicator_output_columns_hash

KIND = "adopt-native-derived-metadata-v1"
# Git 9981781d; CRLF production checkout; numpy2.5.4 / pandas2.3.3.
LEGACY_IMPLEMENTATION = (
    "0d697e2ac44572fa2bb62bf9b153b515bc556a4bf822b4b4700bf991fd6521f9",
    "830d8590da639162d0f716153e25d2b184c8c8688430ae05989d082a770bc3ab",
    "be844f222d8482a3e9a7d8b310b0d7143f4eece0c483bb46fcc6647247fcd6c0",
    "b017e27e3ed51f21b8b0799bbfa06ebf252306c895707b07ed00c29205204796",
    "96f018769806417506bb475ce3b39ca7a69b72fa870f77e3026ac9a39aafc9b4",
    "632fa2f48e4b94f8bd03c789e8a4bc8c03c70291e6b1f39f2440d4f67f1611fa",
)
LEGACY_COMPUTE = "757d937dc25d7ed7e52ba90b7447beca66e7ce173ff7e16aa0fdb549c0b14739"
NATIVE_KERNEL = BOOTSTRAP_KERNELS["l2"]
SOURCE = "research_derived_source_frames"
DERIVED = "research_derived_indicator_frames"
INPUTS = (
    "research_bar_views",
    "research_indicator_frames",
    "research_datasets",
    "continuous_front_adjustment_ladder",
    SOURCE,
    DERIVED,
    "continuous_front_indicator_run_manifest",
)
KEYS = (
    "dataset_version",
    "contour_id",
    "series_mode",
    "series_id",
    "instrument_id",
    "timeframe",
    "indicator_set_version",
)
MUTABLE = (
    "source_indicators_hash",
    "source_inputs_hash",
    "source_snapshot_hash",
    "compute_contract_hash",
    "source_indicator_columns_hash",
    "null_warmup_span",
)


def native_kernel_digest() -> str:
    """One-time legacy adoption uses the audited numerical baseline, not files."""
    from ..compute_contract import kernel_contract
    from ..hash_compatibility import reviewed_current_source
    from ..semantic_fingerprint import engine_identity

    if engine_identity() != BOOTSTRAP_ENGINE or not reviewed_current_source():
        raise ValueError("native fingerprint authority changed; reviewed migration required")
    return kernel_contract()["l2"]


def _writer_binding():
    research = Path(m.__file__).parent.parent
    paths = [
        Path(m.__file__),
        Path(__file__),
        Path(m.__file__).with_name("source_frames.py"),
        research / "derived_indicators/registry.py",
        research / "bar_usage_policy.py",
        research / "continuous_front_indicators/input_projection.py",
        research / "continuous_front_indicators/rules.py",
        research.parent.parent / "spark_jobs/research_derived_source_frames_job.py",
        Path(inspect.getfile(ResearchBarView)),
        Path(inspect.getfile(IndicatorFrameRow)),
        research / "datasets/bar_usage.py",
        Path(inspect.getfile(_normalize_loaded_rows)),
    ]
    package = research.parent.parent
    return {
        str(path.relative_to(package)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
    }


def _assert_writer_owner(writer_root):
    _check_writer(writer_root)
    package = (writer_root / "src/trading_advisor_3000").resolve()
    for module_path in (
        Path(__file__),
        Path(m.__file__),
        Path(inspect.getfile(NativeRefreshScope)),
        Path(inspect.getfile(m.run_research_derived_source_frames_spark_job)),
    ):
        if not module_path.resolve().is_relative_to(package):
            raise ValueError("loaded metadata/scope writer differs from promoted writer root")


def _inventory(root: Path) -> dict[str, dict[str, object]]:
    result = {}
    for name in INPUTS:
        table = DeltaTable(str(root / f"{name}.delta"))
        result[name] = {"id": table.metadata().id, "version": table.version()}
    return result


def _historical_ladder(root: Path, dataset: str, version: int):
    table = DeltaTable(str(root / "continuous_front_adjustment_ladder.delta"), version=version)
    return tuple(
        _normalize_loaded_rows(
            table.to_pyarrow_table(filters=[("dataset_version", "=", dataset)]).to_pylist()
        )
    )


def _historical_rows(root, name, version, filters):
    return _normalize_loaded_rows(
        DeltaTable(str(root / f"{name}.delta"), version=version)
        .to_pyarrow_table(filters=filters)
        .to_pylist()
    )


def _historical_binding(root, dataset, versions, indicator_set, derived_set):
    rows = read_delta_table_rows(
        root / "continuous_front_indicator_run_manifest.delta",
        filters=[("dataset_version", "=", dataset)],
    )
    matches = []
    for row in rows:
        refs = json.loads(str(row["input_delta_versions_json"]))
        accepted = (
            row.get("publish_status") == "accepted"
            and row.get("indicator_set_version") == indicator_set
            and row.get("derived_set_version") == derived_set
        )
        paths_match = all(
            Path(str(refs.get(name, {}).get("path", ""))).resolve()
            == (root / f"{name}.delta").resolve()
            for name in versions
        )
        versions_match = all(
            refs.get(name, {}).get("delta_version") == version for name, version in versions.items()
        )
        if accepted and paths_match and versions_match:
            matches.append(str(row["run_id"]))
    if len(matches) != 1:
        raise ValueError("historical input versions lack one accepted L2 manifest binding")
    return dict(
        run_id=matches[0],
        tables={
            name: {
                "path": str(root / f"{name}.delta"),
                "version": version,
                "id": DeltaTable(str(root / f"{name}.delta"), version=version).metadata().id,
            }
            for name, version in versions.items()
        },
    )


def _legacy_compute(ladder) -> str:
    payload = dict(
        contract="derived-native-v1",
        implementation=LEGACY_IMPLEMENTATION,
        numpy=np.__version__,
        pandas=pd.__version__,
        bar_usage=m.BAR_USAGE_POLICY_ID,
        indicator_usage=m.INDICATOR_USAGE_POLICY_ID,
        mtf=(("1h", "15m"), ("4h", "15m"), ("4h", "1h"), ("1d", "15m"), ("1d", "1h"), ("1d", "4h")),
        ladder=sorted(ladder, key=lambda row: json.dumps(row, sort_keys=True, default=str)),
    )
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _legacy_indicators(rows, columns) -> str:
    payload = [
        dict(
            contract_id=r.contract_id,
            instrument_id=r.instrument_id,
            timeframe=r.timeframe,
            ts=r.ts,
            source_bars_hash=r.source_bars_hash,
            row_count=r.row_count,
            values={c: r.values.get(c) for c in columns},
        )
        for r in rows
    ]
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()[:16].upper()


def _filters(key: dict[str, object]) -> list[tuple[str, str, object]]:
    return [(name, "=", key[name]) for name in KEYS]


def _assert_authoritative(root, key, source_rows, columns):
    filters = [(c, op, v) for c, op, v in _filters(key) if c != "indicator_set_version"]
    bars = read_delta_table_rows(root / "research_bar_views.delta", filters=filters)
    bars = sorted(bars, key=lambda row: str(row["ts"]))
    if len(bars) != len(source_rows):
        raise ValueError(f"authoritative L0 coverage changed: {key}")
    cached_bars = [ResearchBarView.from_dict(row) for row in source_rows]
    if m._bars_hash([ResearchBarView.from_dict(row) for row in bars]) != m._bars_hash(cached_bars):
        raise ValueError(f"authoritative L0 native inputs changed: {key}")
    indicator_rows = read_delta_table_rows(
        root / "research_indicator_frames.delta", filters=_filters(key)
    )
    indicator_rows = sorted(indicator_rows, key=lambda row: str(row["ts"]))
    if len(indicator_rows) != len(source_rows):
        raise ValueError(f"authoritative L1 coverage changed: {key}")
    indicators = [IndicatorFrameRow.from_dict(row) for row in indicator_rows]
    _, cached, _ = m._source_frame_rows_to_inputs(
        rows=source_rows, source_indicator_columns=columns
    )
    if m._indicator_hash(indicators, value_columns=columns) != m._indicator_hash(
        cached, value_columns=columns
    ):
        raise ValueError(f"authoritative L1 native inputs changed: {key}")
    if [(r.profile_version, r.output_columns_hash) for r in indicators] != [
        (r.profile_version, r.output_columns_hash) for r in cached
    ]:
        raise ValueError(f"authoritative indicator profile changed: {key}")


def _partition_plan(
    root,
    key,
    head,
    profile,
    columns,
    legacy_ladder,
    current_ladder,
    legacy_indicator_version,
    legacy_bar_version,
    legacy_derived_version,
):
    series = m._series_key_from_source_metadata(key, requested_series_mode=str(key["series_mode"]))
    source_rows = load_derived_source_frame_partition_rows(
        output_dir=root,
        partition=m._source_frame_partition_key(
            dataset_version=str(key["dataset_version"]),
            indicator_set_version=str(key["indicator_set_version"]),
            series_key=series,
            timeframe=str(key["timeframe"]),
        ),
        source_indicator_columns=(*columns, "adx_14"),
    )
    if any(
        row.get("source_indicator_columns_hash")
        not in {"1E6B1AD5B3491A4D", derived_source_indicator_columns_hash(columns)}
        for row in source_rows
    ):
        raise ValueError(f"unknown current source projection identity: {key}")
    _assert_authoritative(root, key, source_rows, columns)
    existing = read_delta_table_rows(
        root / f"{DERIVED}.delta",
        filters=[
            *_filters(key),
            ("derived_indicator_set_version", "=", head["derived_indicator_set_version"]),
        ],
    )
    existing = sorted(existing, key=lambda row: str(row["ts"]))
    if not existing or len(existing) != int(head["row_count"]):
        raise ValueError(f"partial derived coverage: {key}")
    count = len(existing)
    if len(source_rows) < count:
        raise ValueError(f"source prefix deleted: {key}")
    historical_bars = _historical_rows(
        root,
        "research_bar_views",
        legacy_bar_version,
        [(c, op, v) for c, op, v in _filters(key) if c != "indicator_set_version"],
    )
    historical_bars = sorted(historical_bars, key=lambda row: str(row["ts"]))
    historical = _historical_rows(
        root, "research_indicator_frames", legacy_indicator_version, _filters(key)
    )
    historical = sorted(historical, key=lambda row: str(row["ts"]))
    legacy_indicators = [IndicatorFrameRow.from_dict(row) for row in historical]
    bars = [ResearchBarView.from_dict(row) for row in historical_bars]
    identities = ("ts", "contract_id", "instrument_id", "series_id", "timeframe")
    if (
        len(bars) != count
        or len(legacy_indicators) != count
        or (
            [[row.get(c) for c in identities] for row in historical_bars]
            != [[row.get(c) for c in identities] for row in existing]
        )
    ):
        raise ValueError(f"historical native key/count drift: {key}")
    if [row.ts for row in bars] != [row.ts for row in legacy_indicators]:
        raise ValueError(f"historical L0/L1 key alignment changed: {key}")
    old_columns = tuple(dict.fromkeys((*columns, "adx_14")))
    if (
        m._bars_hash(bars) != head["source_bars_hash"]
        or _legacy_indicators(legacy_indicators, old_columns) != head["source_indicators_hash"]
    ):
        raise ValueError(f"legacy historical inputs do not match stored L2: {key}")
    old_derived = _historical_rows(
        root,
        DERIVED,
        legacy_derived_version,
        [
            *_filters(key),
            ("derived_indicator_set_version", "=", head["derived_indicator_set_version"]),
        ],
    )
    old_derived = sorted(old_derived, key=lambda row: str(row["ts"]))
    preserved = [*identities, *profile.output_columns]
    if not pd.DataFrame(existing)[preserved].equals(pd.DataFrame(old_derived)[preserved]):
        raise ValueError(f"stored native values differ from accepted legacy L2: {key}")
    if head["compute_contract_hash"] != LEGACY_COMPUTE:
        raise ValueError(f"unknown legacy compute identity: {key}")
    prefix = []
    for bar, indicator in zip(bars, legacy_indicators):
        prefix.append(
            {
                **bar.to_dict(),
                **{c: indicator.values.get(c) for c in columns},
                "indicator_set_version": indicator.indicator_set_version,
                "indicator_profile_version": indicator.profile_version,
                "indicator_output_columns_hash": indicator.output_columns_hash,
            }
        )
    output_hash = derived_indicator_output_columns_hash(profile.output_columns)
    if any(
        row.get("output_columns_hash") != output_hash
        or row.get("profile_version") != profile.version
        or row.get("warmup_span") != profile.warmup_bars
        for row in existing
    ):
        raise ValueError(f"native profile drift: {key}")
    # Hash the same projected columns as the ordinary owner, including dtypes.
    native_prefix = prefix
    _, native_indicators, frame = m._source_frame_rows_to_inputs(
        rows=native_prefix, source_indicator_columns=columns
    )
    old_frame = m._with_continuous_front_projection(frame, adjustment_ladder_rows=legacy_ladder)
    current_frame = m._with_continuous_front_projection(
        frame, adjustment_ladder_rows=current_ladder
    )
    inputs_hash = m._source_inputs_hash(profile=profile, base_frame=current_frame)
    if m._source_inputs_hash(profile=profile, base_frame=old_frame) != inputs_hash:
        raise ValueError(f"native ladder inputs changed: {key}")
    metadata = dict(
        source_indicators_hash=m._indicator_hash(native_indicators, value_columns=columns),
        source_inputs_hash=inputs_hash,
        source_snapshot_hash="",  # Filled only after source commit, for complete unchanged pairs.
        compute_contract_hash=m._derived_compute_contract_hash(current_ladder),
        source_indicator_columns_hash=derived_source_indicator_columns_hash(columns),
        null_warmup_span=m._null_warmup_span(
            payload_rows=existing, output_columns=profile.output_columns
        ),
    )
    _, _, current_source = m._source_frame_rows_to_inputs(
        rows=[{c: v for c, v in row.items() if c != "adx_14"} for row in source_rows],
        source_indicator_columns=columns,
    )
    current_source = m._with_continuous_front_projection(
        current_source, adjustment_ladder_rows=current_ladder
    )
    current_inputs_match = (
        m._source_inputs_hash(profile=profile, base_frame=current_source) == inputs_hash
    )
    return dict(
        key=key,
        derived_set=head["derived_indicator_set_version"],
        retained_rows=count,
        appended_source_rows=len(source_rows) - count,
        current_inputs_match=current_inputs_match,
        metadata=metadata,
    )


def plan_adaptation(
    root: Path,
    *,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    contour_id: str,
    instruments=(),
    timeframes=(),
    legacy_ladder_version: int = 23,
    legacy_indicator_version: int = 1064,
    legacy_bar_version: int = 28,
    legacy_derived_version: int = 280,
):
    root = root.resolve()
    if native_kernel_digest() != NATIVE_KERNEL:
        raise ValueError("native kernel differs from audited retirement compatibility")
    before = _inventory(root)
    writer_binding = _writer_binding()
    historical_run = _historical_binding(
        root,
        dataset_version,
        {
            "research_bar_views": legacy_bar_version,
            "research_indicator_frames": legacy_indicator_version,
            "continuous_front_adjustment_ladder": legacy_ladder_version,
            DERIVED: legacy_derived_version,
        },
        indicator_set_version,
        derived_indicator_set_version,
    )
    legacy_ladder = _historical_ladder(root, dataset_version, legacy_ladder_version)
    if _legacy_compute(legacy_ladder) != LEGACY_COMPUTE:
        raise ValueError("legacy release/library/ladder identity not proven")
    current_ladder = m.cf_input_projection.load_adjustment_ladder_rows(
        dataset_output_dir=root, dataset_version=dataset_version
    )
    profile = m.current_derived_indicator_profile()
    columns = m._source_indicator_columns_for_profile(profile)
    physical = DeltaTable(str(root / f"{SOURCE}.delta")).schema()
    fields = {field.name: field.type for field in physical.fields}
    authoritative = {
        field.name: field.type
        for field in DeltaTable(str(root / "research_indicator_frames.delta")).schema().fields
    }
    if any(name not in fields or fields[name] != authoritative.get(name) for name in columns):
        raise ValueError("physical native source columns/types differ from authoritative L1")
    if not set(MUTABLE) <= {
        field.name for field in DeltaTable(str(root / f"{DERIVED}.delta")).schema().fields
    }:
        raise ValueError("native metadata schema migration is incomplete")
    scope = NativeRefreshScope.from_request(timeframes=timeframes, instruments=instruments)
    source_meta = load_derived_source_frame_partition_metadata(
        output_dir=root,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        contour_id=contour_id,
    )
    keys = {
        tuple(row[c] for c in KEYS): {c: row[c] for c in KEYS}
        for row in source_meta
        if row["series_mode"] == "continuous_front"
        and scope.matches(row["instrument_id"], row["timeframe"])
    }
    heads = m.load_derived_indicator_partition_metadata(
        output_dir=root,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        contour_id=contour_id,
    )
    heads = {
        tuple(row[c] for c in KEYS): row
        for row in heads
        if row["series_mode"] == "continuous_front"
        and scope.matches(row["instrument_id"], row["timeframe"])
    }
    actual_pairs = {(key["instrument_id"], key["timeframe"]) for key in keys.values()}
    if (
        (instruments and not set(instruments) <= {p[0] for p in actual_pairs})
        or (timeframes and not set(timeframes) <= {p[1] for p in actual_pairs})
        or (
            instruments
            and timeframes
            and any((ip, tf) not in actual_pairs for ip in instruments for tf in timeframes)
        )
    ):
        raise ValueError("explicit requested native pair is absent")
    if set(keys) != set(heads):
        raise ValueError("selected source/L2 key coverage differs")
    if not keys or any(key not in heads for key in keys):
        raise ValueError("selected native partition coverage is incomplete")
    entries = []
    for key, values in sorted(keys.items(), key=lambda item: str(item[0])):
        entry = _partition_plan(
            root,
            values,
            heads[key],
            profile,
            columns,
            legacy_ladder,
            current_ladder,
            legacy_indicator_version,
            legacy_bar_version,
            legacy_derived_version,
        )
        entries.append(entry)
    if _inventory(root) != before or _writer_binding() != writer_binding:
        raise ValueError("inputs/writer changed during native metadata preflight")
    return dict(
        kind=KIND,
        root=str(root),
        inputs=before,
        historical_run=historical_run,
        writer_binding=writer_binding,
        partitions=entries,
        request=dict(
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            contour_id=contour_id,
            instruments=list(instruments),
            timeframes=list(timeframes),
            legacy_ladder_version=legacy_ladder_version,
            legacy_indicator_version=legacy_indicator_version,
            legacy_bar_version=legacy_bar_version,
            legacy_derived_version=legacy_derived_version,
        ),
    )


def _receipt_write(receipt, path):
    path.write_text(json.dumps(receipt, indent=2), encoding="utf-8")


def _metadata_groups(table, key_names, mutable):
    columns = [*key_names, *sorted(mutable)]
    metadata = table.to_pyarrow_table(columns=columns)
    grouped = metadata.group_by(columns).aggregate([(key_names[0], "count")])
    result = {}
    for row in grouped.to_pylist():
        key = tuple(row[c] for c in key_names)
        result.setdefault(key, []).append(row)
    return {
        key: sorted(rows, key=lambda row: json.dumps(row, sort_keys=True, default=str))
        for key, rows in result.items()
    }


def _verify_metadata_groups(before, after, rows, key_names, expected_counts, mutable):
    selected = {tuple(row[c] for c in key_names): row for row in rows}
    if set(before) != set(after):
        raise RuntimeError("metadata commit changed logical partition keys")
    for key, groups in after.items():
        if key not in selected:
            if groups != before[key]:
                raise RuntimeError("metadata outside selected scope changed")
            continue
        expected = selected[key]
        count = sum(int(group[f"{key_names[0]}_count"]) for group in groups)
        if count != expected_counts[key] or any(
            group[column] != expected[column] for group in groups for column in mutable
        ):
            raise RuntimeError("selected metadata/count readback differs from plan")
    if not set(selected) <= set(after):
        raise RuntimeError("metadata update matched no selected partition")


def apply_adaptation(plan, *, writer_root: Path, receipt_path: Path):
    _assert_writer_owner(writer_root)
    root = Path(plan["root"])
    if plan.get("kind") != KIND or receipt_path.exists():
        raise ValueError("invalid plan or existing receipt")
    if list(root.glob(".*.lock.json")):
        raise ValueError("writer lock present")
    if _inventory(root) != plan["inputs"]:
        raise ValueError("Delta identity/version drift since plan")
    # Re-run every selected partition preflight before the first transaction.
    if plan_adaptation(root, **plan["request"]) != plan:
        raise ValueError("native metadata plan changed")
    receipt = dict(kind=KIND, root=str(root), tables=[])
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    _receipt_write(receipt, receipt_path)
    expected_refs = dict(plan["inputs"])
    for name in (SOURCE, DERIVED):
        path = root / f"{name}.delta"
        before = DeltaTable(str(path))
        mutable = {"source_indicator_columns_hash"} if name == SOURCE else set(MUTABLE)
        preserved = [f.name for f in before.schema().fields if f.name not in mutable]
        fingerprint = _fingerprint(before, preserved)
        rows = []
        for entry in plan["partitions"]:
            updates = dict(entry["metadata"])
            if name == SOURCE:
                updates = {
                    "source_indicator_columns_hash": updates["source_indicator_columns_hash"]
                }
            elif entry["current_inputs_match"]:
                request = plan["request"]
                ladder = m.cf_input_projection.load_adjustment_ladder_rows(
                    dataset_output_dir=root, dataset_version=request["dataset_version"]
                )
                updates["source_snapshot_hash"] = m._derived_source_snapshot_hash(
                    input_refs=m._delta_source_refs(
                        [
                            root / "research_bar_views.delta",
                            root / "research_indicator_frames.delta",
                        ]
                    ),
                    source_path=root / f"{SOURCE}.delta",
                    compute_contract_hash=m._derived_compute_contract_hash(ladder),
                    profile=m.current_derived_indicator_profile(),
                    columns=m._source_indicator_columns_for_profile(
                        m.current_derived_indicator_profile()
                    ),
                    timeframes=request["timeframes"],
                    instruments=request["instruments"],
                )
            key = dict(entry["key"])
            if name == DERIVED:
                key["derived_indicator_set_version"] = entry["derived_set"]
            rows.append({**key, **updates})
        if _inventory(root) != expected_refs or _writer_binding() != plan["writer_binding"]:
            raise ValueError("concurrent input/writer change before metadata commit")
        key_names = [*KEYS, *(["derived_indicator_set_version"] if name == DERIVED else [])]
        source = pa.Table.from_pylist(rows)
        before_metadata = _metadata_groups(before, key_names, mutable)
        expected_counts = {
            tuple(row[c] for c in key_names): entry["retained_rows"]
            + (entry["appended_source_rows"] if name == SOURCE else 0)
            for entry, row in zip(plan["partitions"], rows)
        }
        predicate = " AND ".join(f"t.{c} = s.{c}" for c in key_names)
        before.merge(
            source,
            predicate=predicate,
            source_alias="s",
            target_alias="t",
            commit_properties=CommitProperties(
                custom_metadata={
                    "metadata_adaptation": KIND,
                    "rollback_version": str(before.version()),
                }
            ),
        ).when_matched_update({c: f"s.{c}" for c in mutable}).execute()
        after = DeltaTable(str(path))
        entry = dict(
            table=name,
            version=plan["inputs"][name]["version"],
            table_id=before.metadata().id,
            after_version=plan["inputs"][name]["version"] + 1,
            partition_by=before.metadata().partition_columns,
            verified=False,
        )
        receipt["tables"].append(entry)
        _receipt_write(receipt, receipt_path)
        expected_refs[name] = dict(id=entry["table_id"], version=entry["after_version"])
        if _inventory(root) != expected_refs:
            raise RuntimeError("concurrent writer after metadata commit")
        if before.schema().to_json() != after.schema().to_json():
            raise RuntimeError("metadata adaptation changed physical schema")
        if _fingerprint(after, preserved) != fingerprint:
            raise RuntimeError("native metadata adaptation changed preserved values/keys")
        _verify_metadata_groups(
            before_metadata,
            _metadata_groups(after, key_names, mutable),
            rows,
            key_names,
            expected_counts,
            mutable,
        )
        entry.update(
            verified=True,
            preserved_rows=fingerprint[0],
            preserved_fingerprint=list(fingerprint[1:]),
        )
        _receipt_write(receipt, receipt_path)
    return receipt


def rollback_adaptation(receipt):
    if receipt.get("kind") != KIND:
        raise ValueError("invalid native metadata receipt")
    rollback_retirement(receipt)


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply-plan", type=Path)
    parser.add_argument("--rollback-receipt", type=Path)
    parser.add_argument("--writer-root", type=Path)
    parser.add_argument("--dataset-version")
    parser.add_argument("--indicator-set-version", default="indicators-v1")
    parser.add_argument("--derived-indicator-set-version", default="derived-v1")
    parser.add_argument("--contour-id", default="pit_active_front")
    parser.add_argument("--instruments", nargs="*", default=[])
    parser.add_argument("--timeframes", nargs="*", default=[])
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.apply_plan or args.rollback_receipt:
        if args.writer_root is None or (args.apply_plan and args.rollback_receipt):
            parser.error("apply/rollback require writer-root and mutually exclusive inputs")
        source = args.apply_plan or args.rollback_receipt
        document = json.loads(source.read_text(encoding="utf-8"))
        if Path(document["root"]).resolve() != args.root.resolve():
            parser.error("document root differs from --root")
        if args.apply_plan:
            result = apply_adaptation(
                document, writer_root=args.writer_root, receipt_path=args.output
            )
        else:
            _assert_writer_owner(args.writer_root)
            rollback_adaptation(document)
            result = {"rolled_back": document}
    else:
        if not args.dataset_version:
            parser.error("read-only plan requires --dataset-version")
        result = plan_adaptation(
            args.root,
            dataset_version=args.dataset_version,
            indicator_set_version=args.indicator_set_version,
            derived_indicator_set_version=args.derived_indicator_set_version,
            contour_id=args.contour_id,
            instruments=args.instruments,
            timeframes=args.timeframes,
        )
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({"kind": KIND, "output": str(args.output)}, indent=2))


if __name__ == "__main__":
    main()
