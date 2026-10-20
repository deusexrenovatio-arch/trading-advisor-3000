"""Version-bound retirement of persisted projections; no indicator computation.

Plan is read-only. Apply requires an idle, updated writer checkout. Old Delta
versions remain available for rollback; this command never vacuums data.
"""

from __future__ import annotations

import argparse
import json
from itertools import chain
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import CommitProperties, DeltaTable, write_deltalake

from .registry import current_derived_indicator_profile
from .store import derived_indicator_output_columns_hash

TABLES = (
    "research_derived_indicator_frames",
    "continuous_front_derived_indicator_frames",
    "indicator_roll_rules",
)
LEGACY_HASHES = {"source_mtf_hash", "mtf_source_family_hash"}
# Frozen retirement contract; these fields are never used by active compute.
RETIRED_PROJECTIONS = tuple(
    f"mtf_{source}_to_{target}_{column}"
    for source, target in (
        ("1h", "15m"),
        ("4h", "15m"),
        ("4h", "1h"),
        ("1d", "15m"),
        ("1d", "1h"),
        ("1d", "4h"),
    )
    for column in ("ema_20", "ema_50", "adx_14", "rsi_14")
)
BATCH_ROWS = 8192


def _retired(column: str) -> bool:
    return column.startswith("mtf_") or column in LEGACY_HASHES


def plan_retirement(root: Path) -> dict[str, object]:
    """Inventory current physical schemas; bind every affected table version."""
    entries = []
    for path in sorted(root.glob("*.delta")):
        if not (path / "_delta_log").is_dir():
            continue
        table = DeltaTable(str(path))
        columns = [field.name for field in table.schema().fields]
        removed = [column for column in columns if _retired(column)]
        rule_count = 0
        if path.stem == "indicator_roll_rules":
            names = table.to_pyarrow_table(columns=["output_column"])["output_column"]
            rule_count = int(pc.sum(pc.starts_with(names, "mtf_")).as_py() or 0)
        if not removed and not rule_count:
            continue
        if path.stem not in TABLES:
            raise ValueError(f"unexpected table contains retired fields: {path}")
        entries.append(
            {
                "table": path.stem,
                "version": table.version(),
                "table_id": table.metadata().id,
                "remove_columns": removed,
                "remove_rule_rows": rule_count,
                "partition_by": table.metadata().partition_columns,
            }
        )
    return {"kind": "retire-derived-projections-v1", "root": str(root.resolve()), "tables": entries}


def _check_writer(writer_root: Path) -> None:
    package = writer_root / "src/trading_advisor_3000/product_plane/research/derived_indicators"
    registry = (package / "registry.py").read_text(encoding="utf-8")
    materializer = (package / "materialize.py").read_text(encoding="utf-8")
    if "MTF_MAPPINGS" in registry or "_compute_mtf_overlay" in materializer:
        raise ValueError("production writer still produces retired projections")
    if (
        "retired derived projections" not in registry
        or "def _source_inputs_hash(" not in materializer
    ):
        raise ValueError("writer checkout does not implement the native-only contract")


def _scan(table: DeltaTable, columns: list[str]):
    return table.to_pyarrow_dataset().scanner(columns=columns, batch_size=BATCH_ROWS).to_batches()


def _fingerprint(
    table: DeltaTable, columns: list[str], *, rules: bool = False
) -> tuple[int, int, int]:
    """Order-independent native readback; includes duplicates and NULL values."""
    count = total = squared = 0
    modulus = 2**64
    for batch in _scan(table, columns):
        if rules:
            batch = batch.filter(
                pc.invert(
                    pc.fill_null(pc.starts_with(batch.column("output_column"), "mtf_"), False)
                )
            )
        # Canonical string casts avoid batch-dependent nullable-int coercion.
        # Nested fields occur in the small rules metadata table only.
        frame = pd.DataFrame(
            {
                field.name: (
                    [
                        json.dumps(value, sort_keys=True)
                        for value in batch.column(field.name).to_pylist()
                    ]
                    if pa.types.is_nested(field.type)
                    else pc.cast(batch.column(field.name), pa.string()).to_pandas()
                )
                for field in batch.schema
            }
        )
        hashes = pd.util.hash_pandas_object(frame, index=False).to_numpy(dtype=np.uint64)
        count += len(hashes)
        total = (total + int(hashes.sum(dtype=np.uint64))) % modulus
        squared = (squared + int((hashes * hashes).sum(dtype=np.uint64))) % modulus
    return count, total, squared


def _projected_batches(table: DeltaTable, entry: dict[str, object]):
    from trading_advisor_3000.spark_jobs.continuous_front_indicator_sidecar_job import (
        ROW_HASH_VERSION,
        _sidecar_row_hash_from_payload,
    )

    all_columns = [field.name for field in table.schema().fields]
    removed = set(entry["remove_columns"])
    kept = [column for column in all_columns if column not in removed]
    outputs = current_derived_indicator_profile().output_columns
    old_outputs = (*outputs, *RETIRED_PROJECTIONS)
    old_hash = derived_indicator_output_columns_hash(old_outputs)
    new_hash = derived_indicator_output_columns_hash(outputs)
    table_name = entry["table"]
    batches = iter(_scan(table, kept))
    first = next(batches, None)
    if first is None:
        schema = table.to_pyarrow_dataset().schema
        first = pa.RecordBatch.from_arrays(
            [pa.array([], type=schema.field(name).type) for name in kept], names=kept
        )
    for batch in chain((first,), batches):
        if table_name == "indicator_roll_rules":
            yield batch.filter(
                pc.invert(
                    pc.fill_null(pc.starts_with(batch.column("output_column"), "mtf_"), False)
                )
            )
            continue
        if table_name == "research_derived_indicator_frames":
            if "output_columns_hash" in kept:
                values = batch.column("output_columns_hash")
                unknown = set(values.to_pylist()) - {old_hash, new_hash}
                if unknown:
                    raise ValueError(
                        f"unknown output profile hashes; explicit migration required: {unknown}"
                    )
                batch = batch.set_column(
                    batch.schema.get_field_index("output_columns_hash"),
                    "output_columns_hash",
                    pa.array([new_hash] * len(batch), type=pa.string()),
                )
            # Legacy cross-timeframe checksums cannot prove local input identity.
            if "source_inputs_hash" not in kept:
                batch = batch.append_column(
                    "source_inputs_hash", pa.nulls(len(batch), type=pa.string())
                )
        elif table_name == "continuous_front_derived_indicator_frames":
            if not set(outputs) <= set(kept):
                raise ValueError("sidecar does not contain the complete native profile")
            # Existing owner defines the hash. Only metadata is rehashed, not indicators.
            rows = batch.to_pylist()
            hashes = [_sidecar_row_hash_from_payload(row, outputs) for row in rows]
            batch = batch.set_column(
                batch.schema.get_field_index("derived_row_hash"),
                "derived_row_hash",
                pa.array(hashes, type=pa.string()),
            )
            if "derived_row_hash_version" in kept:
                batch = batch.set_column(
                    batch.schema.get_field_index("derived_row_hash_version"),
                    "derived_row_hash_version",
                    pa.array([ROW_HASH_VERSION] * len(batch), type=pa.string()),
                )
        yield batch


def apply_retirement(
    plan: dict[str, object], *, writer_root: Path, receipt_path: Path | None = None
) -> dict[str, object]:
    """Apply only after production promotion and an operator-controlled writer pause."""
    _check_writer(writer_root)
    root = Path(str(plan["root"]))
    if plan.get("kind") != "retire-derived-projections-v1":
        raise ValueError("invalid retirement plan")
    locks = [path.name for path in root.glob(".*.lock.json")]
    if locks:
        raise ValueError(f"writer locks present: {locks}")
    if plan_retirement(root) != plan:
        raise ValueError("Delta version/schema drift since plan; create a new plan")
    # Validate every affected schema/profile before the first table commit.
    outputs = current_derived_indicator_profile().output_columns
    for entry in plan["tables"]:
        table = DeltaTable(str(root / f"{entry['table']}.delta"))
        names = [field.name for field in table.schema().fields]
        if entry["table"] == "research_derived_indicator_frames" and "output_columns_hash" in names:
            old_outputs = (*outputs, *RETIRED_PROJECTIONS)
            allowed = {
                derived_indicator_output_columns_hash(old_outputs),
                derived_indicator_output_columns_hash(outputs),
            }
            for batch in _scan(table, ["output_columns_hash"]):
                if set(batch.column("output_columns_hash").to_pylist()) - allowed:
                    raise ValueError("unknown output profile hashes; explicit migration required")
        if entry["table"] == "continuous_front_derived_indicator_frames":
            if not set(outputs) <= set(names) or "derived_row_hash" not in names:
                raise ValueError("sidecar does not contain the complete native profile/hash")
    receipt = {"kind": plan["kind"], "root": str(root), "tables": []}
    for entry in plan["tables"]:
        path = root / f"{entry['table']}.delta"
        before = DeltaTable(str(path), version=int(entry["version"]))
        mutable = (
            {"output_columns_hash", "source_inputs_hash"}
            if entry["table"] == TABLES[0]
            else {"derived_row_hash", "derived_row_hash_version"}
        )
        preserved = [
            field.name
            for field in before.schema().fields
            if not _retired(field.name) and field.name not in mutable
        ]
        is_rules = entry["table"] == "indicator_roll_rules"
        expected = _fingerprint(before, preserved, rules=is_rules)
        batches = iter(_projected_batches(before, entry))
        first = next(batches)

        def stream():
            yield first
            yield from batches

        reader = pa.RecordBatchReader.from_batches(first.schema, stream())
        current = DeltaTable(str(path))
        if current.version() != entry["version"] or current.metadata().id != entry["table_id"]:
            raise ValueError(f"concurrent writer changed {path}")
        write_deltalake(
            current,
            reader,
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=entry["partition_by"],
            commit_properties=CommitProperties(
                custom_metadata={
                    "retirement": str(plan["kind"]),
                    "rollback_version": str(entry["version"]),
                }
            ),
        )
        after = DeltaTable(str(path))
        committed = {**entry, "after_version": int(entry["version"]) + 1, "verified": False}
        receipt["tables"].append(committed)
        if receipt_path is not None:
            receipt_path.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
        if (
            after.version() != committed["after_version"]
            or after.metadata().id != entry["table_id"]
        ):
            raise RuntimeError("concurrent writer changed table after retirement commit")
        before_schema = before.to_pyarrow_dataset().schema
        after_schema = after.to_pyarrow_dataset().schema
        if any(before_schema.field(name) != after_schema.field(name) for name in preserved):
            raise RuntimeError(f"native schema changed: {path}")
        actual = _fingerprint(after, preserved)
        if actual != expected:
            # Never automatically restore over an intervening writer commit.
            raise RuntimeError(
                f"native readback mismatch: {path}; restore version {entry['version']}"
            )
        if any(_retired(field.name) for field in after.schema().fields):
            raise RuntimeError(f"retired fields survived: {path}")
        committed.update(
            verified=True, preserved_rows=actual[0], preserved_fingerprint=list(actual[1:])
        )
        if receipt_path is not None:
            receipt_path.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    return receipt


def rollback_retirement(receipt: dict[str, object]) -> None:
    """Restore data AND schema: delta-rs 0.25 restore does not restore the schema."""
    root = Path(str(receipt["root"]))
    for entry in receipt["tables"]:
        current = DeltaTable(str(root / f"{entry['table']}.delta"))
        if (
            current.version() != entry["after_version"]
            or current.metadata().id != entry["table_id"]
        ):
            raise ValueError("Delta version drift; rollback would overwrite newer data")
    for entry in reversed(receipt["tables"]):
        path = root / f"{entry['table']}.delta"
        old = DeltaTable(str(path), version=int(entry["version"]))
        current = DeltaTable(str(path))
        if (
            current.version() != entry["after_version"]
            or current.metadata().id != entry["table_id"]
        ):
            raise ValueError("concurrent writer changed table during rollback")
        reader = old.to_pyarrow_dataset().scanner(batch_size=BATCH_ROWS).to_reader()
        write_deltalake(
            current,
            reader,
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=entry["partition_by"],
            commit_properties=CommitProperties(
                custom_metadata={"retirement_rollback_version": str(entry["version"])}
            ),
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply-plan", type=Path)
    parser.add_argument("--writer-root", type=Path)
    parser.add_argument("--rollback-receipt", type=Path)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.rollback_receipt:
        if args.apply_plan or args.writer_root is None:
            parser.error("rollback requires --writer-root and cannot use --apply-plan")
        _check_writer(args.writer_root)
        receipt = json.loads(args.rollback_receipt.read_text(encoding="utf-8"))
        if Path(receipt["root"]).resolve() != args.root.resolve():
            parser.error("receipt root differs from --root")
        rollback_retirement(receipt)
        result = {"rolled_back": receipt}
    elif args.apply_plan:
        if args.writer_root is None:
            parser.error("--writer-root is required for apply")
        plan = json.loads(args.apply_plan.read_text(encoding="utf-8"))
        if Path(plan["root"]).resolve() != args.root.resolve():
            parser.error("plan root differs from --root")
        result = apply_retirement(plan, writer_root=args.writer_root, receipt_path=args.output)
    else:
        result = plan_retirement(args.root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
