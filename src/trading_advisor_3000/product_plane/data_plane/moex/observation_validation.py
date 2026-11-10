"""Independent, bounded original-byte oracle; not an analytical loading route."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, localcontext
from pathlib import Path

import pyarrow as pa

from trading_advisor_3000.product_plane.common.data_snapshots import open_pinned_table

from ..native_observations import physical_schema
from .observation_acquisition import canonical_bytes, pinned_bytes


def _fits_decimal(value):
    if not isinstance(value, Decimal) or not value.is_finite():
        return False
    _, digits, exponent = value.as_tuple()
    while digits and digits[-1] == 0:
        digits, exponent = digits[:-1], exponent + 1
    return not (digits and exponent < -10) and value.copy_abs() < Decimal(10) ** 28


def _source_valid(row, columns, window):
    if not isinstance(row, list) or len(row) != len(columns):
        return False
    values = dict(zip(columns, row, strict=True))
    for name in ["open", "high", "low", "close", "volume", "open_interest"]:
        value = values.get(name)
        if value is None:
            if name in ["open", "high", "low", "close"]:
                return False
            continue
        if not _fits_decimal(value):
            return False
        if name in ["volume", "open_interest"] and value < 0:
            return False
    try:
        begin, end = values["begin"], values["end"]
        if any(
            not isinstance(key, str)
            or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", key)
            for key in [begin, end]
        ):
            return False
        datetime.strptime(begin, "%Y-%m-%d %H:%M:%S")
        datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        if begin > end or not window["date_from"] <= begin[:10] <= window["date_till"]:
            return False
        if (
            not values["low"]
            <= min(values["open"], values["close"])
            <= max(values["open"], values["close"])
            <= values["high"]
        ):
            return False
    except (KeyError, TypeError, ValueError):
        return False
    return True


def compare_source_to_physical(
    root: Path,
    *,
    source: dict,
    assignments: dict,
    metadata: dict,
    tables: dict,
    overlay: dict | None = None,
    overlay_sha256: str | None = None,
) -> dict:
    """Original lexemes/Decimal oracle is independent of the Spark normalizer."""
    layouts = metadata["storage_layout"]
    actual = {}
    bound = source["limits"]["rows"]
    for name, reference in tables.items():
        dataset = open_pinned_table(reference).to_pyarrow_dataset()
        frame = dataset.scanner(
            columns=[f["name"] for f in layouts[name]["fields"]], batch_size=4096
        ).head(bound + 1)
        if frame.num_rows > bound or not frame.schema.equals(
            physical_schema(layouts[name]), check_metadata=True
        ):
            raise ValueError("oracle physical bound/schema mismatch")
        actual[name] = frame.combine_chunks()
    bindings = {b["source_symbol"]: b for b in assignments["bindings"]}
    native, expected_raw, rejected = [], [], set()
    overlay_hits = 0
    for page in source["pages"]:
        payload = json.loads(
            pinned_bytes(root, page["body"]),
            parse_float=Decimal,
            parse_int=Decimal,
            parse_constant=str,
        )
        block, binding = payload["candles"], bindings[page["source_symbol"]]
        columns = block["columns"]
        for ordinal, source_row in enumerate(block["data"]):
            values = dict(zip(columns, source_row)) if isinstance(source_row, list) else {}
            original_json = json.dumps(
                source_row,
                default=str,
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
            key = (page["receipt"]["sha256"], "candles", ordinal)
            expected_raw.append(
                {
                    "namespace": assignments["namespace"]["namespace"],
                    "asset_id": binding["asset_id"],
                    "instrument_id": binding["instrument_id"],
                    **binding["context"],
                    "engine": binding["engine"],
                    "source_symbol": page["source_symbol"],
                    "timeframe": page["timeframe"],
                    "source_begin": values.get("begin")
                    if isinstance(values.get("begin"), str)
                    else None,
                    "source_end": values.get("end") if isinstance(values.get("end"), str) else None,
                    "source_values_json": original_json,
                    "body_sha256": page["body"]["sha256"],
                    "receipt_sha256": page["receipt"]["sha256"],
                    "source_block": "candles",
                    "page_start": page["page_start"],
                    "row_ordinal": ordinal,
                    **{
                        name + "_lexeme": str(values[name])
                        if _fits_decimal(values.get(name))
                        else None
                        for name in ["open", "high", "low", "close", "volume", "open_interest"]
                    },
                }
            )
            if not _source_valid(source_row, columns, source["window"]):
                rejected.add(key)
                continue
            row = {
                "namespace": assignments["namespace"]["namespace"],
                "asset_id": binding["asset_id"],
                "instrument_id": binding["instrument_id"],
                "provider": binding["context"]["provider"],
                "venue": binding["context"]["venue"],
                "engine": binding["engine"],
                "market": binding["context"]["market"],
                "board": binding["context"]["board"],
                "source_symbol": page["source_symbol"],
                "timeframe": page["timeframe"],
                "source_begin": values["begin"],
                "source_end": values["end"],
                **{n: values[n] for n in ["open", "high", "low", "close"]},
                "source_units_json": canonical_bytes(binding["units"]).decode(),
                "normalization_recipe": metadata["normalization_recipe"],
                "value_origin": "exchange_observation",
                "simulation_id": None,
                "overlay_sha256": None,
                "body_sha256": page["body"]["sha256"],
                "receipt_sha256": page["receipt"]["sha256"],
                "source_block": "candles",
                "page_start": page["page_start"],
                "row_ordinal": ordinal,
            }
            for quantity in ["volume", "open_interest"]:
                not_applicable = binding["kind"] == "reference_index" or (
                    quantity == "open_interest" and binding["kind"] == "equity"
                )
                value = None if not_applicable else values.get(quantity)
                state = (
                    "not_applicable"
                    if not_applicable
                    else "unavailable"
                    if value is None
                    else "known"
                )
                row[quantity] = value
                row[quantity + "_state"] = state
                row[quantity + "_null_reason"] = (
                    None
                    if state == "known"
                    else "instrument_kind"
                    if not_applicable
                    else "not_reported_by_source"
                )
            if overlay:
                for change in overlay["changes"]:
                    if all(row[field] == value for field, value in change["key"].items()):
                        if change["field"] != "volume" or row["volume_state"] != "known":
                            raise ValueError("oracle rejects unsupported simulation")
                        row["volume"] = Decimal(change["new_value"])
                        row.update(
                            value_origin="simulated_correction",
                            simulation_id=overlay["simulation_id"],
                            overlay_sha256=overlay_sha256,
                        )
                        overlay_hits += 1
            native.append(row)
    if overlay and overlay_hits != len(overlay["changes"]):
        raise ValueError("simulation key is absent or ambiguous")
    expected_table = pa.Table.from_pylist(native, schema=physical_schema(layouts["native_bars"]))
    grain = layouts["native_bars"]["grain"]
    if (
        not actual["native_bars"]
        .sort_by([(name, "ascending") for name in grain])
        .equals(
            expected_table.sort_by([(name, "ascending") for name in grain]), check_metadata=True
        )
    ):
        raise ValueError("independent original-byte/native Decimal comparison failed")
    raw = actual["raw_observations"]
    raw_fields = [
        f["name"] for f in layouts["raw_observations"]["fields"] if f["name"] != "rejection_reason"
    ]
    raw_schema = physical_schema(layouts["raw_observations"])
    expected_raw_table = pa.Table.from_pylist(
        expected_raw, schema=pa.schema([raw_schema.field(name) for name in raw_fields])
    )
    raw_sort = [(name, "ascending") for name in layouts["raw_observations"]["grain"]]
    if raw.num_rows != len(expected_raw) or not raw.select(raw_fields).sort_by(raw_sort).equals(
        expected_raw_table.sort_by(raw_sort), check_metadata=True
    ):
        raise ValueError("original RAW row/lexeme accounting mismatch")
    rejected_raw = [row for row in raw.to_pylist() if row["rejection_reason"] is not None]
    q_keys = {tuple(row[k] for k in layouts["raw_observations"]["grain"]) for row in rejected_raw}
    expected_quarantine = pa.Table.from_pylist(
        rejected_raw, schema=physical_schema(layouts["quarantine"])
    )
    if (
        q_keys != rejected
        or any(not row["rejection_reason"] for row in rejected_raw)
        or not actual["quarantine"]
        .sort_by(raw_sort)
        .equals(expected_quarantine.sort_by(raw_sort), check_metadata=True)
        or len(native) + len(rejected) != source["source_rows"]
    ):
        raise ValueError("quarantine/accepted/source accounting mismatch")
    groups = defaultdict(list)
    for row in native:
        day_row = {**row, "source_day": row["source_begin"][:10]}
        groups[tuple(day_row[k] for k in layouts["observed_day_qc"]["grain"])].append(day_row)
    expected_qc = []
    with localcontext() as context:
        context.prec = 50
        for rows in groups.values():
            first = rows[0]
            quantities = [row["volume"] for row in rows if row["volume"] is not None]
            qc = {f["name"]: first.get(f["name"]) for f in layouts["observed_day_qc"]["fields"]}
            qc.update(
                bar_count=len(rows),
                first_source_begin=min(row["source_begin"] for row in rows),
                last_source_begin=max(row["source_begin"] for row in rows),
                volume_sum=sum(quantities, Decimal(0)) if quantities else None,
                simulated_rows=sum(row["value_origin"] == "simulated_correction" for row in rows),
            )
            expected_qc.append(qc)
    qc_table = pa.Table.from_pylist(expected_qc, schema=physical_schema(layouts["observed_day_qc"]))
    qc_sort = [(name, "ascending") for name in layouts["observed_day_qc"]["grain"]]
    if (
        not actual["observed_day_qc"]
        .sort_by(qc_sort)
        .equals(qc_table.sort_by(qc_sort), check_metadata=True)
    ):
        raise ValueError("independent observed-day QC comparison failed")
    return {
        "status": "PASS",
        "source_rows": len(expected_raw),
        "native_rows": len(native),
        "quarantined_rows": len(rejected),
        "observed_day_groups": len(groups),
        "simulation_hits": overlay_hits,
    }
