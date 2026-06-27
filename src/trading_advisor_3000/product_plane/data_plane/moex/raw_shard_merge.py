from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Iterator

import pyarrow as pa
from deltalake import DeltaTable

from trading_advisor_3000.product_plane.data_plane.moex.foundation import RAW_COLUMNS

RAW_SHARD_SCOPE_COLUMNS = ("internal_id", "timeframe", "moex_secid")
RAW_SHARD_ROW_KEY_COLUMNS = (
    "internal_id",
    "timeframe",
    "moex_secid",
    "ts_open",
    "ts_close",
)
RAW_SHARD_ROW_FINGERPRINT_COLUMNS = tuple(RAW_COLUMNS)


def _scope_value(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _raw_shard_scope_keys(
    source_path: Path, *, batch_size: int = 250_000
) -> set[tuple[str, str, str]]:
    scanner = (
        DeltaTable(str(source_path))
        .to_pyarrow_dataset()
        .scanner(
            columns=list(RAW_SHARD_SCOPE_COLUMNS),
            batch_size=batch_size,
        )
    )
    keys: set[tuple[str, str, str]] = set()
    for batch in scanner.to_batches():
        payload = batch.to_pydict()
        for values in zip(*(payload[column] for column in RAW_SHARD_SCOPE_COLUMNS), strict=True):
            keys.add(tuple(_scope_value(value) for value in values))
    return keys


def _format_scope_key(scope_key: tuple[str, str, str]) -> str:
    internal_id, timeframe, moex_secid = scope_key
    return f"internal_id={internal_id}|timeframe={timeframe}|moex_secid={moex_secid}"


def _json_safe_value(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _row_key(row: dict[str, object]) -> tuple[str, ...]:
    return tuple(_scope_value(row.get(column)) for column in RAW_SHARD_ROW_KEY_COLUMNS)


def _row_fingerprint(row: dict[str, object]) -> str:
    payload = {
        column: _json_safe_value(row.get(column)) for column in RAW_SHARD_ROW_FINGERPRINT_COLUMNS
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _format_row_key(row_key: tuple[str, ...]) -> str:
    return "|".join(
        f"{column}={value}" for column, value in zip(RAW_SHARD_ROW_KEY_COLUMNS, row_key)
    )


def validate_disjoint_raw_shard_scopes(
    shards: Iterable[tuple[str, Path]], *, batch_size: int = 250_000
) -> None:
    scope_owners: dict[tuple[str, str, str], str] = {}
    for shard_id, source_path in shards:
        overlapping_scopes: list[tuple[str, str, str]] = []
        for scope_key in sorted(_raw_shard_scope_keys(source_path, batch_size=batch_size)):
            owner = scope_owners.get(scope_key)
            if owner is not None:
                overlapping_scopes.append(scope_key)
                continue
            scope_owners[scope_key] = shard_id
        if not overlapping_scopes:
            continue
        samples = "; ".join(
            f"{_format_scope_key(scope_key)} first_shard={scope_owners[scope_key]} "
            f"duplicate_shard={shard_id}"
            for scope_key in overlapping_scopes[:5]
        )
        raise RuntimeError(
            "MOEX raw shard merge received overlapping shard scopes; "
            "a single raw history scope must be owned by exactly one shard before append. "
            f"Samples: {samples}"
        )


def iter_deduplicated_raw_shard_tables(
    source_path: Path,
    *,
    shard_id: str,
    batch_size: int = 250_000,
) -> Iterator[pa.Table]:
    scanner = DeltaTable(str(source_path)).to_pyarrow_dataset().scanner(batch_size=batch_size)
    seen_fingerprints: dict[tuple[str, ...], str] = {}
    conflict_samples: list[str] = []
    for batch in scanner.to_batches():
        source_table = pa.Table.from_batches([batch], schema=batch.schema)
        deduplicated_rows: list[dict[str, object]] = []
        for row in source_table.to_pylist():
            key = _row_key(row)
            fingerprint = _row_fingerprint(row)
            existing_fingerprint = seen_fingerprints.get(key)
            if existing_fingerprint is None:
                seen_fingerprints[key] = fingerprint
                deduplicated_rows.append(row)
                continue
            if existing_fingerprint == fingerprint:
                continue
            if len(conflict_samples) < 5:
                conflict_samples.append(_format_row_key(key))
        if conflict_samples:
            raise RuntimeError(
                "MOEX raw shard merge received conflicting rows for the same raw bar key "
                f"in shard {shard_id}; samples: {'; '.join(conflict_samples)}"
            )
        if deduplicated_rows:
            yield pa.Table.from_pylist(deduplicated_rows, schema=source_table.schema)


def validate_raw_shard_rows_deduplicable(
    shards: Iterable[tuple[str, Path]], *, batch_size: int = 250_000
) -> None:
    for shard_id, source_path in shards:
        for _table in iter_deduplicated_raw_shard_tables(
            source_path,
            shard_id=shard_id,
            batch_size=batch_size,
        ):
            continue
