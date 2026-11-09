"""Shared bounded Arrow read mechanics; owners supply already verified sources."""

from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds

from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    identity_input_fingerprint,
    snapshot_arrow_rules,
)
from trading_advisor_3000.product_plane.contracts.snapshot_reads import (
    ArrowReadResult,
    decode_projection,
    expected_columns,
    projection_constants,
    projection_result,
    require,
)

from .snapshot_read_cache import SnapshotReadCache


def read_projection(
    query: dict[str, Any],
    *,
    binding: dict[str, Any],
    source: Any,
    namespace: dict[str, Any],
    predicate: ds.Expression | None,
    cache: SnapshotReadCache | None,
) -> ArrowReadResult:
    if source is None:
        return projection_result(
            request=query,
            binding=binding,
            source=None,
            data=None,
            namespace=namespace,
            unavailable_reason="missing_accepted_output",
        )
    rules = snapshot_arrow_rules()
    columns = expected_columns(query)
    require(
        not (set(columns) & set(rules["forbidden_columns"])), "forbidden legacy projection column"
    )
    metadata_columns = set(rules["required_columns"])
    raw_columns = [c for c in columns if c not in metadata_columns]
    require(set(raw_columns) <= set(source.dataset.schema.names), "unknown projection column")
    require(predicate is not None, "bounded native predicate required")
    cache_namespace = rules["metadata_key"]
    key = identity_input_fingerprint(
        {
            "contract": cache_namespace,
            "request": query,
            "identity": binding,
            "source": source.metadata,
        }
    )
    if cache is not None:
        wire = cache.get(cache_namespace, key)
        if wire is not None:
            cached = decode_projection(wire, namespace)
            require(
                cached.envelope["request"] == query
                and cached.envelope["identity"] == binding
                and cached.envelope["source"] == source.metadata,
                "cache provenance mismatch",
            )
            return cached
    raw = source.dataset.scanner(columns=raw_columns, filter=predicate).head(query["row_limit"] + 1)
    require(raw.num_rows <= query["row_limit"], "row budget exceeded; narrow the requested period")
    data = raw
    constants = projection_constants(query, binding)
    for column in rules["required_columns"]:
        data = data.append_column(column, pa.repeat(pa.scalar(constants[column]), raw.num_rows))
    data = data.select(columns)
    require(data.num_rows == raw.num_rows, "metadata projection changed row count")
    result = projection_result(
        request=query, binding=binding, source=source.metadata, data=data, namespace=namespace
    )
    if cache is not None:
        cache.put(cache_namespace, key, result.to_ipc(namespace))
    return result
