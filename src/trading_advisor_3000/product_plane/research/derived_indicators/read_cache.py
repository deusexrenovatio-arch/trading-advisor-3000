"""Disposable, bounded native spill of one Delta snapshot; never durable truth."""

from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

import pyarrow as pa
import pyarrow.dataset as ds

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    DeltaArrowReader,
    _filters_to_dataset_expression,
    _normalize_filters_for_schema,
)


def _filter_groups(items):
    if not items:
        return [[]]
    return items if isinstance(items[0], list) else [items]


class PartitionReadCache:
    def __init__(self, reader: DeltaArrowReader, *, columns, filters, enabled: bool):
        self.reader = reader
        self.columns = [c for c in columns if c in reader.schema.names]
        self.schema = pa.schema([reader.schema.field(c) for c in self.columns])
        self.filters = filters
        self.enabled = enabled
        self.directory: Path | None = None
        self.stats = {"wide_scans": 0, "spill_seconds": 0.0, "read_seconds": 0.0, "spill_bytes": 0}
        self._temporary = None
        self._dataset = None
        self._closed = False

    def __enter__(self):
        if self._closed:
            raise RuntimeError("partition cache is closed")
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        self._dataset = None
        if self._temporary is not None:
            self._temporary.cleanup()
            self._temporary = None
        self._closed = True

    def _build(self):
        started = perf_counter()
        self._temporary = TemporaryDirectory(prefix="ta3000-l2-ipc-")
        self.directory = Path(self._temporary.name)
        try:
            format_ = ds.IpcFileFormat()
            partitioning = ds.partitioning(
                pa.schema([self.schema.field(c) for c in ("instrument_id", "timeframe")]),
                flavor="hive",
            )
            batches = self.reader.batches(columns=self.columns, filters=self.filters)
            with batches:
                ds.write_dataset(
                    batches,
                    str(self.directory),
                    format=format_,
                    file_options=format_.make_write_options(compression="lz4"),
                    partitioning=partitioning,
                    max_open_files=64,
                    min_rows_per_group=0,
                    max_rows_per_group=32_768,
                    max_rows_per_file=131_072,
                )
            files = list(self.directory.rglob("*.arrow"))
            self.stats["spill_bytes"] = sum(p.stat().st_size for p in files)
            self._dataset = ds.dataset(
                str(self.directory) if files else [],
                schema=self.schema,
                format=format_,
                partitioning=partitioning if files else None,
            )
            self.stats["wide_scans"] += 1
        except BaseException:
            self.close()
            raise
        finally:
            self.stats["spill_seconds"] += perf_counter() - started

    def read(self, *, columns=None, filters=None) -> pa.Table:
        if self._closed:
            raise RuntimeError("partition cache is closed")
        if any(
            clause[0] not in self.columns for group in _filter_groups(filters) for clause in group
        ):
            raise ValueError("local predicate column is outside the cache projection")
        selected = self.columns if columns is None else [c for c in columns if c in self.columns]
        if not self.enabled:
            combined = [
                left + right
                for left in _filter_groups(self.filters)
                for right in _filter_groups(filters)
            ]
            return self.reader.read(columns=selected, filters=combined if any(combined) else None)
        if self._dataset is None:
            self._build()
        started = perf_counter()
        try:
            normalized = _normalize_filters_for_schema(filters, self.schema)
            return self._dataset.to_table(
                columns=selected, filter=_filters_to_dataset_expression(normalized)
            )
        finally:
            self.stats["read_seconds"] += perf_counter() - started
