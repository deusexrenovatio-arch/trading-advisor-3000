"""Public Research owner reads of existing native L0/L1/L2 snapshot products."""

from trading_advisor_3000.product_plane.common.snapshot_projection import read_projection
from trading_advisor_3000.product_plane.common.snapshot_read_cache import SnapshotReadCache
from trading_advisor_3000.product_plane.contracts.snapshot_reads import (
    ArrowReadResult,
    NativeReadRequest,
    require,
)
from trading_advisor_3000.product_plane.data_plane.snapshot_reads import (
    PinnedNativeSnapshot,
    native_predicate,
)

TABLES = {
    "L0": "research_bar_views",
    "L1": "research_indicator_frames",
    "L2": "research_derived_indicator_frames",
}


def read_native_frame(
    snapshot: PinnedNativeSnapshot,
    request: NativeReadRequest,
    *,
    cache: SnapshotReadCache | None = None,
) -> ArrowReadResult:
    query, binding = snapshot.resolve(request)
    require(query["layer"] in TABLES, "Research reader accepts L0/L1/L2 only")
    source = snapshot.source(query, binding, table_name=TABLES[query["layer"]])
    return read_projection(
        query,
        binding=binding,
        source=source,
        namespace=snapshot.namespace,
        predicate=None
        if source is None
        else native_predicate(source, query, timestamp="ts", bars=False),
        cache=cache,
    )
