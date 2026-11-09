# Bounded native snapshot reads

Change surface: product-plane. Solution class: staged. This opt-in inspection
slice reads existing accepted verification outputs; it changes no data,
publication, writer or default production binding. CF, PIT and rolling aliases
fail closed. The carrier is `snapshot_read_projection.v1`, not a full canonical
market key: session/publication/availability Q remains explicitly unverified.
Authority: [market identity decision](docs/architecture/product-plane/market-identity.md).

## Public use

Construct `data_plane.snapshot_reads.PinnedNativeSnapshot.from_file(path,
expected_sha256=...)` from an explicitly reviewed read-pin bundle. Use
`contracts.snapshot_reads.NativeReadRequest.from_payload(payload,
snapshot.namespace)`, then `data_plane.snapshot_reads.read_native_bars` or
`research.snapshot_reads.read_native_frame`. Research requires the exact frozen
dataset/contour/set/profile version tuple; irrelevant tuple fields are null.

For the existing HTTP consumer, call
`interfaces.data_inspector.create_app(snapshot_reader=snapshot,
snapshot_cache=SnapshotReadCache(max_bytes=...))`. Nothing configures this route
automatically. POST `/api/data-inspector/snapshot-read` with the closed request:

```text
schema_version: 1
identity: exact snapshot_identity_selector.v1 (including all admission pins)
layer: bars | L0 | L1 | L2
series_type: native
timeframe: exact source timeframe
start/end: UTC timestamps, half-open interval
projection: ordered unique source columns (no legacy identity/grouping columns)
row_limit: 1..500
byte_limit: positive full wire-byte budget
research: null for bars; explicit frozen version tuple for L0/L1/L2
```

HTTP 200 returns Arrow stream, including a typed empty slice; decode with
`contracts.snapshot_reads.decode_projection(raw, namespace)`. Missing accepted
output returns an unavailable envelope/404; an unconfigured route returns 503;
invalid contract/projection returns 422; unavailable physical pins return 409.
The result includes mandatory asset_id/instrument_id/series_type/timeframe and
the unchanged source timestamp, then requested columns. No partial truncation,
deduplication, resampling or indicator computation occurs. Overflow rejects.

## Pin and query boundary

| Step | Required evidence/behavior |
| --- | --- |
| Metadata admission | Original manifest, descriptors, raw metadata, assignments, report/config and invocation hashes; explicit pilot namespace |
| Read-pin bundle | Original admission inputs; public `capture_table` references for each existing output; original Arrow schema hashes; explicit Research selections |
| Table open | Original report/receipt path correspondence; exact UUID, Delta version, active files and Arrow schema; no latest fallback |
| Native predicates | Exact legacy descriptor, native contract/null, native discriminator, null CF scope, context, TF and half-open time; bars security_id; Research version tuple/price basis |
| Arrow projection | Bounded scanner, deterministic source timestamp/grain order; source fields/types/metadata and NULL/NaN preserved; no JSON number conversion |
| Consumer/cache | Shared schema/constants/grain/hash validation; every source pin checked before a memory-cache hit; decoded hit validated again |

The extracted public Delta/Arrow dataset guard preserves the existing reader's
handling of missing Delta min/max statistics; absent statistics must not turn
non-null source values into false empty slices. No Delta state is modified.

## Existing-output evidence

Local evidence root:
`D:/CodexHome/visualizations/2026/10/31/01a19403-e9da-7011-bc5b-51fbe3ea3be9`.
`native-read-pins.json` SHA256:
`33c24bdb3d3a238039a600eb0694e5e28a7b05a1757973aa8944477ff2917e56`.
This metadata-only bundle is not a published snapshot/manifest. Original input
hashes and source routes are in the [admission runbook](docs/runbooks/app/identity-snapshot-admission.md).

| Output | Table UUID | Delta version |
| --- | --- | --- |
| analytical_bars | 942752c1-9c76-4a46-b8f8-fe06132bbaf8 | 0 |
| research_bar_views | 717725f1-058f-4eba-8717-9732f5bc3f59 | 3 |
| research_indicator_frames | 7faa3c7f-26b7-4224-b829-3fd0fc509aef | 3 |
| research_derived_indicator_frames | fc2d798f-3df0-4eff-8be9-5cf4e0c313af | 3 |

`native-read-roundtrip.py` / `.json` record actual existing producer outputs ->
public readers -> DataInspector -> Arrow/schema readback for SBER, IMOEX, SRZ6:
12 slices, 48 rows. An independent direct pinned Parquet/Arrow oracle compares
every projected value/type/field metadata/NULL/NaN mask and grain. Repeat/cache
wire bytes and the old public reader agree; IMOEX execution fields stay NULL.
CF/PIT/legacy selectors, unknown projection and corrupt physical pins reject;
a valid absent-period slice stays typed empty. Input/Delta-log hashes remain
unchanged. The proof guards writer/publication functions and writes evidence
only, outside source/output roots. Fixtures are synthetic regression evidence.

Self-review: the new contract admits bounded native snapshot projections only.
Latest/unpinned cache access, underlying-future substitution, duplicate grain,
silent truncation and invented historical applicability are forbidden and have
negative regressions. Residual limits: incomplete Q/temporal facts, local frozen
verification roots, no production admission/performance claim. The original
read-only proof excluded the full PR write matrix. The November 9 integration
tracks required isolated fixture/CI checks separately; real data/publication
writes and production deployment remain outside scope.

Rollback: omit the opt-in reader from `create_app` or use the previous consumer;
discard the memory cache. No data recovery, manifest update or publication.
