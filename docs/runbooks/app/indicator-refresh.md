# Native indicator refresh

Change surface: product-plane. Solution intent: staged.

Daily L1 writes use the scoped adapter in `research/indicators/batch_writer.py`.
The existing store owns the DTO, schema and authoritative close-year conversion.
Complete logical scopes are packed into native predicate overwrites: at most
32 scopes or 64 MiB of Arrow payload per pack. An oversized complete scope is
written alone. This is a payload budget, not a maximum process-memory bound.
Healthy no-ops produce no data commit and no optimize call. Finalization compacts
changed close-years plus existing fragmentation once, preserving Delta identity.
The raw history fresh-candidate protocol is unchanged.

L2 reads one pinned source snapshot, spills its wide projection once to temporary
Arrow IPC when multiple partitions are requested, then reads exact logical scopes.
The same lazy cache serves old L2 values for complete native prefix comparison.
Temporary files are disposable and cleaned on success or failure. No persistent
prefix proof replaces comparison. The source is prepared and hashed once per
partition; numerical formulas still receive their full required history.

The versioned `columnar-v1:` SHA-256 format replaces repeated JSON fingerprints
in the active refresh path. Standalone DTO/JSON helpers and the native input hash
retain their existing contracts. L1 dependency fingerprints consume only requested
native values, avoiding invalidation from unrelated provenance or VP changes.

L2 serializes and replaces only the suffix after exact comparison of timestamps,
contract, output values, physical close-year, profile/output identities and warmup
metadata. The final row always carries current complete source/compute metadata.
A compute-contract change requires full numerical validation before retaining
any prefix. Snapshot-only no-ops bypass wide reads entirely. Conflicting newest
metadata, bounded projection repairs and changing source snapshots fail closed.

Each pack is atomic. A failed later pack may leave earlier successful packs;
the failed run must not publish an accepted snapshot. Daily retry retains the
same admission, parents, native references and exact windows. Removing windows
is forbidden. A separately selected rebuild revalidates its declared domain. Deleted scopes
are explicit; an omitted update decision is an error.

Validation uses isolated temporary Delta tables. This integration performs no
production data migration, rebuild, service restart or publication.

## Reviewed production hash transition

The accepted post-226 production source `8a4432db` has a finite transition to
the exact reviewed package. Accepted manifests, QC, input/output pins, Data
ancestry, dependencies and configuration must still match. L1 retains its
content-proven metadata adaptation. L2 reconstructs the exact old six-file
compute recipe and old DTO JSON input hashes; native input, profile, projection,
count and unique timestamp/contract keys must match. The existing final carrier
then receives current columnar and actual ten-helper compute identities.
Historical prefix metadata and all numerical/NULL values are preserved.

This audited transition permits metadata-only reuse across the reviewed hash
encoding and implementation packaging changes. `hash_migrated_partition_count`
reports adopted L2 partitions separately from numerical refreshes. Unknown
recipes, changed inputs, damaged output and explicit full refresh never receive
this admission. No arbitrary source alias or permanently frozen compute hash is
allowed. Migration runs only inside the accepted-snapshot scope on the ordinary
scheduled route; no manual full run or pre-nightly production repin is required.

## Daily admission boundary

Architecture and rationale: [Research refresh admission](docs/architecture/product-plane/research-refresh-admission.md).
Explicit ancestry recovery: [Data scope recovery](docs/architecture/product-plane/data-scope-recovery.md).

Daily Data → CF → L0 → L1 → L2 → sidecar/QC → publication carries one
`research_refresh_admission.v1` receipt in existing Dagster config/checkpoints
and Research manifest parameters. The authoritative schema is
`src/trading_advisor_3000/product_plane/contracts/schemas/research_refresh_admission.v1.json`.
It binds immutable Data/Research parents, native input identities/versions,
stable dataset domain, original/effective windows, reason and content digest.

| Decision | Meaning |
| --- | --- |
| NOOP | Proven unchanged inputs; no compute or publish |
| SCOPED | Only admitted instrument/timeframe pairs may compute; a touched series may need full history |
| BLOCKED | Missing, legacy, incompatible or drifting proof; retain accepted Research and pending work |

All former resolver full-refresh reasons block daily execution. `None`, empty
windows and legacy configs never authorize a global calculation. Sidecar filters
exact pairs before windows/UDFs and deletes only their declared output scopes,
including deleted/empty source pairs. Cache misses remain bounded.

Daily manual entry uses the canonical Dagster jobs with a validated receipt.
Direct native Python/CF CLI calls are rebuild-only: select `operation="rebuild"`
or `--operation rebuild`, with explicit roots/selectors. Dedicated rebuild jobs
select that operation themselves. Base-only sidecar cannot serve daily refresh;
the legacy input-frame writer is retired. Read-only strategy dataset projections
retain their existing owner and cannot publish Research.

These controls provide scope correctness, not a measured speedup or a memory
bound for recursive indicators on a single affected full-history series.

## Production rollback after staged tail-compute promotion (2026-10-28)

PR #232 changed KST semantics, kernel/proof identity and checkpoint schemas before
its separately planned production adoption. Existing accepted Research therefore
correctly rejects that code at daily admission. Restore the pre-232 numerical
implementation, retaining the later closed-session calendar fix, before resuming
the original accepted Data snapshot through daily admission.

This is code rollback, with immutable snapshots and pending work retained. Do not
alias the new kernels to accepted legacy P0 results or use an implicit rebuild.
Future tail-compute adoption still requires an isolated full reference, explicit
refresh plan and its own recovery plan before runtime promotion.
