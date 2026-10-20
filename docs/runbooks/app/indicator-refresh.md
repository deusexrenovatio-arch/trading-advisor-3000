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
the failed run must not publish an accepted snapshot. Retry without refresh
windows revalidates content and repairs pending fragmentation. Deleted scopes
are explicit; an omitted update decision is an error.

Validation uses isolated temporary Delta tables. This integration performs no
production data migration, rebuild, service restart or publication.
