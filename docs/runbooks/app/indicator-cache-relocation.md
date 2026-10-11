# Indicator cache relocation

Change surface: product-plane, with required delivery intent metadata.

Status: staged code reconciliation. Accepted numerical R5 proof is retained for
the unchanged numerical symbols; copied primary-candidate migration and reuse
are not yet accepted. No table migration or publication follows from unit proof.

## Owner and retained behavior

The accepted L1 cache/hybrid owner was a frozen executed overlay, not merged
main. Its receipt is `runtime-research-r5-receipt.json`, SHA256
`663661c6e73ec7f43f9a0446c017fccab11a475a780ebd86135eb6e776bd9240`.
This change transfers selected owning functions, not a whole runtime file tree.

The consuming route remains the existing `research_indicator_frames` Dagster
asset → `materialize_indicator_frames`. It forwards existing changed windows.
No scheduler, producer, strategy caller or downstream numerical algorithm changes.

The L1 store reserves and roundtrips `compute_contract_hash`,
`refresh_context_hash`, `input_row_hash` and `volume_profile_input_hash`.
Metadata reads select a complete latest physical carrier, preserving NULL guard values;
legacy NULL/empty series IDs keep contract identity. Missing compute/context guards
fail closed. Full reuse and profile extension require current v2 bindings.
Partition batches retain identity validation. Atomic suffix writes enforce the configured
row bound before row-dictionary/Arrow allocation; oversized suffixes fail before mutation
instead of splitting one predicate replacement across commits. Changed windows reuse compatible
unchanged series before source loading; changed series retain native full-history
calculation and atomically replace the affected suffix.

The exact accepted numerical functions remain: `_compute_spec`,
`_fill_bar_usage_noncomputed_rows`, `_compute_continuous_front_profile_frame`,
`_finite_indicator_history`, `_compute_cached_profile_frame`. Recursive and
continuous-front calculation keep their accepted full-history boundaries.
The current bounded VP source reader and current missing-statistics Delta reader
semantics remain intact. Current main MTF source/publication logic is retained.

## Stable refresh identity

The refresh payload has `refresh_context_version=2`. Only the VP path token
changes to a native Delta identity: contract `delta_source_identity.v1`, role
`volume_profile_1m`, actual table ID, canonical full-schema SHA256 and the hash
of the consumed projection/key/timeframe/half-open close-boundary read contract.
Identity metadata is read through the same pinned reader as the source rows.

Delta version, files and absolute location are excluded from this stable identity.
A byte-identical relocation and an append retain identity; a replacement table,
schema or read-contract change invalidates it. Content revisions still require
the existing changed-window/input-row hash mechanism.

Numerical `_indicator_compute_contract_hash.cache_version=1` remains unchanged.
Compute contract, normalized ladder and instrument tick size remain refresh
dependencies. Explicit in-memory sources retain their existing presence token;
they do not acquire a fabricated Delta identity.

The accepted producer was canonical 1m despite the legacy config name
`volume_profile_raw_1m_table_path`. The copied candidate must keep that explicit
producer. A raw-history table is a different producer and cannot use a canonical
relocation alias. Do not forge table ID/schema to obtain reuse.

## Acceptance order

1. Complete selected-table byte copy and native pinned readback. Freeze writers.
2. Bind exact source/target table IDs, schemas, versions and copied file closure.
   Retain the old immutable source pins.
3. For the accepted 52 keys, verify old v1 contexts against the receipt-bound
   configuration, ladder, tick sizes and dependency versions.
4. Produce the explicit old→new context mapping. Reject mixed/null/unknown
   contexts, unrelated series, source replacement or post-copy drift.
5. In one guarded native Delta commit, update only `refresh_context_hash`.
   Prove exact per-key counts and equality of every other column before/after.
6. Execute the existing owning route on that actual candidate with unchanged
   input scope: 52 reused series, zero numerical calculation, zero write batches.
7. Perform the separately authorized fresh-data update, manifest acceptance and
   cutover. The metadata migration is not a multi-table publication.

A layout rewrite requires a separate native old→new logical equality receipt
and the actual new UUID/schema. This patch adds no general layout migration.
Existing RAW candidate guards and writer ownership remain in force.

## Focused proof and limits

Nine focused cases (six initial RED, one context follow-up, two metadata follow-ups) cover the missing owning contract, numerical version one,
native relocation/append/replacement/schema identity, reserved metadata roundtrip,
single-commit tail replacement, no-load unchanged reuse, and native-full changed
window suffix dispatch. The initial main characterization was six RED cases. The final composed focused
suite passed all nine cases on Windows/Python 3.12; actual accepted-runtime and
52-key candidate admission remain separate.

These are bounded local fixtures, not real 52-key acceptance or a fresh-data run.
Real copied-candidate metadata/equality/reuse proof remains mandatory once, under
the frozen writer window. The accepted source numerical runtime/dependencies must
match; host unit success cannot replace that numerical contract check.

Completeness of historical changed-window propagation, full rebuild memory
closure and multi-table atomic publication remain outside this patch.
Do not run a legacy v1 writer against a migrated v2 root without a reviewed
reverse transition or recomputation.
