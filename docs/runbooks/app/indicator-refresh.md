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
partition. L2 verifies per-row semantic input hashes before formulas, then
computes the changed suffix with eligible lookback and complete touched
session/week scopes. Confirmed swing state is continued from the verified
predecessor. Legacy/missing input proof or changed numerical identity uses full
validation; suffix write comparison remains independent.

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

## Checkpointed continuous-front calculation

Change surface: product-plane. Architecture impact: contract-change (additive
optional metadata, existing schema authorities and writers).

`indicators/store.py` owns optional `compute_state_json`. CF stores sparse
`cf-tail.v2` anchors every 256 raw bars and at the final row, retaining 4096 bars
plus the floor anchor at/before that horizon (at most 18 carriers). Each binds
its own count/timestamp/numerical-prefix digest, profile/native/adapter identity
and semantic CF ladder. Historical eligible buffers, clean_end, Supertrend bands
and direction are captured at that anchor, not copied from latest state.
`native-checkpoint.v3` retains the strict nonfinite JSON codec. Old formats are
not certified for replay. No checkpoint table, service or separate writer exists.

Correction/insertion/deletion restores the newest valid anchor before the first
changed numerical input, then replays the suffix. Only actual dependencies enter
the CF numerical fingerprint: declared profile inputs, native OHLC, volume/OI,
roll epoch, identity and eligibility/session context, plus VP timing when used.
Execution economics/provenance metadata does not trigger formulas; current
snapshot/lineage metadata still advances through the complete final carrier.
Close time is transient on the indicator DTO. For numerically unchanged source
snapshots with new provenance, native projected `ts`/close-year proof also
detects physical-year moves; their preserved suffix is routed through the
existing writer without formulas. No new persisted close-time column is added.
Cleanup widens both the native replacement predicate and preserved row payload
to clear expired/invalid carriers even before the numerical correction window.

The native library has no state-input API. Private function copies receive
checkpointed native EMA/RMA helpers; native module globals are unchanged.
MACD/ADOSC local numpy recurrences and Supertrend bands need explicit state
adapters. A reviewed exact pandas-ta-classic source/backend pin is mandatory;
unknown implementations use the original full producer. The two adapter files
are pinned by complete source bytes in the numerical kernel; their comments
therefore conservatively invalidate checkpoints too. Other numerical functions
retain normal AST identity and unknown callbacks still fail closed.

Unchanged append on the supported mature core computes at most 512 eligible
context rows plus new rows per native operation. A dependency group's complete
causal prefix of at most 512 eligible rows may bootstrap before it is mature;
a clipped 512-row suffix of a longer recurrence cannot certify this. Its old outputs must
still match, otherwise full validation/replacement is required. Counters report
this bootstrap separately. Reads, hashing and prefix proof may still scan the
full partition; bounded formula work is not an end-to-end speedup claim.

Corrections before the retained horizon, changed semantic ladder/unseen roll,
unsupported profile/backend, malformed state or nonmonotonic epochs fail back
explicitly. The complete native zero-range regime is bound independently of the
prefix: adding the first or removing the last zero invalidates the numerical
prefix and requires full replacement. Historical anchors cannot hide that
global library behavior. Known roll groups and session/eligibility gaps retain
their separate native state. L2's
optional `input_row_hash` is owned by `derived_indicators/store.py`; metadata-only
L1 provenance changes do not invalidate identical numerical input rows.

Verification requires exact keys, NULLs and integer/event codes, numeric
`rtol=1e-8, atol=1e-8` (native rolling floating-point order), serialized restart,
eligibility gaps, zero addition/removal, deletion of anchors/final bars, retention
cleanup/retry and native Delta producer/consumer readback. Correction parity and
bounded formula counters must pass together; full fallback cannot satisfy that
acceptance test. Capture count/bytes, peak memory and widened write rows as costs.
All acceptance writes stay in an isolated copy. Legacy state can bootstrap only
through the ordinary requested route; no manual production full rebuild is part
of this change.

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
