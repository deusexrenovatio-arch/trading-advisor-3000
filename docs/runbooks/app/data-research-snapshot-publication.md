# Data and research snapshot publication

Status: staged owner activation. Existing native calculations retained.
Change surface: mixed: product-plane owners and required delivery metadata.

## Existing route

```mermaid
flowchart LR
    raw[Raw refresh] --> econ[Economics: pinned inputs]
    econ --> canonical[Canonical owning writes]
    canonical --> data[Accepted Data snapshot]
    data --> cf[CF catch-up: exact Data parent]
    cf --> research[Data prep: five owning stages]
    research --> candidate[Candidate Research snapshot]
    candidate --> compact[Quiesce and native compaction]
    compact --> final[Final Data + same-parent Research CAS]
```

`run_moex_baseline_update` publishes Data after successful existing baseline,
canonical and economics gates. Pre-calculation economics inputs remain stage
provenance; canonical outputs get their final physical pins after owning writes.
Immutable external inputs must remain unchanged. Failed publication retains
pending changed windows and cannot report an accepted baseline.

`build_research_data_prep_run_config` enables `research_data_snapshot` and forwards
`data_snapshot_manifest_path` plus `data_snapshot_raw_output_dir`. The two
existing Data sensors forward these bindings and the calculated window. An
explicit parent must match accepted current Data. Resuming a checkpoint and
terminal publication recheck its parent ID and digest. Summary success depends
on the terminal publisher.

The materialization owner declares exact writable output paths. All other reads
pin native table identity, version, schema, layout and active files. Strict S1
consumer scopes retain their existing latest/capture rejection. Owner setup
cannot replace an active consumer scope. Subprocess manifests preserve these
pins across host/container mount mapping; unsupported envelopes fail closed.
Stage FAIL/ERROR, unknown status, invalid success or missing mapping result cannot
capture old outputs or stamp a PASS checkpoint. CF acceptance remains mandatory.

## Current control run bindings

- Data/research end: `2026-10-09T21:00:00Z`; raw remains through 10 October.
- Prepared securities: `d550b9e4-e3d6-40dc-93f0-fc0e420e3b43@1`.
- FX: `29b6ad08-7eeb-4dff-97b7-7521de4ce4cd@3`.
- Daily OI: `026176bf-9d3b-4192-adcf-559cd33f7c93@2`.
- Explicit historical margin calibration: `2026-08-14` with its paired switch.
- Parameter evidence: `bounded-tail-20261010-v1`; 13 reviewed regimes,
  exact metadata SHA checks, prepared identity/version admission.

The old bundle and prepared `@0` remain selectable. The second inference interval
extends through 10 October; actual new SETTLE/TC/OI inputs cover 6–9 October.
Current 74-row MC crosscheck is separate from the preserved 674 dated TC checks.
No invented 10 October rows or daily official STEPPRICE coverage.

This cap is one control-run boundary. The existing nightly cutoff remains
calendar-driven. Automatic history-to-raw/prepared/OI append and admitted
parameter renewal are still required before claiming continuing nightly readiness
beyond the bounded bundle. Default economics validation remains strict.

## Promotion and rollback boundary

The new root remains a candidate. Its job terminal is intermediate publication.
After fresh L1/derived MTF, complete native compaction and quiesce writers. Use
existing S1 capture/create/save/publish APIs to capture post-compaction versions,
schema/layout/files for final Data and Research. Preserve calculation QC and
provenance; final Research has the exact final Data ID as parent. CAS each
existing registry against its expected current ID. The two registries are not
one atomic transaction: if the second CAS fails, keep consumers quiesced and
complete its saved Research manifest before cutover.

No production pointers, launcher roots, schedules, deletion, VACUUM or service
state are changed by this code patch. Existing cutover/rollback operations apply
only after accepted candidate data and delivered code. Physical fresh-job and
post-compaction proof are separate from these focused unit guard checks.

## Explicit physical schemas and compatibility

Change surface: product-plane. Architectural impact: contract-change.

`data_snapshot.v2` describes every pinned table in
`physical_schemas[table_name] = {schema, partition_columns}`. `schema` is the
native Delta JSON struct, preserving ordered field names, exact types, nullable
and metadata, including nested fields. Its canonical JSON SHA-256 is exactly
`tables[table_name].schema_hash`; no parallel hand-maintained physical schema
exists. Partition columns, including `ts_close_year`, must be real physical
fields. Capture and enrichment read the exact pinned Delta version. Readers
validate the descriptor/hash and the native version, identity, schema, partition
layout and existing active-file guards. Missing or malformed v2 descriptors
fail closed. Inherited table references remain byte-for-byte equivalent as JSON
values, preserving Data/Research ancestry comparisons.

The descriptions have separate purposes:

- `physical_schemas`: every actual field in every table, including storage and
  retained legacy fields, with native nullability and metadata.
- `semantic_compute_contract.schemas`: logical columns declared by the compute
  owners, with logical type names. It is not a complete physical storage schema.
- Store `physical_columns` and `partition_by`: declared physical carriers and
  partition policy. Their actual types and field metadata live in the manifest.

The L2 source descriptor uses the same existing
`_source_indicator_columns_for_profile(l2)` selector as its producer. The old
all-L1 declaration is admitted only through a read-only metadata correction:
reconstruct that exact old descriptor for the same registered profiles; require
every other compute component to be identical; verify required physical fields,
normalized native types and partitions against the accepted Delta pin. Receipt
v3 binds the accepted snapshot/digest, old/new compute digests and native table
reference. The only extra physical field admitted is retained legacy
`adx_14:double`, explicitly recorded in `retained_legacy_columns`. Existing
migration code already retains it; new source frames do not produce it.
For the inspected default contour, 42 obsolete logical declarations become 41
absent physical fields plus retained `adx_14`; fresh frames have 146 physical
fields and the retained legacy frame has 147. Neither shape is rewritten.

The presemantic bootstrap digest stays frozen. Its compatibility check
reconstructs the old declaration and requires that frozen complete numerical
baseline plus the same native schema proof. Failed metadata proof raises
`ContractError`; it cannot silently request a full refresh. Existing QC,
ancestry, owner and receipt revalidation remain mandatory.

Immutable `data_snapshot.v1` manifests remain readable without rewriting.
Metadata-only host/container remapping preserves v1/v2 envelope versions and
existing descriptors, validates the original digest, and needs no table I/O.
New snapshots always use v2 and enrich inherited refs from pinned native tables.
Rollback retains the old code and immutable v1 snapshots; do not point an old
v1-only reader at a new v2 manifest. Merge, runtime promotion and publication are
separate operations. This change authorizes no rebuild or production mutation.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic upstream proof, full refresh, immutable manifest rewrite
- Closure Evidence: native Delta producer-to-reader regressions; scoped read-only 34-table/1205-column comparison; independent implementation review and focused gates
- Shortcut Waiver: none

Chosen path: metadata descriptions over the existing Delta owner and producer
selector, preserving pinned versions and future ordinary scheduled processing.
This closes the schema-description contract; it does not claim deployed code,
a fresh production calculation, or a completed live trading journey.

## Daily admission and producer retry

Architecture and rationale: [Research refresh admission](docs/architecture/product-plane/research-refresh-admission.md).

Change surface: product-plane. Architectural impact: contract-change.

Daily execution follows [daily admission](indicator-refresh.md#daily-admission-boundary).
The publisher revalidates inputs, parents, exact config, successful native step
owners and all QC before CAS. Repeating the same accepted attempt returns its
same snapshot only after checkpoint/config/table/QC comparison; it cannot bless
another attempt or changed output. Failed or partial attempts retain the prior
accepted Research snapshot. Retry transfers only verified successful steps.

Daily OI writes save old/new contract/date/instrument keys in the existing
pending-changed-windows payload before committing Delta. Baseline merges them
into canonical dirty windows before its no-op decision, preserving them until
accepted Data publication. Coverage requires the complete native reference chain
from the previous accepted Data pin to the producer output. Economics-only scope
resolves instrument identity from pinned canonical columns; ambiguous aliases block.

An unreported RAW/canonical commit detected before a later baseline cannot be
certified by that later run's narrower windows. Its accepted Data manifest keeps
unbounded ancestry, so Research blocks and retains backlog. Only an exact existing
OI write-ahead chain can explain the corresponding support drift. This does not
schedule a rebuild. The explicit native scope recovery below can prove the complete
unknown segment without numerical materialization; otherwise Research stays blocked.

Producer scope v2 supersedes legacy v1 transitions after the accepted Research
anchor. RAW binds actual pre/post native versions and permits only its one data
commit plus native compaction. Canonical binds its native pre-publish versions,
target paths and commit ownership through Delta userMetadata; unexpected commits
break coverage. Legacy evidence and missing producer-chain proof block daily
scope instead of being promoted by a later narrow update.


## Explicit scope recovery

Architecture and authority boundary: [Data scope recovery](docs/architecture/product-plane/data-scope-recovery.md).

Change surface: product-plane. Architectural impact: contract-change.

A new successful baseline can replace `legacy_bound_data_scope` with a different
valid blocker: its v2 scope retains an unproven native transition from a failed
prior run. A later narrow child cannot conceal that immutable transition.

The existing Data owner offers `recover-scope`: native audit by default, optional
metadata-only publication with `--publish`. It compares the exact accepted
Research Data parent with exact accepted current Data. Spark reads both pinned
Delta versions, compares complete changed physical partitions with symmetric
`EXCEPT ALL`, and verifies reader row counts against native AddFile statistics.
Unpartitioned tables are compared in full. Old/new semantic effect starts include
deletions, old aliases and changed interval ends. Unknown features, identities,
schemas, temporal scope or null timestamps reject; ingestion time is not effect time.
The union of old/new canonical pairs defines the conservative dirty domain.

`data_scope_recovery.v1` is the single report contract, embedded in the existing
Data manifest's downstream scope. No new registry or table writer is introduced.
Its source/target/Research IDs, manifest digests, native reference hashes, counts,
full changed range, checked operator lower bound and retained historical prefix
bind one complete ancestry segment. The publishing owner runs the native audit;
an externally supplied report cannot authorize publication. The lower bound
rejects earlier changes instead of truncating them. Historical all-domain
recovery without a retained prefix is blocked; it cannot silently select rebuild.

A published bridge has exactly the prior current Data tables and that Data
snapshot as parent. Old manifests remain unchanged. The resolver may discharge
only the proved segment to the exact accepted Research anchor. New ordinary v2
children continue through their normal producer scopes. Retry uses exact expected
parent IDs and cannot publish the same stale attempt again.

Apply while existing controls are paused and native runs drained, preserving
cursors. Publication uses one Data CAS. Research identity is checked before/after
CAS and at admission; this is not an atomic two-registry transaction. A concurrent
Research change leaves any published bridge unusable until recovery is repeated
against the new accepted state. Native Data drift or changed Research outputs
reject before publication. Restore the prior controls after verification.

The operator entrypoint is
`python -m trading_advisor_3000.product_plane.data_plane.data_snapshots recover-scope`.
Required arguments: `--canonical-root`, `--research-root`, `--not-before`,
`--expected-data-id`, `--expected-research-id`; optional `--report-path` and
`--publish`. Publication alone does not trigger a baseline-success sensor.
Activate through the next ordinary scheduled baseline or an existing Dagster
daily job with a validated SCOPED admission and `force_full_refresh=false`.
No fake baseline success, cursor rewind, or full refresh is permitted.

The composed compatibility receipt v4 admits only the audited fingerprint-engine
contextvars allowance and complete pair restriction in the sidecar, plus existing
native source-schema correction. Every other kernel, quality/proof codec,
profile and numerical dependency stays equal. Bootstrap constants stay frozen.
`operation` and `refresh_admission` are execution metadata under the existing
scope-parameter owner; changing roots, periods, selectors or numerical policies
still blocks. An unsupported compute contract produces observable BLOCKED.

The audited contextvars-only engine transition also retains the old L1/L2
numerical hash identity through `numerical_identity()`. Only the exact reviewed
engine is aliased; live kernels and dependencies still invalidate carriers.
Raw semantic contracts and native operation entry/exit drift checks retain the
real engine. This prevents an operational compatibility allowance from widening
a changed suffix into an otherwise unnecessary complete-series replacement.
