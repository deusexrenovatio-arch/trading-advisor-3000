# Economics canonical migration

Change surface: product-plane. Owner: Market Data Foundation.
Decision scope: user-authorized production-baseline migration, 2026-11-04.
Independent design/implementation admission belongs to the PR review evidence.

## Accepted outcome

The user explicitly accepts the current computed product-runtime economics as
the migration oracle and authorizes working-data and updater-route migration.
This replaces a requirement to replay historical formulas before admission.
It does not relabel model assumptions as original exchange observations.

Accepted initial pins: prepared parameters v20, TableID
`d550b9e4-e3d6-40dc-93f0-fc0e420e3b43`; economics v18, TableID
`58418381-67b9-4c02-8cb9-8c6d2f8829aa`; full published snapshot
`2d4febedf3724f88a47d7f1bf319a9d4`. The retained acceptance receipt is
`.tmp/economics-production-parity-20261104/accepted-production-baseline.json`.
It records native all-row equality: 124,347 parameter records and 124,256
economics records. The 91 parameter-only records remain present, as in the oracle.
If working references advance, freeze and verify the new exact references before
cutover; never substitute latest into a frozen plan.

## Ownership and runtime

Reuse the existing v6 producer and thirteen-asset continuation policy. Its rules,
dated-evidence hashes, calibration date and buffer policy remain unchanged.
The separate v8/source-certified development branch changes numerical semantics
and is outside this cutover. Do not import its alternate money path.

Spark owns full-table copy, payload projection, uniqueness and bidirectional
equality. Delta owns authoritative tables, native commit identity and retained
versions. Dagster retains the existing schedule/job/renewal writer. Python owns
bounded admission metadata, process coordination and exact-key readback only.
No new service, calculation engine or active economics owner is introduced.

One explicit adapter exception: Spark distributes the existing Python JSON
logical-hash codec (`ensure_ascii=False`, sorted keys, compact separators,
UTF-8 SHA256) solely to validate retained payload digests. Spark 3.5 native
untyped JSON serializers do not guarantee this legacy numeric/escaping codec.
The UDF has scalar input/output, no I/O or driver row collection; malformed or
nonfinite JSON and digest mismatch refuse admission. All table transforms,
joins, uniqueness and equality checks remain native Spark.

## Canonical preparation and lineage

Create `canonical/economics/canonical_contract_parameters.delta` from the entire
accepted prepared pin. Preserve all 21 original columns and values; append
explicit origin/admission metadata. Initial status is
`ACCEPTED_PRODUCTION_BASELINE`, with a resolvable old table/version/key and the
owner acceptance reference. No active `NOT_REVALIDATED` status is carried over.

An immutable canonical binding records the old pin, new canonical initial pin,
business-column equality and accepted v6 policy/evidence hashes. This is an
explicit clone admission, never a claim that a new TableID is the old d550 ID.
The existing continuation validator, initial binder, expired-contract lookup,
owned-renewal validation, writer and baseline snapshot input alias must all use
this binding. Legacy unregistered calls keep their existing checks.

Future rows use the same bounded v6 acquisition/crosscheck and owned-renewal
policy. They are `CALCULATED_WITH_ACCEPTED_MODEL`, not retrospectively covered
by initial parity. Their origins identify the actual RAW pin/key/hash, immutable
policy/evidence and initial admission. Metadata must remain stable on identical
input retry. New specifications that fail existing unit/current-source checks
stop the producer rather than silently extend incompatible constants.

Economics monetary and original provenance columns remain unchanged on first
publication. Their source hashes and native input snapshot resolve through the
canonical parameter row/policy to accepted old pins and original RAW. Existing
missing original historical attachments remain an accepted baseline limitation.
The normal snapshot records acceptance by production parity separately from
independent formula or exchange-fact certification.

## RAW and retained history

Historical RAW restoration columns are replaced only by projections of their
retained original payload. Preserve received payloads and recoverable original
record identity; inferred fields never become fabricated observations.
Use a native Delta commit retaining the existing RAW TableID and old versions,
which the v6 renewal validator requires. Future v6 acquisition already produces
unmodified RAW and writes restoration into the new canonical target.
Current specification records and their actual `source_pages` are also captured
in RAW, with an optional `snapshot_metadata_json` column; they are not silently
backdated daily observations. Missing legacy source tags may be recovered only
from the exact allowlisted history URL, matching payload identity and verified
original hash. Missing publication times remain unknown; duplicates refuse.
Keep the old prepared table and its admitted versions as frozen lineage/rollback
data. It is no longer an active writer target after cutover.

## Activation, recovery and rollback

Integrate through the normal PR-only main workflow; freeze the reviewed release
SHA, then promote that main release to the production branch and checkout.
Do not develop or patch directly in the production checkout.

Before working writes, record and temporarily stop the baseline schedule and
dependent CF/research sensors, and prove no owning runs are active. Record the
full current snapshot, exact source pins and previous control states. Stage and
verify native outputs under one owned cutover intent. Foreign or changed partial
targets conflict; native commits without acknowledgements are reconciled, not
overwritten. Candidate identity survives retry.
Retain the complete owner acceptance receipt, not only its digest. Recovery
checks both issued intent and saved candidate manifests. Own partial native
commits can be aborted before publication, with RAW data/schema restored and
the previous writer route retained. Default rollback uses the cutover-aware
release to select the legacy route; the former binary is retained separately.

Publish a complete child Data snapshot through the existing compare-and-swap
publisher, preserving all unrelated table references. It includes the canonical
parameters and production-parity/origin QC, with the v6 economics business
projection unchanged. Validate the selected physical tables and a real consumer.
An incomplete publication/registration must fail closed; it must not resume a
writer against the retired prepared route or expose an unverified partial clone.

Rollback retains the former complete snapshot, frozen code and prepared source.
Restore the full snapshot and updater registration together while writers remain
quiesced; refuse blind rollback after unrelated publication or new writer commits.
Never delete retained Delta histories. Restore only the previously running
schedule/sensor states after accepted cutover/readback or a completed rollback.

## Done evidence

Negative checks cover wrong pins, changed clone/acceptance, missing origin,
unowned partial targets, publication races and interrupted recovery. Native
proof compares all old business columns against the accepted oracle and follows
origins. The canonical-target v6 producer must match the legacy-target producer
on identical frozen inputs for the entire affected output, including historical
recalibration. Prove the next bounded renewal and identical-input retry on the
new route. Publish/read back the complete production snapshot, then restore
normal Dagster ownership and verify the active release, mounts and route.

## Data to Research scope transition (2026-11-05)

Decision admission: independent checkpoints in the existing PR review evidence.
Implementation and deployment require separate proof. Surface: product-plane; architectural impact:
contract-change. Solution intent: target, within the accepted v6 migration.

The initial cutover preserved the old downstream scope while changing the
prepared parameter alias to canonical parameters and restoring RAW. Research's
daily admission correctly rejects that unproved table/contract transition.
The existing scope-recovery report requires an unchanged physical table set and
schema, so it cannot represent this migration without changing its meaning.

Reuse the existing cutover owner, Spark equality primitives, immutable Data
manifests and CAS publisher. Add one typed `data_economics_scope_transition.v1`
report inside the existing downstream scope. New cutovers issue it after their
native writes and equality checks, before creating the initial manifest. For an
already published cutover, the operator audits that exact issued cutover and its
previous Data snapshot, then optionally publishes a metadata-only child with
the same complete table refs as the cutover. Native
proof compares all accepted parameter columns and origin metadata, restores the
original RAW projection, and requires every other reference (including all money
outputs) to remain identical. This proves zero Research numerical effect.

The report binds the previous manifest identity, exact target-table digest,
cutover intent, canonical binding, row counts, zero differences and audit runtime.
A repair also binds the original cutover manifest identity. The initial report
never embeds its own enclosing manifest hash. There is no caller-supplied
proof admission, new registry, table writer or numerical rebuild. The existing
cutover writer still requires its frozen release; the new audit may run from a
reviewed later release only with the identical accepted calculation policy.

The Research resolver validates the report and exact physical pins, then skips
only the named zero-effect cutover segment to its previous Data snapshot.
Older backlog and later ordinary descendants retain their normal scope checks.
All other table additions, removals, schema changes or uncovered changes still
block. The transition does not rebind or rewrite accepted Research snapshots.

Publication requires the exact initial cutover to be selected and all working
pins unchanged. A stale expected Data ID, advanced producer, unrelated child or
changed report conflicts. Lost acknowledgements reconcile the selected child;
retry returns that exact child. Source history, the original candidate, code
receipt and prior manifests remain immutable. A concurrent Research advance
cannot enlarge this zero-effect transition; normal ancestry/admission checks
still apply. Rollback selects the original cutover only before a later child or
producer write, while controls remain quiesced; otherwise use forward repair.

Ordinary native renewals must also cover `canonical_contract_parameters`.
The Data owner binds the exact previous parameter reference to the renewal's
input reference, exact parameter output and snapshot input.
It admits coverage only when the full economics producer supplies bounded
effects and all three bound money outputs, or when validated source-unavailable
reuse keeps the exact previous economics reference. Parameters remain lineage
inputs; Research executes against the published money outputs. Unknown renewal,
unbound output or unproved fallback remains uncovered and blocks.

Proof: a regression reproduces the current rejection before the patch; native
Spark produces the transition from actual copied history; daily admission reads
that output without full refresh. Negative checks mutate pins, schema, money,
report/parent identity and later uncovered changes. Verify retry after Data CAS,
complete source/output readback, normal-renewal coverage, required CI and the
deployed Dagster route before restoring the original controls.
