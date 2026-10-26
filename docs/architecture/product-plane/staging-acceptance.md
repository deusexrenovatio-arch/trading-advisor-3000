# Staging acceptance before main

Change surface: mixed. Architectural impact: contract-change.
Decision: accepted. Implementation: staged; activation requires separate route acceptance.
Runtime/deployment: not established by this decision; require pinned live readback.

## Decision context and alternatives

Repeated materialization makes development costly; green tests do not establish
the deployed producer-to-consumer route. This decision fixes cost and proof order.
Tests-only or main-first proof was rejected: both permit unverified integration.
Per-SHA retry/rebuild budgets were rejected: they erase spend history and grow cost.
A new compute/quota service or separate reference environment was rejected: reuse
native Dagster/Delta ownership, existing Postgres and the acceptance environment.

Build/review inert foundations through ordinary fast CI. Activate the protected
compute gate only after its native pilot; a changed product route still requires
full candidate acceptance before main. Rollback
disables activation and restores accepted code/image/checkpoint together; preserve
quota history and receipts. No automatic rebuild, quota reset or acceptance-data copy.
Sources: [native owners](docs/architecture/product-plane/native-runtime-ownership.md),
[schema](src/trading_advisor_3000/product_plane/contracts/schemas/staging_acceptance.v1.json),
[admission](scripts/validate_staging_acceptance.py). Scoped PR receipts and
runtime readback own implementation/deployment evidence. Revisit on native runtime,
storage/ownership or recurring-cost changes; reuse this record rather than add a registry.

## Delivery contract

Ordinary PR -> fast CI. Changed executable product route -> one integration
candidate -> reuse applicable proof or one acceptance graph -> required pr-lane
-> main -> production promotion. Branch native perf is optional, never a prerequisite.
The integration candidate pins the current main commit, selected branch heads,
resulting tree, executable dependency closure, runtime image, configuration and
input snapshot versions. A batch lands as one integration PR. A stale main or
candidate cannot use a previous admission. Reattestation can reuse computation
only when the entire executable/configuration/input/coverage closure is unchanged.
Server-side strict up-to-date main protection closes the gate-to-merge race.

Acceptance has two protected modes in the same environment. Candidate mode uses
the fixed task proof budget. Reference mode maintains the last accepted main
between candidates, through the same ordinary production daily schedule. Only
the protected host can activate reference mode after validating the signed
acceptance receipt, actual main/tree merge and immutable artifact binding.
Reference evidence is labelled reference and cannot admit a new candidate.

Each reference graph reserves one slot from the real recorded Dagster tick and
its scheduled execution time/timezone, never from caller tags or wall-clock date.
Manual/retry/resume requests cannot create another slot. Per-cycle deadlines and
resource caps remain bounded, and input pins are captured from the last accepted
snapshot at cycle start. A proven NOOP is a normal reference outcome. The first
failed, cancelled or BLOCKED reference graph pauses subsequent cycles until
diagnosis and a protected-host decision. Switching modes preserves all task
attempt history; a new day cannot reset a failed candidate's proof budget.

## Single candidate command

The protected host runs `run_staging_control.py --candidate <candidate.json>`
with its existing env/plan/manifest and signer configuration. The host validates
current main and candidate binding, then selects protected read-only reattestation
or the existing supervisor. An original receipt must verify before it can influence
routing. Reuse requires identical executable/config/image and original plan; the
existing observer must establish unchanged pinned inputs/outputs, environment and
fresh consumer readback. SHA alone is not a compute identity.

A missing proof or proven changed closure selects one acceptance graph, only with
a plan bound to the new candidate. An invalid signature, unknown applicability,
failed observer or failed graph stops the command. Never fall back from a failed
reuse attempt to computation; never retry, rebuild, reseed or extend quota inside
this command. Existing quotas guard repeated invocations. Setup and incident
repair are agent work; ordinary integrations use this fixed machine route.
Implementation and real-route activation remain separate evidence.

## Native owners and isolation

Dagster owns schedules, jobs, sensors, run metadata, retries and persistent quota
reservations. Delta owns input/output versions and accepted publication. Spark,
native indicator producers and existing public Research consumers keep their work.
Python adapts admission, candidate identity and evidence; it adds no compute route.

The session-schedule owner persists open intervals and explicit closed dates from
the same validated raw evidence. Both projections replace only that evidenced
instrument/date scope; provenance and conflicts are checked before writes.
Baseline budgets and pins the effective closed-date input before Spark, and records
the actual owned projection version in scoped dependency evidence. Explicit read
overrides retain their own immutable input pin; canonical-root publication keeps
the owned table authority. Existing table identities, layouts, published keys and
required-table membership remain unchanged. Only explicit validated closed-date
records qualify as closure evidence; missing candles supply no such authority.

Branch and acceptance use the production product_plane_definitions entrypoint.
Each has independent HOME, port, data root and bounded CPU/RAM/scratch. Code is
read-only. Quota storage is durable outside disposable stage output. A protected
control plan and verifier checkout are mounted separately from candidate code.
The runner has no production writable mount, Docker socket or broker credentials.
The host control boundary is trusted; an administrator deleting quota storage or
changing protected policy is an explicit operating change, not a supported bypass.

Resource admission follows the native heavy owner: branch webserver, acceptance daemon.
Defaults: branch webserver 9 GiB; acceptance webserver 3 GiB and daemon 9 GiB.
Driver heap 3 GiB and scratch 2 GiB remain configured caps.
Sum of service memory caps plus a 2 GiB host policy reserve must fit Docker memory.
Heavy cap covers heap + scratch + 2 GiB; control cap covers scratch + 1 GiB.
Actual MemorySwap equals its memory cap; extra swap is forbidden.
Reserves are explicit policy floors, not measured peaks or a guaranteed OOM cure.
Observed OOM and later JVM loss do not identify the killed PID, Xmx, RSS or tmpfs use.
Failed attempts stay spent. Additional budget requires explicit user approval and
an append-only signed task-limit extension; plan deadlines remain independently pinned.

Acceptance uses the same logical container data path as production so that pinned
absolute Delta paths and ancestry remain valid. Initial seeding copies an accepted
manifest and its complete referenced Delta versions/files independently; no writable
hardlinks. Thereafter normal scheduled incremental updates maintain the stage.
An exact protected prior native readback can freeze an accepted Research checkpoint
and its Data parent after CURRENT advances. Preserve observation time, raw readback
and digest; candidates cannot nominate stale inputs. This proves recovery from that
accepted checkpoint, while promotion checks the actual production state separately.
An unsuccessful candidate cannot become the next baseline. Retain recoverable
accepted versions, including schema/layout state; reverting code is not data rollback.

## Execution budget

The authoritative task-plan, native-grant and signed-receipt envelope schemas live
in src/trading_advisor_3000/product_plane/contracts/schemas/staging_acceptance.v1.json,
with matching fixtures and
contract tests. The shared staging_contracts validator loads that authority; runtime,
collector and gates do not redefine DTO meaning. Additive optional fields preserve
v1; required-field/meaning changes require a new version and explicit compatibility.

The protected task plan fixes a stable task identity, candidate identity, stage,
source scope, allowed graph/steps, output roots, deadline and attempt limits.
New-plan defaults: zero small attempts, one acceptance graph per batch, zero
rebuilds. Optional branch perf requires an explicit small-attempt budget. Existing
plans and append-only spend remain unchanged; changing these defaults cannot reset them.
Task identity and prior spend survive SHA/branch/PR/HOME/output recreation.

Reserve before expensive work through native Dagster PostgresRunStorage
uniqueness, using deterministic reservation IDs. A separate quota database uses
the existing PostgreSQL service. Protected host credentials initialize its schema
and migrations. Candidate roles have SELECT/INSERT and required sequence USAGE
only: no UPDATE/DELETE/TRUNCATE, ownership, privileged role membership, CREATE or
TEMP. Runtime verifies database/role capabilities and refuses SQLite or owner
fallback. Quota connection configuration is mounted read-only, independently of
disposable run HOME. Journal databases and schemas are unchanged.
Queued UI/CLI/schedule runs and in-process/native calls consume the same
budget. Each graph stage has a bounded execution count; retries/resume cannot
silently add computations. Started failed/cancelled attempts remain spent.
Read-only preflight that finds no change remains pending and consumes no compute
attempt. A launched NOOP is spent and is not proof of the changed compute route.
Unknown scope, expired plans, incompatible identity and unapproved rebuilds fail
closed. No automatic budget extension or full-history fallback is allowed.

The existing Research admission decorators and baseline producer entry enforce
the protected plan before native work. Stage paths require the protected marker
and plan. The protected native run launcher reserves attempts at STARTING before
candidate code; queued waiting stays free. Removing candidate decorators cannot
create free launched attempts. Asset/native guards also cover execute_in_process
and direct CLI. Branch uses native direct submission with no daemon; acceptance
uses the native queue, schedule and sensors. Production's current scheduled route
is not modified or launched by introducing the staging control configuration.
Native Spark subprocesses receive a one-use invocation grant minted by the bound
Dagster scope. It binds the recorded root attempt, exact allowed native entry,
output roots and plan digest; the subprocess validates and consumes the grant in
the same durable quota store before computation. Bare CLI calls cannot mint a
grant. Child process handoff therefore preserves the real route without an
unrestricted environment-variable bypass.
Every grant must consume one of the bounded deterministic native claim slots
exactly once as well as its nonce. Native work requires a currently STARTED run;
SUCCESS only authorizes a completed upstream edge. Detection also consults the
immutable control mount when process environment is removed; writing under /tmp
cannot disable the staging admission boundary.

The protected host supervisor enforces the run deadline and container resource
limits externally, including termination of the isolated stage on timeout. Native
guards also check the deadline at handoff and before snapshot publication. Timed
out/terminated runs cannot supply accepted evidence or advance the accepted stage
baseline; partial Delta writes remain isolated until recovery.

The genuine acceptance schedule may record and queue its tick while production
is busy. The protected coordinator keeps dequeue at zero until a short-lived,
read-only host execution permit matches the plan and shared heavy-work lock.
The host grants it only in a declared safe execution window or after the matching
production tick/chain has completed; a single idle poll at the common cron is
insufficient. Quota/runtime start at actual execution, not while queued. Stale
queued candidates are held before allocating any new candidate quota. Permit
expiry stops new native handoffs/publication; native run monitoring and the
external supervisor enforce termination. Production is never paused to test staging.
Production does not yet share this execution authority: an unexpected/manual
production launch can race an issued permit. The supervisor revokes the permit
and stops staging on detection, with bounded monitoring overlap; this does not
prove atomic cross-runtime exclusion. Resource caps must be safe during that interval.

Actual baseline parameters, including universe/mapping paths, discovery/refresh
windows, timeframes and economics options, must match protected producer_parameters
before extraction. Declared source-byte bounds are enforced from native Delta
active-file metadata or regular input file sizes before input scans/Spark, not
inferred from written-row counters.

## Evidence and trust

A protected collector independently reads Dagster records/events, published Delta
versions and the normal research.io.load_backtest_frames consumer response in a
fresh process with snapshot_read_scope and no read cache. Evidence contains:
candidate/main/branch/tree identities, executable/runtime/configuration digests,
input/accepted-output pins, actual schedule tick and root/child run identities,
nonempty change and stage execution, QC, publication and fresh consumer readback.
The relevant correction/rollover/migration scenario is required when changed.
The fresh consumer process loads the candidate artifact, so changed consumer code
is exercised. Its launch, independent storage/run readback and receipt signing
belong to the protected collector, not to candidate-authored validation code.
The collector keeps the original Dagster HOME read-only. After the complete
recorded graph is idle and its owned daemon stopped, bounded stable database
families, including every WAL, are captured into observer-owned tmpfs. Before,
copy and after hashes/file sets must agree; any change or rollback journal blocks
observation. Only derived SHM is omitted. Native SQLite online backup and integrity
checks then operate on the private capture; Dagster reads its observation snapshot.
It is not an authoritative store or a cross-database transaction. Total bytes,
files/time, exact run/tick identities, live state and publication are checked before
signing. Original HOME writes and immutable SQLite mode remain forbidden.
Native scheduler catch-up may exercise a declared preceding real cron instant;
the daemon creates its tick/run/configuration itself. Evidence labels catch-up
separately from a later wall-clock scheduled observation.
Protected metadata may propose the next native cron before it occurs; the proposal
is not an observed tick. Admission reads Dagster's canonical hidden scheduled-time
tag. Native dequeue and launch require the matching persisted successful tick and
run link before spending quota; future, unreachable or forged links cannot compute.

Candidate self-reported JSON, tag counts, kind=real and raw-table inspection alone
cannot establish acceptance. The collector runs outside candidate containers and
signs its receipt with a protected Ed25519 key. Gates verify against public keys
from the protected verifier revision; candidates cannot nominate keys or verifier.
Signed evidence must still match current candidate/main, required route and scope.
Missing, stale, forged or unreachable evidence blocks pr-lane. There is no skip
flag that turns a product route change green. Docs-only changes reuse applicable
proof after fresh scope classification. Evidence retention exceeds admission life.
The required status is emitted by a protected-base workflow/verifier with read-only
credentials and no signing secret; candidate installation/imports do not run in
that verifier's trust context. Hosted compute being disabled must not turn this
required admission into a skipped-success job. Bootstrap admission is independently
reviewed before the verifier revision/key become protected; that adoption limitation
must remain explicit until remote branch protection and the active workflow agree.
Required status names alone do not bind a GitHub workflow. The canonical protected
merge controller additionally checks the exact protected workflow/event/run,
successful steps, admission artifact and current base/head before SHA-bound merge.
Until remote rules bind that authority, other UI/admin merge routes remain an
explicit adoption limitation; this controller does not prove all-path enforcement.

For a personal GitHub repository, close that limitation with a separate active
`main` update-restriction ruleset. Its only bypass is a private controller App,
limited to pull requests. Keep the existing strict `pr-lane`/PR ruleset without
bypass. The App receives Contents write and Actions read for this repository only;
its key remains on the protected host. The host wrapper obtains a temporary token
only for the existing canonical controller, checks actual rules and revokes it
afterward. Registration, installation, policy readback and measured refusal are
operational acceptance requirements; a proposed ruleset is not enforcement proof.
The repository owner can still change policy settings.

## Production NOOP admission

The normal coexistence proof is a matching production tick and successful native
three-job chain. A successful baseline without a child is not sufficient. A NOOP
exception requires an explicit native NOOP sensor tick whose cursor resolves to
that exact root's RUN_SUCCESS record, zero children and no active production work.

Public GraphQL cannot resolve that global cursor alone. A separate protected
metadata reader uses native Dagster APIs on read-only main/WAL file aliases with
private derived WAL/SHM in bounded tmpfs. Original family sets, identities and
hashes must stay unchanged throughout observation; new/mutated WAL blocks it.
Root/tick/event and runtime identity are rechecked before/after. No production
pause, original write, `immutable=1`, data mount or private key is permitted.

This reader bounds metadata source size to 384 MiB, RAM to 2 GiB, CPU to one,
scratch to 256 MiB and elapsed work to 30 seconds. It does not copy the complete
303 MiB schedules database into scratch. Three failed observations require
diagnosis. A successful immutable witness is reused for the same runtime/tick/root
with fresh status/zero-child/zero-active checks. Generic empty skips are not NOOP.
Native fixtures prove the positive branch; actual production readback may honestly
return NOT_NOOP. Neither establishes full market-route acceptance by itself.

## Promotion and rollout

Promote the verified immutable artifact after main matches the admitted tree.
Validate transition from actual production schema/layout/checkpoint, including
skipped intermediate versions. Acceptance data is not copied into production.
The next ordinary production cycle supplies operational acceptance; no unscheduled
full rebuild is used for confirmation.

Implement contracts, budget guards, isolated stages, producer evidence and verifier
before enabling mandatory pre-main admission. First exercise a bounded real native
pilot, then connect its producer-generated receipt to existing gates. While evidence
or deployment is missing, report that boundary as pending; tests are not deployment.

## Required proof

Atomic concurrent reservation; exhausted-budget refusal; failed/retry/resume spend;
SHA/PR/HOME recreation cannot reset spend; direct native and UI bypass refusal;
production-path rejection; safe seed/restore; actual scheduled nonempty producer
to published snapshot to normal consumer; forged/stale receipt refusal; fresh
base/tree merge binding; unchanged executable proof reuse; production transition
compatibility. One bounded native pilot may satisfy several applicable checks.
