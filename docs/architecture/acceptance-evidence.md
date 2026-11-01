# Admission evidence and bounded native acceptance

Change surface: mixed. Impact: contract-change. Decision: accepted user scope,
2026-11-01; code/control admission and native activation are separate claims.


## Governance-shell owns admission

Every PR uses the existing shell surface resolver and PR gate. The existing
`configs/change_surface_mapping.yaml` declares the required evidence level on
its contours; `compute_change_surface.py` resolves the strongest matched level.
Code checks remain mandatory for every PR. Changes to Data/Research computation,
read/write contracts, core Dagster definitions, shared snapshot contracts, ordinary
data runtime configuration/deployment and dependencies require native evidence.
The explicit staging adapters/fixtures and proof-only Compose are delivery support:
they select code checks and the governance test matrix, including when introduced.
They do not alter domain calculations. Ordinary Dagster definitions stay unchanged;
the proof entrypoint composes them with a bounded executor. Documentation, tests,
shell configuration and API-only changes need no Data/Research calculation.
Mixed changes retain native admission whenever any affected contour requires it.

The shell validates a generic versioned result envelope: level, status, exact
base/head/merge tree and CI run/attempt. It imports no product package and knows
no product job, asset, QC or consumer names. The product-owned staging acceptance
adapter validates actual native lineage, image/input pins, QC/publication and the
fresh normal consumer before it emits that envelope. The existing CI job connects
that adapter to the shell gate; it is not a second policy owner or a new registry.
A branch/short result cannot satisfy native admission. Code-level routing never
skips the ordinary PR/code gates. Runtime ownership, budget and draft exclusion
below remain unchanged; code/source review is not native execution proof.

## Three responsibilities

1. One branch command runs a selected existing Data or Research Dagster recipe on
   immutable miniature inputs. Fixed input rows/bytes/files, monitored output growth, RAM,
   elapsed time and a stable task attempt limit bound the whole graph. Multiple
   explicitly budgeted short runs are allowed; no automatic retry or large seed
   fallback. Existing source, formula, Delta and public consumer owners remain.
2. For native-selected changes, one pre-main run tests the frozen integration tree in the existing
   working staging: native update -> native Research sensors/job -> QC -> Delta
   publication -> fresh normal consumer. A short fixture result never grants main.
3. The existing PR/CI lane consumes the configured producer result, checks its
   generic level/status/base/head/tree envelope and blocks main
   on failure, missing/stale evidence or skipped execution. No separate merger.

## Keep versus remove

Keep isolated code/input/output mounts, native Dagster persistent run metadata,
fixed miniature mounts, one task attempt counter outside disposable outputs, native
QC/publication and a fresh public consumer. Dagster owns jobs/sensors/run IDs;
Delta owns versions and publication; Python only coordinates limits and evidence.

Remove dedicated quota PostgreSQL/schema/role bootstrap, signed permits and
receipts/keyring, GitHub App and exclusive merge controller integration, protected
workflow witnesses, reattestation/reuse machinery, reference-mode activation,
production observation and promotion validators. Their necessity is not proven
for this scope. Existing PR protection/CI trust and one configured staging worker
are the trust boundary. No hostile-admin or arbitrary-code sandbox is claimed.

Deep compute/subprocess grants and production observers are removed. Immutable
miniature inputs plus an internal network bound short runs; a native staging-only
executor disables retries and uses one step at a time. A small session launcher
admits only the planned root UUID and its real successful native children, once
per job. It is not installed on the ordinary staging scheduler.

A persistent native Dagster instance records task limits and reservations under
a single host lock. Reserve before launching; failure/crash/cancel spends the
slot. The instance path is fixed by worker configuration, never a per-run flag.
A missing/uninitialized task budget blocks; SHA/PR/HOME/output changes cannot
initialize or reset it. Existing task staging-admission-guardrails-20261025 keeps
small 3/3, acceptance 0/1, rebuild 0. Its previous PostgreSQL records, keys and
outputs remain untouched; replacement metadata must import prior spend before
activation. No new native run, quota extension or service migration is authorized.

Acceptance starts the normal update job once; normal sensors launch its native
children. Waiting for a particular daily cron, proving production NOOP or observing
production state is unnecessary. Existing regular staging scheduling stays native.
The candidate's exact source tree, image and input pins are recorded. Retries are
disabled/denied before native work. A failed or incomplete graph is terminal.

## Migration, cost and proof

This replaces the unactivated large package in the same branch, not a PR stack.
Restore unrelated upstream code; preserve native refresh admission and ownership.
Provide a branch command, one CI acceptance job and one PR result check. Configure
worker roots/image/fixtures once. Ordinary integration must need no agent-led
assembly, signing, observer, promotion ceremony or incident recovery loop.

The control package is admitted through actual shell selector/CLI/gate execution
on the frozen Git diff, regression checks and independent review. It does not need
a Data/Research calculation to introduce those checks. Any actual product source
change in the same PR still selects native evidence and the data test matrix.
Operational staging readiness remains pending until separately authorized native
execution; code admission never asserts that the environment or data route passed.
Source/config tests and independent review do not prove native operation.
A real bounded branch pilot and a full source-to-consumer staging run remain
pending; activation and production readiness cannot be inferred from tests.
Rollback restores the previous unactivated branch version; operational historical
evidence is retained. Existing production is not modified by this reduction.

## Minimal operator interface (code only; not activated)

`python scripts/run_staging_control.py branch --target data --output <new-report>`
or `--target research` runs one short recipe. Both share the same branch budget.
`acceptance --base-ref <base-SHA> --head-ref <PR-head-SHA> --output <new-report>`
requires a clean committed checkout of that exact merge tree. The archive is
mounted read-only; no image build/pull or implicit data seeding occurs.

One external `TA3000_STAGING_CONFIG` JSON supplies `task_id`, `budget_home`,
`work_root`, `acceptance_data`, `fixture_root`, immutable `fixture_sha256`, local
`image_digest` (sha256), and separate `branch_consumer`/`acceptance_consumer`
normal ResearchSliceRequest objects (dataset/indicator versions, contour,
instruments, timeframe, populated price/L1/L2 columns). The worker passes the
consumer dataset to native sensors through their existing environment setting.
Worker roots must exist
and be separate from each other and the checkout/config. Acceptance data needs
`.ta3000-acceptance-stage` containing `TA3000_ACCEPTANCE_STAGE`; this marks an
existing working stage, never creates or copies one. Budget setup/history import
is a separate explicit operation; neither command initializes it.

The proof-only Compose session has no host socket or budget mount, no restart,
9 GiB RAM, 2 CPUs, 2 GiB executable scratch and a container-enforced lifetime:
600 seconds branch, 1800 acceptance. Branch network is internal; inputs remain
at most 32768 rows/16 MiB/256 files. Host polling stops output growth above
4 GiB branch/8 GiB acceptance; this is a sampled threshold, not a disk quota.
Cleanup stops the complete container graph and preserves data/evidence/history.
Unconfirmed cleanup retains the existing worker lock for explicit recovery.
Regular staging updates must use the same host worker lock; do not install the
session launcher on their scheduler or allow concurrent writes to stage data.

Existing quick code checks must pass before spending acceptance.
Existing `pr-ci` adds one native job on a Windows `ta3000-staging` runner with
`TA3000_STAGING_PYTHON` and external configuration. The existing hosted-CI opt-in
is preserved. Draft PRs cannot launch native acceptance even when CI is enabled;
only code checks run. For native-selected PRs, moving to ready enables the native job. `pr-lane` requires that job to succeed, downloads
only this workflow attempt's artifact and validates its base/head/merge tree,
image/input pins, native lineage, QC and fresh consumer. A rerun is blocked before
launch; there is no old-artifact fallback or automatic recomputation.

Agents can already run code checks with the existing GitHub Actions dispatch:
`gh workflow run branch-diagnostic.yml --ref <branch>` (repository write access).
Pushes and PR events also start their existing jobs. Native branch jobs belong on
the configured staging runner; dispatch does not extend task limits or authorize
stage activation. No additional CI service or runner installation is required by
this code change.
