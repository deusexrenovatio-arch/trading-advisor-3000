# Staging acceptance before main

Change surface: mixed. Impact: contract-change. Decision: accepted user scope,
2026-10-27; implementation and native activation remain separately unproven.

## Three responsibilities

1. One branch command runs a selected existing Data or Research Dagster recipe on
   immutable miniature inputs. Fixed input rows/bytes/files, monitored output growth, RAM,
   elapsed time and a stable task attempt limit bound the whole graph. Multiple
   explicitly budgeted short runs are allowed; no automatic retry or large seed
   fallback. Existing source, formula, Delta and public consumer owners remain.
2. One automatic pre-main run tests the frozen integration tree in the existing
   working staging: native update -> native Research sensors/job -> QC -> Delta
   publication -> fresh normal consumer. A short fixture result never grants main.
3. The existing PR/CI lane consumes the result artifact from its native staging
   job, checks exact base/head/tree and complete route evidence, and blocks main
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

Source/config tests and independent review prove only implementation structure.
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
instruments, timeframe, populated price/L1/L2 columns). Worker roots must exist
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
only code checks run. Moving the PR to ready is a separate runtime-admission step. `pr-lane` requires that job to succeed, downloads
only this workflow attempt's artifact and validates its base/head/merge tree,
image/input pins, native lineage, QC and fresh consumer. A rerun is blocked before
launch; there is no old-artifact fallback or automatic recomputation.
