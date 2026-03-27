# Release-Readiness Prerequisites Closure Program

Status: working technical assignment
Date: 2026-03-27

## 1. Purpose

This document defines the pre-readiness implementation program required to move the repository from the current honest `DENY_RELEASE_READINESS` state to a future `ALLOW_RELEASE_READINESS` decision in Phase 10.

The program is not a shortcut around the existing governed module.
It is the prerequisite implementation wave that must complete before `F1` can be rerun with a real chance of passing.

## 2. Why This Program Exists

The current stack-conformance remediation module is blocked at Phase 10 because the truth source still contains unresolved readiness blockers:

| Blocker | Current truth-source state | Why it blocks release readiness |
| --- | --- | --- |
| `contracts_freeze` | `partial` | Not every public/config/runtime/external envelope is fully versioned and regression-protected. |
| `real_broker_process` | `planned` | Real broker-process closure does not exist yet beyond sidecar and transport baseline. |
| `production_readiness` | `not accepted` | Final operational readiness is explicitly denied in the truth source. |
| `delta_lake` | `partial` | Physical/bounded proof exists, but not full supported baseline closure. |
| `apache_spark` | `partial` | Docker/Linux proof exists, but not full supported execution closure. |
| `dagster` | `partial` | Bounded materialization proof exists, but not broader supported orchestration closure. |
| `postgresql` | `partial` | Durable runtime path exists, but operational baseline and full contract closure are not complete. |
| `stocksharp` | `partial` | Sidecar and transport contour exist, but real broker-process closure is still open. |

This program exists to close those blockers without weakening the truth model.

## 3. Target Outcome

The program is complete only when all of the following become true:

1. `contracts_freeze` is `implemented`.
2. `real_broker_process` is `implemented`.
3. `production_readiness` is no longer `not accepted`.
4. `delta_lake`, `apache_spark`, `dagster`, `postgresql`, and `stocksharp` are no longer `partial`.
5. Every public claim in docs, registry, tests, runbooks, and proof artifacts is mutually consistent.
6. A rerun of `F1` can legitimately produce `ALLOW_RELEASE_READINESS`.

## 4. Architectural Principles

### 4.1 Truth-source first
- `docs/architecture/app/STATUS.md` remains the current authority on implemented reality.
- No checklist, phase note, test summary, or smoke output may outrun the truth source.

### 4.2 Contract-first public surfaces
- Every public runtime, sidecar, admin, config, stream, and recovery envelope must have:
  - a versioned contract,
  - a fixture set,
  - compatibility tests,
  - change rules.

### 4.3 Transport-neutral Python core
- Python runtime and execution logic remain transport-neutral.
- Broker-specific logic stays behind sidecar or adapter boundaries.
- Direct embedding of broker semantics into core runtime is prohibited.

### 4.4 Sidecar isolation
- The `.NET` sidecar remains a separately buildable and smokeable process.
- Broker-session management, session health, and transport-specific failure handling stay on the sidecar side.
- Python interacts only through the declared wire contract.

### 4.5 Fail-closed operations
- Missing secrets, missing DSN, missing broker session, stale connectivity, or unsupported environment must fail closed.
- No silent fallback from durable to in-memory, or from real transport to stub transport, is allowed in supported runtime profiles.

### 4.6 Supported-environment matrix over ad hoc portability
- Heavy data/orchestration proofs use a canonical Linux/Docker baseline.
- Broker-process proofs use a canonical Windows staging baseline if the broker stack requires it.
- Unsupported environments may exist for developer convenience, but they do not count for readiness closure.

### 4.7 Black-box proof over narrative proof
- Every promoted surface requires executable proof and at least one disprover.
- Internal implementation tests help, but black-box proofs are mandatory for closure.

### 4.8 Operational readiness is part of product readiness
- Deployment, rollback, recovery, observability, secrets, and runbooks are first-class deliverables.
- A feature is not operationally closed if the operator cannot reproducibly build, run, validate, and recover it.

## 5. Supported Environment Strategy

| Contour | Canonical environment | Why |
| --- | --- | --- |
| Delta / Spark / Dagster proofs | Linux Docker profile | Most reproducible baseline for data/orchestration stack and CI parity. |
| Runtime API + Postgres | Linux Docker profile | Aligns API/runtime proving with operational deployment style. |
| Real sidecar build/test/publish | Windows build agent plus repo-local `.NET 8` toolchain | Matches sidecar and broker-adjacent ecosystem constraints. |
| Real broker-process proof | Windows staging profile with real broker dependencies | Avoids pretending that broker closure is portable where it is not. |
| Final readiness replay | Combined supported matrix only | `ALLOW_RELEASE_READINESS` must be based on supported environments, not convenience environments. |

## 6. Scope

### In scope
- Contract freeze completion for all public surfaces.
- Promotion of currently partial foundation technologies to implemented supported contours.
- Real broker-process closure for the agreed staging-grade scope.
- Operational readiness closure for supported deployment profiles.
- Rerun of `F1` after prerequisites are complete.

### Out of scope
- New strategy logic or alpha-generation work.
- Expansion to unrelated product capabilities.
- Production volume scaling beyond the agreed supported baseline.
- Relaxing the `F1` acceptance bar.

## 7. Workstream A - Contract Freeze Completion

### Objective
Promote `contracts_freeze` from `partial` to `implemented`.

### Required deliverables
- Full inventory of all public payload surfaces:
  - runtime API payloads,
  - sidecar wire payloads,
  - admin/ops payloads,
  - config/env/profile contracts,
  - broker stream payloads,
  - recovery and incident payloads.
- Versioned schema for each surface.
- Matching fixtures for valid, invalid, and backward-compatible cases.
- Contract tests that assert shape, required fields, compatibility, and error model.
- Change policy that blocks payload drift without schema and fixture updates.

### Architecture rules
- No public JSON payload may be described only in Markdown.
- Runtime config and environment profiles count as contracts when they affect supported behavior.
- Error envelopes and recovery envelopes are public surfaces and must be versioned like success payloads.

### Acceptance gate
- Every public surface is represented in [CONTRACT_SURFACES.md](docs/architecture/app/CONTRACT_SURFACES.md).
- Every surface has schema + fixture + contract test coverage.
- Contract drift is caught automatically in CI/gates.

### Disprovers
- Change a public field name without updating schema/fixture/tests and confirm failure.
- Introduce a new admin/readiness payload field without contract update and confirm failure.

## 8. Workstream B - Foundation Promotion

This workstream promotes currently partial technology surfaces into implemented supported contours.

### B1. Delta Lake promotion

#### Objective
Move from bounded physical proof to supported durable-data baseline.

#### Deliverables
- Explicit supported Delta table inventory for the readiness baseline.
- Storage layout, lifecycle, retention, recovery, and compatibility rules.
- Write/read/recovery/disprover proofs for the full supported table set.
- Operational guidance for backup, restore, and corruption detection.

#### Acceptance gate
- Every supported Delta table has physical proof and recovery proof.
- Deleting or corrupting a declared table fails the relevant readiness lane.

### B2. Spark promotion

#### Objective
Move from canonical proof job to supported compute baseline.

#### Deliverables
- One canonical Linux/Docker Spark execution profile.
- Supported job inventory and resource profile.
- Deterministic job bootstrap and dependency model.
- Full Delta integration proof for the supported compute contour.

#### Acceptance gate
- Supported Spark jobs execute in the canonical profile.
- Output contracts match the declared Delta baseline.
- Failure of the real execution path cannot be hidden by plan-string or Python fallback paths.

### B3. Dagster promotion

#### Objective
Move from bounded materialization slice to supported orchestration baseline.

#### Deliverables
- Executable `Definitions` for the supported slice.
- Real jobs/resources/config contracts for supported assets.
- Partial-selection and failure-recovery semantics explicitly proven.
- Operator runbook for the supported orchestration contour.

#### Acceptance gate
- Supported assets materialize through Dagster semantics, not Python side effects.
- Jobs/resources/load/materialization/recovery work in the canonical environment.

### B4. PostgreSQL promotion

#### Objective
Move from durable bootstrap proof to full supported operational baseline.

#### Deliverables
- Schema evolution discipline for supported runtime schema.
- Backup/restore proof.
- Drift detection for schema and bootstrap assumptions.
- Recovery drill for restart, replay, and operator validation.

#### Acceptance gate
- Runtime durability works across restart and restore scenarios.
- Schema drift and missing migration state fail closed.

## 9. Workstream C - Real Broker Process Closure

### Objective
Promote `real_broker_process` from `planned` to `implemented`.

### Target architecture
- Python runtime emits transport-neutral execution intents.
- Python transport talks to compiled sidecar.
- Sidecar talks to the real broker-process contour.
- Broker feedback returns through sidecar streams and is reconciled into runtime state.

### Required deliverables
- Real broker-side adapter implementation behind the sidecar.
- Session bootstrap and readiness model for broker connectivity.
- Submit/cancel/replace/update/fill path against the real broker contour.
- Authentication and secret contracts for the supported staging environment.
- Idempotency, reconnect, retry, and kill-switch behavior.
- Audit trail and replayable event capture for broker feedback.

### Acceptance gate
- Black-box staging proof for submit/cancel/replace/stream/fill.
- Recovery after sidecar restart and broker/session interruption.
- Operator-visible readiness reasons for unavailable broker state.

### Disprovers
- Break broker session and confirm fail-closed readiness.
- Return malformed broker payload and confirm contract rejection.
- Trigger reconnect/recovery path and confirm state reconciliation remains consistent.

### Design constraints
- Do not bypass sidecar and call broker logic directly from Python.
- Do not declare real broker closure based only on sidecar-local stub behavior.
- Do not mix production-rollout concerns into the initial staging-grade closure.

## 10. Workstream D - Operational Readiness

### Objective
Promote `production_readiness` from `not accepted` to a readiness state that can support `ALLOW_RELEASE_READINESS`.

### Required deliverables
- Canonical deployment profiles for supported environments.
- Secrets lifecycle model:
  - source,
  - rotation,
  - startup validation,
  - redaction policy.
- Observability baseline on the chosen stack:
  - metrics,
  - structured logs,
  - dashboards,
  - alerts,
  - incident entrypoints.
- Backup/restore and disaster recovery drills.
- Rollback plan and operator runbooks.
- Soak or stability proving for the supported contour.

### Acceptance gate
- Supported profiles can be built, started, validated, and recovered by documented operator steps.
- Alerts and health surfaces expose actionable failure reasons.
- Rollback and restore are tested rather than only described.

### Disprovers
- Remove a required secret and confirm startup fails closed.
- Break a dependency and confirm alerting/health surfaces show actionable failure.
- Attempt restore from stale or missing backup inputs and confirm the lane fails.

## 11. Workstream E - Final F1 Rerun

### Objective
Rerun Phase 10 after all prerequisites are complete.

### Required inputs
- Updated truth source with no blocking `partial`, `planned`, or `not accepted` surfaces relevant to readiness.
- Refreshed registry and baseline docs.
- New evidence pack for all promoted surfaces.
- Regenerated checklists and red-team review.

### Acceptance gate
- `F1` produces `ALLOW_RELEASE_READINESS`.
- Final report, checklist set, evidence pack, and red-team result are mutually consistent.
- No overclaiming language appears in any final artifact.

## 12. Recommended Governed Decomposition

This program should run as a new governed module rather than as ad hoc follow-up work under Phase 10.

| Recommended phase | Purpose | Depends on |
| --- | --- | --- |
| `C2` | Contract Freeze Completion | none |
| `D4` | Delta and PostgreSQL promotion | `C2` design contracts |
| `D5` | Spark promotion | `C2`, `D4` |
| `D6` | Dagster promotion | `C2`, `D4` |
| `E2` | Real Broker Process Closure | `C2`, `phase-08` sidecar baseline |
| `O1` | Operational Readiness Closure | `D4-D6`, `E2` |
| `F1-rerun` | Final readiness rerun | all previous phases |

## 13. Patch Set Policy

- One patch set must close one architectural concept.
- Preferred order inside each workstream:
  1. contracts and registry,
  2. implementation,
  3. tests and disprovers,
  4. docs and runbooks,
  5. evidence and acceptance artifacts.
- Do not mix broker-process implementation with broad contract-freeze edits in one patch.
- Do not mix operational readiness hardening with final `F1` reporting in one patch.

## 14. Required Artifact Contract

Every workstream closure artifact must contain:
- git SHA,
- phase id,
- supported environment,
- surfaces touched,
- commands executed,
- command exit codes,
- produced artifacts,
- negative tests executed,
- registry diff,
- reviewer verdict.

Missing any field means closure is invalid.

## 15. CI and Proving Lanes

The following lanes must exist by the end of the program:

| Lane | Purpose |
| --- | --- |
| Conformance lane | stack-conformance and contract drift checks |
| Contracts lane | schema + fixture + compatibility tests |
| Foundation lane | Delta/Spark/Dagster/Postgres proofs in canonical environment |
| Sidecar lane | `.NET` build/test/publish + Python compiled-binary smoke |
| Broker lane | real broker-process staging proof and recovery tests |
| Ops lane | deployment/bootstrap/restore/alerting/rollback drills |
| Final readiness lane | rerun of the final `F1` package against the completed baseline |

## 16. Operational Invariants

- No supported runtime profile may silently downgrade to non-durable or stub behavior.
- No supported operator command may rely on undocumented local machine state.
- No public endpoint may be undocumented or untested in its supported contour.
- No release-readiness claim may depend on historical evidence from a narrower contour.
- No readiness promotion may occur without at least one disprover path remaining active in CI or governed proving.

## 17. Risks To Manage Explicitly

| Risk | Why it matters | Control |
| --- | --- | --- |
| Environment drift | Different proofs pass on different machines | Canonical environment matrix and reproducible bootstrap |
| Contract erosion | Runtime and docs drift apart under pressure | Contract-first change policy and CI enforcement |
| Sidecar leakage into Python core | Broker complexity pollutes runtime | Transport-neutral Python core and sidecar isolation |
| Ops blind spots | Functional proof exists but failures are not operable | Ops lane, runbooks, alerts, restore drills |
| False final closure | `F1` rerun passes on narrative instead of executable state | Black-box proofs, red-team review, truth-source gate |

## 18. Definition Of Done

This program is done only when:
- `contracts_freeze`, `real_broker_process`, and `production_readiness` are no longer blocking truth-source states,
- current `partial` foundation technologies are promoted or the architecture target is formally narrowed,
- supported environments are explicit and reproducibly proven,
- `F1-rerun` returns `ALLOW_RELEASE_READINESS`,
- no artifact in the final package overclaims beyond the proved baseline.
