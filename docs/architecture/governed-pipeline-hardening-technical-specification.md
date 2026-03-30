# Governed Pipeline Hardening Technical Specification

Status: draft for implementation
Date: 2026-03-30
Scope: AI delivery shell and governed orchestration layer

## 1. Purpose

Define the next hardening wave for the governed delivery pipeline so that future package intake, phase continuation, contour splitting, PR flow, and release-readiness work do not require manual branch surgery, ad hoc remediation, or environment-specific debugging.

This specification is based on failures and frictions that appeared during a real multi-phase remediation cycle:
- explicit package intake followed by governed continuation;
- rollback and re-acceptance after finding overclaimed closure;
- split of one large remediation branch into policy-compliant contour branches;
- sequential merge of those branches into `main`;
- hosted CI remediation across Windows local execution and Linux GitHub-hosted runners.

The target is not a faster pipeline. The target is a more truthful, deterministic, auditable, and evolution-friendly pipeline.

## 2. Problem Statement

The current governed flow is functionally usable, but several thin weaknesses create recurring friction:

1. The pipeline does not natively support sequential contour waves after partial merge.
2. Validation reads a mixed state: part from git range, part from working tree.
3. Task-session bootstrap mutates repo state too early and too eagerly.
4. Hosted CI remains too monolithic for contour-isolated work.
5. Dependency installation is not segmented by contour or runtime profile.
6. Governed route selection fails when multiple active modules exist.
7. Proof runners depend on platform-specific filesystem and container behavior.
8. Git mutation operations are not sufficiently serialized or idempotent.
9. Truth-source recomposition after split-branch merge is manual and error-prone.

These weaknesses do not merely slow the operator down. They create future risk:
- false-red and false-green gate outcomes;
- manual branch rebuilding after the first contour lands;
- accidental regression of already merged truth-source surfaces;
- hosted-only failures that were not visible in local validation;
- inability to scale governed work to multiple concurrent modules.

## 3. Objectives

The hardening program must achieve the following outcomes.

### O1. Sequential contour delivery must be first-class
After contour A merges into `main`, contour B must be able to continue from the new truth base without manual branch reconstruction.

### O2. Validation must be state-consistent
All policy-critical validators must evaluate the same snapshot model instead of combining git diff and mutable working-tree state implicitly.

### O3. Session management must stop polluting the repo by default
The pipeline must support governed work without forcing immediate edits to tracked docs before design intent is stable.

### O4. CI must reflect declared contour scope
Hosted CI must remain fail-closed, but it must not force unrelated heavy surfaces onto every PR.

### O5. Runtime proofs must be portable and reproducible
Docker/Linux proof paths must behave deterministically across hosted and local environments.

### O6. Multi-module governed execution must scale
The governed entrypoint must support more than one active module without collapsing into a hard failure.

## 4. Non-Goals

The hardening program does not aim to:
- re-open already accepted product-plane phases by default;
- change business or trading logic;
- remove fail-closed behavior from current gates;
- bypass PR-only policy for `main`;
- replace existing module/phase governance with a separate workflow engine;
- collapse architecture truth-source into a single file.

## 5. Architecture Principles

### P1. Truth over convenience
If the pipeline can either stay strict or look smoother, strictness wins.

### P2. One snapshot per decision
Every gate decision must clearly state what state it evaluated:
- working tree;
- staged tree;
- git range;
- explicit artifact snapshot.

Hidden mixing is forbidden.

### P3. Contour isolation with recomposition support
The system must preserve single-contour patch discipline while still supporting later recomposition against updated `main`.

### P4. Runtime-profile explicitness
Every proof path must declare its supported profile:
- local Windows;
- local Docker/Linux;
- hosted Ubuntu;
- governed simulation.

Profile assumptions must never remain implicit.

### P5. Idempotent governance operations
Task-session bootstrap, route discovery, branch binding, and proof artifact generation must be safely repeatable.

## 6. Current Weakness Map

| ID | Weakness | Operational Impact |
| --- | --- | --- |
| W1 | Pre-push and contour validation always reason against `origin/main` only | Forces manual branch re-cut after first contour merge |
| W2 | Validators read git diff but task note from working tree | Allows mismatch between evaluated code and evaluated intent |
| W3 | `task_session.py begin` writes tracked docs immediately | Creates dirty tree and branch-switch friction before stable intent |
| W4 | PR lane always runs full app matrix | Small contour PRs fail on unrelated hosted/runtime issues |
| W5 | Heavy dependencies live in base install path | Any PR inherits Spark/Dagster/Delta setup cost and failure risk |
| W6 | Governed route selection supports only one active module | Parallel governed programs are blocked or require manual override |
| W7 | Docker proof scripts assume filesystem/container behavior too loosely | Hosted Linux failures emerge late and expensively |
| W8 | Git mutation flow is vulnerable to lock/contention issues | Commit/push automation becomes flaky under repeated operations |
| W9 | Post-merge truth-source recomposition is manual | Split branches encode temporary downgrades that are hard to reassemble safely |

## 7. Target Operating Model

The target operating model is a governed pipeline with five explicit layers:

1. Route layer
Determines package intake, module continuation, stacked follow-up, or explicit module selection.

2. Session layer
Tracks operator/session/task intent in durable but mostly untracked state until promotion is required.

3. Validation layer
Evaluates an explicit snapshot and contour contract, not a hidden mix of sources.

4. Proof/CI layer
Runs profile-aware proofs and only the required surface-aware test matrix.

5. Recomposition layer
Supports safe continuation after partial merges and safe promotion of split branches back into unified truth.

## 8. Functional Requirements

### FR-01. Stacked Follow-Up Route

The governed entrypoint shall support an explicit route mode for post-merge continuation, referred to in this specification as `stacked-followup`.

Required behavior:
- Operator may point to:
  - a source branch;
  - a merged predecessor PR or merge commit;
  - the surviving contour/module contract.
- The entrypoint resolves:
  - current `origin/main`;
  - already-merged contour history;
  - the remaining contour to be rebuilt.
- The route emits a machine-readable continuation contract stating:
  - predecessor contour already merged;
  - new base ref;
  - surfaces allowed to carry forward;
  - temporary downgrade surfaces that must not survive recomposition.

Acceptance:
- A second contour can be rebuilt from updated `main` without manual branch archaeology.
- Route report explicitly captures predecessor merge and new base.

### FR-02. Explicit Validation Snapshot Mode

All policy-critical validators shall support an explicit snapshot selector.

Required snapshot modes:
- `working-tree`
- `staged`
- `git-range`
- `artifact-snapshot`

Required behavior:
- `run_loop_gate.py` and `run_pr_gate.py` must declare the snapshot mode they use in their output.
- `validate_solution_intent.py` and `validate_critical_contour_closure.py` must evaluate intent and code against the same snapshot class.
- If snapshot sources disagree and no policy defines precedence, the validator must fail closed.

Acceptance:
- The pipeline must no longer treat uncommitted task-note edits and committed code diff as one implicit state.

### FR-03. Two-Phase Task Session

Task-session lifecycle shall be split into:
- `begin-ephemeral`
- `promote-tracked`

Required behavior:
- `begin-ephemeral` writes only untracked or runlog state.
- `promote-tracked` creates or updates:
  - `docs/session_handoff.md`
  - active task note
  - active index entry
- Promotion happens only when:
  - first code/doc patch is about to be committed; or
  - operator explicitly requests tracked session state.

Acceptance:
- A task can be explored and reframed without dirtying tracked docs.
- Branch switching does not require session-state stash churn by default.

### FR-04. Multi-Module Governed Selection

The governed entrypoint shall support more than one active module.

Required behavior:
- When multiple modules are active, route selection must not hard-fail immediately.
- The system must support:
  - explicit module slug selection;
  - explicit priority rule;
  - machine-readable ambiguity report.
- Auto mode may still fail, but only after generating a precise resolution contract.

Acceptance:
- Two concurrent governed programs can coexist without forcing manual file inspection.

### FR-05. Contour-Aware Branch Policy

Branch governance shall distinguish:
- first contour against `main`;
- subsequent contour after first contour merge;
- explicit integration/recomposition branch when policy allows it.

Required behavior:
- Pre-push policy must understand whether a branch is:
  - first-wave contour;
  - stacked follow-up contour;
  - integration branch with declared prior merged contour.
- Validation against `origin/main` remains valid for first-wave work.
- For stacked follow-up, policy must use the new declared base contract instead of treating the branch as a forbidden mixed patch.

Acceptance:
- Second contour does not require ad hoc branch re-splitting solely because first contour already merged.

### FR-06. CI Dependency Profiles

Python dependency model shall be split into at least the following profiles:
- base shell/governance
- runtime/API
- data-proof
- proof-docker
- dev/test umbrella

Required behavior:
- CI jobs install only the dependency profile they need.
- `tests/app` must not be the only supported mode for PR validation.
- Profile names must be documented and stable.

Acceptance:
- Runtime PR can pass hosted CI without pulling the full data-proof stack unless it truly changes that surface.

### FR-07. Surface-Aware PR Matrix

Hosted PR lane shall select test and proof sets by surface.

Required behavior:
- runtime/publication contour:
  - runtime tests
  - sidecar tests
  - relevant docs/contract checks
- data contour:
  - Delta/Spark/Dagster proofs
  - research handoff tests
- mixed contour:
  - explicit fail-closed or integration profile

Acceptance:
- Unrelated heavy proofs are not mandatory for every PR.
- Surface-aware selection is visible in CI logs and summary artifacts.

### FR-08. Portable Proof Runner Contract

All governed proof runners that use Docker or host-container path exchange shall follow a shared runtime contract.

Required contract points:
- explicit runtime root inside container;
- deterministic writable cache locations;
- deterministic host-to-container and container-to-host path normalization;
- post-run ownership normalization when required;
- explicit profile marker in proof output;
- negative tests for:
  - unwritable output path;
  - missing runtime root;
  - cross-platform path normalization.

Acceptance:
- Hosted Linux and local Windows do not diverge silently on artifact semantics.

### FR-09. Git Mutation Serialization

Governed tooling that stages, commits, updates indexes, or pushes must serialize git mutations.

Required behavior:
- No parallel git write operations in the same repo.
- A lightweight repo mutation lock must exist.
- If lock exists, the caller must wait or fail with a precise retry contract.
- `index.lock` recovery must never hide a still-running git process.

Acceptance:
- Repeated commit/push sequences do not intermittently fail due to self-inflicted lock contention.

### FR-10. Truth Recomposition Support

The pipeline shall support truth-source recomposition after split-branch merge.

Required behavior:
- A branch may declare temporary truth downgrades that exist only to keep a split contour honest before merge.
- After predecessor merge, follow-up branch must be able to:
  - detect obsolete temporary downgrades;
  - restore already merged truth;
  - add new contour truth on top.
- This recomposition must be machine-assisted, not manual-only.

Acceptance:
- Second contour does not regress already merged status/registry claims by accident.

### FR-11. Route-State and Audit Enrichment

Route state artifacts shall include:
- chosen route type;
- predecessor merge context if any;
- snapshot mode used by validators;
- dependency profile used by CI/proof;
- contour class;
- recomposition status.

Acceptance:
- A future operator can reconstruct why the pipeline made a decision without reading chat history.

## 9. Non-Functional Requirements

### NFR-01. Fail-Closed
Any unresolved ambiguity in route, snapshot, contour, or proof profile must fail closed.

### NFR-02. Determinism
The same repo state and same declared profile must produce the same validator result.

### NFR-03. Platform Transparency
If a step is profile-dependent, the profile must be logged and carried into artifacts.

### NFR-04. Low Governance Drift
Tracked governance docs must not be rewritten unless content actually changes.

### NFR-05. Backward-Compatible Adoption
Existing commands may remain as compatibility wrappers, but must emit deprecation guidance and consistent behavior.

## 10. Workstreams

### WS-1. Route and Continuation Hardening
Primary outcome:
- `stacked-followup` route
- multi-module selection
- richer route-state contract

Primary surfaces:
- governed entrypoint
- route-state artifacts
- module selection logic

### WS-2. Session Lifecycle Hardening
Primary outcome:
- ephemeral session mode
- explicit promotion to tracked docs
- reduced handoff/index churn

Primary surfaces:
- task-session lifecycle
- handoff writer
- active/archive indexes

### WS-3. Validation Consistency
Primary outcome:
- explicit snapshot mode
- shared note/code evaluation contract
- consistent loop/pr gate reporting

Primary surfaces:
- solution-intent validator
- contour-closure validator
- loop/pr gate wrappers

### WS-4. CI and Dependency Profiling
Primary outcome:
- install profiles
- surface-aware test matrix
- reduced hosted-only surprise failures

Primary surfaces:
- `pyproject.toml`
- GitHub Actions workflows
- docs/checks and workflow docs

### WS-5. Proof Runner Portability
Primary outcome:
- common proof runtime contract
- Linux/Windows path and ownership normalization
- negative coverage for filesystem and container assumptions

Primary surfaces:
- proof scripts
- proof fixtures
- proof unit/integration tests

### WS-6. Git Operation Safety
Primary outcome:
- mutation serialization
- lock protocol
- safe retry semantics

Primary surfaces:
- git-writing scripts
- optional repo mutation lock helper

### WS-7. Truth Recomposition
Primary outcome:
- formal model for temporary split-branch truth
- recomposition helper and validator

Primary surfaces:
- stack-conformance registry tools
- branch/merge workflow docs
- recomposition artifacts

## 11. Deliverables

Mandatory deliverables:
1. Updated governed route contract and route-state schema.
2. Ephemeral/promoted task-session lifecycle implementation.
3. Snapshot-aware validator contract.
4. CI dependency profiles and surface-aware PR matrix.
5. Shared proof-runner runtime contract and regression tests.
6. Git mutation serialization helper and tests.
7. Truth recomposition helper, rules, and regression tests.
8. Updated documentation:
   - workflow docs
   - checks matrix
   - remediation/runbook docs
   - architecture/governance references

## 12. Acceptance Matrix

### A1. Sequential Contour Acceptance
- Given contour A is merged into `main`,
- when contour B is continued through governed route,
- then the route resolves a new base and produces a valid follow-up branch contract without manual branch reconstruction.

### A2. Snapshot Consistency Acceptance
- Given a diff and a task note,
- when validators run in `git-range` mode,
- then note and code are evaluated from the same declared snapshot model or the validator fails closed.

### A3. Session Cleanliness Acceptance
- Given a new task begins,
- when operator only explores and does not promote,
- then tracked docs remain unchanged.

### A4. Multi-Module Acceptance
- Given two active modules,
- when auto route is ambiguous,
- then the operator receives a machine-readable selection contract rather than a bare hard failure.

### A5. CI Scope Acceptance
- Given a runtime-only PR,
- when hosted CI runs,
- then it uses runtime-relevant dependency and test profiles and does not require unrelated data-proof execution.

### A6. Proof Portability Acceptance
- Given the same proof script runs on hosted Ubuntu and local Windows via Docker profile,
- when outputs are materialized,
- then artifact paths, ownership, and runtime caches remain valid and readable on the host.

### A7. Git Safety Acceptance
- Given repeated commit/push automation,
- when two repo-writing operations are attempted concurrently,
- then one waits or fails clearly without corrupting git state.

### A8. Truth Recomposition Acceptance
- Given a split contour branch contains temporary downgrades,
- when predecessor contour has already merged,
- then follow-up recomposition restores merged truth and applies only the remaining contour deltas.

## 13. Test Strategy

Test layers required:

1. Unit
- route selection
- snapshot mode selection
- session state transitions
- dependency profile resolution
- proof path normalization
- git mutation lock semantics

2. Integration
- split branch -> first merge -> follow-up rebuild
- multi-module active state
- hosted profile proof execution
- truth recomposition after predecessor merge

3. Governance regression
- loop gate against first contour
- loop gate against follow-up contour
- PR gate against runtime-only contour
- PR gate against data-only contour
- fail-closed mixed-contour rejection without explicit integration route

4. Hosted CI replay
- runtime/publication PR on Ubuntu
- data-proof PR on Ubuntu
- selected local Windows replay for proof scripts that remain operator-facing

## 14. Rollout Plan

### Phase H0. Contract Introduction
- introduce route/snapshot/profile schemas
- document ephemeral session concept
- no behavior switch yet

### Phase H1. Dual-Mode Operation
- old commands still work
- new route and session modes available
- validators report snapshot/profile metadata

### Phase H2. CI and Proof Refactor
- dependency profiles
- surface-aware PR matrix
- portable proof runner contract

### Phase H3. Recomposition and Multi-Module Enablement
- stacked follow-up route
- multi-module selection
- truth recomposition helper

### Phase H4. Enforcement Upgrade
- deprecate legacy implicit behavior
- require explicit snapshot/profile markers
- enable serialization guard for git writes

## 15. Risks

### R1. Overfitting to current contours
The implementation must stay generic enough for future contours beyond data/runtime.

### R2. Governance complexity creep
If new modes are added without operator ergonomics, the system becomes technically correct but unusable.

### R3. CI fragmentation
Surface-aware CI must not turn into undocumented matrix sprawl.

### R4. Backward-compatibility drag
Compatibility wrappers must not preserve hidden old behavior indefinitely.

## 16. Open Questions

1. Should stacked follow-up route require explicit predecessor PR number, or may it infer by merge commit ancestry?
2. Which snapshot mode should be canonical for local pre-commit validation: `staged` or `git-range`?
3. Should truth recomposition write back directly, or produce a proposed patch/report for operator confirmation?
4. Is multi-module auto-priority ever acceptable, or should ambiguity always require explicit module slug?

## 17. Definition of Done

This hardening program is complete only when all of the following are true:

1. Sequential contour follow-up no longer requires manual branch reconstruction.
2. Validators evaluate a declared single snapshot model.
3. Session bootstrap no longer dirties tracked docs by default.
4. Hosted CI uses surface-aware dependency/test profiles.
5. Docker/Linux proof paths are stable across hosted and local environments.
6. Multiple active modules are supported through governed selection, not hard failure.
7. Git mutation contention is explicitly managed.
8. Truth recomposition after split-branch merge is supported by tooling, not only by operator memory.
9. Workflow and remediation docs are updated to the new model.
