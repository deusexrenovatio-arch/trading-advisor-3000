# Route Snapshot Profile Contract (H0)

Updated: 2026-04-01 20:15 UTC

## Purpose

Declare one shared vocabulary for governed route, snapshot, profile, and session lifecycle terms.

This phase is declaration-only:
- no runtime entrypoint behavior is switched;
- no validator/gate enforcement is switched.

## Contract Status

- Phase: `H0 - Contract Introduction`
- Contract Level: `declared`
- Enforcement Level: `none` (enforcement is owned by later phases)

## Route Contract

### Route layers

1. Entry route
- Values: `auto`, `package`, `continue`, `stacked-followup`
- Owner: `scripts/codex_governed_entry.py`
- Meaning: how governed flow is selected before work starts.

2. Phase route
- Values: `worker`, `acceptance`, `remediation`, `unlock`
- Owner: `scripts/codex_phase_orchestrator.py`
- Meaning: phase execution and decision lane inside one module.

3. Route signal
- Format: `<role>:<scope>`
- Current examples: `worker:phase-only`, `acceptance:governed-phase-route`, `remediation:phase-only`
- Meaning: explicit machine-readable claim of which governed role produced the payload.

### Route record (declared schema)

Required fields:
- `route_mode`: orchestration mode id (for example `governed-phase-orchestration`)
- `entry_route`: one of `auto|package|continue|stacked-followup`
- `route_signal`: role-scoped signal string
- `phase_brief`: current phase brief path
- `reason`: explicit route reason (human-readable)

Optional fields:
- `attempt`: positive integer for phase attempts
- `module`: `{slug, execution_contract, parent_brief, current_phase}` when module route is active
- `module_resolution`: selection rationale when route resolution involved module selection policy
- `module_candidates`: active module candidates considered during route selection
- `module_ambiguity_report`: machine-readable ambiguity contract path when unresolved selection blocked continuation
- `continuation_contract_path`: stacked follow-up continuation contract artifact path

## Snapshot Contract

### Snapshot classes

- `changed-files`: file-delta snapshot for one phase attempt.
- `phase-state`: orchestration state snapshot for one phase run.
- `route-report`: human-readable route trace snapshot.
- `contract-only`: declaration marker used when no additional runtime snapshot is introduced yet.

### Snapshot record (declared schema)

Required fields:
- `snapshot_class`: one of the classes above
- `artifact_path`: repository-relative artifact path
- `captured_at_utc`: UTC timestamp

Optional fields:
- `git_ref`: commit or symbolic ref used for capture
- `producer`: script or role that created the snapshot

## Profile Contract

Profiles are explicit execution markers for governed roles and evidence runs.

### Profile record (declared schema)

Required fields:
- `role`: `worker|acceptor|remediation|gate|proof`
- `profile`: profile id string, or `none` when unset

Optional fields:
- `model`: model id for the role when relevant
- `source`: `cli`, `default`, or `inherited`
- `notes`: human-readable constraint notes

## Session Lifecycle Contract

The lifecycle is expressed as two levels:

1. `ephemeral_begin`
- Route is opened, but promotion boundary is not yet crossed.
- No acceptance or unlock decision is implied.

2. `tracked_session`
- Durable state is present (task note, session handoff pointer, orchestration artifacts, or equivalent governed state files).
- Phase attempts and acceptance outcomes are traceable.

Promotion boundary:
- `ephemeral_begin -> tracked_session` happens when durable governed state is written.

Terminal statuses:
- `completed`
- `partial`
- `blocked`

H0 compatibility note:
- current runtime may create tracked state immediately at begin;
- this document defines vocabulary only and does not rewire lifecycle behavior in H0.

## H0 Non-Enforcement Clause

- Missing snapshot/profile markers must not be treated as a new failure condition in H0.
- Existing entrypoints and gate commands remain unchanged in this phase.
- Any enforcement upgrade belongs to later phases (`H1` and beyond).

## Forward Binding

- `H1`: expose dual-mode route/session semantics and snapshot/profile metadata in outputs.
- `H2`: bind profile-aware CI/proof behavior with staging-real evidence.
- `H3`: bind stacked follow-up continuation contracts, multi-module ambiguity contracts, and recomposition helper/validator artifacts.
- `H4`: enforce fail-closed marker requirements in policy-critical validations.

## H1 Runtime Binding Note

`H1` runtime outputs now expose these markers explicitly while preserving legacy command compatibility:

- governed entry route-state fields: `route_mode`, `session_mode`, `snapshot_mode`, `profile`, `entry_route`, `route_signal`;
- loop/pr gate status lines: `snapshot_mode`, `profile`.
- governed bootstrap normalizes `route_mode` and `snapshot_mode` to canonical values before writing durable bootstrap state.
- governed bootstrap session reuse is fail-closed for mode drift: when active task-session mode and requested mode differ, bootstrap stops with an error and does not emit conflicting state.

This phase is still `prep_closed` only; it does not claim real contour closure for CI/proof portability.

## H2 Runtime Binding Note

`H2` binds profile metadata into hosted PR execution and proof portability operations:

- PR contour planner/executor: `scripts/run_surface_pr_matrix.py`;
- CI artifacts include contour/profile plans and gate summaries under `artifacts/ci/`;
- shared docker proof runtime contract utilities live in `scripts/proof_runtime_contract.py`;
- `scripts/run_phase2a_spark_proof.py` now uses shared runtime-root/path/output guards.

This update raises CI/proof behavior to `staging-real` for the owned contour only.

## H3 Runtime Binding Note

`H3` binds stacked follow-up continuation and recomposition artifacts into governed route execution:

- entry route now supports `stacked-followup` with required merge/base context (`predecessor-ref`, `source-branch`, `new-base-ref`);
- multi-module selection supports explicit `module-slug` and `module-priority` contracts;
- unresolved multi-module auto/continue selection now emits machine-readable ambiguity report before fail-closed exit:
  - `.runlogs/codex-governed-entry/module-ambiguity-report.json`
- stacked follow-up route emits machine-readable continuation contract artifact:
  - `.runlogs/codex-governed-entry/stacked-followup-contract.json`
- truth recomposition helper/validator scripts are available:
  - `scripts/truth_recomposition.py build`
  - `scripts/truth_recomposition.py validate`

This phase raises stacked follow-up/recomposition contour behavior to `staging-real` for owned surface closure only.
