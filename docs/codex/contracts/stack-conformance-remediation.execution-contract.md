# Execution Contract

Updated: 2026-03-27 10:18 UTC

## Source Package

- Package Zip: C:/Users/Admin/Downloads/trading_advisor_3000_stack_conformance_remediation_2026-03-24 (1).zip
- Package Manifest: D:/trading advisor 3000/artifacts/codex/package-intake/20260325T152331Z-trading-advisor-3000-stack-conformance-remediati/manifest.md
- Manifest Suggested Primary Document: D:/trading advisor 3000/artifacts/codex/package-intake/20260325T152331Z-trading-advisor-3000-stack-conformance-remediati/extracted/ta3000_stack_conformance_remediation_2026-03-24/00_validation_verdict.md
- Source Title: Trading Advisor 3000 stack-conformance remediation package

## Primary Source Decision

- Selected Primary Document: D:/trading advisor 3000/artifacts/codex/package-intake/20260325T152331Z-trading-advisor-3000-stack-conformance-remediati/extracted/ta3000_stack_conformance_remediation_2026-03-24/02_corrective_technical_assignment.md
- Selection Rule: the manifest produced a top-score tie across several substantive markdown documents, so the primary was resolved by preferring the directive execution source that defines objective, scope, hard rules, deliverables, and success criteria over verdict, matrix, checklist, or appendix documents.
- Supporting Documents:
  - the package validation verdict for issue confirmation and severity.
  - the package phase plan and acceptance gates for the atomic merge phases, acceptance gates, and disprovers.
  - the package anti-self-deception controls for the evidence hierarchy and no-proxy rules.
  - the package stack matrix, backlog, registry template, evidence template, red-team checklist, and evidence map for prioritization, templates, and traceability.
- Conflict Status: no material contradiction was found between the selected primary and its supporting documents.

## Prompt / Spec Quality

- Verdict: READY
- Why: the package defines the problem statement, success criteria, hard rules, phase order, acceptance/disprover logic, and machine-readable templates clearly enough for governed phase planning without extra clarification.

## Normalization Note

- This governed intake does not implement the remediation itself.
- The package explicitly declares atomic merge phases from governance repair through re-acceptance, so this run normalizes the package into the canonical module path under `docs/codex/` and stops before any declared remediation phase is collapsed into the intake patch.
- A follow-on package dated 2026-03-30 converted the denied F1 outcome into `docs/codex/contracts/f1-full-closure.execution-contract.md`; this earlier contract remains historical route evidence rather than the active continuation pointer.

## Objective

- Convert the stack-conformance remediation package into explicit governed module-phase orchestration so future work can proceed phase by phase instead of as one package-wide implementation burst.

## In Scope

- One canonical execution contract under `docs/codex/contracts/`.
- One module parent brief under `docs/codex/modules/`.
- One phase brief per declared package phase under `docs/codex/modules/`.
- Task-note normalization that records the chosen primary document and the tie-break rule.

## Out Of Scope

- Implementing the planned stack-conformance baseline doc, acceptance vocabulary doc, machine-readable stack registry, or stack-conformance validator in this intake patch.
- Closing Delta, Spark, Dagster, Postgres default-runtime, FastAPI, aiogram, vectorbt, Alembic, OpenTelemetry, Polars, DuckDB, or .NET sidecar surfaces.
- Re-accepting product phases or generating release evidence.

## Constraints

- Do not call the governed launcher again from inside this runtime prompt.
- Keep `docs/session_handoff.md` as a lightweight pointer shim.
- Use canonical gate names only.
- Respect the package merge policy: governance phases first, then product/runtime closure phases, then final re-acceptance.
- Do not mix governance-repair, validator, runtime-closure, and release-claim work into one patch set.
- Keep shell control-plane surfaces free of trading logic.

## Contract Amendment (Phase 10 F1)

- Amendment ID: `F1-2026-03-27-release-readiness-decision-contract`
- Trigger: acceptance attempt-02 blocker set (`B1`, `B2`) on governed phase-10 route.
- Package baseline retained: source package F1 acceptance bar still requires full implemented/ADR-removed readiness state before any `ALLOW_RELEASE_READINESS` claim.
- Governed clarification for this phase contract:
  - F1 deliverable is an explicit release-readiness decision package, not an implied unlock.
  - `ALLOW_RELEASE_READINESS` remains forbidden while any architecture-critical surface is `partial`, `planned`, or `not accepted`, or while any replaceable surface is neither implemented nor ADR-removed.
  - `DENY_RELEASE_READINESS` is valid only when every blocker is explicitly mapped to truth-source state with no overclaiming language.
- Unlock rule under this amendment:
  - A `DENY_RELEASE_READINESS` package closes evidence assembly for F1 review but does not unlock progression.
  - Unlock remains blocked until a later governed continuation closes prerequisite surfaces with executable proof and a new acceptance cycle returns `ALLOW_RELEASE_READINESS`.

## Done Evidence

- `docs/codex/contracts/stack-conformance-remediation.execution-contract.md` exists.
- `docs/codex/modules/stack-conformance-remediation.parent.md` exists.
- `docs/codex/modules/stack-conformance-remediation.phase-01.md` through `docs/codex/modules/stack-conformance-remediation.phase-10.md` exist.
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Primary Change Surfaces

- PROCESS-STATE
- GOV-DOCS
- ARCH-DOCS

## Routing

- Path: module
- Rationale: the source package explicitly decomposes work into ordered atomic phases with acceptance and disprover rules, so the correct continuation is governed module execution rather than a one-shot package implementation patch.

## Mode Hint

- continue

## Next Allowed Unit Of Work
- Continue through `docs/codex/contracts/f1-full-closure.execution-contract.md` and `docs/codex/modules/f1-full-closure.parent.md`; do not reopen the denied phase-10 decision route inside this historical module path.
## Suggested Branch / PR

- Branch: codex/stack-conformance-remediation-phase01
- PR Title: Normalize stack conformance remediation module and start G0 claim freeze
