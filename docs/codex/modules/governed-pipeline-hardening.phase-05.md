# Module Phase Brief

Updated: 2026-04-01 11:20 UTC

## Parent

- Parent Brief: docs/codex/modules/governed-pipeline-hardening.parent.md
- Execution Contract: docs/codex/contracts/governed-pipeline-hardening.execution-contract.md

## Phase

- Name: H4 - Enforcement Upgrade
- Status: blocked

## Objective

- Deprecate implicit legacy behavior, require explicit snapshot/profile markers for policy-critical decisions, and enforce serialized git mutations for governed writes.

## In Scope

- Deprecation guidance for legacy implicit flows.
- Enforcement of explicit snapshot/profile marker requirements in loop/pr critical validators.
- Repository mutation serialization guard for git write operations and retry semantics.
- Explicit release decision package emission (`ALLOW_RELEASE_READINESS` or `DENY_RELEASE_READINESS`) under live-real evidence rules.

## Out Of Scope

- Reopening H0-H3 design assumptions except via explicit remediation.
- Product-plane runtime feature delivery.
- Downgrading fail-closed behavior for operator convenience.

## Change Surfaces

- GOV-RUNTIME
- GOV-DOCS
- PROCESS-STATE

## Constraints

- Any unresolved ambiguity in route/snapshot/profile/contour semantics must fail closed.
- `index.lock` handling must not hide still-running git operations.
- Release decision output must map directly to truth-source and immutable evidence artifacts.

## Acceptance Gate

- Legacy implicit behavior is deprecated with consistent operator guidance.
- Missing or conflicting snapshot/profile markers in policy-critical validation paths fail closed.
- Concurrent governed git write attempts serialize safely with clear retry contracts.
- Final decision package emits explicit `ALLOW_RELEASE_READINESS` or `DENY_RELEASE_READINESS` grounded in live-real evidence.
- Final release-decision emission is acceptance-owned and must bind to acceptance artifacts from a real governed `continue` route run (`--dry-run` evidence is not valid live-real closure).

## Disprover

- If snapshot/profile markers are removed and policy-critical gates still pass, or concurrent git writes proceed without serialization/clear failure, this phase fails acceptance.

## Release Gate Impact

- Surface Transition: enforcement and serialization contour `staging-real -> live-real decision`
- Minimum Proof Class: live-real
- Accepted State Label: release_decision

## Release Surface Ownership

- Owned Surfaces: enforcement_serialization_contour
- Delivered Proof Class: live-real
- Required Real Bindings: explicit snapshot/profile markers in gate output, repo mutation lock evidence, live-real release decision artifact bundle
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- A positive release decision is not automatic: if prerequisites remain open, only `DENY_RELEASE_READINESS` is valid.
- This phase does not retroactively prove earlier phases if their required evidence is missing.

## Rollback Note

- Revert enforcement-level marker and serialization changes together if they produce contradictory gate outcomes or unsafe repository mutation behavior.
