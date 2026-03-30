# Module Phase Brief

Updated: 2026-03-30 10:08 UTC

## Parent

- Parent Brief: docs/codex/modules/f1-full-closure.parent.md
- Execution Contract: docs/codex/contracts/f1-full-closure.execution-contract.md

## Phase

- Name: F1-A - Truth-Source and Validator Repair
- Status: completed

## Objective

- Remove false closure language, align the truth-source bundle, and extend stack-conformance validation so acceptance-report surfaces fail closed instead of overclaiming.

## In Scope

- `docs/architecture/app/STATUS.md`
- `docs/architecture/app/stack-conformance-baseline.md`
- `docs/architecture/app/phase10-stack-conformance-reacceptance-report.md`
- `artifacts/acceptance/f1/*`
- `docs/codex/modules/*` where stack or acceptance claims materially affect governed reading
- `registry/stack_conformance.yaml`
- `scripts/validate_stack_conformance.py`
- validator tests

## Out Of Scope

- implementing real Telegram closure
- closing replaceable technologies by runtime work
- broker or sidecar implementation changes beyond validator-proof needs
- final F1 decision regeneration

## Change Surfaces

- ARCH-DOCS
- CTX-CONTRACTS
- GOV-RUNTIME
- PROCESS-STATE

## Constraints

- Either add real ADR-supported retirement decisions for claimed removed technologies or delete unsupported removal claims; do not keep mixed truth.
- `docs/architecture/app/STATUS.md`, the baseline, acceptance reports, registry, stack spec, and ADR set must agree after the phase.
- Route reports stay orchestration metadata, not capability proof.

## Acceptance Gate

- A deliberate contradiction between report, registry, spec, ADR, or status surfaces fails validation.
- The validator covers reacceptance reports, red-team result docs, module briefs carrying stack claims, and claim-carrying evidence-pack text.
- Touched docs contain no unsupported `accepted`, retirement-claim, or readiness language.

## Disprover

- Reinsert an unsupported phrase such as `aiogram removed by ADR` into a claim-carrying report without matching registry/spec/ADR updates and confirm validation fails.

## Done Evidence

- Truth-source drift across `STATUS`, baseline, phase10 report, red-team result, and registry is closed.
- The stack-conformance validator and tests fail closed on the newly covered claim surfaces.
- Acceptance-report level overclaims are blocked by deterministic checks.

## Release Gate Impact

- Surface Transition: truth-source integrity contradictions `reopened -> aligned`
- Minimum Proof Class: doc
- Accepted State Label: prep_closed

## Release Surface Ownership

- Owned Surfaces: truth_source_and_validator_integrity
- Delivered Proof Class: doc
- Required Real Bindings: none
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Live contour closure: this phase does not prove real data, real publication chat, or real broker readiness.

## Rollback Note

- Revert the validator expansion and truth-source wording together if the phase broadens scope without restoring mutually consistent evidence.
