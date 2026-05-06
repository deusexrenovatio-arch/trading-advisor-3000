# Module Phase Brief

Updated: 2026-03-30 10:08 UTC

## Parent

- Parent Brief: docs/codex/modules/f1-full-closure.parent.md
- Execution Contract: docs/codex/contracts/f1-full-closure.execution-contract.md

## Phase

- Name: F1-A - Truth-Source and Validator Repair
- Status: completed

## Objective

- убрать ложные truth-source claims и сделать validator реально fail-closed.

## In Scope

- `docs/archive/legacy-app-docs/2026-05-06/STATUS.md`
- `docs/archive/legacy-app-docs/2026-05-06/stack-conformance-baseline.md`
- `docs/archive/legacy-app-docs/2026-05-06/phase10-stack-conformance-reacceptance-report.md`
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
- `docs/archive/legacy-app-docs/2026-05-06/STATUS.md`, the baseline, acceptance reports, registry, stack spec, and ADR set must agree after the phase.
- Route reports stay orchestration metadata, not capability proof.

## Acceptance Gate

- Любой deliberate contradiction между report/registy/spec/ADR должен ломать validator.
- `docs/archive/legacy-app-docs/2026-05-06/STATUS.md`, baseline, phase10 report, red-team result и registry согласованы.
- В touched docs нет overclaiming beyond truth-source state.

## Disprover

- Снова вернуть фразу `aiogram removed by ADR` только в phase10 report, не меняя registry/spec/ADR, и убедиться, что CI падает.

## Done Evidence

- Truth-source drift across `STATUS`, baseline, phase10 report, red-team result, and registry is closed.
- The stack-conformance validator and tests fail closed on the newly covered claim surfaces.
- Acceptance-report level overclaims are blocked by deterministic checks.
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD` passes for the phase-scoped diff.
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD` passes and is recorded in worker evidence.

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
