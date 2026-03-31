# Execution Contract

Updated: 2026-03-30 10:08 UTC

## Source Package

- Package Zip: C:/Users/Admin/Downloads/trading_advisor_3000_phase_acceptance_upto_F1_2026-03-30.zip
- Package Manifest: D:/trading advisor 3000/artifacts/codex/package-intake/20260330T100458Z-trading-advisor-3000-phase-acceptance-upto-f1-20/manifest.md
- Manifest Suggested Primary Document: D:/trading advisor 3000/artifacts/codex/package-intake/20260330T100458Z-trading-advisor-3000-phase-acceptance-upto-f1-20/extracted/01_phase_acceptance_verdict.md
- Source Title: Trading Advisor 3000 phase acceptance up to F1 and F1 full-closure package

## Primary Source Decision

- Selected Primary Document: D:/trading advisor 3000/artifacts/codex/package-intake/20260330T100458Z-trading-advisor-3000-phase-acceptance-upto-f1-20/extracted/03_f1_full_closure_TZ.md
- Selection Rule: the manifest produced a top-score tie across several substantive markdown documents, so the primary was resolved by preferring the directive execution source that defines the F1 objective, ordered rollout phases, hard rules, acceptance gates, disprovers, and release-decision semantics over verdict, findings, matrix, or evidence-manifest documents.
- Supporting Documents:
  - `artifacts/codex/package-intake/20260330T100458Z-trading-advisor-3000-phase-acceptance-upto-f1-20/extracted/01_phase_acceptance_verdict.md` for the accepted versus rejected phase baseline that constrains what the F1 closure route may assume.
  - `artifacts/codex/package-intake/20260330T100458Z-trading-advisor-3000-phase-acceptance-upto-f1-20/extracted/02_detailed_phase_findings.md` for blocker detail, cross-phase contradictions, and the evidence gaps behind `G0`, `G1`, `R2`, `S1`, and conditional `E1`.
  - `artifacts/codex/package-intake/20260330T100458Z-trading-advisor-3000-phase-acceptance-upto-f1-20/extracted/04_anti_self_deception_controls.md` for truth-source precedence, negative-test obligations, and the no-proxy evidence model.
  - `artifacts/codex/package-intake/20260330T100458Z-trading-advisor-3000-phase-acceptance-upto-f1-20/extracted/05_source_evidence_manifest.md` and `07_phase_status_matrix.json` for source traceability and machine-readable scope reconstruction.
- Conflict Status: no material contradiction was found between the selected primary and its supporting documents; the supporting set sharpens blocker detail and evidence rules without changing the F1 phase order.

## Prompt / Spec Quality

- Verdict: READY
- Why: the selected primary states the closure objective, current blockers, ordered phases, allowed outcomes, acceptance gates, disprovers, and evidence discipline clearly enough to normalize the package into an explicit governed module path without further clarification.

## Normalization Note

- This intake run does not implement F1 closure itself.
- The selected primary defines an explicit follow-on rollout `F1-A` through `F1-F`, so this patch only materializes the canonical execution contract and phase briefs under `docs/codex/`.
- The earlier `stack-conformance-remediation` module remains historical evidence of the denied release-readiness decision, but its active continuation pointer is retired so the new F1 closure module becomes the single governed next path.

## Objective

- Convert the F1 acceptance and full-closure package into an explicit governed follow-on module that closes release blockers phase by phase instead of collapsing governance repair, stack cleanup, contract hardening, broker closure, and final readiness into one implementation burst.

## Release Target Contract

- Target Decision: ALLOW_RELEASE_READINESS
- Target Environment: real production contour with live data, configured publication chat, real broker process, governed secrets/config, and immutable release artifacts
- Forbidden Proof Substitutes: docs-only, schema-only, fixture-only, mock-only, stub-only, smoke-only, staging-only, route-report-only
- Release-Ready Proof Class: live-real

## Mandatory Real Contours

- publication_chat_contour: configured real publication chat/channel with real credentials, real message lifecycle, and replayable evidence
- broker_execution_contour: real broker connector contour with real config/secrets, executable order lifecycle, and governed failure-mode proof
- production_live_readiness: integrated live-real readiness bundle proving the live data, publication, broker, and operational contours together

## Release Surface Matrix

- Surface: truth_source_and_validator_integrity | Owner Phase: F1-A | Required Proof Class: doc | Must Reach: aligned_truth_source
- Surface: replaceable_stack_alignment | Owner Phase: F1-B | Required Proof Class: integration | Must Reach: terminal_non_ghost_state
- Surface: publication_chat_contour | Owner Phase: F1-B | Required Proof Class: live-real | Must Reach: real_configured_chat
- Surface: contracts_freeze | Owner Phase: F1-C | Required Proof Class: integration | Must Reach: versioned_release_blocking_boundaries
- Surface: sidecar_immutable_evidence | Owner Phase: F1-D | Required Proof Class: staging-real | Must Reach: immutable_replayable_sidecar_proof
- Surface: broker_execution_contour | Owner Phase: F1-E | Required Proof Class: staging-real | Must Reach: real_broker_connector_contour
- Surface: production_live_readiness | Owner Phase: F1-F | Required Proof Class: live-real | Must Reach: integrated_release_decision_bundle

## In Scope

- One canonical execution contract under `docs/codex/contracts/`.
- One module parent brief under `docs/codex/modules/`.
- One phase brief per declared F1 closure phase under `docs/codex/modules/`.
- Retirement of the old `stack-conformance-remediation` active pointer so governed auto-routing keeps one unambiguous active module.
- Task-note normalization that records the selected primary document and the tie-break rule.

## Out Of Scope

- Implementing truth-source, validator, Telegram, replaceable-stack, contracts, sidecar, broker, or operational-readiness changes from the new module phases.
- Rewriting the package evidence bundle itself during intake.
- Downgrading the strong release-readiness bar established by the package.
- Any trading or strategy logic.

## Constraints

- Do not call the governed launcher again from inside this runtime prompt.
- Keep `docs/session_handoff.md` as a lightweight pointer shim.
- Use canonical gate names only.
- Treat the `zip` as one source package, not as an already-clean single spec.
- Keep one active governed module pointer after intake.
- Preserve the package phase order: governance repair before stack cleanup, before contract freeze, before sidecar evidence hardening, before real broker closure, before final release decision.
- Keep shell control-plane surfaces free of trading logic.

## Done Evidence

- `docs/codex/contracts/f1-full-closure.execution-contract.md` exists.
- `docs/codex/modules/f1-full-closure.parent.md` exists.
- `docs/codex/modules/f1-full-closure.phase-01.md` through `docs/codex/modules/f1-full-closure.phase-06.md` exist.
- `docs/codex/modules/stack-conformance-remediation.parent.md` no longer points to an active next phase.
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Primary Change Surfaces

- PROCESS-STATE
- GOV-DOCS
- ARCH-DOCS

## Routing

- Path: module
- Rationale: the package explicitly decomposes F1 closure into ordered atomic phases with per-phase acceptance and disprover logic, so the correct governed continuation is a module route rather than a one-shot package implementation patch.

## Mode Hint

- continue

## Next Allowed Unit Of Work
- Execute F1-E - Real Broker Process Closure only: move `StockSharp/QUIK/Finam real broker process` from `planned` to implemented release-blocking surface via Finam-native session and transport proof.
## Suggested Branch / PR

- Branch: codex/f1-full-closure-phase01
- PR Title: Normalize F1 full-closure module and start truth-source repair

