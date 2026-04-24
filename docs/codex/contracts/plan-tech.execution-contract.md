# Execution Contract

Updated: 2026-03-25 13:55 UTC

## Source Package

- Package Zip: D:/trading advisor 3000/docs/codex/packages/chat-intake-inbox/PLAN_tech-chat-intake-20260325T125858Z.zip
- Package Manifest: D:/trading advisor 3000/artifacts/codex/chat-intake/package-intake/20260325T131337Z-plan-tech-chat-intake-20260325t125858z/manifest.md
- Suggested Primary Document: D:/trading advisor 3000/artifacts/codex/chat-intake/package-intake/20260325T131337Z-plan-tech-chat-intake-20260325t125858z/extracted/PLAN_tech.md
- Source Title: План: Lightweight Anti-Shortcut Governance для AI-Shell

## Prompt / Spec Quality

- Verdict: READY
- Why: The source plan states the objective, target constraints, rollout phases, tests, and pilot-only boundaries clearly enough for explicit phase orchestration.

## Normalization Note

- The earlier governed package run materially landed the foundations, validator/gate wiring, and pilot passport surfaces in one patch set before canonical module-phase state existed.
- This contract normalizes that outcome into the governed module path without pretending those earlier slices already passed independent phase acceptance.
- The governed continuation for the remaining observation and expansion-criteria phase has now been accepted, so the module no longer has an unlocked next phase.

## Objective

- Complete PLAN_tech through explicit governed module-phase orchestration instead of a single package-derived implementation run.

## Release Target Contract

- Target Decision: DENY_RELEASE_READINESS
- Target Environment: shell governance pilot for critical-contour anti-shortcut enforcement, limited to configured pilot contours and local gate execution
- Forbidden Proof Substitutes: docs-only for enforced gate behavior, unchecked phase closure, manual chat-only continuation, silent fallback, synthetic closure evidence
- Release-Ready Proof Class: staging-real

## Mandatory Real Contours

- critical_contour_gate_enforcement: changed-file loop/pr gate execution must exercise configured pilot contours and fail closed on shortcut closure claims.

## Release Surface Matrix

- Surface: anti_shortcut_policy_contour | Owner Phase: Policy and Critical Contour Foundations | Required Proof Class: doc | Must Reach: policy_and_solution_class_contract_defined
- Surface: critical_contour_gate_enforcement | Owner Phase: Validator and Gate Enforcement | Required Proof Class: staging-real | Must Reach: fail_closed_validator_and_gate_execution
- Surface: pilot_passport_routing_contour | Owner Phase: Routing and Pilot Passports | Required Proof Class: doc | Must Reach: routing_and_passport_contracts_defined
- Surface: observation_expansion_contour | Owner Phase: Observation Counters and Expansion Criteria | Required Proof Class: doc | Must Reach: expansion_decision_contract_defined

## In Scope

- One canonical execution contract and module parent brief under `docs/codex/`.
- Explicit phase reconstruction for the anti-shortcut pilot plan.
- Governed continuation of the remaining observation/counters phase only.

## Out Of Scope

- Reverting the already-landed pilot anti-shortcut patch.
- Re-running earlier implementation slices as if they were absent.
- Repo-wide rollout beyond the pilot contours.
- Product or trading logic changes.

## Constraints

- Keep the anti-shortcut discipline limited to critical contours.
- Preserve loop/pr/nightly and Shell delivery proving as the main harness.
- Do not add a new mandatory lane or manual approval ceremony.
- Keep shell control-plane files domain-free.
- Use the governed launcher for continuation instead of manual chat continuation.

## Done Evidence

- `docs/codex/contracts/plan-tech.execution-contract.md` exists.
- `docs/codex/modules/plan-tech.parent.md` exists.
- `docs/codex/modules/plan-tech.phase-01.md` exists.
- `docs/codex/modules/plan-tech.phase-02.md` exists.
- `docs/codex/modules/plan-tech.phase-03.md` exists.
- `docs/codex/modules/plan-tech.phase-04.md` exists.
- `python scripts/codex_governed_bootstrap.py --request "<request>" --route continue --execution-contract docs/codex/contracts/plan-tech.execution-contract.md --parent-brief docs/codex/modules/plan-tech.parent.md --skip-clean-check`

## Primary Change Surfaces

- PROCESS-STATE
- GOV-DOCS
- GOV-RUNTIME

## Routing

- Path: module
- Rationale: the plan explicitly decomposes into ordered phases, and the remaining governed work is now a phase continuation problem rather than a new package-intake problem.

## Mode Hint

- continue

## Next Allowed Unit Of Work
- All planned phases are accepted. Prepare closeout or a new module run.
## Suggested Branch / PR

- Branch: codex/plan-tech-phase04
- PR Title: Governed anti-shortcut pilot observation phase
