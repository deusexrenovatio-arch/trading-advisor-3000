# Task Note
Updated: 2026-03-25 12:59 UTC

## Goal
- Deliver: translate the governed package intake of the selected package plan into one focused pilot anti-shortcut governance patch for critical contours.

## Package Intake
- Package Type: single zip source package, not an already-clean specification.
- Primary Document: the manifest-selected package plan document named PLAN_tech
- Selection Rule: use the manifest highest-ranked candidate; here it is also the only package markdown document and the suggested primary with score `63`.
- Supporting Documents: none inside the package beyond the manifest and the selected primary.
- Mode Hint: `auto`

## Task Request Contract
- Objective: add pilot anti-shortcut governance for critical contours without adding new lanes or manual approval flow.
- In Scope: `configs/critical_contours.yaml`, critical-contour policy/check/routing docs, new solution-intent and contour-closure validators, scoped gate wiring, two pilot acceptance passports, and targeted process tests.
- Out of Scope: product trading logic, repo-wide rollout beyond the two pilot contours, a general static analyzer, new approval committees, and changes to `docs/session_handoff.md` beyond pointer-shim safety.
- Constraints: do not call the governed launcher again; keep `docs/session_handoff.md` lightweight; use canonical gate names only; keep low-risk/docs-only tasks free from new mandatory fields; make rules deterministic and fail-closed.
- Done Evidence: `python scripts/validate_solution_intent.py --from-git --git-ref HEAD`, `python scripts/validate_critical_contour_closure.py --from-git --git-ref HEAD`, targeted `pytest` for critical contour coverage, and `python scripts/run_loop_gate.py --from-git --git-ref HEAD`.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Mandatory repository docs, session handoff pointer, package manifest, and the suggested primary document were read first.
- The package was treated as one intake source; the package plan was accepted as primary by the manifest ranking rule, not by a silent assumption.
- Change surface is shell governance plus limited product-plane acceptance docs for pilot passports; no trading logic moves into shell files.
- Pilot anti-shortcut governance was landed for two critical contours through config, validators, routing signals, and short closure passports.
- Verification completed with targeted critical-contour tests plus green loop and PR gates on the working tree.

## First-Time-Right Report
1. Confirmed coverage: the package asks for a pilot-only governance hardening slice with config, validators, routing, and acceptance docs, and that scope is now explicit.
2. Missing or risky scenarios: routing language could blur contexts vs skills, and critical-contour detection must stay strict enough to block shortcuts without punishing low-risk diffs.
3. Resource/time risks and chosen controls: keep the rollout config-driven, scope new checks to non-trivial diffs, and avoid any new gate lane or broad analyzer.
4. Highest-priority fixes or follow-ups: land the contract/config first, then wire validators into the existing loop flow and prove the behavior with targeted tests.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: completed
- Decision Quality: high
- Final Contexts: CTX-CONTRACTS, GOV-RUNTIME, GOV-DOCS, PROCESS-STATE, ARCH-DOCS
- Route Match: package intake matched and continued through the governed execution flow
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: add pilot anti-shortcut governance for critical contours without creating new mandatory lanes
- Improvement Artifact: configs/critical_contours.yaml

## Blockers
- No blocker.

## Next Step
- Close the current governed task lifecycle and prepare the change for PR-oriented review.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_solution_intent.py --from-git --git-ref HEAD`
- `python scripts/validate_critical_contour_closure.py --from-git --git-ref HEAD`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
