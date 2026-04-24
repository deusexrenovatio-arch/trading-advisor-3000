# Task Note
Updated: 2026-04-24 10:18 UTC

## Goal
- Deliver: Integrate dual-surface safe rename closeout and add naming guardrail for active product surfaces

## Task Request Contract
- Objective: integrate the dual-surface rename closeout with an explicit active product-surface naming rule and executable guardrail.
- In Scope: shell governance docs, product-plane architecture/runbook headings needed to close active naming drift, loop-gate wiring, and focused process tests.
- Out of Scope: product trading/business logic, historical artifact rewrites, broad cleanup of immutable route evidence, and direct mainline push.
- Constraints: change surface is `mixed`; shell governance owns the new guardrail, while product-plane edits are limited to rename cleanup and active naming hygiene. Trading/business logic remains out of scope.
- Done Evidence: run focused naming tests, docs/session validators, and the loop gate with explicit changed-files markers.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Integrated active product-surface capability naming rule and executable guardrail.
- Cleaned rename closeout blockers surfaced by loop/PR gates without claiming product target closure.

## Solution Intent
- Solution Class: staged
- Critical Contour: multi-contour
- Forbidden Shortcuts: scaffold-only, sample artifact, synthetic upstream, smoke only, manifest only
- Closure Evidence: staged guardrail and cleanup evidence only; required contour markers remain integration test, canonical dataset, downstream research, runtime-ready surface, runtime output, durable store, publication contour, and end-to-end publication.
- Staged Note: partial closure only, future shape preserved, not full target closure for the product data/runtime contours.
- Shortcut Waiver: none

## First-Time-Right Report
1. Confirmed coverage: objective and acceptance path are explicit.
2. Missing or risky scenarios: unknown integrations and policy drifts.
3. Resource/time risks and chosen controls: phased patches and deterministic checks.
4. Highest-priority fixes or follow-ups: stabilize contract and validation first.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: completed
- Decision Quality: correct_after_replan
- Final Contexts: CTX-OPS, GOV-DOCS, GOV-RUNTIME, ARCH-DOCS, PROCESS-STATE
- Route Match: expanded
- Primary Rework Cause: workflow_gap
- Incident Signature: none
- Improvement Action: validator
- Improvement Artifact: scripts/validate_product_surface_naming.py

## Blockers
- No blocker.

## Next Step
- Prepare PR-oriented change summary.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python -m pytest tests/process/test_validate_product_surface_naming.py tests/process/test_gate_scope_routing.py::test_scope_validate_command_uses_stdin_for_product_surface_naming -q --basetemp=.tmp/pytest-naming`
- `python scripts/validate_product_surface_naming.py`
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
- `python scripts/validate_legacy_namespace_growth.py`
- `python scripts/validate_phase_planning_contract.py --changed-files docs/codex/contracts/plan-tech.execution-contract.md docs/codex/modules/plan-tech.phase-01.md docs/codex/modules/plan-tech.phase-02.md docs/codex/modules/plan-tech.phase-03.md docs/codex/modules/plan-tech.phase-04.md`
- `python scripts/validate_solution_intent.py --from-git --git-ref HEAD`
- `python scripts/validate_critical_contour_closure.py --from-git --git-ref HEAD`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
