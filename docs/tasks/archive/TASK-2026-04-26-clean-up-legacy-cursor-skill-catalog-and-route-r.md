# Task Note
Updated: 2026-04-26 13:12 UTC

## Goal
- Deliver: Clean up legacy Cursor skill catalog and route repo-local skills through product-plane scoped .codex placement

## Task Request Contract
- Objective: retire tracked generic `.cursor/skills` and make `.codex/skills` the repo-local product-plane skill location.
- In Scope: skill catalog source, skill validators/gates, routing docs, prompt references, tests, and task/session state.
- Out of Scope: product-plane business logic, trading algorithms, and global `D:/CodexHome/skills` contents.
- Constraints: no direct main push; keep generic engineering skills global; keep repo-local skills product-plane/trading/data/compute scoped.
- Done Evidence: generated catalog shows zero active repo-local skills, legacy skill deletions are tracked, strict skill checks and PR gate pass.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Session started and baseline scope captured.
- Change surface: `shell`.
- Boundary decision: generic skills route through global Codex skills; repo-local skills are allowed only under `.codex/skills` for TA3000-specific product-plane/trading/data/compute knowledge.
- Out of scope: product-plane business logic and trading runtime changes.

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
- Final Contexts: CTX-SKILLS, CTX-ARCHITECTURE, CTX-GOVERNANCE
- Route Match: matched
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: pending
- Improvement Artifact: pending

## Blockers
- No blocker.

## Next Step
- Publish the validated branch through PR flow toward `main`.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `py -3.11 scripts/sync_skills_catalog.py --check`
- `py -3.11 scripts/validate_skills.py --strict`
- `py -3.11 scripts/skill_update_decision.py --strict --from-git --git-ref HEAD --format text`
- `py -3.11 scripts/skill_precommit_gate.py --from-git --git-ref HEAD`
- `py -3.11 -m pytest tests/process/test_sync_skills_catalog.py tests/process/test_validate_skills.py tests/process/test_skill_update_decision.py tests/process/test_skill_precommit_gate.py tests/process/test_harness_contracts.py tests/architecture/test_governance_policies.py -q --basetemp .tmp/pytest-skills-cleanup`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `py -3.11 scripts/run_pr_gate.py --base-ref origin/main --head-ref HEAD --snapshot-mode changed-files --profile none --summary-file artifacts/ci/pr-gate-summary.md`
