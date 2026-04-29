# Task Note
Updated: 2026-04-29 16:15 UTC

## Goal
- Deliver: Publish skill routing boundary hot-doc updates for GraphQL/Node guard and global skill sequence rules

## Task Request Contract
- Objective: publish hot-doc routing rules that keep global skills sequenced, non-overlapping, and aligned with active TA3000 surfaces.
- In Scope: `AGENTS.md`, `docs/agent/entrypoint.md`, `docs/agent/skills-routing.md`, and the lightweight task-session pointer required by the loop gate.
- Out of Scope: global skill filesystem changes under `D:/CodexHome/skills`, product-plane runtime implementation, native-runtime ownership docs already present on `main`, and unrelated dirty worktree files.
- Constraints: mainline remains PR-only; GraphQL/Node routing must stay disabled unless active repo files/contracts justify it; document cross-layer routing must stay limited to document retrieval layers.
- Done Evidence: docs link validation, strict repo-local skill validation, skill catalog drift check, diff hygiene, and loop gate on changed files.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Added a GraphQL/Node baseline guard to the agent hot docs.
- Added global skill sequence rules so adjacent skills are loaded by artifact phase rather than keyword overlap.
- Replaced retired global skill names in routing docs with the current isolated skill names.
- Kept native-runtime ownership as already published on `origin/main`; this PR only completes the routing hot-doc layer.

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
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: pending
- Route Match: pending
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: pending
- Improvement Artifact: pending

## Blockers
- No blocker.

## Next Step
- Publish the focused patch through PR-only main integration.

## Validation
- `py -3.11 scripts/sync_skills_catalog.py --check`
- `py -3.11 scripts/validate_skills.py --strict`
- `py -3.11 scripts/validate_docs_links.py --roots AGENTS.md docs/agent`
- `py -3.11 scripts/validate_task_request_contract.py`
- `py -3.11 scripts/validate_session_handoff.py`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
