# Task Note
Updated: 2026-05-07 09:01 UTC

## Goal
- Deliver: Harden Superpowers-first routing against behavior-change bypass

## Task Request Contract
- Objective: define one measurable process/governance outcome.
- In Scope: list explicit files/surfaces that can change now.
- Out of Scope: list deferred items and non-goals.
- Constraints: list risk/time/policy/runtime constraints.
- Done Evidence: list exact commands/artifacts proving completion.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Superpowers-first routing hardened against "small diff" and "semantic correction" bypasses.
- Behavior, bugfix, data/compute semantic, contract, and user-facing changes now explicitly trigger `superpowers:test-driven-development`.
- Behavior/contract closeout now requires explicit self-review plus fresh verification.
- Global skill files updated outside the repo: `codex-skill-routing`, `code-implementation-worker`, and `verification-before-completion`.

## Self-Review
- Changed behavior: agent routing now treats semantic risk as the process trigger, not diff size.
- Moved contract: Superpowers-first routing now explicitly owns behavior-change and closeout process selection.
- Old behavior now forbidden: direct patch/test/gate after labeling a behavior or contract change as small.
- Proof: docs validators, loop gate, and global skill smoke check cover the changed routing surfaces.
- Uncovered edge cases: prompt-level routing cannot mechanically prevent every future skip without a dedicated executable detector.
- Residual risk: future agents may still skip skills if session metadata hides Superpowers; the required fallback is now explicit.

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
- Final Contexts: CTX-OPS
- Route Match: matched
- Primary Rework Cause: workflow_gap
- Incident Signature: behavior_change_routing_bypass
- Improvement Action: skill
- Improvement Artifact: docs/agent/skills-routing.md

## Blockers
- No blocker.

## Next Step
- Prepare PR-only integration branch from this worktree.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
- global skill smoke for `codex-skill-routing`, `code-implementation-worker`, and `verification-before-completion`
