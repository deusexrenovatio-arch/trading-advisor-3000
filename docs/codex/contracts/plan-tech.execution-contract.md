# Execution Contract

Updated: 2026-03-24 15:08 UTC

## Source Package

- Package Zip: D:/trading advisor 3000/.tmp/spec-intake-lab/docs/codex/packages/inbox/PLAN_tech-package.zip
- Package Manifest: D:/trading advisor 3000/.tmp/spec-intake-lab/artifacts/codex/package-intake/20260324T143907Z-plan-tech-package/manifest.md
- Suggested Primary Document: D:/trading advisor 3000/.tmp/spec-intake-lab/artifacts/codex/package-intake/20260324T143907Z-plan-tech-package/extracted/PLAN_tech.md
- Source Title: План: Lightweight Anti-Shortcut Governance для AI-Shell

## Prompt / Spec Quality

- Verdict: READY
- Why: The source plan already states the objective, target constraints, intended changes, test plan, rollout stages, and assumptions clearly enough to decompose into a module path without additional clarification.

## Repaired Assumptions

- This run is planning-only: create the execution contract and phase briefs now, and defer implementation to the first planned phase.
- Preserve the repository's existing governance harness and add anti-shortcut rules as a lightweight extension, not a replacement.
- Keep the pilot initially limited to the two contours named in the source plan: data integration closure and runtime publication closure.

## Objective

- Convert PLAN_tech into an executable module-path plan for lightweight anti-shortcut governance in the AI shell.

## In Scope

- One execution contract for the package.
- One module parent brief plus explicit phase briefs.
- Lifecycle/task-note synchronization for this planning run.
- Phase decomposition that preserves the intended rollout order and existing repository policy.

## Out Of Scope

- Implementing the validators or policy changes in this run.
- Hard multi-agent enforcement beyond what the current harness already supports.
- Product or trading logic changes.
- Repo-wide rollout beyond the pilot contours.

## Constraints

- No new mandatory gate lanes.
- No manual approval workflow or heavyweight waiver registry.
- Keep rules deterministic, machine-checkable, and fail-closed.
- Apply the extra discipline only to critical contours, not to every task.
- Keep shell surfaces domain-free and preserve canonical gate names.

## Done Evidence

- docs/codex/contracts/plan-tech.execution-contract.md exists.
- docs/codex/modules/plan-tech.parent.md exists.
- docs/codex/modules/plan-tech.phase-01.md exists.
- docs/codex/modules/plan-tech.phase-02.md exists.
- docs/codex/modules/plan-tech.phase-03.md exists.
- docs/codex/modules/plan-tech.phase-04.md exists.
- python scripts/validate_task_request_contract.py
- python scripts/validate_session_handoff.py
- python scripts/validate_docs_links.py --roots AGENTS.md docs
- python scripts/run_loop_gate.py --from-git --git-ref HEAD

## Primary Change Surfaces

- GOV-DOCS
- PROCESS-STATE

## Routing

- Path: module
- Rationale: the plan spans policy, task contracts, config, validators, gate wiring, routing, tests, and pilot acceptance docs across multiple high-risk surfaces, so it should be executed in explicit phases instead of one patch.

## Mode Hint

- plan-only

## Next Allowed Unit Of Work

- Execute phase 02 only: add validator and gate enforcement on top of the completed phase-01 policy, config, and task-contract foundations.

## Suggested Branch / PR

- Branch: codex/plan-tech-phase01
- PR Title: Plan anti-shortcut governance pilot module

## How to make the next prompt better

- If you already know the desired first implementation slice, say whether the next run should be `plan-only` or `implement phase 01`.
- If a package contains multiple plans, mark one document as the canonical source so intake does not need to infer the primary document.
