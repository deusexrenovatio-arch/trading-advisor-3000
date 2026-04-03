# Skills Routing Policy

## Runtime Model
- Primary runtime catalog: local skill descriptors under `.cursor/skills/*/`.
- Mirror artifact: `docs/agent/skills-catalog.md` (generated only).
- Hot-context policy: `.cursor/skills/**` stays cold-by-default.
- Retrieval rule: open only targeted, specific skill files selected by routing triggers.

## Generic-First Routing
1. Start from generic process/architecture/testing/governance skills.
2. Apply stack skills only after stack surfaces are present and validated.
3. Keep domain-specialized skills outside baseline shell runtime.
4. Prefer integration into an existing skill when overlap is high; add new skills only for missing, non-trivial capabilities.
5. When multiple skills match, pick the smallest set that covers intent.

## Code Review Routing
- When the request is review-focused (`review`, `PR review`, `find risks`, `actionable findings`), load:
  - `code-reviewer` (primary)
- Co-load conditionally:
  - `architecture-review` when boundary or dependency direction risk is present
  - `testing-suite` when changed-path test adequacy is unclear
  - `secrets-and-config-hardening` for secret or configuration risk
  - `dependency-and-license-audit` when dependency updates are in scope

## Worker Coding Routing
- When the active phase is implementation and the request is code-writing focused, load:
  - `code-implementation-worker` (primary)
- Co-load conditionally:
  - `registry-first` when contracts/schemas/interfaces change
  - `validate-crosslayer` when changes cross multiple layers or boundaries
  - `testing-suite` for primary changed-path coverage only
- Do not auto-load intake-oriented or acceptance-only skills for worker coding by default.

## Intake Routing
- When the request is intake-phase and pre-code flow shaping (`intake`, `workflow map`, `failure branches`, `handoff contracts`), load:
  - `workflow-architect` (primary)
- Governed package intake (`docs/codex/prompts/entry/from_package.md`) must always bind `workflow-architect` as a required intake lens.
- Co-load conditionally:
  - `business-analyst` for requirement decomposition and acceptance framing
  - `architecture-review` when intake decisions affect boundaries or dependency direction
  - `agents-orchestrator` when intake map is being translated into a governed execution pipeline

## Data Routing
- When changes touch data pipeline surfaces or the request is ETL/ELT/data reliability focused, load:
  - `data-engineer` (primary)
- Co-load conditionally:
  - `source-onboarding` for source onboarding and canonical mapping
  - `registry-first` when schemas/contracts are changed
  - `validate-crosslayer` when pipeline changes cross service or layer boundaries
  - `testing-suite` for changed-path data checks

## Acceptance Routing
- When a task involves phase acceptance, acceptor flows, unblock decisions, or explicit guardrails against fallbacks/skips, load `phase-acceptance-governor` first.
- Co-load `architecture-review`, `code-reviewer`, `testing-suite`, and `docs-sync` when acceptance covers architecture fit, implementation risk, executed tests, and documentation closure.
- Co-load `verification-before-completion` whenever completion claims must be fail-closed on executable evidence.

## Pipeline Routing
- When changes touch `.github/workflows/**` or lane wiring, load:
  - `ci-bootstrap`
  - `github-actions-ops`
  - `commit-and-pr-hygiene`
- Use this set for both lane design and failing-check remediation so CI changes remain policy-aligned and reviewable.

## Orchestration Routing
- When the request is to design or run multi-role execution flow with retries/escalation and phase gates, load:
  - `agents-orchestrator`
  - `ai-agent-architect`
  - `phase-acceptance-governor`
- When changes touch governed orchestration surfaces:
  - `scripts/codex_phase_orchestrator.py`
  - `scripts/codex_phase_policy.py`
  - `docs/codex/prompts/phases/**`
  load:
  - `phase-acceptance-governor`
  - `verification-before-completion`
  - `testing-suite`
- Orchestration acceptance is considered incomplete if completion-verification evidence is missing.

## Critical Contour Routing
- When changed files match `configs/critical_contours.yaml`, route `CTX-ARCHITECTURE` as a companion context even when no architecture doc changed directly.
- For critical contours, load `architecture-review` and `qa-test-engineer` before implementation so the chosen path is checked against target shape and executable evidence.
- If the task claims contour closure or acceptance, also load `phase-acceptance-governor` and `verification-before-completion`.
- Critical contour work must declare `target`, `staged`, or `fallback` in the active task note before code changes begin.
- If the chosen path is simpler than the target shape, the agent must name that fact explicitly instead of presenting it as target closure.

## Class Policy

| Class | Baseline runtime | Policy |
| --- | --- | --- |
| `KEEP_CORE` | allowed | baseline required |
| `KEEP_OPTIONAL` | blocked | separate phase gate |
| `DEFER_STACK` | blocked | stack activation gate |
| `EXCLUDE_DOMAIN_INITIAL` | blocked | non-baseline by default |

## Lifecycle Rules

### Add Skill
1. Create a new skill folder under `.cursor/skills/` with a metadata-complete descriptor file.
2. Run `python scripts/sync_skills_catalog.py`.
3. Update routing policy only if class policy or routing behavior changed.
4. Run strict validators and skill tests.

### Change Skill
1. Edit skill metadata/body.
2. Regenerate catalog.
3. Update routing doc only when routing metadata changed.
4. Run `python scripts/skill_update_decision.py --strict ...`.

### Remove Skill
1. Remove skill directory.
2. Regenerate catalog.
3. Update roadmap if the skill moves out of runtime baseline.
4. Validate strict parity and change-surface gates.

### Rename Skill
1. Rename directory and frontmatter `name` together.
2. Regenerate catalog.
3. Update routing references if triggers or class placement changed.
4. Run strict decision and precommit gates.

## What Requires Which Docs
- Only catalog sync required:
  - content edits without routing metadata/class changes.
- Routing policy update required:
  - `routing_triggers` changes;
  - class policy rule changes;
  - add/remove/rename rules change.
- Workflow update required:
  - process contract changes in validation/remediation flow.

## Validation Commands
- `python scripts/sync_skills_catalog.py --check`
- `python scripts/validate_skills.py --strict`
- `python scripts/skill_update_decision.py --strict --from-git --git-ref HEAD`
- `python scripts/skill_precommit_gate.py --from-git --git-ref HEAD`
