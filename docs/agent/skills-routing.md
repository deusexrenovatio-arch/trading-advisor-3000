# Skills Routing Policy

## Runtime Model
- Ordinary-chat catalog: global Codex skills under `D:/CodexHome/skills`.
- Repo-local catalog: local descriptors under `.codex/skills/*/`, only for TA3000-specific trading, product-plane data/research, or compute-runtime knowledge that should not affect other repositories.
- Legacy Cursor catalog: `.cursor/skills/*/` is not an active skill location; tracked generic skills must not be added there.
- Mirror artifact: `docs/agent/skills-catalog.md` is generated only from `.codex/skills/*/SKILL.md`.
- Hot-context policy: repo-local skills stay cold-by-default.
- Retrieval rule: use global skills first; open repo-local skill files only when a targeted, project-specific trigger requires them.

## Ordinary Chat Guard
1. Use `codex-skill-routing` for questions about skill selection, prompt routing, and missed-skill protection.
2. Before substantial work, name the selected global skills briefly and why they apply.
3. If a needed global skill is not present in current session metadata but exists under `D:/CodexHome/skills`, read its main instruction file directly and state the fallback.
4. If a needed skill is generic, keep it in the global Codex skill root instead of adding it to this repo.
5. If the task is genuinely TA3000-specific, open the repo-local skill narrowly and do not load the whole skill corpus.

## Generic-First Routing
1. Start from generic process/architecture/testing/governance skills.
2. Apply stack skills only after stack surfaces are present and validated.
3. Keep domain-specialized skills outside baseline shell runtime.
4. Prefer integration into an existing skill when overlap is high; add new skills only for missing, non-trivial capabilities.
5. When multiple skills match, pick the smallest set that covers intent.

## Architecture Orientation Routing
- When a task asks for the architecture map, system shape, module boundaries, or
  shell/product ownership, open:
  - `docs/architecture/trading-advisor-3000.md`
  - `docs/architecture/repository-surfaces.md`
- When implementation status matters, also open:
  - `docs/architecture/product-plane/STATUS.md`
- Route `architecture-review` first for design and boundary questions.
- Use Graphify as an optional companion context for architecture mapping,
  ownership, cross-module relationships, dependency tracing, or "where does this
  concept live?" questions when a local Graphify report or graph JSON exists.
- Before relying on Graphify for architecture orientation, compare the graph
  freshness with the relevant changed areas. If the graph is stale for the
  active question, refresh it with `graphify update .` for code-oriented context
  or the explicit Graphify skill flow for semantic docs/images; if refresh is
  skipped, state that the graph is stale and use it only as an orientation aid.
- Graphify belongs to architecture orientation, not the general agent baseline:
  do not run a Graphify pass after every Serena lookup or every code task.
- Do not let Graphify override hot docs, source code, runtime evidence, or
  governed artifacts. Use it to locate the relevant area, then use Serena or
  direct source inspection for exact implementation proof.
- Do not run Graphify semantic extraction across secrets, production data,
  generated artifacts, archives, memory, or plans. Keep `.graphifyignore`
  aligned with that boundary.
- Co-load `docs-sync` when the architecture docs themselves need to be corrected
  or synchronized.
- Co-load `module-scaffold` when the task creates a new module or moves a module
  between architectural zones.
- Co-load `validate-crosslayer` when the task crosses shell/product boundaries or
  multiple product-plane layers.

## Worker Coding Routing
- When the active phase is implementation and the request is code-writing focused, load:
  - `code-implementation-worker` (primary)
- Co-load conditionally:
  - `registry-first` when contracts/schemas/interfaces change
  - `validate-crosslayer` when changes cross multiple layers or boundaries
  - `testing-suite` for primary changed-path coverage only
- Do not auto-load intake-oriented or acceptance-only skills for worker coding by default.

## Acceptance Routing
- When a task involves phase acceptance, acceptor flows, unblock decisions, or explicit guardrails against fallbacks/skips, load `phase-acceptance-governor` first.
- Co-load `architecture-review`, `testing-suite`, and `docs-sync` when acceptance covers architecture fit, executed tests, and documentation closure.
- Co-load `verification-before-completion` whenever completion claims must be fail-closed on executable evidence.

## Pipeline Routing
- When changes touch `.github/workflows/**` or lane wiring, load:
  - `ci-bootstrap`
  - `github-actions-ops`
  - `commit-and-pr-hygiene`
- Use this set for both lane design and failing-check remediation so CI changes remain policy-aligned and reviewable.

## Orchestration Routing
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
| `KEEP_CORE` | allowed | repo-local active skill |
| `KEEP_OPTIONAL` | blocked | separate phase gate |
| `DEFER_STACK` | blocked | stack activation gate |
| `EXCLUDE_DOMAIN_INITIAL` | blocked | non-baseline by default |

## Lifecycle Rules

### Add Skill
1. Create a new skill folder under `.codex/skills/` with a metadata-complete descriptor file.
2. Confirm the skill is TA3000-specific and owned by a product-plane/data/research/compute surface; generic engineering skills belong under `D:/CodexHome/skills`.
3. Run `python scripts/sync_skills_catalog.py`.
4. Update routing policy only if class policy or routing behavior changed.
5. Run strict validators and skill tests.

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
