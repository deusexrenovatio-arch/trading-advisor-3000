# Skill Governance Sync Workflow

## Purpose
Keep repo-local skills, generated catalog, routing policy, and governance validators in deterministic sync.

## Source of Truth
1. Repo-local catalog: TA3000-specific descriptors under `.codex/skills/*/`.
2. Generated mirror: `docs/agent/skills-catalog.md`.
3. Routing policy: `docs/agent/skills-routing.md`.
4. Process workflow: `docs/workflows/skill-governance-sync.md`.

Generated catalog must not be edited manually.
Generic engineering skills are sourced from `D:/CodexHome/skills`, not from this repository.

## Cold-Context Rule
- `.cursorignore` must keep `.codex/skills/**` and legacy `.cursor/skills/**`.
- Load repo-local skill files only for targeted TA3000/product-plane triggers.

## Required Commands
- `python scripts/sync_skills_catalog.py`
- `python scripts/sync_skills_catalog.py --check`
- `python scripts/validate_skills.py --strict`
- `python scripts/skill_update_decision.py --strict --from-git --git-ref HEAD`
- `python scripts/skill_precommit_gate.py --from-git --git-ref HEAD`

## Add Flow
1. Create a new child directory under `.codex/skills/` with a skill descriptor file.
2. Confirm the skill is TA3000-specific and product-plane/data/research/compute scoped.
3. Regenerate catalog.
4. Update routing policy only if routing/class rules changed.
5. Run strict validators and skill tests.

## Update Flow
1. Edit existing skill metadata/content.
2. If overlap is high, extend an existing skill before introducing a new runtime skill.
3. Add a new runtime skill only when capability is materially missing and cannot stay maintainable as a subsection of an existing skill.
4. For any new repo-local skill, wire activation behavior into routing only when a TA3000-specific trigger is required.
5. Regenerate catalog.
6. If routing metadata changed, update routing policy.
7. If process contract changed, update this workflow doc.
8. When a runtime skill becomes part of a hard unblock decision, document the acceptance/governance effect here explicitly so strict review can see why the runtime skill matters.
9. Run strict decision + precommit gate.

## Remove/Rename Flow
1. Apply remove or rename in `.codex/skills`.
2. Regenerate catalog immediately.
3. Update routing policy if references or trigger policy changed.
4. Update roadmap when class placement changed.
5. Run strict validators for parity and drift.

## Evidence Checklist
1. Strict validator output is green.
2. Catalog check reports no drift.
3. Skill update decision reports no missing required docs.
4. Relevant tests for sync/validation/decision/precommit are green.
5. If the skill affects phase acceptance or unblock policy, the workflow doc explicitly records that no silent fallback/skip/assumption path may pass through runtime review.
6. If orchestration required skills changed, `tests/process/test_codex_phase_orchestrator.py` must stay green.
7. When `scripts/validate_skills.py` baseline rules are modified, this workflow file is updated in the same change set.

## Remediation Path
1. If catalog drift: run sync script and commit generated file.
2. If repo-local/catalog mismatch: fix skill metadata or catalog generation inputs.
3. If routing metadata drift: update `docs/agent/skills-routing.md`.
4. If process contract drift: update this workflow doc.
5. Re-run strict validators before gate rerun.
