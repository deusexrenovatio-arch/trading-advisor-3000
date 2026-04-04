# Skill Governance Sync Workflow

## Purpose
Keep local runtime skills, generated catalog, routing policy, and governance validators in deterministic sync.

## Source of Truth
1. Runtime catalog: local skill descriptors under `.cursor/skills/*/`.
2. Generated mirror: `docs/agent/skills-catalog.md`.
3. Routing policy: `docs/agent/skills-routing.md`.
4. Process workflow: `docs/workflows/skill-governance-sync.md`.

Generated catalog must not be edited manually.

## Cold-Context Rule
- `.cursorignore` must keep `.cursor/skills/**`.
- Load only targeted skill files selected by routing triggers.

## Required Commands
- `python scripts/sync_skills_catalog.py`
- `python scripts/sync_skills_catalog.py --check`
- `python scripts/validate_skills.py --strict`
- `python scripts/skill_update_decision.py --strict --from-git --git-ref HEAD`
- `python scripts/skill_precommit_gate.py --from-git --git-ref HEAD`

## Skillkit-Assisted Intake
When importing skills from external repositories:
1. Use `skillkit` to discover and transform candidate skills.
2. Treat transformed output as input draft, not production-ready runtime text.
3. Normalize imported content to repository policy:
   - metadata-complete frontmatter;
   - dual-surface constraints;
   - no domain leakage into baseline shell skills.
4. Revalidate routing triggers and owner surface before runtime inclusion.

## Add Flow
1. Create a new skill folder with a metadata-complete descriptor file.
2. Ensure class policy allows runtime inclusion (`KEEP_CORE` for baseline).
3. If baseline runtime set changed, update `scripts/validate_skills.py` (`KEEP_CORE_BASELINE`) in the same patch.
4. Regenerate catalog.
5. Update routing policy only if routing/class rules changed.
6. Run strict validators and skill tests.

## Update Flow
1. Edit existing skill metadata/content.
2. If overlap is high, extend an existing skill before introducing a new runtime skill.
3. Add a new runtime skill only when capability is materially missing and cannot stay maintainable as a subsection of an existing skill.
4. For any new runtime skill, wire activation behavior into routing and, when relevant, orchestration/pipeline enforcement.
5. Regenerate catalog.
6. If routing metadata changed, update routing policy.
7. If process contract changed, update this workflow doc.
8. When a runtime skill becomes part of a hard unblock decision, document the acceptance/governance effect here explicitly so strict review can see why the runtime skill matters.
9. Run strict decision + precommit gate.

## Baseline Contract Note
If `scripts/validate_skills.py` baseline set changes:
1. This workflow file must be updated in the same change set.
2. Routing policy must reflect any new runtime activation behavior.
3. Catalog sync output must be regenerated and committed.

## Remove/Rename Flow
1. Apply remove or rename in `.cursor/skills`.
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
2. If runtime/catalog mismatch: fix skill metadata or catalog generation inputs.
3. If routing metadata drift: update `docs/agent/skills-routing.md`.
4. If process contract drift: update this workflow doc.
5. Re-run strict validators before gate rerun.
