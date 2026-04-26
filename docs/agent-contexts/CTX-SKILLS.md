# CTX-SKILLS

## Scope
Repo-local product-plane skills catalog and skill governance policy.

## Inside This Context
- Repo-local skill catalog policy, generated skill catalog, validation scripts, and legacy Cursor cleanup state.
- This context governs when TA3000-specific skill material is allowed in the repo.
- Typical questions: is this skill generic or repo-local, is the catalog generated from the active root, did legacy `.cursor/skills` grow?
- Not inside: global Codex skills themselves.

## Access Policy
- Keep `.codex/skills/**` and legacy `.cursor/skills/**` cold-by-default in hot context.
- Open repo-local skill files only for targeted TA3000/product-plane signals.

## Owned Paths
- `.codex/skills/`
- `docs/agent/skills-catalog.md`
- `docs/agent/skills-routing.md`
- `docs/workflows/skill-governance-sync.md`

## Guarded Paths
- `.cursor/skills/` legacy cleanup
- `src/trading_advisor_3000/`

## Navigation Facets
- skill-routing
- repo-local-catalog
- legacy-cursor-cleanup

## Search Seeds
- `docs/agent/skills-routing.md`
- `scripts/sync_skills_catalog.py`
- `scripts/validate_skills.py`
- `tests/process/test_validate_skills.py`

## Navigation Notes
- Keep repo-local skill files cold unless the task is explicitly TA3000-specific.
- Generic process, testing, architecture, and review skills belong in the global Codex skill root.

## Minimum Checks
- `python scripts/validate_skills.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
