# CTX-SKILLS

## Scope
Repo-local product-plane skills catalog and skill governance policy.

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

## Minimum Checks
- `python scripts/validate_skills.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
