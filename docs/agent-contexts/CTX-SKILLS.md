# CTX-SKILLS

## Scope
Generic skills catalog, local skill files, and skill governance policy.

## Access Policy
- Keep `.cursor/skills/**` cold-by-default in hot context.
- Open only the specific skill file needed for the current task signal.

## Owned Paths
- `.cursor/skills/`
- `docs/agent/skills-catalog.md`
- `docs/agent/skills-routing.md`
- `docs/workflows/skill-governance-sync.md`

## Guarded Paths
- `src/trading_advisor_3000/`

## Minimum Checks
- `python scripts/validate_skills.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
