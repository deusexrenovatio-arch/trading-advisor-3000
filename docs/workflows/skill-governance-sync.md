# Skill Governance Sync Workflow

## Purpose
- Keep local skill catalog aligned with repository governance rules.
- Enforce generic-first skill rollout and domain-skill exclusion in baseline shell.

## Mandatory Baseline
1. Prefer generic skills first.
2. Defer stack-specific skills until the stack exists.
3. Exclude domain-specialized skill packs from baseline.
4. Keep one source of truth:
   - runtime files: `.cursor/skills/*/SKILL.md`
   - catalog metadata: `docs/agent/skills-catalog.md`

## Validation
- `python scripts/validate_skills.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Update Procedure
1. Add/update local skill files in `.cursor/skills/`.
2. Sync catalog status in `docs/agent/skills-catalog.md`.
3. Verify routing policy in `docs/agent/skills-routing.md`.
4. Run validation commands.
