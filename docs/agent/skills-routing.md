# Skills Routing Policy

## Goal
Use skills as a managed capability layer, not as an uncontrolled markdown archive.

## Routing order
1. Use generic process and architecture skills first.
2. Add stack-specific skills only when the stack is actually present.
3. Exclude domain-specialized skills from baseline shell.

## Baseline decision
- Active source: local `.cursor/skills/*`.
- Catalog source: `docs/agent/skills-catalog.md`.

## Wave policy
1. Wave 1: generic shell skills (`ai-agent-architect`, `docs-sync`, `testing-suite`, `architecture-review`, etc.).
2. Wave 2: governance/CI hardening skills.
3. Wave 3: stack-specific skills.
4. Wave 4: domain-specialized skills only after explicit business model definition.

## Update rule
When skill files are introduced or changed:
1. update catalog metadata,
2. update routing policy if trigger logic changed,
3. run `python scripts/validate_skills.py`,
4. add/adjust validation in future skill governance gates.
