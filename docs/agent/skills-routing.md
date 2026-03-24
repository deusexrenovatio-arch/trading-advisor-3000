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
4. When multiple skills match, pick the smallest set that covers intent.

## Acceptance Routing
- When a task involves phase acceptance, acceptor flows, unblock decisions, or explicit guardrails against fallbacks/skips, load `phase-acceptance-governor` first.
- Co-load `architecture-review`, `testing-suite`, and `docs-sync` when acceptance covers architecture fit, executed tests, and documentation closure.

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
