# Phase 0/1 Bootstrap Baseline

## 1) Target repository skeleton

### Root governance
- `AGENTS.md`
- `agent-runbook.md`
- `harness-guideline.md`
- `CODEOWNERS`
- `.cursorignore`
- `.githooks/pre-push`

### Hot documentation
- `docs/README.md`
- `docs/DEV_WORKFLOW.md`
- `docs/agent/entrypoint.md`
- `docs/agent/domains.md`
- `docs/agent/checks.md`
- `docs/agent/runtime.md`
- `docs/agent/skills-routing.md`
- `docs/agent/skills-catalog.md` (template baseline)

### Operational script
- `scripts/install_git_hooks.py`

### Structure prepared for next phases
- `docs/agent-contexts/`
- `docs/checklists/`
- `docs/workflows/`
- `docs/runbooks/`
- `docs/architecture/`
- `docs/tasks/{active,archive}/`
- `plans/items/`
- `memory/{decisions,incidents,patterns}/`
- `src/trading_advisor_3000/`
- `tests/{process,architecture,app}/`
- `.github/workflows/`

## 2) Migration map (source -> target)

| Source reference | Decision | Target |
| --- | --- | --- |
| `AGENTS.md` | `ADAPT_CORE`; remove domain mentions; keep machine-enforced policy | `AGENTS.md` |
| `agent-runbook.md` | `ADAPT_CORE`; preserve multi-session discipline | `agent-runbook.md` |
| `harness-guideline.md` | `ADAPT_CORE`; preserve principle -> check mapping | `harness-guideline.md` |
| `CODEOWNERS` | `ADAPT_CORE`; ownership by surface | `CODEOWNERS` |
| `.cursorignore` | `ADAPT_CORE`; narrow hot context | `.cursorignore` |
| `.githooks/pre-push` | `ADAPT_CORE`; PR-only main, emergency reason, loop gate call | `.githooks/pre-push` |
| `scripts/install_git_hooks.py` | `ADAPT_CORE`; one-command hook bootstrap | `scripts/install_git_hooks.py` |
| `docs/README.md` | `ADAPT_CORE`; keep as entry index | `docs/README.md` |
| `docs/DEV_WORKFLOW.md` | `ADAPT_CORE`; canonical process flow, no legacy wrappers | `docs/DEV_WORKFLOW.md` |
| `docs/agent/entrypoint.md` | `ADAPT_CORE`; read order and startup path | `docs/agent/entrypoint.md` |
| `docs/agent/domains.md` | `ADAPT_CORE`; explicit change surfaces and split policy | `docs/agent/domains.md` |
| `docs/agent/checks.md` | `ADAPT_CORE`; loop/pr/nightly checks matrix | `docs/agent/checks.md` |
| `docs/agent/runtime.md` | `ADAPT_CORE`; Python entrypoints only | `docs/agent/runtime.md` |
| `docs/agent/skills-routing.md` | `ADAPT_CORE`; generic-skill-first routing | `docs/agent/skills-routing.md` |
| `docs/agent/skills-catalog.md` | `ADAPT_TEMPLATE`; seed catalog for Wave 1 generic skills | `docs/agent/skills-catalog.md` |

## 3) Implementation backlog (Phase 0/1)

1. Freeze boundaries and naming:
   - confirm excluded assets (legacy lean gate script, domain env vars),
   - enforce neutral naming (`AI_SHELL_*`),
   - freeze context-card renames (`CTX-DOMAIN`, `CTX-EXTERNAL-SOURCES`).
2. Build root governance shell:
   - create policy docs,
   - create ownership and context control files,
   - create pre-push enforcement hook.
3. Bootstrap operations:
   - add hook installation script,
   - define minimum verification checks.
4. Coherence verification:
   - ensure hot docs and AGENTS are aligned,
   - ensure no legacy gate references in new shell docs/tests.

## 4) Risks and assumptions

### Assumptions
1. This repository is initialized as a new shell-first baseline.
2. The markdown package in this repo is the primary source of truth for Phase 0/1.
3. Domain logic migration is intentionally deferred.

### Risks
1. Shell and business logic can accidentally mix if scope discipline weakens.
2. Hot context can bloat if `.cursorignore` is loosened too early.
3. Governance can degrade without enforced checks and owners.
4. If source reference path `../trading_advisor` is unavailable in local environment, textual package guidance is used as fallback source.
