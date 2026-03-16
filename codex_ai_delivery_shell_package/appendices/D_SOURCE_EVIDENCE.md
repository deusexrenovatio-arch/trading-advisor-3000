# Приложение D — Source evidence

Ниже — ссылки на документы и страницы, на основе которых формировался пакет.

| Источник | URL |
| --- | --- |
| Repo root | https://github.com/deusexrenovatio-arch/trading_advisor |
| Merged PR #43 | https://github.com/deusexrenovatio-arch/trading_advisor/pull/43 |
| AGENTS.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/AGENTS.md |
| docs/README.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/README.md |
| docs/DEV_WORKFLOW.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/DEV_WORKFLOW.md |
| docs/session_handoff.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/session_handoff.md |
| docs/agent/entrypoint.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/agent/entrypoint.md |
| docs/agent/domains.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/agent/domains.md |
| docs/agent/checks.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/agent/checks.md |
| docs/agent/runtime.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/agent/runtime.md |
| docs/agent/skills-routing.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/agent/skills-routing.md |
| docs/agent/skills-catalog.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/agent/skills-catalog.md |
| docs/checklists/first-time-right-gate.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/checklists/first-time-right-gate.md |
| docs/checklists/task-request-contract.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/checklists/task-request-contract.md |
| docs/planning/plans-registry.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/planning/plans-registry.md |
| docs/workflows/context-budget.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/workflows/context-budget.md |
| docs/workflows/skill-governance-sync.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/workflows/skill-governance-sync.md |
| docs/workflows/agent-practices-alignment.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/workflows/agent-practices-alignment.md |
| docs/workflows/worktree-governance.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/workflows/worktree-governance.md |
| docs/runbooks/governance-remediation.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/runbooks/governance-remediation.md |
| docs/runbooks/flaky-tests-policy.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/runbooks/flaky-tests-policy.md |
| docs/agent-contexts/README.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/agent-contexts/README.md |
| docs/architecture/trading-advisor.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/architecture/trading-advisor.md |
| docs/architecture/layers-v2.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/architecture/layers-v2.md |
| docs/architecture/entities-v2.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/architecture/entities-v2.md |
| docs/architecture/architecture-map-v2.md | https://raw.githubusercontent.com/deusexrenovatio-arch/trading_advisor/main/docs/architecture/architecture-map-v2.md |

## Наблюдавшиеся ключевые факты

- В корне репозитория присутствуют governance-артефакты (`AGENTS.md`, `agent-runbook.md`, `harness-guideline.md`, `CODEOWNERS`, `plans`, `memory`, `docs`, `scripts`, `tests`).
- В `AGENTS.md` описан hot/warm/cold source-of-truth и не-negotiable loop.
- `docs/README.md` и `docs/DEV_WORKFLOW.md` формируют индекс и нормативный workflow.
- `docs/session_handoff.md` используется как pointer shim.
- PR #43 зафиксировал переход на `run_loop_gate.py`, ужесточение task contract, расширение PR-only policy, removal deprecated override и доведение task-session lifecycle до конца.
- `.githooks/pre-push` в текущем состоянии блокирует прямой push в `main` и гоняет scoped loop gate.
