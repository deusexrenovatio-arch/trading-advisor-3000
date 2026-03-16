# Приложение A — шаблон структуры репозитория

```text
/
├─ AGENTS.md
├─ agent-runbook.md
├─ harness-guideline.md
├─ CODEOWNERS
├─ .cursorignore
├─ .githooks/
│  └─ pre-push
├─ .cursor/
│  └─ skills/
│     ├─ ai-agent-architect/
│     │  └─ SKILL.md
│     ├─ architecture-review/
│     │  └─ SKILL.md
│     ├─ business-analyst/
│     │  └─ SKILL.md
│     ├─ module-scaffold/
│     │  └─ SKILL.md
│     └─ ...
├─ docs/
│  ├─ README.md
│  ├─ DEV_WORKFLOW.md
│  ├─ session_handoff.md
│  ├─ agent/
│  │  ├─ entrypoint.md
│  │  ├─ domains.md
│  │  ├─ checks.md
│  │  ├─ runtime.md
│  │  ├─ skills-routing.md
│  │  └─ skills-catalog.md
│  ├─ agent-contexts/
│  │  ├─ README.md
│  │  ├─ CTX-DATA.md
│  │  ├─ CTX-DOMAIN.md
│  │  ├─ CTX-RESEARCH.md
│  │  ├─ CTX-EXTERNAL-SOURCES.md
│  │  ├─ CTX-ORCHESTRATION.md
│  │  ├─ CTX-API-UI.md
│  │  ├─ CTX-CONTRACTS.md
│  │  └─ CTX-OPS.md
│  ├─ checklists/
│  │  ├─ first-time-right-gate.md
│  │  └─ task-request-contract.md
│  ├─ planning/
│  │  └─ plans-registry.md
│  ├─ workflows/
│  │  ├─ context-budget.md
│  │  ├─ skill-governance-sync.md
│  │  ├─ agent-practices-alignment.md
│  │  └─ worktree-governance.md
│  ├─ runbooks/
│  │  ├─ governance-remediation.md
│  │  └─ flaky-tests-policy.md
│  ├─ architecture/
│  │  ├─ trading-advisor-3000.md
│  │  ├─ layers-v2.md
│  │  ├─ entities-v2.md
│  │  └─ architecture-map-v2.md
│  └─ tasks/
│     ├─ active/
│     └─ archive/
├─ plans/
│  ├─ items/
│  └─ PLANS.yaml
├─ memory/
│  ├─ agent_memory.yaml
│  ├─ task_outcomes.yaml
│  ├─ decisions/
│  ├─ incidents/
│  └─ patterns/
├─ scripts/
│  ├─ task_session.py
│  ├─ compute_change_surface.py
│  ├─ context_router.py
│  ├─ handoff_resolver.py
│  ├─ gate_common.py
│  ├─ run_loop_gate.py
│  ├─ run_pr_gate.py
│  ├─ run_nightly_gate.py
│  ├─ install_git_hooks.py
│  ├─ validate_task_request_contract.py
│  ├─ validate_plans.py
│  ├─ validate_session_handoff.py
│  ├─ validate_skills.py
│  ├─ validate_pr_only_policy.py
│  ├─ validate_agent_contexts.py
│  ├─ validate_agent_memory.py
│  ├─ validate_task_outcomes.py
│  ├─ measure_dev_loop.py
│  ├─ agent_process_telemetry.py
│  ├─ process_improvement_report.py
│  ├─ build_governance_dashboard.py
│  ├─ harness_baseline_metrics.py
│  ├─ autonomy_kpi_report.py
│  ├─ skill_update_decision.py
│  ├─ skill_precommit_gate.py
│  └─ sync_architecture_map.py
├─ src/
│  └─ trading_advisor_3000/
│     ├─ __init__.py
│     ├─ domain/
│     ├─ application/
│     ├─ adapters/
│     ├─ contracts/
│     ├─ ui_or_api/
│     └─ ops/
├─ tests/
│  ├─ process/
│  ├─ architecture/
│  └─ app/
└─ .github/
   └─ workflows/
      ├─ loop-gate.yml
      ├─ pr-gate.yml
      ├─ nightly-gate.yml
      └─ dashboard.yml
```
