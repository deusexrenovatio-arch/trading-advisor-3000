# Стартовый prompt для Codex

Ниже — готовый prompt, который можно вставить в Codex.

---

You are implementing a new repository called **Trading Advisor 3000**.

Your goal is **not** to port the trading business logic from the source repository.  
Your goal is to extract and rebuild the **AI delivery shell** from the repository `deusexrenovatio-arch/trading_advisor`, including:

- hot/warm/cold source-of-truth docs
- task session lifecycle
- task request contract
- context routing and context cards
- loop / PR / nightly gates
- plans registry and durable memory
- skills governance
- architecture-as-docs
- process telemetry and governance reporting

Rules:

1. Do not port business logic, trading logic, MOEX-specific logic, or domain skills.
2. Do not reintroduce `run_lean_gate.py` or any legacy equivalent.
3. Use `run_loop_gate.py` as the canonical hot-path gate.
4. `docs/session_handoff.md` must remain a lightweight pointer shim, not a long narrative document.
5. `plans/items/` is canonical; `plans/PLANS.yaml` is generated compatibility output.
6. Archived/completed task notes must not be inherited without explicit closeout evidence.
7. Direct push to `main` must be blocked by default. Use neutral env vars:
   - `AI_SHELL_EMERGENCY_MAIN_PUSH`
   - `AI_SHELL_EMERGENCY_MAIN_PUSH_REASON`
8. Keep hot context narrow. Do not open all docs or all skills by default.
9. Port only generic skills first; defer stack-specific skills; exclude trading-domain skills initially.
10. Implement in small patch sets and report after each phase:
   - changed files
   - commands/tests run
   - completion evidence
   - remaining risks

Target structure:

- root governance files
- docs/agent/*
- docs/agent-contexts/*
- docs/checklists/*
- docs/workflows/*
- docs/runbooks/*
- docs/architecture/*
- docs/tasks/{active,archive}
- plans/items/
- memory/{decisions,incidents,patterns}
- scripts/
- src/trading_advisor_3000/
- tests/process/
- tests/architecture/

Follow this phase order:

1. Root governance shell
2. Session lifecycle and task artifacts
3. Context routing
4. Gates and validators
5. Durable state and reporting
6. Skills governance
7. Architecture package and app placeholder
8. CI and pilot task

For each phase, satisfy the documented Definition of Done before moving forward.

---

Короткая рекомендация: вставлять этот prompt вместе с файлами `05_TRANSFER_PLAN_FOR_CODEX.md`, `06_PHASES_AND_DOD.md` и `07_TARGET_ARCHITECTURE_TRADING_ADVISOR_3000.md`.
