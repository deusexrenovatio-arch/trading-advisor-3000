# Trading Advisor 3000 - AI Delivery Shell

This repository hosts the AI delivery control-plane shell for Trading Advisor 3000:
- governance and workflow policy,
- hot/warm/cold documentation system,
- task lifecycle and request contracts,
- context routing and gate stack,
- durable plans, memory, and process telemetry.

Start here:
1. `AGENTS.md`
2. `docs/agent/entrypoint.md`
3. `docs/DEV_WORKFLOW.md`

PR-only main policy:
- direct push to `main` is blocked by default;
- emergency override requires both:
  - `AI_SHELL_EMERGENCY_MAIN_PUSH=1`
  - `AI_SHELL_EMERGENCY_MAIN_PUSH_REASON=<non-empty>`

Source package used for baseline bootstrap:
- `codex_ai_delivery_shell_package/`
