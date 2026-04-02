# Trading Advisor 3000 - Dual-Surface Repository

This repository operates with two explicit surfaces that are developed in one place:
1. Delivery Shell (control plane for process/governance).
2. Product Plane (application code, contracts, and operations).

## At a glance

| Surface | Purpose | Canonical paths |
| --- | --- | --- |
| Delivery Shell | governance policy, lifecycle, validation, gates, durable process state | `AGENTS.md`, `docs/agent/*`, `scripts/*`, `configs/*`, `plans/*`, `memory/*` |
| Product Plane | app runtime, data/research/execution flows, contracts, app docs/runbooks, deployment surfaces | `src/trading_advisor_3000/*`, `tests/app/*`, `docs/architecture/app/*`, `docs/runbooks/app/*`, `deployment/*` |

## Fast navigation

- Shell hub: `shell/README.md`
- Product hub: `product-plane/README.md`
- Boundary map: `docs/architecture/repository-surfaces.md`
- Product truth source: `docs/architecture/app/STATUS.md`

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
