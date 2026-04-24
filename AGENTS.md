# AGENTS.md - Trading Advisor 3000 Dual-Surface Delivery

## Purpose
This repository is a dual-surface delivery workspace:
- the AI delivery shell (control plane for process, governance, validation, and durable development state);
- the product plane (isolated app code, contracts, tests, and app operations documentation).

Business and trading logic remain out of scope for shell control-plane surfaces.

## Non-Negotiable Rules
1. Do not port domain-specific trading logic into the shell layer.
2. Direct push to `main` is blocked by default.
3. `run_loop_gate.py` is the only canonical hot-path gate name.
4. `docs/session_handoff.md` must stay a lightweight pointer shim.
5. `plans/items/` is canonical and `plans/PLANS.yaml` is generated compatibility output (enabled in Phase 5).
6. Domain skills are excluded from the baseline shell.
7. Package intake and governed phase continuation must start through `python scripts/codex_governed_entry.py ...`; manual chat-only continuation is not a valid governed route.
8. Every change set must declare a change surface: `shell`, `product-plane`, or `mixed`.

## Source-Of-Truth Layers
### Hot (read first)
- `docs/agent/entrypoint.md`
- `docs/agent/domains.md`
- `docs/agent/checks.md`
- `docs/agent/runtime.md`
- `docs/agent/skills-routing.md`
- `docs/DEV_WORKFLOW.md`

### Warm (read by signal)
- `README.md`
- `shell/README.md`
- `product-plane/README.md`
- `docs/README.md`
- `docs/architecture/repository-surfaces.md`
- `docs/planning/bootstrap-and-foundations.md`
- `docs/checklists/README.md`
- `docs/workflows/README.md`
- `docs/runbooks/README.md`
- `docs/architecture/README.md`
- `docs/architecture/product-plane/STATUS.md`
- `docs/architecture/product-plane/CONTRACT_SURFACES.md`
- `docs/runbooks/app/bootstrap.md`

### Cold (do not load by default)
- `codex_ai_delivery_shell_package/**`
- `docs/tasks/archive/**`
- `memory/**`
- `plans/**`
- historical artifacts, generated reports, and archives

## Phase-Aware Delivery Loop
### Phase 1 baseline
1. Read hot docs.
2. Confirm change surface and keep domain logic out of shell files.
3. Implement one small patch set.
4. Run checks from `docs/agent/checks.md`.
5. Prepare PR-oriented change summary.

### Target loop (Phase 2+)
`begin -> task note -> contract validation -> loop gate -> pr gate -> end`

## PR-Only Main Policy
- Mainline changes must go through PR flow.
- Emergency direct-main push is allowed only with:
  - `AI_SHELL_EMERGENCY_MAIN_PUSH=1`
  - `AI_SHELL_EMERGENCY_MAIN_PUSH_REASON=<non-empty>`

## Patch Set Policy
1. Keep each patch set focused on one governance concept.
2. For high-risk surfaces, split by order: `contracts -> code -> docs`.
3. If the same failure repeats twice, stop expansion and perform remediation before continuing.

## Phase Status
- Active scope: Phase 0 through Phase 8 for the shell baseline, with product-plane implementation now present under isolated app paths.
- The shell layer remains process/governance focused and intentionally excludes product business logic from shell surfaces.
