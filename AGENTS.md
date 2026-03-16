# AGENTS.md — Trading Advisor 3000 AI Delivery Shell

## Purpose
This repository is the control plane for AI-first delivery of Trading Advisor 3000.
It defines process, governance, validation, and durable development state.

Business and trading logic are out of scope for this shell baseline.

## Non-Negotiable Rules
1. Do not port domain-specific trading logic into the shell layer.
2. Direct push to `main` is blocked by default.
3. `run_loop_gate.py` is the only canonical hot-path gate name.
4. `docs/session_handoff.md` must stay a lightweight pointer shim.
5. `plans/items/` is canonical and `plans/PLANS.yaml` is generated compatibility output (enabled in Phase 5).
6. Domain skills are excluded from the baseline shell.

## Source-Of-Truth Layers
### Hot (read first)
- `docs/agent/entrypoint.md`
- `docs/agent/domains.md`
- `docs/agent/checks.md`
- `docs/agent/runtime.md`
- `docs/agent/skills-routing.md`
- `docs/DEV_WORKFLOW.md`

### Warm (read by signal)
- `docs/README.md`
- `docs/planning/phase0-phase1-bootstrap.md`
- `docs/checklists/README.md`
- `docs/workflows/README.md`
- `docs/runbooks/README.md`
- `docs/architecture/README.md`

### Cold (do not load by default)
- `codex_ai_delivery_shell_package/**`
- `docs/tasks/archive/**`
- `memory/**`
- `plans/**`
- historical artifacts, generated reports, and archives

## Phase-Aware Delivery Loop
### Phase 1 baseline
1. Read hot docs.
2. Confirm change surface and no domain contamination.
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
- Active scope: Phase 0 through Phase 8 (shell baseline).
- The repository remains process/governance focused and intentionally excludes product business logic.
