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
9. Generic engineering skills belong in the global Codex skill root (`D:/CodexHome/skills`); repo-local skills are only for TA3000-specific trading, product-domain, or compute-runtime knowledge.
10. `.cursor/skills` is a retired legacy skill catalog. Do not add skills there; use global Codex skills for ordinary chat routing and `.codex/skills` only for TA3000-specific product-plane/trading/data/compute knowledge.
11. GraphQL and Node.js are not active TA3000 baseline surfaces. Do not route GraphQL/Node-specific skills unless active source files or contracts appear outside ignored temporary, generated, archive, or package-intake paths.

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

## Semantic Code Navigation
- Before broad repository reading on a non-trivial task, run context routing or consume its current result:
  `python scripts/context_router.py --from-git --format text`.
- Read the primary context card in `docs/agent-contexts/`, then follow `navigation_order` only as far as the matched files require.
- Use each context card's `Inside This Context` and `Search Seeds` sections to choose where Serena should start.
- For non-trivial code changes or new code inside an existing subsystem, Serena is the mandatory first route for code discovery, local pattern learning, impact analysis, and reference checks before editing or adding code.
- Use Serena for symbol overview, symbol lookup, nearby implementation patterns, references, rename/refactor planning, and precise implementation proof before broad text scans, whole-file reads, or multi-file code dumps.
- Before expanding beyond the primary context route into memory, current diff, logs, generated artifacts, live process state, Graphify, web docs, or broad file reads, leave a short Context Expansion Reason: what uncertainty is being resolved, which source/tool will answer it, why the current route is insufficient, and when to stop expanding.
- For new isolated files, first use Serena to inspect the closest existing module or pattern unless the task is truly standalone.
- Direct search/read fallback is allowed only for docs-only work, generated/artifact paths, config/non-code-only tasks, tiny already-localized edits, unsupported file types, or Serena unavailability.
- If Serena is skipped or unavailable on a code task, state the fallback reason briefly and continue with the lightest reliable tools.

## Ordinary Chat Skill Routing
- In normal chat, route through global Codex skills first. Use `codex-skill-routing` when the task is about skill selection, prompt routing, or preventing missed skills.
- Before substantial work, name the selected skills briefly and why they apply. If no skill is needed, say why.
- Select skills by sequence, not keyword count: start with the skill that owns the current artifact, add neighboring skills only when their phase is reached, and keep evidence/acceptance skills for closeout.
- For non-trivial implementation, use `code-implementation-worker` plus the relevant architecture, contract, executable-test, documentation, and verification skills in that order.
- For review, acceptance, unblock, or "is this done?" questions, use `code-reviewer`, `phase-acceptance-governor`, and/or `verification-before-completion` as appropriate.
- If a required skill is missing from the current session metadata but exists on disk under `D:/CodexHome/skills`, read that skill's main instruction file directly and state it as a fallback.
- Open repo-local skills only for TA3000-specific product/trading/data/compute knowledge under `.codex/skills`.

## Phase-Aware Delivery Loop
### Phase 1 baseline
1. Read hot docs.
2. Confirm change surface and keep domain logic out of shell files.
3. For non-trivial code work, start code discovery through Serena before broad scans or implementation.
4. For architecture-heavy or cross-module work, follow architecture routing in `docs/agent/skills-routing.md`.
5. Implement one small patch set.
6. Run checks from `docs/agent/checks.md`.
7. Prepare PR-oriented change summary.

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
