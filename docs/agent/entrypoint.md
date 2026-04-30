# Agent Entrypoint

## Read this first
1. `AGENTS.md`
2. `docs/agent/domains.md`
3. `docs/agent/checks.md`
4. `docs/agent/runtime.md`
5. `docs/DEV_WORKFLOW.md`
6. `docs/architecture/repository-surfaces.md`

If the task touches the product plane, also read:
1. `docs/architecture/product-plane/STATUS.md`
2. `docs/architecture/product-plane/CONTRACT_SURFACES.md`

If the task touches product-plane data, research, compute, optimization, or
orchestration runtimes, also read:
1. `docs/architecture/product-plane/native-runtime-ownership.md`
2. `docs/agent/native-runtime-selection.md`

## Startup checklist
1. Confirm the task change surface: `shell`, `product-plane`, or `mixed`.
2. Record that surface in the active task note and keep the same declaration in PR metadata.
3. Run context routing before broad repo reading: `python scripts/context_router.py --from-git --format text`.
4. Read the primary context card first and follow `navigation_order` only as far as the matched files require.
5. For code work, use the primary context card's `Search Seeds` as Serena entrypoints before opening whole files.
6. For ordinary chat, select global Codex skills first and name them briefly before substantial work. Use `codex-skill-routing` when the task is about skill routing or prompt protection.
7. Apply skill sequence rules from `docs/agent/skills-routing.md`: load the skill that owns the current artifact first, add adjacent skills only when their phase is reached, and keep verification/acceptance skills for closeout.
8. If a relevant global skill is not present in session metadata but exists under `D:/CodexHome/skills`, read that skill's main instruction file directly and state the fallback.
9. Open repo-local skills only for TA3000-specific product/trading/data/compute knowledge under `.codex/skills`; `.cursor/skills` is legacy cleanup state only.
10. For non-trivial code changes or new code inside an existing subsystem, start code discovery through Serena before broad text scans, whole-file reads, or implementation.
11. Use Serena to inspect relevant symbols, nearby patterns, and references. For new isolated files, inspect the closest existing module or pattern first unless the task is truly standalone.
12. Before expanding beyond the primary route into memory, current diff, logs, generated artifacts, live process state, Graphify, web docs, or broad file reads, record a short Context Expansion Reason: question, source/tool, why current context is insufficient, and stop condition.
13. Skip Serena only for docs-only work, already localized tiny edits, generated/artifact paths, config/non-code-only tasks, unsupported file types, or Serena unavailability; state the fallback reason briefly.
14. For architecture-heavy, cross-module, ownership-sensitive, or concept-location uncertain code tasks, follow Architecture Orientation Routing in `docs/agent/skills-routing.md`.
15. For product-plane runtime work, record the Native Runtime Choice from `docs/architecture/product-plane/native-runtime-ownership.md` before implementation.
16. Confirm no business/domain logic is being imported into shell control-plane files.
17. If the diff hits a configured critical contour, read `docs/agent/critical-contours.md` and add `## Solution Intent` to the active task note before coding.
18. For package intake or governed continuation, prefer the one-step bootstrap: `python scripts/codex_governed_bootstrap.py --request "<request>" ...`.
19. Otherwise start lifecycle directly: `python scripts/task_session.py begin --request "<request>"`.
20. If the operator asks to take a package or continue a module phase, do not start manual inline execution. Route first through `python scripts/codex_governed_entry.py ...`.
21. Confirm patch set is small and explicit.
22. Run loop gate before PR prep: `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.

## Critical constraints
- Mainline is PR-only by default.
- `run_loop_gate.py` is canonical hot-path gate.
- `docs/session_handoff.md` stays a pointer-shim.
- Global Codex skills are the ordinary-chat routing source. Repo-local `.codex/skills` are cold-by-default and only for targeted TA3000/product-plane signals.
- Serena is the mandatory first route for non-trivial code discovery, but not a CI gate; do not add heavy checks unless they pay for themselves on the active task.
- Emergency main push requires explicit neutral variables.
- Product-plane work is allowed in isolated app paths; shell surfaces stay domain-free.
- Product-plane research/backtest inputs must use native Delta/Arrow/Spark reads with predicates and column projection; do not use Python row-object loaders as an active fallback for Delta-backed analytical tables.
- For package intake or module continuation, manual chat-only execution is not a valid governed route. The governed entry launcher must be used first.
- Spark, Delta Lake, Dagster, pandas-ta-classic, vectorbt, Optuna, and DuckDB must be considered as native runtime owners before custom Python owns product-plane data, compute, optimization, or orchestration logic.

## Escalate when
- one patch mixes multiple high-risk surfaces,
- the same validation failure repeats twice,
- one patch triggers multiple critical contours,
- runtime entrypoints are missing for the requested flow,
- the task spans shell and product-plane boundaries and the split is unclear.
