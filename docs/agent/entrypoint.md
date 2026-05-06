# Agent Entrypoint

## Read this first
1. `AGENTS.md`
2. `docs/agent/domains.md`
3. `docs/agent/checks.md`
4. `docs/agent/runtime.md`
5. `docs/DEV_WORKFLOW.md`
6. `docs/architecture/repository-surfaces.md`

If the task touches the product plane, also read:
1. `docs/project-map/current-truth-map-2026-05-05.md`
2. `docs/architecture/product-plane/STATUS.md`
3. `docs/architecture/product-plane/CONTRACT_SURFACES.md`

Reality rule:
- Treat old task notes, package-intake artifacts, TZs, and target-shape specs as
  historical evidence unless the current truth map or current product docs
  explicitly promote them.
- Treat `docs/archive/` as off-route by default. Open it only for explicit
  forensic/audit work, broken-reference remediation, or when a current truth
  document names a specific archived artifact as evidence.

If the task touches product-plane data, research, compute, optimization, or
orchestration runtimes, also read:
1. `docs/architecture/product-plane/native-runtime-ownership.md`
2. `docs/agent/native-runtime-selection.md`

## Startup checklist
1. Confirm the task change surface: `shell`, `product-plane`, or `mixed`.
2. Record that surface in the active task note and keep the same declaration in PR metadata.
3. When Superpowers skills are available, invoke the relevant Superpowers process skill before clarification, repo reading, implementation, review, verification, or closeout. If unavailable, state the fallback and continue through global Codex skills.
4. Run context routing before broad repo reading: `python scripts/context_router.py --from-git --format text`.
5. Read the primary context card first and follow `navigation_order` only as far as the matched files require.
6. For code work, use the primary context card's `Search Seeds` as Serena entrypoints before opening whole files.
7. For ordinary chat, select global Codex skills after the Superpowers process check and name selected skills briefly before substantial work. Use `codex-skill-routing` when the task is about skill routing or prompt protection.
8. Apply skill sequence rules from `docs/agent/skills-routing.md`: load the skill that owns the current artifact first, add adjacent skills only when their phase is reached, and keep verification/acceptance skills for closeout.
9. If a relevant global skill is not present in session metadata but exists under `D:/CodexHome/skills`, read that skill's main instruction file directly and state the fallback.
10. Open repo-local skills only for TA3000-specific product/trading/data/compute knowledge under `.codex/skills`; `.cursor/skills` is legacy cleanup state only.
11. For non-trivial code changes or new code inside an existing subsystem, start code discovery through Serena before broad text scans, whole-file reads, or implementation.
12. Use Serena to inspect relevant symbols, nearby patterns, and references. For new isolated files, inspect the closest existing module or pattern first unless the task is truly standalone.
13. Before expanding beyond the primary route into memory, current diff, logs, generated artifacts, live process state, Graphify, web docs, or broad file reads, record a short Context Expansion Reason: question, source/tool, why current context is insufficient, and stop condition.
14. Skip Serena only for docs-only work, already localized tiny edits, generated/artifact paths, config/non-code-only tasks, unsupported file types, or Serena unavailability; state the fallback reason briefly.
15. For architecture-heavy, cross-module, ownership-sensitive, or concept-location uncertain code tasks, follow Architecture Orientation Routing in `docs/agent/skills-routing.md`.
16. For product-plane runtime work, record the Native Runtime Choice from `docs/architecture/product-plane/native-runtime-ownership.md` before implementation.
17. Confirm no business/domain logic is being imported into shell control-plane files.
18. If the diff hits a configured critical contour, read `docs/agent/critical-contours.md` and add `## Solution Intent` to the active task note before coding.
19. For package intake or governed continuation, prefer the one-step bootstrap: `python scripts/codex_governed_bootstrap.py --request "<request>" ...`.
20. Otherwise start lifecycle directly: `python scripts/task_session.py begin --request "<request>"`.
21. If the operator asks to take a package or continue a module phase, do not start manual inline execution. Route first through `python scripts/codex_governed_entry.py ...`.
22. Confirm patch set is small and explicit.
23. Run loop gate before PR prep: `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.

## Critical constraints
- Mainline is PR-only by default.
- `run_loop_gate.py` is canonical hot-path gate.
- `docs/session_handoff.md` stays a pointer-shim.
- Superpowers process skills are the first routing check when available. Global Codex skills remain the ordinary-chat engineering source. Repo-local `.codex/skills` are cold-by-default and only for targeted TA3000/product-plane signals.
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
