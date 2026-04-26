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

## Startup checklist
1. Confirm the task change surface: `shell`, `product-plane`, or `mixed`.
2. Record that surface in the active task note and keep the same declaration in PR metadata.
3. For non-trivial code changes or new code inside an existing subsystem, start code discovery through Serena before broad text scans, whole-file reads, or implementation.
4. Use Serena to inspect relevant symbols, nearby patterns, and references. For new isolated files, inspect the closest existing module or pattern first unless the task is truly standalone.
5. Skip Serena only for docs-only work, already localized tiny edits, generated/artifact paths, config/non-code-only tasks, unsupported file types, or Serena unavailability; state the fallback reason briefly.
6. For architecture-heavy, cross-module, ownership-sensitive, or concept-location uncertain code tasks, follow Architecture Orientation Routing in `docs/agent/skills-routing.md`.
7. Confirm no business/domain logic is being imported into shell control-plane files.
8. If the diff hits a configured critical contour, read `docs/agent/critical-contours.md` and add `## Solution Intent` to the active task note before coding.
9. For package intake or governed continuation, prefer the one-step bootstrap: `python scripts/codex_governed_bootstrap.py --request "<request>" ...`.
10. Otherwise start lifecycle directly: `python scripts/task_session.py begin --request "<request>"`.
11. If the operator asks to take a package or continue a module phase, do not start manual inline execution. Route first through `python scripts/codex_governed_entry.py ...`.
12. Confirm patch set is small and explicit.
13. Run loop gate before PR prep: `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.

## Critical constraints
- Mainline is PR-only by default.
- `run_loop_gate.py` is canonical hot-path gate.
- `docs/session_handoff.md` stays a pointer-shim.
- Skills corpus is cold-by-default; open only targeted skill files by signal.
- Serena is the mandatory first route for non-trivial code discovery, but not a CI gate; do not add heavy checks unless they pay for themselves on the active task.
- Emergency main push requires explicit neutral variables.
- Product-plane work is allowed in isolated app paths; shell surfaces stay domain-free.
- For package intake or module continuation, manual chat-only execution is not a valid governed route. The governed entry launcher must be used first.

## Escalate when
- one patch mixes multiple high-risk surfaces,
- the same validation failure repeats twice,
- one patch triggers multiple critical contours,
- runtime entrypoints are missing for the requested flow,
- the task spans shell and product-plane boundaries and the split is unclear.
