# Agent Entrypoint

## Read this first
1. `AGENTS.md`
2. `docs/agent/domains.md`
3. `docs/agent/checks.md`
4. `docs/agent/runtime.md`
5. `docs/DEV_WORKFLOW.md`

If the task touches the product plane, also read:
1. `docs/architecture/app/STATUS.md`
2. `docs/architecture/app/CONTRACT_SURFACES.md`

## Startup checklist
1. Confirm the task change surface: shell, product plane, or both.
2. Confirm no business/domain logic is being imported into shell control-plane files.
3. If the diff hits a configured critical contour, read `docs/agent/critical-contours.md` and add `## Solution Intent` to the active task note before coding.
4. For package intake or governed continuation, prefer the one-step bootstrap: `python scripts/codex_governed_bootstrap.py --request "<request>" ...`.
5. Otherwise start lifecycle directly: `python scripts/task_session.py begin --request "<request>"`.
6. If the operator asks to take a package or continue a module phase, do not start manual inline execution. Route first through `python scripts/codex_governed_entry.py ...`.
7. Confirm patch set is small and explicit.
8. Run loop gate before PR prep: `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`.

## Critical constraints
- Mainline is PR-only by default.
- `run_loop_gate.py` is canonical hot-path gate.
- `docs/session_handoff.md` stays a pointer-shim.
- Skills corpus is cold-by-default; open only targeted skill files by signal.
- Emergency main push requires explicit neutral variables.
- Product-plane work is allowed in isolated app paths; shell surfaces stay domain-free.
- For package intake or module continuation, manual chat-only execution is not a valid governed route. The governed entry launcher must be used first.

## Escalate when
- one patch mixes multiple high-risk surfaces,
- the same validation failure repeats twice,
- one patch triggers multiple critical contours,
- runtime entrypoints are missing for the requested flow,
- the task spans shell and product-plane boundaries and the split is unclear.
