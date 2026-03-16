# Agent Entrypoint

## Read this first
1. `AGENTS.md`
2. `docs/agent/domains.md`
3. `docs/agent/checks.md`
4. `docs/agent/runtime.md`
5. `docs/DEV_WORKFLOW.md`

## Startup checklist
1. Confirm task belongs to AI delivery shell scope.
2. Confirm no business/domain logic is being imported.
3. Start lifecycle: `python scripts/task_session.py begin --request "<request>"`.
4. Confirm patch set is small and explicit.
5. Run loop gate before PR prep.

## Critical constraints
- Mainline is PR-only by default.
- `run_loop_gate.py` is canonical hot-path gate.
- `docs/session_handoff.md` stays a pointer-shim.
- Emergency main push requires explicit neutral variables.

## Escalate when
- one patch mixes multiple high-risk surfaces,
- the same validation failure repeats twice,
- runtime entrypoints are missing for the requested flow.
