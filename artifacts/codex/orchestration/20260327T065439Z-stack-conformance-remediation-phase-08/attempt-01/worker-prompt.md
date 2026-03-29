You are the phase worker.

Read first:

1. `AGENTS.md`
2. `docs/agent/entrypoint.md`
3. `docs/agent/domains.md`
4. `docs/agent/checks.md`
5. `docs/agent/runtime.md`
6. `docs/DEV_WORKFLOW.md`
7. `docs/session_handoff.md`
8. the execution contract
9. the module parent brief
10. the current phase brief
11. `docs/codex/orchestration/acceptance-contract.md`

Rules:

- Implement only the current phase.
- Do not open the next phase.
- Do not claim acceptance.
- If a remediation blockers file is present, address only those blockers plus the minimum required supporting fixes.
- Run only the checks that match the current phase.
- Keep the patch reviewable and phase-scoped.
- Do not silently introduce assumptions, fallback paths, skipped checks, or deferred critical work.
- If any assumption, skip, fallback, or deferral exists, report it explicitly. Acceptance will treat it as a blocker.
- Make the route explicit so the operator can see that the governed phase route is in use.

Return a short human summary and finish with this exact marker block:

BEGIN_PHASE_WORKER_JSON
{"status":"DONE","summary":"...","route_signal":"worker:phase-only","files_touched":["..."],"checks_run":["..."],"remaining_risks":["..."],"assumptions":[],"skips":[],"fallbacks":[],"deferred_work":[]}
END_PHASE_WORKER_JSON

Route Guardrails:
- no silent assumptions
- no skipped required checks
- no silent fallbacks
- no deferred critical work
- acceptance requires architecture, test, and docs closure

Execution Contract: D:/trading advisor 3000/docs/codex/contracts/stack-conformance-remediation.execution-contract.md
Module Parent Brief: D:/trading advisor 3000/docs/codex/modules/stack-conformance-remediation.parent.md
Phase Brief: D:/trading advisor 3000/docs/codex/modules/stack-conformance-remediation.phase-08.md
Attempt Number: 1
Remediation Blockers: NONE
