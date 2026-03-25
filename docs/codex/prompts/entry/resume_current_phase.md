Use the existing module execution state for this repository.

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
11. the latest orchestration state, if present
12. `docs/codex/orchestration/acceptance-contract.md`

Execution rules:

- Do not re-intake a package when a valid module execution contract already exists.
- Continue only from the current phase pointer in the parent brief.
- The first execution action must be the governed launcher:
  - `python scripts/codex_governed_entry.py continue --execution-contract <path> --parent-brief <path>`
- Use the governed route only:
  - worker,
  - acceptance,
  - remediation when blocked,
  - unlock next phase only after `PASS`.
- Do not silently introduce assumptions, fallbacks, skipped checks, or deferred critical work.
- Make the route explicit in reports so the operator can see the path taken.
- If the launcher was not run, the route was not started.

The launcher or operator appends lines in this exact form:

Execution contract path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Module parent brief path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Current phase path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Latest orchestration state: <ABSOLUTE_OR_REPO_RELATIVE_PATH_OR_NONE>
Backend hint: <simulate|codex-cli>
Mode hint: continue
