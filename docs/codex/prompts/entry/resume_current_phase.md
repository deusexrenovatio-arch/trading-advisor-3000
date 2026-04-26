Use the existing module execution state for this repository.

Read first:

1. `AGENTS.md`
2. `docs/agent/entrypoint.md`
3. `docs/agent-contexts/README.md`
4. `docs/agent/domains.md`
5. `docs/agent/checks.md`
6. `docs/agent/runtime.md`
7. `docs/DEV_WORKFLOW.md`
8. `docs/session_handoff.md`
9. the execution contract
10. the module parent brief
11. the current phase brief
12. the latest orchestration state, if present
13. `docs/codex/orchestration/acceptance-contract.md`

Execution rules:

- Do not re-intake a package when a valid module execution contract already exists.
- Continue only from the current phase pointer in the parent brief.
- The governed launcher must already have been the first execution action in this continuation lifecycle:
  - `python scripts/codex_governed_entry.py continue --execution-contract <path> --parent-brief <path>`
- This prompt does not authorize route selection by itself; if launcher evidence, route state, or phase pointers are missing or mismatched, report a route blocker instead of pretending continuation started.
- Before opening broad context, run or consume `python scripts/context_router.py --from-git --format text` and use the primary card's `Inside This Context` plus `Search Seeds` as the search route.
- Use the governed route only:
  - worker,
  - acceptance,
  - remediation when blocked,
  - unlock next phase only after `PASS`.
- Do not silently introduce assumptions, fallbacks, skipped checks, or deferred critical work.
- Make the route explicit in reports so the operator can see the path taken.
- Keep reports operator-usable: state current phase goal, chosen path, why it is not a shortcut, and the exact unlock condition before expanding into detailed evidence.
- When blocked, give a terminal verdict and exact next step, not only a process note.
- Prefer target architecture fit, proof strength, and reversibility over the smallest local patch.
- If the launcher was not run, the route was not started.

The launcher or operator appends lines in this exact form:

Execution contract path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Module parent brief path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Current phase path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Latest orchestration state: <ABSOLUTE_OR_REPO_RELATIVE_PATH_OR_NONE>
Backend hint: <simulate|codex-cli>
Mode hint: continue
