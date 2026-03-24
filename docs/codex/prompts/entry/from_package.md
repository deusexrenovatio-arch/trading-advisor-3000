Use the package-intake flow for this repository.

Read first:

1. `AGENTS.md`
2. `docs/agent/entrypoint.md`
3. `docs/agent/domains.md`
4. `docs/agent/checks.md`
5. `docs/agent/runtime.md`
6. `docs/DEV_WORKFLOW.md`
7. `docs/session_handoff.md`
8. the package manifest
9. the suggested primary document from the package
10. supporting documents only as needed

Execution preferences:

- Treat the `zip` archive as one source package, not as one already-clean spec.
- Choose one primary document and record the rule used.
- Treat remaining documents as supporting material unless they contradict the primary document.
- Ask at most one compact clarification block only if safe progress is impossible.
- Do not rely on silent assumptions; if package ambiguity materially changes execution, surface one compact clarification block or classify the package as repairable.
- Keep `docs/session_handoff.md` as a lightweight pointer shim.
- Use canonical gate names only.
- After intake, continue the normal execution-contract and loop-gate flow.

The launcher or operator appends lines in this exact form:

Package zip path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Extracted package root: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Package manifest path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Suggested primary document: <ABSOLUTE_OR_REPO_RELATIVE_PATH_OR_NONE>
Mode hint: <auto|plan-only|implement-only|continue|repair>
