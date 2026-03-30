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
- The governed launcher has already resolved the package route before this prompt starts.
- Do not call the launcher again from inside this runtime prompt.
- Choose one primary document and record the rule used.
- Treat remaining documents as supporting material unless they contradict the primary document.
- Ask at most one compact clarification block only if safe progress is impossible.
- Do not rely on silent assumptions; if package ambiguity materially changes execution, surface one compact clarification block or classify the package as repairable.
- Keep `docs/session_handoff.md` as a lightweight pointer shim.
- Use canonical gate names only.
- If the selected source document declares an explicit phase/module rollout, first materialize the canonical execution contract and phase briefs under `docs/codex/contracts/` and `docs/codex/modules/`.
- Before accepting that phase plan, bind a `## Release Target Contract` in the execution contract.
- Add `## Mandatory Real Contours` with one bullet per contour that must become real before final allow.
- Add `## Release Surface Matrix` and make every mandatory contour owned by exactly one phase.
- Every phase brief must include `## Release Gate Impact` and `## What This Phase Does Not Prove`.
- Every phase brief must include `## Release Surface Ownership`.
- Use accepted-state labels honestly:
  - `prep_closed`
  - `real_contour_closed`
  - `release_decision`
- If a mandatory real contour is owned only by a `prep_closed` phase, the plan is invalid.
- Do not let a docs/schema/mock/stub/smoke phase read as release readiness when the target decision requires `live-real` proof.
- For such phase-driven packages, stop after module-path normalization and phase planning; do not collapse multiple declared phases into one package-run implementation patch.
- After intake, continue the normal execution-contract and loop-gate flow.

The launcher or operator appends lines in this exact form:

Package zip path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Extracted package root: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Package manifest path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Suggested primary document: <ABSOLUTE_OR_REPO_RELATIVE_PATH_OR_NONE>
Mode hint: <auto|plan-only|implement-only|continue|repair>
