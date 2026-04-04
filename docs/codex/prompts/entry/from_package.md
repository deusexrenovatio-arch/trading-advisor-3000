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
11. `docs/agent/skills-catalog.md`
12. `.cursor/skills/` (quick directory-level scan only)

Execution preferences:

- Treat the `zip` archive as one source package, not as one already-clean spec.
- The governed launcher has already resolved the package route before this prompt starts.
- Do not call the launcher again from inside this runtime prompt.
- Choose one primary document and record the rule used.
- Run a quick skills coverage pass against `docs/agent/skills-catalog.md`:
  - identify which current skills are sufficient for this package;
  - identify whether additional skills are needed for downstream phase execution;
  - keep this pass lightweight (catalog-level, not deep per-skill analysis).
- Treat remaining documents as supporting material unless they contradict the primary document.
- Ask at most one compact clarification block only if safe progress is impossible.
- Do not rely on silent assumptions; if package ambiguity materially changes execution, surface one compact clarification block or classify the package as repairable.
- Keep `docs/session_handoff.md` as a lightweight pointer shim.
- Use canonical gate names only.
- If the selected source document declares an explicit phase/module rollout, first materialize the canonical execution contract and phase briefs under `docs/codex/contracts/` and `docs/codex/modules/`.
- If `Suggested phase compiler artifact` is not `NONE`, treat it as the deterministic phase IR for the suggested primary document.
- If `Suggested phase ids` is not `NONE`, keep a strict 1:1 mapping between source phase ids and generated phase briefs.
- Do not merge, rename, reorder, or soften source phases when a deterministic phase IR is present.
- Preserve source phase ids, objectives, acceptance gates, and disprovers unless the package is explicitly classified as repairable.
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
- Intake phase contract is mandatory:
  - perform documentation intake;
  - decompose work into explicit phases;
  - produce/refresh canonical planning docs (execution contract + parent brief + phase briefs).
- Intake must run as two explicit analytical sub-phases before handoff:
  - `technical_intake`: architecture fit, delivery/quality risks, implementation blockers;
  - `product_intake`: product value, user impact, business viability, value-risk blockers.
- Intake role lenses are mandatory in this phase:
  - `architecture-review`: architecture fit, boundary correctness, conflicts with existing app architecture;
  - `business-analyst`: use cases and acceptance test cases for generated docs;
  - `product-owner`: product value and business proposal clarity;
  - `tz-oss-scout`: quick OSS/options scan with links for reuse where appropriate.
- Intake is allowed to return `BLOCKED` and request explicit operator input when:
  - product value is not proven;
  - architecture review finds blocking mismatch/risk;
  - source package lacks enough information for safe phase planning.
- Include one compact `Skills Coverage Check` section in the intake output with:
  - `Current skills sufficient: yes/no`;
  - `Suggested additional skills` (if any);
  - `Why needed` (one-line per suggestion).
- Runtime lanes:
  - `technical_intake` lane returns only one tagged JSON block with `BEGIN_TECHNICAL_INTAKE_JSON` / `END_TECHNICAL_INTAKE_JSON`;
  - `product_intake` lane returns only one tagged JSON block with `BEGIN_PRODUCT_INTAKE_JSON` / `END_PRODUCT_INTAKE_JSON`;
  - `materialization` lane consumes lane artifacts and writes canonical docs; it does not emit lane-gate JSON tags.
- Each lane payload must include:
  - `created_docs`, `review_summary`, `blockers`;
  - each blocker: `id`, `severity` (`P0|P1|P2`), `scale` (`S|M|L|XL`), `title`, `why`, `required_action`.
- Formal combined gate is computed outside your narrative:
  - if any `P0` or `P1` exists across both lanes, the intake gate is blocked.

The launcher or operator appends lines in this exact form:

Package zip path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Extracted package root: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Package manifest path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Suggested primary document: <ABSOLUTE_OR_REPO_RELATIVE_PATH_OR_NONE>
Suggested phase compiler artifact: <ABSOLUTE_OR_REPO_RELATIVE_PATH_OR_NONE>
Suggested phase ids: <CSV_PHASE_IDS_OR_NONE>
Mode hint: <auto|plan-only|implement-only|continue|repair>
