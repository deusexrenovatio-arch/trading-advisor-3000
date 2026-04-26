Use the governed package-intake flow for this repository.

Read first:

1. `AGENTS.md`
2. `docs/agent/entrypoint.md`
3. `docs/agent-contexts/README.md`
4. `docs/agent/domains.md`
5. `docs/agent/checks.md`
6. `docs/agent/runtime.md`
7. `docs/DEV_WORKFLOW.md`
8. `docs/session_handoff.md`
9. the package manifest
10. the suggested primary document from the package
11. supporting source documents only as needed

Intake mission:

- Treat source package documents as canonical requirements.
- Do not rewrite or reinterpret source requirements unless you explicitly mark a blocker.
- Produce fail-closed intake decisions and phase planning artifacts.

Core rules:

- No silent assumptions.
- No hidden quality-bar downgrade.
- No phase reordering when deterministic phase ids are present.
- Keep `docs/session_handoff.md` as a lightweight pointer shim.
- Before opening broad repo context, run or consume `python scripts/context_router.py --from-git --format text`; use the primary context card to locate relevant repo state, and keep package source documents as the requirement source of truth.
- Use canonical gate names only.
- Do not call governed launcher scripts from inside this runtime prompt.
- Keep lane outputs operator-readable and low-noise: prefer meaning, decision rationale, and blockers over path/file inventory.
- When multiple source documents compete, name the primary-source tie-break rule explicitly; if the ambiguity remains material, return blockers instead of guessing.
- Prefer phase slicing that preserves target architecture, proof strength, and reversibility over the shortest or easiest patch series.
- If the source implies consolidation or simplification, state what should become the source of truth and which duplicate or temporary layer is expected to remain or be removed.

Lossless transfer contract:

- Preserve source objectives.
- Preserve mandatory constraints and forbidden shortcuts.
- Preserve acceptance gates and disprovers.
- Preserve source phase ids and ordered phase mapping when provided.
- If any item above cannot be preserved safely, intake must return blockers (not optimistic assumptions).

Sequential intake gate policy:

1. Product completeness gate.
2. Technical integrity gate.
3. Phase slicing and materialization only after gate `PASS`.

Mandatory intake output contract (fail-closed):

- `review_summary` must be a concise operator-facing verdict that states:
  - what source was treated as primary and why,
  - whether intake is ready, advisory-only, or blocked,
  - what the next governed step is.
- Preserve source goals as a concise `goals_digest`.
- Preserve source acceptance logic as `acceptance_criteria_digest`.
- Keep `goals_digest` and `acceptance_criteria_digest` measurable and human-readable; avoid path-heavy or artifact-heavy wording.
- Provide `structural_recommendations` as an explicit list of critical structural improvements/changes (`id`, `priority`, `title`, `why`, `proposal`, `impact_on_tz`), or an explicit empty list when none are needed.
- `structural_recommendations` must explain why the recommendation matters for target quality, system shape, or evidence strength, not only what to edit.
- Score `intake_quality` honestly; do not use high scores to paper over blockers or unresolved ambiguity.
- If any of the three items above cannot be produced with confidence, return blockers instead of optimistic assumptions.

Materialization policy:

- Materialize canonical execution contract and module phase briefs under:
  - `docs/codex/contracts/`
  - `docs/codex/modules/`
- Keep generated phase docs reference-first and evidence-oriented.
- Every phase brief must include mandatory sections for traceability, scope limits, assumptions/open questions, dependencies, evidence contract, conflict resolution, source versioning, and risk/rollback triggers.
- Phase docs must stay explicit about what each phase does not prove, which target shape is preserved when a path is staged, and which owned surfaces are actually being advanced.
- Do not invent extra phases or collapse source phases into one implementation patch.

The launcher or operator appends lines in this exact form:

Package zip path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Extracted package root: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Package manifest path: <ABSOLUTE_OR_REPO_RELATIVE_PATH>
Suggested primary document: <ABSOLUTE_OR_REPO_RELATIVE_PATH_OR_NONE>
Suggested phase compiler artifact: <ABSOLUTE_OR_REPO_RELATIVE_PATH_OR_NONE>
Suggested phase ids: <CSV_PHASE_IDS_OR_NONE>
Required technical intake skills: <COMMA_SEPARATED_SKILL_PATHS_OR_NONE>
Mode hint: <auto|plan-only|implement-only|continue|repair>
