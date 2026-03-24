# Package Intake Lab Contract

Updated: 2026-03-22 14:22 UTC

## Goal

- Build a package-first Codex entry flow that starts from one `zip` archive containing a full task/TZ document set.

## Why this patch exists

- The real operator workflow is package-oriented, not single-markdown-oriented.
- Voice-mode operation should minimize back-and-forth and avoid asking the operator to run local commands.
- Codex needs a deterministic intake step before the normal execution-contract and implementation flow.

## In Scope

- one tracked inbox for incoming `zip` packages,
- one package-entry prompt for Codex,
- one deterministic launcher that unpacks the package and generates a manifest,
- one ranking heuristic that chooses a suggested primary document,
- one compact clarification policy for ambiguous packages.

## Out Of Scope

- hard-enforced reviewer subagent orchestration,
- full acceptance-pipeline automation,
- changes to canonical gate names or PR-only policy,
- any trading or business logic.

## Input Contract

- The operator may drop one or more `zip` packages into `docs/codex/packages/inbox/`.
- If no path is passed explicitly, the newest `zip` package is the default intake target.
- The launcher must unpack the package into `artifacts/codex/package-intake/`.
- The launcher must create a manifest that lists candidate primary documents and the ranking rationale.

## Primary Decision Rules

1. Prefer one primary document over many equal peers.
2. Prefer deterministic ranking over ad-hoc choice.
3. Prefer `READY` or `REPAIRABLE` progress over blocking.
4. Ask at most one compact clarification block only when safe progress is not possible.
5. Keep the rest of the governance loop unchanged after intake.

## Candidate Ranking Rules

- Prefer filenames that look like requirements/spec/TZ documents.
- Prefer markdown and text documents first, then docx/pdf when needed.
- Penalize generic `README`-style files when more specific documents exist.
- Record the chosen primary document and the reasons in the manifest.

## Done Evidence

- `python -m py_compile scripts/codex_from_package.py`
- `python -m pytest tests/process/test_codex_from_package.py -q`
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

## Remaining Risks

- The ranking heuristic may need tuning after the first real package.
- PDF-heavy bundles will still need a later parsing/normalization patch if the primary document is not markdown/text/docx.
