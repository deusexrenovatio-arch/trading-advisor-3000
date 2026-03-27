# Trading Advisor 3000 — stack-conformance remediation package

This package validates the systemic acceptance-model drift found in the repository and defines a corrective technical assignment for Codex.

## Package contents

- `00_validation_verdict.md` — validation of the reported issue against current `main`, PR #2, and the mirrored product spec.
- `01_current_stack_conformance_matrix.md` — current target-vs-implemented stack matrix with recommended action per surface.
- `02_corrective_technical_assignment.md` — corrective technical assignment.
- `03_phase_plan_and_acceptance_gates.md` — phased, atomic remediation and re-acceptance plan.
- `04_anti_self_deception_controls.md` — controls to prevent proxy evidence and false closure.
- `05_codex_backlog.csv` — machine-readable execution backlog.
- `06_stack_conformance_registry_template.yaml` — machine-readable registry template.
- `07_acceptance_evidence_template.json` — acceptance artifact template.
- `08_red_team_checklist.md` — disproof-oriented checklist for acceptance review.
- `09_evidence_map.md` — source map used for validation.

## Top-line verdict

The core issue is **validated**:

1. Early product phases were accepted on scaffold / manifest / sample-artifact evidence rather than on executable proof of the declared target stack.
2. CI and check gates proved process correctness and local test consistency, but did **not** prove that the declared technologies were actually present in runnable runtime paths.
3. Current `main` partially corrected the narrative through `STATUS.md` and related truth-source documents, but the acceptance/checklist model still contains historical “full DoD” language that can mislead future work unless the conformance model is repaired first.

## Recommended execution order

1. Freeze false closure and repair the acceptance vocabulary.
2. Add machine-verifiable stack-conformance gates.
3. Close architecture-critical foundation surfaces first: Delta, Spark, Dagster, durable runtime defaults, and real sidecar status.
4. For replaceable technologies (FastAPI, aiogram, vectorbt, Alembic, OpenTelemetry, Polars, DuckDB), force an **implement-or-ADR-de-scope** decision.
5. Re-run phase acceptance only after black-box proof and disproof-oriented checks are green.
