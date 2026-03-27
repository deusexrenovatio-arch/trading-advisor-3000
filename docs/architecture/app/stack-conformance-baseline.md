# Product Stack-Conformance Baseline

## Purpose
Define a conservative baseline for what can be claimed today in product-plane architecture docs.

This baseline is claim-control only.
It does not replace implementation docs, checklists, or phase narratives.

## Truth Source Hierarchy
1. [STATUS.md](docs/architecture/app/STATUS.md) is the current truth source.
2. [CONTRACT_SURFACES.md](docs/architecture/app/CONTRACT_SURFACES.md) is the contract boundary inventory.
3. Phase docs and checklists are evidence history and must not override `docs/architecture/app/STATUS.md`.

## Claim Classes
| Class | Meaning | Minimum evidence | Prohibited interpretation |
| --- | --- | --- | --- |
| `implemented` | Slice exists and is exercised by code/tests/docs. | Code path + tests + referenced docs/runbook. | Not a claim of full production readiness. |
| `partial` | Slice is materially present but has declared gaps. | Evidence for implemented subset + explicit gap statement. | Not a claim of closure for the full target architecture. |
| `planned` | Work is defined but not landed. | Explicit design/backlog reference. | Not a claim of runtime availability. |
| `not accepted` | Closure claim is intentionally blocked. | Truth-source statement in `docs/architecture/app/STATUS.md` or acceptance note. | Must not be rewritten as accepted by checklist wording. |

## Current Baseline (G0)
- Product-plane production readiness remains `not accepted`.
- Real broker process closure remains `planned`.
- Delta Lake closure is `partial`: physical Delta runtime proof exists for the phase2 data/research slice only.
- Apache Spark closure is `partial`: the phase2 canonical job is executable in a Docker/Linux proof profile, but full distributed orchestration remains open.
- Dagster closure is `partial`: executable `Definitions` and local materialization proof exist for the phase2a canonical slice, while broader orchestration coverage remains open.
- FastAPI service/API closure is `implemented` for runtime/admin bootstrap and health/readiness smoke paths.
- aiogram is `removed` from the current chosen stack baseline; Telegram publication is currently implemented via the deterministic in-repo adapter.
- vectorbt, Alembic, OpenTelemetry, Polars, and DuckDB are `removed` from the current chosen stack baseline under ADR-012; they require explicit ADR + executable proof before any reintroduction.
- Live execution transport baseline is `implemented` at contract level with an in-repo .NET sidecar plus compiled-binary smoke path; real broker process closure remains `planned`.
- Shell proving artifacts do not equal product capability closure.

## Writer Rules
- Use vocabulary from [restricted-acceptance-vocabulary.md](docs/architecture/app/restricted-acceptance-vocabulary.md).
- If wording conflicts with `docs/architecture/app/STATUS.md`, downgrade wording to the truth-source level.
- Keep historical evidence, but label it as baseline or partial when full closure is not proven.

## Machine Gate
- Stack-claim enforcement is codified in `registry/stack_conformance.yaml`.
- `python scripts/validate_stack_conformance.py` is fail-closed for:
  - status drift between registry and `docs/architecture/app/STATUS.md`,
  - `implemented` claims without runtime/dependency/test proof,
  - forbidden full-closure wording while non-implemented surfaces remain,
  - removed technologies still declared as chosen in the stack spec.
