# Product Stack-Conformance Baseline

## Purpose
Define a conservative baseline for what can be claimed today in product-plane architecture docs.

This baseline is claim-control only.
It does not replace implementation docs, checklists, or capability narratives.

## Truth Source Hierarchy
1. [STATUS.md](docs/architecture/product-plane/STATUS.md) is the current truth source.
2. [CONTRACT_SURFACES.md](docs/architecture/product-plane/CONTRACT_SURFACES.md) is the contract boundary inventory.
3. Capability docs and checklists are evidence history and must not override `docs/architecture/product-plane/STATUS.md`.

## Claim Classes
| Class | Meaning | Minimum evidence | Prohibited interpretation |
| --- | --- | --- | --- |
| `implemented` | Slice exists and is exercised by code/tests/docs. | Code path + tests + referenced docs/runbook. | Not a claim of full production readiness. |
| `partial` | Slice is materially present but has declared gaps. | Evidence for implemented subset + explicit gap statement. | Not a claim of closure for the full target architecture. |
| `planned` | Work is defined but not landed. | Explicit design/backlog reference. | Not a claim of runtime availability. |
| `not accepted` | Closure claim is intentionally blocked. | Truth-source statement in `docs/architecture/product-plane/STATUS.md` or acceptance note. | Must not be rewritten as accepted by checklist wording. |

## Current Baseline (G0)
- Product-plane production readiness remains `not accepted`.
- Real broker process closure remains `planned`.
- Delta Lake closure is `partial`: physical Delta runtime proof exists for the historical-data and research slices only.
- Apache Spark closure is `partial`: the canonicalization job is executable in a Docker/Linux proof profile, but full distributed orchestration remains open.
- Dagster closure is `partial`: executable `Definitions` and local materialization proof exist for the historical-data proof slice, while broader orchestration coverage remains open.
- Durable runtime, service/API, Telegram adapter, and real sidecar closure are not part of this branch baseline and remain unresolved here.
- Live execution transport baseline remains `partial` on this branch and must not be described as real sidecar closure.
- Vectorbt status is governed by ADR-012 as a bounded research-only contour (`planned` in stack conformance), not as runtime/live execution core.
- Legal/commercial envelope for vectorbt is restricted to internal governed research use; external commercialization requires a dedicated legal/product decision.
- Approved universe contract for strategy promotion is versioned in `docs/architecture/product-plane/approved-universe-v1.md` and is mandatory for multi-asset promotion evidence.
- Shell proving artifacts do not equal product capability closure.

## Writer Rules
- Use vocabulary from [restricted-acceptance-vocabulary.md](docs/architecture/product-plane/restricted-acceptance-vocabulary.md).
- If wording conflicts with `docs/architecture/product-plane/STATUS.md`, downgrade wording to the truth-source level.
- Keep historical evidence, but label it as baseline or partial when full closure is not proven.

## Machine Gate
- Stack-claim enforcement is codified in `registry/stack_conformance.yaml`.
- `python scripts/validate_stack_conformance.py` is fail-closed for:
  - status drift between registry and `docs/architecture/product-plane/STATUS.md`,
  - `implemented` claims without runtime/dependency/test proof,
  - forbidden full-closure wording while non-implemented surfaces remain,
  - stack/spec claim drift (including removed technologies declared as chosen or planned contours missing from spec).
