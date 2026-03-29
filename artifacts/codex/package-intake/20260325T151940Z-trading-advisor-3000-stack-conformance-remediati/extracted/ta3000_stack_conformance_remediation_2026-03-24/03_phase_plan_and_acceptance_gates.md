# Phase plan and acceptance gates

The plan is intentionally split into **atomic merge phases**.  
Each phase has a single acceptance target and a single disproof target.

---

## G0 — Claim freeze and checklist repair

### Type
Governance patch only.

### Goal
Stop new false closure immediately.

### Change surface
- `docs/architecture/app/*`
- `docs/checklists/app/*`

### Deliverables
- Replace or annotate all historical “full DoD” wording that is not backed by current executable proof.
- Add `STACK_CONFORMANCE.md` and restricted acceptance vocabulary.
- Explicitly mark phases 2A / 2B / 2C / 2D / 4 / 5 as `scaffold`, `partial`, or `runtime-closed` only where actually supported.

### Acceptance
- No document in the repo uses `full DoD`, `full acceptance`, `live ready`, or `production ready` for mismatched phases.
- `STATUS.md`, `README.md`, phase docs, and checklists are mutually consistent.

### Disprover
- Introduce a deliberate false “full DoD” claim in one checklist; validator must fail in G1.

---

## G1 — Machine-verifiable stack-conformance gate

### Type
Governance patch only.

### Goal
Make stack drift impossible to hide.

### Change surface
- `registry/stack_conformance.yaml`
- `scripts/validate_stack_conformance.py`
- `tests/process/*`
- `.github/workflows/ci.yml`
- `configs/change_surface_mapping.yaml` if needed

### Deliverables
- Registry with one entry per target technology.
- Validator that cross-checks:
  - spec/docs claims,
  - dependency declarations,
  - runtime entrypoints,
  - generated evidence,
  - ADR replacement state.
- CI lane that runs validator for changed surfaces.

### Acceptance
- Validator fails when a surface is marked `implemented` without runtime proof.
- Validator fails when docs claim full closure against non-implemented registry state.
- Validator fails when a removed/replaced technology still appears as `chosen` in spec docs.

### Disprover
- Mark `FastAPI` as implemented without ASGI entrypoint or dependency; CI must fail.

---

## D1 — Physical Delta closure

### Type
Product patch.

### Goal
Replace manifest-only Delta evidence with real physical Delta tables.

### Change surface
- `src/trading_advisor_3000/app/data_plane/*`
- `src/trading_advisor_3000/app/research/*`
- `tests/app/*`
- product docs/runbooks

### Deliverables
- Real Delta table writer/reader for:
  - canonical bars
  - feature snapshots
  - signal candidates / research outputs (minimum agreed set)
- Versioned storage layout and local test profile.
- Migration of sample acceptance artifacts from JSONL-only to Delta-backed fixtures.

### Acceptance
- Integration tests verify actual `_delta_log` exists.
- Delta runtime can read written tables.
- Data/research outputs are produced as physical Delta tables, not just manifests or JSONL.

### Disprover
- Delete physical Delta output and leave manifest intact; tests must fail.

---

## D2 — Spark execution closure

### Type
Product patch.

### Goal
Turn SQL plan strings into real executed Spark work.

### Change surface
- `src/trading_advisor_3000/spark_jobs/*`
- Spark bootstrap scripts / container profile
- tests/docs

### Deliverables
- Runnable local Spark entrypoint (`spark-submit` local mode or equivalent).
- Job that reads agreed input and writes agreed Delta outputs.
- CI-compatible smoke profile for the smallest supported dataset.

### Acceptance
- Spark job executes in CI/staging profile.
- Output Delta tables match contract.
- No acceptance test relies only on plan-string inspection.

### Disprover
- Leave SQL builders but break Spark execution; phase must fail.

---

## D3 — Dagster execution closure

### Type
Product patch.

### Goal
Turn asset specs into real orchestration assets.

### Change surface
- `src/trading_advisor_3000/dagster_defs/*`
- resources/config
- tests/docs

### Deliverables
- Real Dagster `Definitions`.
- Materializable assets for the agreed data/research slice.
- One asset job / selection wired for local proof.

### Acceptance
- Dagster definitions load.
- Materialization of selected assets succeeds and produces Delta outputs.
- Asset lineage is executable, not descriptive only.

### Disprover
- Replace definitions with plain asset metadata; tests must fail.

---

## R1 — Durable runtime default and service closure

### Type
Product patch.

### Goal
Make the default runtime path durable and expose an actual service/runtime entrypoint.

### Change surface
- `src/trading_advisor_3000/app/runtime/*`
- `src/trading_advisor_3000/app/interfaces/*`
- migrations/runbooks/tests

### Deliverables
- Default runtime entrypoint that requires Postgres in staging/prod profile.
- In-memory store allowed only in explicit test/dev path.
- Service/API entrypoint:
  - implement FastAPI if retained, or
  - remove FastAPI from target stack through ADR and docs.
- Restart/recovery smoke proving state restoration.

### Acceptance
- Runtime boot with durable store works from env/CLI profile.
- Restart preserves signal/publication state.
- API/service surface is black-box testable.

### Disprover
- Start staging profile without Postgres and silently fall back to in-memory; must fail.

---

## R2 — Telegram adapter closure

### Type
Product patch or ADR patch.

### Goal
Resolve the Telegram-stack mismatch honestly.

### Change surface
- runtime publishing adapter
- dependencies/docs/tests

### Deliverables
Either:

1. **Implement aiogram path**
   - aiogram dependency
   - real adapter using mocked Telegram Bot API in tests

or

2. **De-scope aiogram**
   - ADR approving custom adapter path
   - spec/docs/registry updated
   - acceptance language corrected

### Acceptance
- Registry and implementation match.
- No doc claims aiogram unless actual aiogram runtime exists.

### Disprover
- Keep aiogram in `chosen` status but use only custom in-memory adapter; must fail.

---

## E1 — Real .NET sidecar closure

### Type
Product + deployment patch.

### Goal
Land a real in-repo sidecar project, not only docs and Python transport.

### Change surface
- `deployment/stocksharp-sidecar/*`
- `.NET` project files
- staging compose/profile
- Python transport integration tests
- docs/runbooks

### Deliverables
- `.sln` / `.csproj`
- minimal runnable HTTP sidecar implementing current wire contract
- build/test/publish script
- staging compose profile that boots compiled binary
- Python integration smoke against compiled sidecar

### Acceptance
- `dotnet build`
- `dotnet test`
- `dotnet publish`
- sidecar boots and answers `/health`, `/ready`, submit/cancel/stream endpoints used by Python transport

### Disprover
- Leave only README/stub transport; phase must fail.

---

## S1 — Replaceable stack decisions

### Type
Mixed; split by surface if needed.

### Goal
Resolve all remaining ghost technologies.

### Surfaces
- FastAPI
- vectorbt
- Alembic
- OpenTelemetry
- Polars
- DuckDB

### Deliverables
For each surface, one of:

- executable implementation proof, or
- ADR-based removal/replacement and doc update.

### Acceptance
- No target-stack item remains in ambiguous “chosen but not real” state.

### Disprover
- Any such item left in `chosen` with no proof or ADR => phase fails.

---

## F1 — Full re-acceptance and release-readiness proof

### Type
Governance + release patch.

### Goal
Re-close product phases under the repaired evidence model.

### Deliverables
- regenerated phase checklists
- acceptance evidence pack
- final stack-conformance report
- red-team review result

### Acceptance
- Every accepted phase maps to registry state and real evidence.
- Every architecture-critical surface is implemented.
- Every replaceable surface is implemented or removed by ADR.
- Red-team checklist passes.
- Final report contains no overclaiming language.

### Disprover
- Any accepted phase lacking black-box proof or negative-test evidence => release readiness denied.

---

## Required CI lanes after remediation

### Fast lane
Current process/gate checks.

### Conformance lane
Runs `validate_stack_conformance.py` for touched surfaces.

### Foundation lane
Runs Delta/Spark/Dagster proofs for relevant changes.

### Sidecar lane
Runs .NET build/test/publish + Python transport smoke.

### Release acceptance lane
Runs all phase re-acceptance proofs; mandatory for phase closure tags.

## Merge policy

- Governance phases (`G0`, `G1`) must merge before any new product phase closure.
- Product phases may merge independently once their own acceptance and disprover are green.
- `F1` may start only after all prerequisite phases are green and the registry has no critical `scaffold` entries.
