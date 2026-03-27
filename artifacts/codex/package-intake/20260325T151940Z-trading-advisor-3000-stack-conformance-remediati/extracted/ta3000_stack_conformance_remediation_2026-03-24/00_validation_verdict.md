# Validation verdict

Date: 2026-03-24  
Scope reviewed: current `main`, PR #2, app architecture docs, phase checklists, CI workflow, and the mirrored product spec package.

## Executive verdict

The reported issue is **real and systemic**. It is not a single missing implementation. It is a mismatch between:

- the **declared target architecture** in the product spec,
- the **phase acceptance rules and checklists** used to close work,
- the **tests and CI evidence** actually collected,
- the **runtime surfaces** that are really executable in the repo.

The repository has already started correcting the public narrative with `STATUS.md` and the superseded Phase 0–3 note, but the acceptance model still needs structural repair before more product work lands.

## Finding-by-finding validation

### F1. Phase 2A accepted a lighter contour than the target stack

**Status:** validated.

Evidence in current `main`:

- `product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md` still names Delta Lake, Spark, Dagster, Polars/DuckDB, PostgreSQL, vectorbt, FastAPI, aiogram, Alembic, OpenTelemetry, .NET 8 sidecar, and StockSharp as the selected stack.
- `phase2a-data-plane-mvp.md` explicitly says the phase delivers a “runnable data-plane baseline” with a Delta schema manifest and initial Dagster/Spark skeleton artifacts.
- The same Phase 2A doc explicitly says **full Delta/Spark runtime wiring is out of scope**.
- The Phase 2A checklist still marks the phase as accepted as MVP **and** accepted as full module DoD.

**Conclusion:** acceptance language advanced beyond the proven runtime.

### F2. Phase 2B accepted Delta-compatible sample artifacts rather than real Delta closure

**Status:** validated.

Evidence in current `main`:

- `phase2b-research-plane-mvp.md` says research artifacts are written as **Delta-compatible JSONL sample outputs** for acceptance replay.
- The same phase marks distributed Delta/Spark runtime orchestration as out of scope.
- The Phase 2B checklist still marks the phase as accepted as MVP **and** accepted as full module DoD.
- `app/research/backtest/engine.py` writes `*.sample.jsonl` outputs and returns a manifest rather than materialized Delta tables.

**Conclusion:** acceptance evidence proves contract-shaped artifacts, not physical Delta-backed research closure.

### F3. Tests validated proxy evidence, not the declared runtime stack

**Status:** validated.

Evidence in current `main`:

- `tests/app/unit/test_phase2a_manifests.py` checks the Delta schema manifest, asset specs, and SQL plan strings.
- `tests/app/integration/test_phase2a_data_plane.py` writes and reads JSONL sample outputs and checks for manifest keys.
- `tests/app/integration/test_phase2b_research_plane.py` checks reproducibility and JSONL outputs plus a manifest.
- `app/data_plane/pipeline.py` writes `raw_market_backfill.sample.jsonl`, `canonical_bars.sample.jsonl`, and related JSONL artifacts.
- `spark_jobs/canonical_bars_job.py` defines SQL plan builders, not an executed Spark runtime path.
- `dagster_defs/phase2a_assets.py` defines asset specs, not runnable Dagster asset materialization.

**Conclusion:** tests prove scaffolding correctness, not real use of Delta/Spark/Dagster.

### F4. Gate stack and CI did not prove “declared stack is actually used”

**Status:** validated.

Evidence in current `main`:

- `docs/agent/checks.md` lists process validators, gate lanes, and QA matrix, but it does not define a conformance proof that ties declared technologies to runnable entrypoints.
- `.github/workflows/ci.yml` installs only `pytest` and `pyyaml` before running gates and tests.
- The same CI workflow runs `tests/process`, `tests/architecture`, and `tests/app`, but it does not install or boot FastAPI, aiogram, Delta, Spark, Dagster, vectorbt, Alembic, OpenTelemetry, or .NET sidecar tooling.

**Conclusion:** a green CI run could never be treated as proof of the declared product stack.

### F5. Current docs re-baselined reality after drift happened

**Status:** validated in substance; partially different filenames than the note reported.

Evidence in current `main`:

- PR #2 description explicitly says it “rebaselines acceptance claims to match implemented reality” and does not claim full product acceptance, live readiness, or operational readiness.
- `STATUS.md` says the product plane is not accepted as live/prod ready, durable runtime state is partial, live execution transport baseline is partial, and a real .NET StockSharp sidecar project is not landed in the repo.
- `phase0-3-acceptance-verdict-2026-03-17.md` says the earlier outcome was scaffold and partial integration acceptance, not full target closure.

**Important note:** the specific filenames `current-repo-baseline-2026-03-18.md` and `phase9-*` from the external note are **not present in current `main`**. Current equivalent truth-source evidence exists, but those exact references were not directly verifiable in the reviewed tree.

### F6. Later product surfaces normalized scaffold reality instead of proving target-stack closure

**Status:** validated in principle; exact “phase9” citations not verifiable in current `main`.

Evidence in current `main`:

- `phase5-review-analytics-observability.md` makes observability explicitly file-contract based (`metrics.txt` + JSONL) for reproducible acceptance rather than OpenTelemetry-backed tracing.
- `mcp-and-real-execution-rollout.md` defines a staging-first real HTTP transport path and explicitly keeps direct production rollout out of scope.
- `deployment/stocksharp-sidecar/README.md` explicitly says the directory is **not** a real StockSharp/.NET sidecar implementation.
- `docs/architecture/app/README.md` lists phases only through Phase 8 plus MCP rollout; the cited Phase 9 docs are not present in the current tree.

**Conclusion:** the current truth-source is more honest than earlier closure language, but the repo still needs a formal conformance model to stop future normalization of “scaffold == accepted”.

## Current stack conformance summary

### Architecture-critical surfaces

- **Delta Lake:** target in spec, but current code/tests show manifests and JSONL sample outputs rather than physical Delta tables.
- **Apache Spark:** target in spec, but current repo shows SQL plan builders and no proven runtime execution path.
- **Dagster:** target in spec, but current repo shows asset specs and no proven materialization runtime.
- **PostgreSQL:** real and partially integrated. `psycopg` dependency, migrations, and `PostgresSignalStore` exist, but the default runtime stack still falls back to `InMemorySignalStore`.
- **.NET/StockSharp sidecar:** not landed as a real in-repo sidecar project; only transport contract, Python HTTP adapter, and staging stub surfaces are present.
- **Prometheus/Grafana/Loki + Docker Compose:** implemented as a local observability stack, but file-bridge based.

### Replaceable / policy-level tool choices

- **FastAPI:** not evidenced in project dependencies or runtime entrypoints reviewed.
- **aiogram:** not evidenced; Telegram is currently represented by a custom in-memory publication engine.
- **vectorbt:** not evidenced; research uses a custom backtest engine.
- **Alembic:** not evidenced; migrations use a custom checksum-based runner.
- **OpenTelemetry:** not evidenced; observability is file-contract based.
- **Polars / DuckDB:** selected in spec, but not evidenced in the reviewed runtime/dependency surfaces.

## Why this is a major problem

1. Future contributors can build on assumptions that are not backed by runtime truth.
2. CI can stay green while the architecture silently diverges.
3. “Full DoD” language can leak scaffold assumptions into prod planning.
4. The repo can accumulate integration debt that looks already paid.
5. At launch time, the hardest gaps remain exactly the ones most dangerous in production: durable data, real execution bridge, and runnable operational surfaces.

## Required response

Do **not** try to fix this with only wording changes.

First repair the acceptance model and the evidence model.  
Then close or formally de-scope each declared technology surface with explicit proof.
