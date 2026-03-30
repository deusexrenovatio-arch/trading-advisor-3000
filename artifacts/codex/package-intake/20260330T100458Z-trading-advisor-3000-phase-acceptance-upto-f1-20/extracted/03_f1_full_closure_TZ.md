# Техническое задание на полноценное закрытие F1

Дата: 2026-03-30

## 0. Цель
Получить **честный и воспроизводимый** `ALLOW_RELEASE_READINESS` только в том случае, если:
1. все release-blocking architecture-critical surfaces реально implemented;
2. все replaceable stack surfaces либо реально implemented, либо честно removed/replaced через ADR;
3. truth-source docs, registry, runtime code, tests, CI artifacts и acceptance package не противоречат друг другу;
4. отрицательные тесты подтверждают, что closure нельзя подделать docs-only, script-only или stub-only способом.

## 1. Что сейчас блокирует F1
Даже собственные F1 docs прямо сохраняют blockers:
- `production_readiness` = `not accepted`;
- `real_broker_process` = `planned`;
- `contracts_freeze` = `partial`.

Дополнительно моя приёмка считает открытыми ещё и preconditions:
- reopened `G0`;
- reopened `G1`;
- rejected `R2`;
- rejected `S1`;
- `E1` needs evidence hardening.

## 2. Обязательный preparatory step: перепрошить модель F1
Перед любым runtime work нужно устранить ambiguity в самом F1 contract.

### Требование F1-00 — registry must know release semantics
Расширить `registry/stack_conformance.yaml` так, чтобы для каждой surface/technology были поля:
- `category`: `architecture_critical | replaceable | supporting_bounded`;
- `release_gate_required`: `true | false`;
- `decision_source`: путь до truth-source doc;
- `acceptance_artifacts_any`: список immutable artifacts;
- `negative_tests_any`: список disprover tests;
- `ci_jobs_any`: какие CI jobs обязаны это доказательство воспроизводить.

### Acceptance gate
- Нельзя получить `ALLOW_RELEASE_READINESS`, пока любой объект с `release_gate_required=true` не переведён в честный terminal state.
- `partial` как terminal state допустим только для `supporting_bounded` и только если это явно записано в registry и F1 contract.

### Disprover
- Оставить technology `partial`, но не пометить её как `supporting_bounded`, и убедиться, что F1 validator block-ит release decision.
- Пометить technology `removed`, не имея ADR/replacement markers, и убедиться, что validator block-ит release decision.

## 3. Атомарный план закрытия F1

### Phase F1-A — Reopen and close G0/G1 honestly
**Objective:** убрать ложные truth-source claims и сделать validator реально fail-closed.

**Scope**
- `docs/architecture/app/STATUS.md`
- `docs/architecture/app/stack-conformance-baseline.md`
- `docs/architecture/app/phase10-stack-conformance-reacceptance-report.md`
- `artifacts/acceptance/f1/*`
- `docs/codex/modules/*` where claim language matters
- `registry/stack_conformance.yaml`
- `scripts/validate_stack_conformance.py`
- validator tests

**Required changes**
1. Либо реально оформить ADR-removals для `polars`, `duckdb`, `vectorbt`, `alembic`, `opentelemetry`, `aiogram`, либо убрать claims про их removal из phase10/red-team docs.
2. Проверить, что baseline не противоречит STATUS.
3. Расширить validator так, чтобы он проверял не только stack spec и checklist docs, но и:
   - reacceptance reports,
   - red-team result docs,
   - module-phase briefs, если они содержат stack claims,
   - acceptance evidence packs с human-readable claims.
4. Добавить semantic checks:
   - report cannot say `removed by ADR`, если registry не `removed` и ADR markers не найдены;
   - report cannot say `accepted`, если phase acceptance gate не satisfied by truth sources;
   - route reports must be explicitly treated as meta-orchestration artifacts, not capability proof.

**Acceptance gate**
- Любой deliberate contradiction между report/registy/spec/ADR должен ломать validator.
- `STATUS.md`, baseline, phase10 report, red-team result и registry согласованы.
- В touched docs нет overclaiming beyond truth-source state.

**Disprover**
- Снова вернуть фразу `aiogram removed by ADR` только в phase10 report, не меняя registry/spec/ADR, и убедиться, что CI падает.

### Phase F1-B — Close R2 and S1 without ghost states
**Objective:** убрать все replaceable ghost technologies и закрыть Telegram mismatch.

**Scope**
- `aiogram`
- `Polars`
- `DuckDB`
- `vectorbt`
- `Alembic`
- `OpenTelemetry`

**Allowed outcomes**
- `implemented` + runtime proof + dependency proof + test proof;
- `removed` or `replaced` + explicit ADR + updated stack spec + registry + docs.

**Hard rule**
Для каждой технологии разрешён только один из трёх честных финалов:
1. `implemented`
2. `removed`
3. `replaced_by:<tech>`
Статус `chosen but planned` после этой фазы запрещён.

**Telegram special rule**
Для `aiogram`:
- либо реализовать реальный aiogram adapter с mocked Telegram API proof;
- либо выпустить ADR, где chosen stack меняется на custom Telegram Bot API engine, и затем убрать `aiogram` из chosen stack docs и registry.

**Acceptance gate**
- Stack spec, registry, ADR set, runtime code и tests совпадают по каждой технологии.
- Ни одна replaceable technology не остаётся ambiguous.
- Negative tests покрывают случай ghost chosen state.

**Disprover**
- Оставить `OpenTelemetry` как chosen в stack spec без runtime proof и без ADR, и убедиться, что phase fail-ится.

### Phase F1-C — Close `contracts_freeze`
**Objective:** перевести `contracts_freeze` из `partial` в fully governed implemented release surface.

**What must be versioned**
1. Runtime API request/response envelopes.
2. Telegram publication contract (если Telegram path retained).
3. Sidecar HTTP wire envelopes:
   - submit
   - replace
   - cancel
   - updates
   - fills
   - health
   - readiness
   - metrics-related expectations
   - kill-switch admin endpoints
4. Runtime configuration envelope:
   - required env vars
   - profile matrix
   - default semantics
   - secrets contract
5. Persistence and migration boundary:
   - DB schema evolution approach
   - migration ownership
   - compatibility guarantees
6. External rollout envelopes:
   - deployment profile config
   - sidecar connectivity config
   - broker routing mode config

**Artifacts required**
- expanded `CONTRACT_SURFACES.md`;
- versioned schemas/fixtures/tests for every public boundary;
- compatibility matrix;
- change policy with backward/forward compatibility rules.

**Acceptance gate**
- Нельзя изменить публичный boundary без schema + fixture + test delta.
- Contract tests cover all release-blocking envelopes.
- `contracts_freeze` can honestly move from `partial` to `implemented`.

**Disprover**
- Изменить любой public payload без schema/fixture/test update и убедиться, что CI падает.

### Phase F1-D — Harden E1 evidence for release gating
**Objective:** оставить E1 accepted, но убрать remaining evidence debt.

**Required changes**
1. Вынести compiled sidecar proof в canonical immutable artifacts:
   - `dotnet-build.txt`
   - `dotnet-test.txt`
   - `dotnet-publish.txt`
   - smoke JSON
   - environment manifest (`dotnet --info`, OS, commit SHA, script hashes)
2. Добавить CI/self-hosted lane, который реально прогоняет:
   - `dotnet build`
   - `dotnet test`
   - `dotnet publish`
   - Python smoke against published binary
3. Сделать artifact hashing mandatory.
4. Добавить negative test:
   - broken sidecar binary path;
   - failing readiness under kill-switch;
   - mismatch between published binary hash and recorded artifact hash.

**Acceptance gate**
- Build/test/publish/smoke are reproducible from clean checkout.
- Evidence artifacts are machine-linked to commit SHA and command exit codes.
- E1 no longer relies on docs or one-off local proof.

### Phase F1-E — Close `real_broker_process`
**Objective:** перевести `StockSharp/QUIK/Finam real broker process` из `planned` в implemented release-blocking surface.

**Important principle**
Это не может быть закрыто только HTTP stub, mock gateway или wire docs. Нужен реальный process contour с настоящим connector logic.

**Minimum required implementation**
1. In-repo sidecar process must include real broker connector path for the agreed broker surface.
2. Secrets/config contract for staging connector must be versioned and documented.
3. There must be a staging-first proof profile that demonstrates:
   - process boot with real connector configuration;
   - readiness semantics;
   - kill-switch semantics;
   - submit/replace/cancel path;
   - event/update ingestion;
   - reconciliation visibility.
4. Failure modes must be explicit:
   - no credentials;
   - connector unavailable;
   - session not ready;
   - kill-switch active;
   - rejected order;
   - timeout/retry semantics.

**Required tests**
- connector boot smoke;
- request/response contract tests;
- Python integration path against real sidecar process with broker connector enabled in staging profile;
- recovery test after sidecar restart;
- negative test for secrets/config miswire.

**Acceptance gate**
- `real_broker_process` can move from `planned` to `implemented`.
- STATUS, registry, docs and runbooks reflect the same scope.
- No doc uses words like `production ready` unless release readiness phase also passes.

### Phase F1-F — Operational readiness closure and final re-acceptance
**Objective:** перевести `production_readiness` из `not accepted` в explicit allow decision.

**Required evidence**
1. Canonical runbooks:
   - startup
   - shutdown
   - rollback
   - incident response
   - kill-switch procedure
   - sidecar recovery
   - credential rotation
2. Environment inventory:
   - Python runtime
   - Postgres
   - Delta storage path
   - Spark proof profile
   - Dagster proof profile
   - sidecar runtime
   - observability stack
3. Release checklist:
   - all release-blocking surfaces implemented;
   - all replaceable surfaces implemented/removed;
   - all CI release lanes green;
   - all immutable artifacts attached.
4. Red-team review:
   - doc/runtime alignment;
   - evidence quality;
   - negative-test completeness;
   - rollback credibility.

**Final F1 acceptance gate**
`ALLOW_RELEASE_READINESS` legal only when all below are true:
- G0/G1 contradictions closed.
- R2/S1 ghost technologies closed.
- E1 evidence hardened.
- `contracts_freeze` = implemented.
- `real_broker_process` = implemented.
- `production_readiness` can move from `not accepted` to implemented/accepted truth-source state.
- Release gate validator and red-team both pass.

**Mandatory disprovers**
- delete or bypass one immutable artifact and confirm F1 fails;
- restore ghost chosen technology in spec and confirm F1 fails;
- run staging profile without Postgres and confirm F1 fails;
- run sidecar smoke against non-compiled stub and confirm F1 fails;
- mutate one public payload without schema/fixture/test and confirm F1 fails.

## 4. Как не заниматься самообманом
1. Route reports — это orchestration telemetry, а не capability proof.
2. Descriptive docs never count as implementation evidence.
3. Script existence never counts as execution evidence.
4. One-off local success never counts as reproducible closure.
5. Acceptance package must always include negative tests proving that fake closure paths are rejected.
6. Any contradiction between STATUS, registry, spec, ADR, code, tests or evidence artifacts is an automatic deny until resolved.

## 5. Рекомендуемый порядок интеграции
1. F1-A — governance and validator repair.
2. F1-B — replaceable/Telegram closure.
3. F1-C — contracts freeze.
4. F1-D — sidecar proof hardening.
5. F1-E — real broker process.
6. F1-F — operational readiness and F1 decision.

Этот порядок нужен, чтобы не строить release decision на лживом метаданных фоне.
