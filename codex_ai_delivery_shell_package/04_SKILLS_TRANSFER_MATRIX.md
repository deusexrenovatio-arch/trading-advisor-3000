# Матрица переноса skills

## Принцип

Skills нужно переносить **в два прохода**:

1. Сначала — только generic process/architecture/testing/governance skills.
2. Потом — stack-specific skills по мере появления реальных технологических потребностей.
3. Доменно-трейдинговые skills — не переносить в базовый пакет.

> Эта классификация сделана по каталогу skills и назначению навыков.  
> В новом repo содержимое каждого `SKILL.md` лучше адаптировать после запуска core shell, а не до него.

## Классы

- `KEEP_CORE` — включить в baseline shell.
- `KEEP_OPTIONAL` — перенести позже при необходимости.
- `DEFER_STACK` — не переносить, пока не выбран стек/подсистема.
- `EXCLUDE_DOMAIN_INITIAL` — исключить из первой версии нового repo.

| Skill file | Классификация | Решение |
| --- | --- | --- |
| .cursor/skills/ai-agent-architect/SKILL.md | KEEP_CORE | Полезен для проектирования agent-first репозитория и нормализации runtime/governance shell. |
| .cursor/skills/ai-change-explainer/SKILL.md | KEEP_CORE | Помогает делать change summary и PR narrative для AI-generated изменений. |
| .cursor/skills/archctl-policy-authoring/SKILL.md | KEEP_CORE | Нужен для написания policy docs и rule systems. |
| .cursor/skills/architecture-review/SKILL.md | KEEP_CORE | Нужен для architecture-as-docs и boundary reviews. |
| .cursor/skills/business-analyst/SKILL.md | KEEP_CORE | Полезен для task contract, scope framing и DoD decomposition. |
| .cursor/skills/ci-bootstrap/SKILL.md | KEEP_CORE | Нужен для быстрого воспроизведения CI lanes. |
| .cursor/skills/codeowners-from-registry/SKILL.md | KEEP_CORE | Автоматизация ownership routing хорошо ложится на новый shell. |
| .cursor/skills/commit-and-pr-hygiene/SKILL.md | KEEP_CORE | Поддерживает PR-only policy и чистые change sets. |
| .cursor/skills/composition-contracts/SKILL.md | KEEP_CORE | Важен для explicit module boundaries. |
| .cursor/skills/contract-first-graphql/SKILL.md | DEFER_STACK | Оставить только если будущий API стек реально использует GraphQL. |
| .cursor/skills/data-lineage/SKILL.md | KEEP_OPTIONAL | Полезно для data-heavy приложений, но можно отложить. |
| .cursor/skills/data-quality-gates/SKILL.md | KEEP_OPTIONAL | Нужно при появлении data pipelines. |
| .cursor/skills/dependency-and-license-audit/SKILL.md | KEEP_CORE | Полезен как часть governance shell. |
| .cursor/skills/docs-sync/SKILL.md | KEEP_CORE | Критичен для docs-as-source-of-truth. |
| .cursor/skills/event-contracts/SKILL.md | KEEP_OPTIONAL | Полезно, если появится event-driven orchestration. |
| .cursor/skills/frontend-behavior-check/SKILL.md | KEEP_OPTIONAL | Переносить только при наличии UI. |
| .cursor/skills/golden-tests-and-fixtures/SKILL.md | KEEP_CORE | Нужен для стабильных AI-friendly regression packs. |
| .cursor/skills/incident-runbook/SKILL.md | KEEP_CORE | Хорошо дополняет memory/incidents и governance-remediation. |
| .cursor/skills/integration-connector/SKILL.md | KEEP_OPTIONAL | Переносить, если приложение интеграционное. |
| .cursor/skills/layer-diagnostics-debug/SKILL.md | KEEP_CORE | Нужен для межслойных проблем и boundary debugging. |
| .cursor/skills/module-scaffold/SKILL.md | KEEP_CORE | Ускоряет тиражирование модулей и шаблонов. |
| .cursor/skills/observability-slo/SKILL.md | KEEP_OPTIONAL | Полезно на этапе productionization. |
| .cursor/skills/openai-ocr-cost-and-reliability-guardrails/SKILL.md | KEEP_OPTIONAL | Специализированный навык; переносить только если OCR реально используется. |
| .cursor/skills/parallel-worktree-flow/SKILL.md | KEEP_CORE | Отлично соответствует worktree governance и multi-agent flow. |
| .cursor/skills/patch-series-splitter/SKILL.md | KEEP_CORE | Критичен для ordered patch series и high-risk surfaces. |
| .cursor/skills/product-owner/SKILL.md | KEEP_CORE | Полезен для decomposition, priorities и done evidence. |
| .cursor/skills/qa-test-engineer/SKILL.md | KEEP_CORE | Нужен для сильного тестового слоя вокруг AI-разработки. |
| .cursor/skills/registry-first/SKILL.md | KEEP_CORE | Хорошо соответствует plans/items и state registries. |
| .cursor/skills/release-notes/SKILL.md | KEEP_OPTIONAL | Переносить, если релизный процесс формализован с начала. |
| .cursor/skills/release-notes-and-changelog/SKILL.md | KEEP_OPTIONAL | То же; возможно оставить только один из двух навыков. |
| .cursor/skills/repeated-issue-review/SKILL.md | KEEP_CORE | Поддерживает repetition control и process improvement. |
| .cursor/skills/risk-profile-gates/SKILL.md | KEEP_CORE | Полезен для route-by-risk и surface-aware checks. |
| .cursor/skills/secrets-and-config-hardening/SKILL.md | KEEP_CORE | Нужен для AI-first репозитория с automation. |
| .cursor/skills/security-compliance/SKILL.md | KEEP_OPTIONAL | Переносить в зависимости от требований окружения. |
| .cursor/skills/skill-creator/SKILL.md | KEEP_CORE | Нужен, если skill system остаётся живым активом. |
| .cursor/skills/skill-installer/SKILL.md | KEEP_CORE | Полезен для bootstrap нового repo. |
| .cursor/skills/source-onboarding/SKILL.md | KEEP_CORE | Ускоряет старт Codex/AI в новом репозитории. |
| .cursor/skills/testing-suite/SKILL.md | KEEP_CORE | Поддерживает стандартизированный тестовый слой. |
| .cursor/skills/ui-decision-log/SKILL.md | KEEP_OPTIONAL | Нужен только если UI станет значимой частью приложения. |
| .cursor/skills/validate-crosslayer/SKILL.md | KEEP_CORE | Очень полезен для запрета нелегальных cross-layer зависимостей. |
| .cursor/skills/computer-vision-expert/SKILL.md | DEFER_STACK | Переносить только при наличии CV use cases. |
| .cursor/skills/gis-data-layer-react-query/SKILL.md | DEFER_STACK | Не нужен по умолчанию. |
| .cursor/skills/index-vector/SKILL.md | DEFER_STACK | Нужен только если планируется vector/RAG subsystem. |
| .cursor/skills/ingest-postgres/SKILL.md | KEEP_OPTIONAL | Оставить, если новый app строится вокруг Postgres ingestion. |
| .cursor/skills/minute-candle-performance/SKILL.md | EXCLUDE_DOMAIN_INITIAL | Сильно завязан на trading performance profile. |
| .cursor/skills/ml-backtest-hpo-lab/SKILL.md | EXCLUDE_DOMAIN_INITIAL | Доменно-зависимо. |
| .cursor/skills/neo4j-migrations-and-constraints/SKILL.md | DEFER_STACK | Нужен только если реально будет Neo4j. |
| .cursor/skills/preferences-presets-migrations/SKILL.md | DEFER_STACK | Стек-специфично. |
| .cursor/skills/rbac-layer-gating/SKILL.md | KEEP_OPTIONAL | Переносить, если появится multi-user access layer. |
| .cursor/skills/schema-migrations-postgres/SKILL.md | KEEP_OPTIONAL | Нужен при Postgres-backed schema evolution. |
| .cursor/skills/tz-oss-scout/SKILL.md | DEFER_STACK | Специализированный OSINT/OSS discovery skill. |
| .cursor/skills/update-neo4j/SKILL.md | DEFER_STACK | Только при использовании Neo4j. |
| .cursor/skills/commodity-news-linking/SKILL.md | EXCLUDE_DOMAIN_INITIAL | Трейдинговая/новостная доменная специализация. |
| .cursor/skills/intraday-futures-trading-advisor/SKILL.md | EXCLUDE_DOMAIN_INITIAL | Явно доменный навык исходного продукта. |
| .cursor/skills/moex-instruments-costs/SKILL.md | EXCLUDE_DOMAIN_INITIAL | Сильно привязан к исходному домену и бирже. |
| .cursor/skills/news-geopolitics-filter/SKILL.md | EXCLUDE_DOMAIN_INITIAL | Доменно-новостная логика. |
| .cursor/skills/news-impact-backtest-lab/SKILL.md | EXCLUDE_DOMAIN_INITIAL | Доменно-исследовательская логика. |
| .cursor/skills/signals-news-bridge-v2/SKILL.md | EXCLUDE_DOMAIN_INITIAL | Сигнально-новостная связка исходного продукта. |
| .cursor/skills/spread-arbitrage/SKILL.md | EXCLUDE_DOMAIN_INITIAL | Трейдинговая стратегия. |
| .cursor/skills/trading-ui-dashboard/SKILL.md | EXCLUDE_DOMAIN_INITIAL | Конкретный UI исходного трейдингового продукта. |

## Рекомендация по порядку подключения skills

### Wave 1 — базовый shell
`ai-agent-architect`, `architecture-review`, `business-analyst`, `qa-test-engineer`, `module-scaffold`, `testing-suite`, `docs-sync`, `patch-series-splitter`, `parallel-worktree-flow`, `repeated-issue-review`, `risk-profile-gates`, `registry-first`, `source-onboarding`, `skill-creator`, `skill-installer`.

### Wave 2 — governance/CI
`ci-bootstrap`, `codeowners-from-registry`, `commit-and-pr-hygiene`, `dependency-and-license-audit`, `secrets-and-config-hardening`, `validate-crosslayer`.

### Wave 3 — по фактическому стеку
Postgres / GraphQL / RBAC / observability / eventing / vector / Neo4j и т.д.

### Wave 4 — доменные навыки
Только после того, как бизнес-модель нового приложения будет реально определена.
