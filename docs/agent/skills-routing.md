# Skills Routing Policy

## Runtime Model
- Ordinary-chat catalog: global Codex skills under `D:/CodexHome/skills`.
- Repo-local catalog: local descriptors under `.codex/skills/*/`, only for TA3000-specific trading, product-plane data/research, or compute-runtime knowledge that should not affect other repositories.
- Legacy Cursor catalog: `.cursor/skills/*/` is not an active skill location; tracked generic skills must not be added there.
- Mirror artifact: `docs/agent/skills-catalog.md` is generated only from `.codex/skills/*/SKILL.md`.
- Hot-context policy: repo-local skills stay cold-by-default.
- Retrieval rule: use global skills first; open repo-local skill files only when a targeted, project-specific trigger requires them.

## Global Skill Layer
- Matt Pocock skills are the ordinary-chat baseline for reusable engineering process: planning, implementation, tests, review, debugging, architecture vocabulary, tickets, and handoff.
- Other global skills remain active only for core data-engineering surfaces not covered by Matt: data pipeline design, source onboarding, lineage, quality gates, and external connector design.
- Global skills are optional engineering lenses, not a mandatory preflight.
- Use global skills for reusable engineering behavior when they own the next artifact or decision.
- Use repo-local skills only for TA3000-specific trading semantics, product-plane data/research knowledge, local data roots, or compute-runtime constraints.
- An empty repo-local skill catalog is valid when the needed behavior is generic and already covered globally.
- A global skill may be only a working lens; it does not always imply an extra tool call.

## Risk-Triggered Process Layer
- Process skills are risk-triggered process owners, not a universal preflight.
- Use a process skill only when it owns the current phase: written plan execution, behavior-test shaping, systematic debugging, code review, completion verification, branch finishing, or explicit process coordination.
- Do not load a process skill only because it exists. If the task is docs-only, investigation-only, generated/mechanical, or already owned by a narrower global skill, state the narrow route and continue.
- Classify by semantic risk, not by patch size. Behavior changes, bugfixes, data/compute semantic changes, contract movement, and user-facing output changes require focused proof, but not a mandatory extra process layer.
- For behavior changes and bugfixes, prefer failing regression or characterization proof before implementation when practical. If prior failing proof was missed, closeout must provide focused post-change proof or residual risk.
- Before closeout, use completion verification for behavior/contract changes and pair it with explicit self-review: what changed, which contract moved, which old behavior is forbidden, which test catches it, which edge cases remain, and what risk is still accepted.
- Process skills decide how to approach the task. Global Codex skills and repo-local TA3000 skills still decide engineering/domain specifics.
- User instructions, AGENTS.md, TA3000 shell/product-plane boundaries, PR-only main, and Serena/context-routing requirements remain higher-priority constraints.

## Tool Reality Check
- Repo-local active skills are whatever `.codex/skills/*/SKILL.md` contains; the generated catalog can validly be empty.
- Global skills are the ordinary-chat baseline; generic process rules belong there, not in `.cursor/skills` or repo-local skills.
- GraphQL, Node.js, npm, pnpm, yarn, JavaScript, and TypeScript are not active TA3000 baseline surfaces. Do not route GraphQL/Node-specific global skills for this repo unless active source files or contracts appear outside ignored temporary, generated, archive, or package-intake paths.
- Serena is the default exact-symbol/navigation tool for non-trivial code discovery in this repo.
- Graphify is optional architecture orientation. Use it only when a local graph/report exists or the task explicitly needs that map; it is not a baseline pass after every Serena lookup.
- Browser, GitHub, documents, spreadsheets, and presentation plugin tools are trigger/lazy capabilities, not hot context to load preemptively.
- Tracked MCP templates and live session tools can differ. Use validators for repository contracts and live tool discovery for current-session truth.

## Ordinary Chat Guard
1. Classify the change surface and semantic risk.
2. Run the semantic-risk check before deciding a task is small: behavior, bugfix, data/compute semantics, contract, user-facing output, docs-only, generated/mechanical, or investigation-only.
3. Use `ask-matt` for questions about ordinary engineering skill selection, prompt routing, and missed-skill protection.
4. Before substantial work, name the selected skill or direct route briefly and why it applies.
5. If a needed global skill is not present in current session metadata but exists under `D:/CodexHome/skills`, read its main instruction file directly and state the fallback.
6. If a needed skill is generic, keep it in the global Codex skill root instead of adding it to this repo.
7. If the task is genuinely TA3000-specific, open the repo-local skill narrowly and do not load the whole skill corpus.
8. Before expanding beyond the selected skill/context route into memory, current diff, logs, generated artifacts, live process state, web docs, Graphify, or broad file reads, leave a Context Expansion Reason: evidence question, source/tool, insufficiency, and stop condition.

## Global Skill Sequence Rules
- Route by the artifact or decision currently being produced, not by keyword count.
- Start with the skill that owns the next decision, then hand off when the artifact changes.
- Load adjacent skills only when their output is immediately needed; do not preload a whole skill family.
- Keep closeout verification near the end, after evidence exists; use the Matt implementation/review loop first.
- Treat `code-review` as the default self-review closeout lens for behavior or contract changes, even when no external review was requested.
- Use `to-spec` or `to-tickets` when the work needs durable planning or tracker-ready breakdown before implementation.

## Common Global Skill Sequences
- Normal implementation:
  `implement` -> `tdd` when tests are affected -> `grill-with-docs` or `domain-modeling` when docs/domain terms changed -> self-review through `code-review` for behavior/contract changes -> closeout evidence.
- Architecture-sensitive implementation:
  `codebase-design` or `improve-codebase-architecture` -> direct contract/data-product source-of-truth update when contracts move -> `implement` -> `tdd` -> closeout evidence.
- Review or acceptance:
  `code-review` -> `tdd` when tests must be added or run -> closeout evidence.
- Event contracts:
  use direct contract/source-of-truth inspection and `tdd` when tests are affected -> closeout evidence.
- Data or integration:
  `integration-connector` for external access/raw landing -> `source-onboarding` for canonical keys/crosswalks -> `data-quality-gates` for QC -> `data-lineage` for provenance/ownership -> `data-engineer` for transforms/storage/orchestration -> `tdd` when tests are affected -> closeout evidence.
- Document knowledge pipeline:
  use direct source inspection, data-quality checks, and `tdd` when tests are affected -> closeout evidence.
- CI and PR:
  use direct repo checks, GitHub CLI, and `to-spec` or `to-tickets` when PR scope needs planning -> closeout evidence.

## Memory-Backed Failure Routing
- When the user says "again", "still broken", "not that", or the same symptom repeats after a focused fix, route through `diagnosing-bugs` before another patch.
- Use scoped local memory or repo history for history-sensitive repeated failures, but keep it advisory. Verify any drift-prone memory hit against live repo/runtime/log/artifact evidence before completion.
- If a repeated failure pattern is stable and reusable, promote it into an active global or repo-local skill rather than leaving it only in raw memory.
- Keep memory recall lightweight: use it when it can reduce wrong turns or context load, not as a default hard gate for every localized edit.

## Failure-Pattern Skill Map
- Local Codex Desktop, Windows path/session/env/temp/interpreter, or runaway-service issues: use direct runtime inspection; the archived Windows recovery skill is not part of the active data-engineering skill baseline.
- TA3000 active product-surface naming, phase/debug labels, capability naming, and active/archive/provenance separation: repo-local `ta3000-product-surface-naming-cleanup`.
- TA3000 data-plane proof on `D:/TA3000-data`, Delta `_delta_log`, row counts, report JSON, canonical tail alignment, or real production-route materialization: repo-local `ta3000-data-plane-proof`.
- TA3000 futures contract economics, MOEX money math, margin/tick/step value, research `execution_*` propagation, vectorbt-vs-ledger money truth, fees/slippage/PnL, or risk sizing: repo-local `ta3000-futures-money-math-and-execution-economics`.
- TA3000 vectorbt, pandas-ta-classic, signal matrices, indicator/derived compute, or research backtest integration: repo-local `ta3000-quant-compute-methodology`.
- TA3000 strategy hypothesis, trading intent, market regimes, research protocol, acceptance, or rejection: repo-local `ta3000-strategy-research-methodology`.
- TA3000 technical-analysis system design, trend, momentum, mean reversion, breakout, volatility, volume, divergence, or multi-timeframe logic: repo-local `ta3000-technical-analysis-system-design`.
- TA3000 backtest validation, walk-forward, out-of-sample evidence, robustness, overfitting, costs, slippage, lookahead, or survivorship risk: repo-local `ta3000-backtest-validation-and-overfit-control`.
- TA3000 signal delivery, Telegram/advisory alerts, webhook payloads, paper trading, live mode, robot lifecycle, or signal-to-action chains: repo-local `ta3000-signal-to-action-lifecycle`.

## Product-Plane Research Routing
- For new or revised trading ideas, start with `ta3000-strategy-research-methodology`; add `ta3000-technical-analysis-system-design` when the idea is expressed through indicators, chart structure, or technical-analysis regimes.
- For compute implementation of indicators, derived indicators, signal matrices, vectorbt, or pandas-ta-classic, add `ta3000-quant-compute-methodology` before editing code so library-native patterns are checked before local adaptation.
- For futures money math, margin estimates, fees/slippage/PnL assumptions, or propagation of contract economics into research/backtest/ranking outputs, add `ta3000-futures-money-math-and-execution-economics` and keep contract economics, vectorbt simulation truth, and execution ledger truth separate.
- For strategy testing, promotion, or "does this work?" questions, add `ta3000-backtest-validation-and-overfit-control` before claiming strategy quality.
- For user-facing output, Telegram-style advisory signals, paper routing, semi-auto approval, live execution, or robot questions, add `ta3000-signal-to-action-lifecycle` and keep the output mode explicit.
- Pair these repo-local skills with global implementation, testing, review, and verification skills as needed; repo-local research skills do not replace executable evidence or standard PR hygiene.

## Native Runtime Routing
- For product-plane data, research, compute, optimization, or orchestration work, read `docs/architecture/product-plane/native-runtime-ownership.md` and the agent shim `docs/agent/native-runtime-selection.md` before implementation.
- Record a Native Runtime Choice whenever the task touches Spark, Delta Lake, Dagster, pandas-ta-classic, vectorbt, Optuna, DuckDB, or a replacement custom Python path.
- Prefer library-native primitives before custom Python:
  - Spark SQL/DataFrames for large structured transforms;
  - Delta Lake for durable table writes, merges, schema control, and storage proof;
  - Dagster assets/jobs/sensors/schedules for repeatable operational routes;
  - pandas-ta-classic for standard technical indicators and indicator bundles;
  - vectorbt for signal/order/portfolio simulation and matrix-shaped parameter surfaces;
  - Optuna for adaptive optimizer search and trial provenance;
  - DuckDB for local SQL validation, profiling, and query-plan evidence.
- If custom Python owns logic in one of those zones, state the fallback reason and the proof that the native primitive did not fit.
- Treat loops over bars, instruments, parameter rows, or table rows as review triggers when a native runtime can express the same operation.

## Generic-First Routing
1. Start from Matt process/architecture/testing skills.
2. Apply stack skills only after stack surfaces are present and validated.
3. Keep domain-specialized skills outside baseline shell runtime.
4. Prefer integration into an existing skill when overlap is high; add new skills only for missing, non-trivial capabilities.
5. When multiple skills match, pick the smallest set that covers intent.

## Architecture Orientation Routing
- When a task asks for the architecture map, system shape, module boundaries, or
  shell/product ownership, open:
  - `docs/architecture/trading-advisor-3000.md`
  - `docs/architecture/repository-surfaces.md`
- When implementation status matters, also open:
  - `docs/architecture/product-plane/STATUS.md`
- Route `codebase-design` first for design and boundary questions; use `improve-codebase-architecture` when scanning for deeper architecture opportunities.
- Use Graphify as an optional companion context for architecture mapping,
  ownership, cross-module relationships, dependency tracing, or "where does this
  concept live?" questions when a local Graphify report or graph JSON exists.
- Before relying on Graphify for architecture orientation, compare the graph
  freshness with the relevant changed areas. If the graph is stale for the
  active question, refresh it with `graphify update .` for code-oriented context
  or the explicit Graphify skill flow for semantic docs/images; if refresh is
  skipped, state that the graph is stale and use it only as an orientation aid.
- Graphify belongs to architecture orientation, not the general agent baseline:
  do not run a Graphify pass after every Serena lookup or every code task.
- Do not let Graphify override hot docs, source code, runtime evidence, or
  process artifacts. Use it to locate the relevant area, then use Serena or
  direct source inspection for exact implementation proof.
- Do not run Graphify semantic extraction across secrets, production data,
  generated artifacts, archives, memory, or plans. Keep `.graphifyignore`
  aligned with that boundary.
- Co-load `grill-with-docs` or `domain-modeling` when the architecture docs themselves need to be corrected, synchronized, or tied to glossary/ADR decisions.
- For new modules or moved modules, follow local repository patterns directly and update registry/docs when contracts move.
- When a task crosses Postgres, Neo4j, FAISS, and provenance links, inspect those layers directly and document the consistency proof.

## Worker Coding Routing
- When the active phase is implementation and the request is code-writing focused, load:
  - `implement` (primary)
- Co-load conditionally:
  - direct contract/source-of-truth update when contracts/schemas/interfaces change
  - direct cross-layer proof when changes cross document retrieval layers or boundaries
  - `tdd` for primary changed-path coverage only
- Do not auto-load intake-oriented or acceptance-only skills for worker coding by default.

## Pipeline Routing
- When changes touch `.github/workflows/**` or lane wiring, use direct repo checks, GitHub CLI evidence, and `to-spec` or `to-tickets` when CI/PR scope needs durable planning.
- CI changes must remain policy-aligned and reviewable even without a dedicated CI skill.

## Critical Contour Routing
- When changed files match `configs/critical_contours.yaml`, route `CTX-ARCHITECTURE` as a companion context even when no architecture doc changed directly.
- For critical contours, load `codebase-design` before implementation so the chosen path is checked against target shape and executable evidence.
- If the task claims contour closure or acceptance, include explicit closeout evidence.
- Critical contour work must declare `target`, `staged`, or `fallback` in PR evidence before code changes begin.
- If the chosen path is simpler than the target shape, the agent must name that fact explicitly instead of presenting it as target closure.

## Class Policy

| Class | Baseline runtime | Policy |
| --- | --- | --- |
| `KEEP_CORE` | allowed | repo-local active skill |
| `KEEP_OPTIONAL` | blocked | separate phase gate |
| `DEFER_STACK` | blocked | stack activation gate |
| `EXCLUDE_DOMAIN_INITIAL` | blocked | non-baseline by default |

## Lifecycle Rules

### Add Skill
1. Create a new skill folder under `.codex/skills/` with a metadata-complete descriptor file.
2. Confirm the skill is TA3000-specific and owned by a product-plane/data/research/compute surface; generic engineering skills belong under `D:/CodexHome/skills`.
3. Run `python scripts/sync_skills_catalog.py`.
4. Update routing policy only if class policy or routing behavior changed.
5. Run strict validators and skill tests.

### Change Skill
1. Edit skill metadata/body.
2. Regenerate catalog.
3. Update routing doc only when routing metadata changed.
4. Run `python scripts/skill_update_decision.py --strict ...`.

### Remove Skill
1. Remove skill directory.
2. Regenerate catalog.
3. Update roadmap if the skill moves out of runtime baseline.
4. Validate strict parity and change-surface gates.

### Rename Skill
1. Rename directory and frontmatter `name` together.
2. Regenerate catalog.
3. Update routing references if triggers or class placement changed.
4. Run strict decision and precommit gates.

## What Requires Which Docs
- Only catalog sync required:
  - content edits without routing metadata/class changes.
- Routing policy update required:
  - `routing_triggers` changes;
  - class policy rule changes;
  - add/remove/rename rules change.
- Workflow update required:
  - process contract changes in validation/remediation flow.

## Validation Commands
- `python scripts/sync_skills_catalog.py --check`
- `python scripts/validate_skills.py --strict`
- `python scripts/skill_update_decision.py --strict --from-git --git-ref HEAD`
- `python scripts/skill_precommit_gate.py --from-git --git-ref HEAD`
