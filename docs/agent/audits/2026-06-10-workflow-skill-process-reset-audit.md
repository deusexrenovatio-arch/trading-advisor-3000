# Workflow, Skills, And Gate Reset Audit

Date: 2026-06-10
Change surface: shell

## Decision
Reset the ordinary TA3000 delivery route to a short, explicit workflow:

`prompt -> surface/risk classification -> minimal context -> one owning skill when needed -> patch -> focused verification -> PR`

Separate session lifecycle routing is retired from the live delivery path.
Historical orchestration artifacts may remain as evidence, but they are not an
entry route, gate mode, or default context source.

## External Principles Used
- OpenAI agent guidance favors simpler maintainable patterns, well-defined tools,
  eval baselines, and adding orchestration only when the application owns that
  complexity: <https://openai.com/business/guides-and-resources/a-practical-guide-to-building-ai-agents/>
- OpenAI Agents SDK guidance separates simple Responses/tool usage from SDK-level
  orchestration, tool execution, approvals, and state:
  <https://developers.openai.com/api/docs/guides/agents>
- Anthropic guidance favors simple composable workflows before heavier agent
  frameworks and emphasizes adding complexity only when it improves outcomes:
  <https://www.anthropic.com/engineering/building-effective-agents>
- Anthropic context guidance treats context as a limited resource that must be
  actively curated:
  <https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents>

## Findings
- `context_router.py` loaded legacy handoff state as default intent even when no
  diff was present, causing ordinary routing to begin from stale process
  residue.
- `run_loop_gate.py` and `run_pr_gate.py` required an active task session by
  default, so ordinary PR evidence depended on legacy lifecycle state.
- Live docs required Superpowers/process routing before broad phases, which
  encouraged loading process skills even when a narrower skill or no skill owned
  the next artifact.
- Several live commands still depended on session bypass/enforcement toggles,
  making the intended ordinary route depend on an escape hatch rather than a
  first-class default.
- Content-only repo-local skill edits were treated like catalog/routing contract
  changes even when generated metadata did not change.
- No active `docs/agent/plugin-eval` run-output tree was present on this branch.
  The active generated-output problem was instead `artifacts/codex/orchestration/`:
  the root was already ignored, but 310 historical run-output files were still
  tracked.

## Reset Contract
- Ordinary gates are session-free by default.
- Session enforcement flags are removed from live gates instead of inverted.
- Legacy handoff state is not a context-router input.
- Process skills are risk-triggered and phase-owned, not always-on ceremony.
- Superpowers and governed-process skills are not default route dependencies.
- Default MCP/tool exposure is reduced to Serena for repo navigation; PR,
  OpenAI docs, data, Dagster, graph, browser, office, and plugin-eval tools are
  explicit routes only.
- Governed launcher/orchestrator/package-intake/inbox-daemon code and their
  process tests are retired from the live repository surface.
- Governance dashboard reporting no longer computes governed orchestration
  quality; process quality is measured from the short-route lifecycle signals.
- The repo-level MCP rollout bundle, preflight validator, runbook, and former
  MCP governance workflow job are retired. Secret scanning remains as a
  standalone security check.
- Repo-local MemPalace hooks and global context-mode hook automation are removed
  from live Codex config. Codex app hooks stay disabled by default.
- CodexHome global process skills that pushed ceremony into ordinary delivery
  are moved to a disabled skill root for rollback instead of remaining in the
  active global skill catalog.
- Repo-local skills remain only for TA3000 trading/product/data/research/compute
  knowledge; generic engineering process stays in the global skills root.
- Content-only repo-local skill edits require catalog check and strict skill
  validation, but do not require a no-op catalog diff.
- Ignored generated run-output roots must not stay tracked.

## Acceptance Evidence
- Red tests were added before implementation for:
  - context router rejecting retired handoff input;
  - ordinary loop/PR gates passing without active session;
  - retired session flags being rejected;
  - live policy files rejecting retired defaults.
- Additional regression coverage verifies content-only skill updates and ignored
  generated run roots.
- Focused process tests must pass before PR publication.
