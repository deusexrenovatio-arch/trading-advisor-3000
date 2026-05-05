# Superpowers Baseline Skill Audit

Date: 2026-05-05
Change surface: shell
Scope: active skill catalogs only; no skill instruction files changed.

## Decision Frame

Use Superpowers as the canonical baseline for generic engineering workflow skills.
This does not mean copying the plugin catalog into local skills. It means using
Superpowers skill quality as the standard for trigger precision, hard gates,
red flags, stop conditions, and verification discipline.

Precedence stays:

1. User and repository instructions.
2. Superpowers generic workflow discipline.
3. Global Codex skill extensions.
4. Repo-local TA3000 domain/runtime skills.

Repo-local TA3000 skills remain domain-specific. They should not become generic
engineering skills.

## Inventory

| Catalog | Count | Notes |
|---|---:|---|
| Superpowers plugin skills | 14 | Candidate canonical baseline for generic workflows. |
| Global Codex skills | 56 | Main audit target. |
| TA3000 repo-local skills | 8 | Domain/runtime skills; keep separate. |
| System skills | 5 | Packaging/tooling/system helpers. |

Quality scan:

| Catalog | Strict language | When-not-use coverage | Red flags | Evidence language |
|---|---:|---:|---:|---:|
| Global Codex skills | 45/56 | 26/56 | 6/56 | 46/56 |
| TA3000 repo-local skills | 8/8 | 5/8 | 6/8 | 8/8 |
| System skills | 4/5 | 1/5 | 1/5 | 5/5 |

Interpretation: TA3000 repo-local skills are already comparatively strict.
The global catalog has useful coverage, but many skills need sharper triggers,
explicit exclusions, and stronger stop/evidence rules.

## Verdict Types

| Verdict | Meaning |
|---|---|
| `canonical-superpowers` | Superpowers skill should own the generic workflow. |
| `extension` | Local skill stays, but only for additional Codex/Windows/runtime/project constraints. |
| `rewrite` | Local skill is useful but should be rewritten to Superpowers quality. |
| `compose` | Both skills stay; routing must say which one owns which phase. |
| `domain-keep` | Repo-local/domain skill stays separate; only style hardening may apply. |
| `retire-or-redirect` | Local skill is likely redundant unless it becomes a thin extension. |

## High-Priority Merge Map

| Local skill | Superpowers baseline | Verdict | Recommendation |
|---|---|---|---|
| `verification-before-completion` | `verification-before-completion` | `extension` | Keep local native-runtime/data-proof requirements. Absorb Superpowers hard gate: no fresh command/artifact means no completion claim. Add red flags and when-not-use section. |
| `repeated-issue-review` | `systematic-debugging` | `retire-or-redirect` | Make Superpowers debugging canonical for bugs/failures. Keep local skill only as escalation extension for repeated user reports, frustration, or full component review. |
| `code-implementation-worker` | `test-driven-development` | `extension` | Keep requirements traceability and safe error-path discipline. Add TDD as default for behavior changes and bugfixes, with explicit exceptions for docs/config/generated-only edits. |
| `executable-test-suite` | `test-driven-development` | `compose` | TDD owns order of behavior work. `executable-test-suite` owns test layer selection, fixtures, contract/e2e coverage, and native-runtime proof tests. |
| `code-reviewer` | `requesting-code-review`, `receiving-code-review` | `compose` | Keep local finding contract and native-runtime/test adequacy review. Absorb review-reception rule: verify feedback before implementing, push back when technically wrong. |
| `idea-clarifier` | `brainstorming` | `compose` | Superpowers owns creative design exploration. Local skill owns resistant clarification, explicit uncertainty, and blocking handoff when understanding is not ready. |
| `parallel-worktree-flow` | `using-git-worktrees`, `dispatching-parallel-agents` | `extension` | Keep merge-order and parallel-stream policy. Absorb ignored-worktree check, clean baseline check, and focused parallel task criteria. |
| `agents-orchestrator` | `subagent-driven-development`, `executing-plans` | `compose` | Keep governed phase gates and evidence progression. Borrow task-by-task implementation and two-stage review shape where subagents are explicitly allowed. |
| `pr-commit-history-and-summary` | `finishing-a-development-branch` | `compose` | Keep commit/PR shape ownership. Borrow finish discipline: verify before options. Override local-merge option for TA3000 PR-only main policy. |

## Immediate Rewrite Candidates

These global skills are not necessarily wrong, but they are weaker than the
Superpowers standard and should be hardened before more specialized work:

| Skill | Problem | Suggested action |
|---|---|---|
| `business-analyst` | Soft role prompt; weak exclusions and evidence rules. | Rewrite with concrete triggers, when-not-use, acceptance artifacts, and handoff boundaries. |
| `computer-vision-expert` | Soft role prompt. | Rewrite as a technique/decision skill or retire if not used. |
| `product-owner` | Soft role prompt despite strict marker. | Rewrite around prioritization decisions, not generic persona behavior. |
| `observability-slo` | Soft and broad. | Add when-not-use, evidence artifacts, minimum metrics/logs/traces contract. |
| `risk-profile-gates` | Useful but underspecified. | Add hard pass/block criteria and red flags. |
| `schema-migrations-postgres` | Useful but soft. | Add migration safety gates, rollback evidence, and when-not-use. |
| `incident-runbook` | Evidence-aware but not strict enough. | Add incident/postmortem stop conditions and required artifacts. |
| `document-crosslayer-consistency` | Evidence-aware but soft. | Add explicit inconsistency classes, proof commands, and blocker rules. |

## Keep As Domain Or Runtime Extensions

These are valuable because they encode local constraints that Superpowers does
not know:

- `codex-windows-runtime-recovery`
- `mempalace-healthcheck-and-sync`
- `data-engineer`
- `data-quality-gates`
- `data-lineage`
- `integration-connector`
- `source-onboarding`
- `openai-ocr-cost-and-reliability-guardrails`
- `architecture-review`
- `phase-acceptance-governor`
- `registry-first`
- TA3000 repo-local skills under `.codex/skills`

Action: do not replace them. Harden their structure only where needed.

## TA3000 Repo-Local Skills

Repo-local TA3000 skills are already in better shape than most generic global
skills:

- all 8 contain strict language;
- all 8 contain evidence/proof language;
- 6 of 8 contain red flags;
- 5 of 8 contain explicit when-not-use coverage.

Recommendation: keep them as `domain-keep`. Do not merge them into
Superpowers. Later style pass should add missing when-not-use sections to:

- `ta3000-signal-to-action-lifecycle`
- `ta3000-strategy-research-methodology`
- `ta3000-technical-analysis-system-design`

## Proposed Execution Order

1. Establish `writing-skills` as the canonical quality rubric for generic skill edits.
2. Merge `verification-before-completion` first. It has the clearest overlap and highest safety payoff.
3. Merge debugging next: make `systematic-debugging` canonical and reduce `repeated-issue-review` to escalation extension.
4. Merge code review behavior: integrate `receiving-code-review` rules into local review workflows.
5. Merge implementation/testing behavior: compose TDD with `code-implementation-worker` and `executable-test-suite`.
6. Merge worktree/branch-finish behavior with TA3000 PR-only overrides.
7. Harden soft role skills or retire them if no concrete trigger remains.

## Pass Status

2026-05-05 high-priority pass completed for the generic global skill layer:

| Skill | Status |
|---|---|
| `codex-skill-routing` | Updated to check Superpowers process skills first when available. |
| `verification-before-completion` | Updated with fresh-evidence iron law, evidence map, and red flags. |
| `repeated-issue-review` | Reduced to escalation extension after `superpowers:systematic-debugging`. |
| `code-implementation-worker` | Updated with TDD default and stop conditions. |
| `executable-test-suite` | Updated to compose with TDD and clarify test-layer ownership. |
| `code-reviewer` | Updated with review-feedback reception discipline. |
| `idea-clarifier` | Updated to compose with `superpowers:brainstorming`. |
| `parallel-worktree-flow` | Rewritten as multi-worktree/parallel-stream extension over Superpowers worktree/dispatch skills. |
| `agents-orchestrator` | Updated to compose with Superpowers subagent/plan execution while preserving governed phase gates. |
| `pr-commit-history-and-summary` | Updated to borrow branch-finish discipline while preserving PR/history ownership. |

Remaining candidates are lower-priority soft role or specialist skills. They
should be handled in separate passes rather than mixed into this merge.

## Non-Goals

- Do not copy Superpowers skill text into local skills wholesale.
- Do not create `docs/superpowers/plans` for TA3000 planning.
- Do not put generic engineering behavior into repo-local TA3000 skills.
- Do not add `.cursor/skills`.
- Do not turn Superpowers into a CI gate.
- Do not allow branch finish workflow to bypass PR-only main policy.

## Open Decisions

1. Should redundant local skills become thin redirect skills, or should they be removed from the global root after migration?
2. Should there be one local `skill-quality-standard` skill, or should `writing-skills` remain the direct rubric?
3. Should rewritten skills preserve current names for routing stability, or should canonical Superpowers names become the preferred public names?
