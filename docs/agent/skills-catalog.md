# Skills Catalog

<!-- generated-by: scripts/sync_skills_catalog.py -->
<!-- source-of-truth: .cursor/skills/*/SKILL.md -->
<!-- generated-contract: do not edit manually; run python scripts/sync_skills_catalog.py -->
<!-- catalog-sha256: 9d6bdd5a8ecb3d2b9ed2bdcb3da129d8f9f9008ebba5c071f7839e357b7ee941 -->

| skill_id | classification | wave | status | scope | owner_surface | routing_triggers | source | hot_context_policy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ai-agent-architect` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | agent delivery planning and execution boundaries | `CTX-OPS` | agent; orchestration; pipeline; plan; strategy | `local_runtime` | `cold-by-default` |
| `ai-change-explainer` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | change explanation and PR narrative quality | `CTX-OPS` | change summary; pr narrative; diff explanation; impact report | `local_runtime` | `cold-by-default` |
| `architecture-review` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | architecture boundary review and dependency control | `CTX-ARCHITECTURE` | architecture; boundaries; dependencies; module review | `local_runtime` | `cold-by-default` |
| `business-analyst` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | requirements framing and acceptance decomposition | `CTX-OPS` | requirements; scope; acceptance; traceability; stakeholder | `local_runtime` | `cold-by-default` |
| `composition-contracts` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | interface composition and contract ownership | `CTX-CONTRACTS` | composition; contract; ownership; resolver mapping | `local_runtime` | `cold-by-default` |
| `docs-sync` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | docs-as-source-of-truth synchronization | `CTX-OPS` | documentation; sync docs; docs as code; policy docs | `local_runtime` | `cold-by-default` |
| `golden-tests-and-fixtures` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | golden fixtures and deterministic regression protection | `CTX-OPS` | golden tests; fixtures; regression protection; deterministic tests | `local_runtime` | `cold-by-default` |
| `incident-runbook` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | incident handling and remediation flow | `CTX-OPS` | incident; runbook; postmortem; remediation | `local_runtime` | `cold-by-default` |
| `layer-diagnostics-debug` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | cross-layer diagnostics and root-cause reporting | `CTX-ARCHITECTURE` | layer diagnostics; debug path; cross-layer; visibility check | `local_runtime` | `cold-by-default` |
| `module-scaffold` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | module initialization with governance defaults | `CTX-ARCHITECTURE` | module scaffold; new module; bounded context; scaffold | `local_runtime` | `cold-by-default` |
| `parallel-worktree-flow` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | parallel worktree operations and branch isolation | `CTX-OPS` | worktree; parallel streams; branch isolation; integration branch | `local_runtime` | `cold-by-default` |
| `patch-series-splitter` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | patch decomposition and change sequencing | `CTX-OPS` | patch series; split diff; ordered patches; review sequence | `local_runtime` | `cold-by-default` |
| `phase-acceptance-governor` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | hard phase acceptance policy and evidence-based unblock rules | `CTX-OPS` | phase acceptance; acceptance gate; acceptor; fallback; skip checks | `local_runtime` | `cold-by-default` |
| `product-owner` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | value-based prioritization and roadmap sequencing | `CTX-OPS` | value; priorities; roadmap; mvp | `local_runtime` | `cold-by-default` |
| `qa-test-engineer` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | quality planning and end-to-end verification | `CTX-OPS` | qa; test plan; regression; validation | `local_runtime` | `cold-by-default` |
| `registry-first` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | registry-first governance for contract evolution | `CTX-CONTRACTS` | registry; schema; contract; catalog | `local_runtime` | `cold-by-default` |
| `repeated-issue-review` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | repeat-failure analysis and prevention strategy | `CTX-OPS` | repeated issue; root cause; stability; full review | `local_runtime` | `cold-by-default` |
| `risk-profile-gates` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | risk classification and gating rules | `CTX-CONTRACTS` | risk profile; risk gate; release gate; policy threshold | `local_runtime` | `cold-by-default` |
| `skill-creator` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | skill authoring and lifecycle maintenance | `CTX-SKILLS` | create skill; update skill; skill design; skill authoring | `local_runtime` | `cold-by-default` |
| `skill-installer` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | skill onboarding and local catalog installation | `CTX-SKILLS` | install skill; catalog install; skill onboarding; skill source | `local_runtime` | `cold-by-default` |
| `source-onboarding` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | source onboarding with quality and provenance controls | `CTX-DATA` | source onboarding; ingestion; lineage; provenance | `local_runtime` | `cold-by-default` |
| `testing-suite` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | test suite strategy and maintenance | `CTX-OPS` | tests; coverage; integration; contract tests | `local_runtime` | `cold-by-default` |
| `archctl-policy-authoring` | `KEEP_CORE` | `WAVE_2` | `ACTIVE` | policy gate authoring and fitness rule design | `CTX-CONTRACTS` | policy gate; fitness rule; architecture policy; ci blocking | `local_runtime` | `cold-by-default` |
| `ci-bootstrap` | `KEEP_CORE` | `WAVE_2` | `ACTIVE` | ci lane setup and gate wiring | `CTX-OPS` | ci; pipeline; merge gate; workflow; hosted runners | `local_runtime` | `cold-by-default` |
| `codeowners-from-registry` | `KEEP_CORE` | `WAVE_2` | `ACTIVE` | ownership routing and review coverage | `CTX-CONTRACTS` | codeowners; ownership; review routing; owner mapping | `local_runtime` | `cold-by-default` |
| `commit-and-pr-hygiene` | `KEEP_CORE` | `WAVE_2` | `ACTIVE` | commit hygiene and pull request structure | `CTX-OPS` | commit hygiene; pr hygiene; atomic changes; reviewability | `local_runtime` | `cold-by-default` |
| `dependency-and-license-audit` | `KEEP_CORE` | `WAVE_2` | `ACTIVE` | dependency risk and license governance | `CTX-OPS` | dependency audit; license audit; supply chain; vulnerability | `local_runtime` | `cold-by-default` |
| `secrets-and-config-hardening` | `KEEP_CORE` | `WAVE_2` | `ACTIVE` | secrets management and config safety controls | `CTX-CONTRACTS` | secrets; configuration; hardening; sensitive data | `local_runtime` | `cold-by-default` |
| `validate-crosslayer` | `KEEP_CORE` | `WAVE_2` | `ACTIVE` | cross-layer consistency and boundary validation | `CTX-ARCHITECTURE` | crosslayer; boundary validation; consistency checks; layer contract | `local_runtime` | `cold-by-default` |

## Generation
- Generate: `python scripts/sync_skills_catalog.py`
- Check drift: `python scripts/sync_skills_catalog.py --check`
