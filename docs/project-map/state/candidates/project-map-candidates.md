---
title: Project Map Candidate Problems
type: project-candidate-report
status: candidate
generated_on: 2026-05-05
candidate_count: 10
tags:
  - ta3000/project-map
  - ta3000/project-candidates
---

# Project Map Candidate Problems

This is an inbox, not current project truth. Promote a candidate to a project item only after live verification.

## Operating Rule

- MemPalace is advisory recall, not authoritative state.
- Keep candidates here until the current repo, artifact, or user decision confirms them.
- Do not let this report affect cockpit roll-up status directly.

## Summary

- By priority: p0=4, p1=6
- By node: data-plane=1, delivery-gates=9

## Candidate Queue

### C001: BLOCKED. Governed continuation itself is in the right route, and my independent reruns of the E1 technical checks pas...

- Candidate id: `candidate-ef5780263c`
- Suggested node: `delivery-gates`
- Suggested type: `problem`
- Suggested priority: `p0`
- Confidence: `low`
- Source: `rollout-2026-04-05T14-48-26-019d5d79-0778-7440-8307-0112ed258863.jsonl`
- Query: `trading advisor open question unknown state`

Excerpt:

> BLOCKED. Governed continuation itself is in the right route, and my independent reruns of the E1 technical checks passed: preflight, both phase tests, validate_skills, sync_skills_catalog, loop gate, and PR gate. But E1 still cannot unlock because worker-report.json explicitly records a failed canonical clean bootstrap, a fallback execution path, and deferred critical work, which conflicts with the E1 acceptance gate in ta3000-vectorbt-pandasta.phase-E1.md. I also found stale operator guidance in bootstrap.md versus the canonical explicit gate commands in runtime.md and checks.md.

### C002: BLOCKED. Governed route evidence is present, and I independently reran the cited validators, phase tests, and run_loo...

- Candidate id: `candidate-3404acf6cd`
- Suggested node: `delivery-gates`
- Suggested type: `problem`
- Suggested priority: `p0`
- Confidence: `medium`
- Source: `rollout-2026-04-01T00-19-13-019d45c3-cff0-71f2-9be4-33d1494840b1.jsonl`
- Query: `trading advisor blocked unresolved proof gap`

Excerpt:

> BLOCKED. Governed route evidence is present, and I independently reran the cited validators, phase tests, and run_loop_gate: they pass in the current tree. Documentation and status are now aligned with a blocked F1-E state, but the phase still cannot unlock because the bounded worker report contains non-empty assumptions and deferred_work, the current run ended at secret_validation, and the required staging-real proof of executable Finam binding plus real external broker lifecycle transport is still missing.

### C003: E1 itself is close: dependency proof, targeted tests, docs, and even an independent rerun of the canonical loop gate...

- Candidate id: `candidate-d6942df262`
- Suggested node: `delivery-gates`
- Suggested type: `problem`
- Suggested priority: `p0`
- Confidence: `low`
- Source: `rollout-2026-04-05T14-05-55-019d5d52-1dc0-7ab2-826d-e815ecaa7dea.jsonl`
- Query: `trading advisor outdated task worktree not merged`

Excerpt:

> E1 itself is close: dependency proof, targeted tests, docs, and even an independent rerun of the canonical loop gate all check out in the current workspace. But this governed attempt still cannot unlock because the supplied remediation evidence explicitly leaves critical work deferred in remediation-report.json, and the non-dry-run governed continuation still stopped at clean_worktree_check: FAILED in governed-continue-route.log. By the acceptance contract, that means route integrity is not closed and PASS is forbidden.

### C004: Worker evidence is auto-blocking under the acceptance contract

- Candidate id: `candidate-15667c73ca`
- Suggested node: `delivery-gates`
- Suggested type: `problem`
- Suggested priority: `p0`
- Confidence: `low`
- Source: `rollout-2026-03-31T17-53-40-019d4462-d261-72d1-b124-45519a619050.jsonl`
- Query: `trading advisor missing proof acceptance blocker`

Excerpt:

> BEGIN_PHASE_ACCEPTANCE_JSON
> {"verdict":"BLOCKED","summary":"Phase 05 cannot unlock: the worker report contains explicit fallback and deferred critical work, the governed proof artifacts are still tied to a local stub contour, and the proof validator remains too weak to reject stub-derived bindings as real contour evidence.","route_signal":"acceptance:governed-phase-route","used_skills":["phase-acceptance-governor","architecture-review","testing-suite","docs-sync"],"blockers":[{"id":"B1","title":"Worker evidence is auto-blocking under the acceptance contract","why":"The remediation report has non-empty fallbacks and deferred_work, which the phase acceptor was explicitly instructed to treat as mandatory blockers.","remediation":"Remove the fallback/deferred state by completing the phase with acceptance-grade real contour evidence instead of recording local stub proof plus future rerun work."},{"id":"B2","title":"Real broker contour is still not proven","why":"Phase 05 requires real_broker_process to move from planned to implemented, but the current truth source still keeps it planned and the artifact chain shows connector_binding_source=local-remediation-stub rather than a real external StockSharp/QUIK/Finam staging session.","remediation":"Run the canonical F1-E route against the agreed real external staging connector with operator-managed secrets and replayable artifacts, then update truth sources only after that evidence is produced."},{"id":"B3","title":"Acceptance path still permits stub-shaped proof","why":"validate_real_connector_health() only rejects exact values such as stub or mock, so stub-derived markers like local-remediation-stub pass validation and can produce a misleading successful proof bundle.","remediation":"Tighten F1-E validation so stub/mock/simulated/local-remediation sources are fail-closed for acceptance proof, and add regression coverage proving those sources are rejected."}],"rerun_checks":["python scripts/run_f1e_real_broker_process.py --output-root artifacts/f1/phase05/real-broker-process --dotnet \"C:\\\\Program Files\\\\dotnet\\\\dotnet.exe\"","python -m pytest tests/process/test_run_f1e_real_broker_process.py -q","python -m pytest tests/app/contracts/test_phase5_real_broker_process_contracts.py -q","python -m pytest tests/app/integration/test_real_execution_staging_rollout.py -q","python scripts/validate_stack_conformance.py","python scripts/run_loop_gate.py --from-git --git-ref HEAD"],"evidence_gaps":["No replayable acceptance-grade artifact proving a real external StockSharp/QUIK/Finam staging session.","Current successful artifact chain records connector_binding_source=local-remediation-stub, which is weaker than the phase brief requires."],"prohibited_findings":["fallbacks present in worker report","deferred critical work present in worker report","stub-derived connector binding accepted as real contour proof"]}
> END_PHASE_ACCEPTANCE_JSON

### C005: The remaining blocker is documentation integrity in STATUS.md. This phase touched that truth-source document, but it...

- Candidate id: `candidate-4317bd837f`
- Suggested node: `data-plane`
- Suggested type: `problem`
- Suggested priority: `p1`
- Confidence: `medium`
- Source: `rollout-2026-04-02T12-57-32-019d4da0-6bd3-77f1-ab97-921f0d06f5fc.jsonl`
- Query: `trading advisor missing proof acceptance blocker`

Excerpt:

> The remaining blocker is documentation integrity in STATUS.md. This phase touched that truth-source document, but it still says Phase 01 acceptance is “not yet granted”, which contradicts the accepted Phase 01 record in acceptance.json. Required docs are therefore stale, so Phase 03 should stay locked until that is corrected.

### C006: BLOCKED: governed route integrity is intact, the remediation report is phase-scoped with empty assumptions/skips/fall...

- Candidate id: `candidate-ecab8399eb`
- Suggested node: `delivery-gates`
- Suggested type: `problem`
- Suggested priority: `p1`
- Confidence: `medium`
- Source: `rollout-2026-03-30T20-00-28-019d3fb0-8c34-7e72-a5cd-0deb5390a6fa.jsonl`
- Query: `trading advisor blocked unresolved proof gap`

Excerpt:

> Findings
> BLOCKED: governed route integrity is intact, the remediation report is phase-scoped with empty assumptions/skips/fallbacks/deferred_work, and the listed checks rerun cleanly. But Phase 01 is still not closed against its own contract. The phase brief requires fail-closed contradiction detection across reports, specs, ADRs, and module-phase briefs in f1-full-closure.phase-01.md, f1-full-closure.phase-01.md, and f1-full-closure.phase-01.md. The actual guard still scans only a narrow document list in stack_conformance.yaml through validate_stack_conformance.py. I confirmed this with an independent temp-repo disprover: injecting aiogram removed by ADR into docs/architecture/app/product-plane-spec-v2/04_ADRs.md or into the current phase brief still leaves python scripts/validate_stack_conformance.py green. That means the required fail-closed contour is incomplete.

### C007: Fresh governed broker proof is still not closed

- Candidate id: `candidate-51fc5f055e`
- Suggested node: `delivery-gates`
- Suggested type: `problem`
- Suggested priority: `p1`
- Confidence: `medium`
- Source: `rollout-2026-03-31T22-21-01-019d4557-96b3-75e3-a900-6ab84746d127.jsonl`
- Query: `trading advisor blocked unresolved proof gap`

Excerpt:

> BEGIN_PHASE_ACCEPTANCE_JSON
> {"verdict":"BLOCKED","summary":"Phase 05 remains blocked: route and tests are intact, but the fresh governed broker proof did not succeed, deferred critical rerun work remains, and broker-contour docs are not fully synchronized.","route_signal":"acceptance:governed-phase-route","used_skills":["phase-acceptance-governor","architecture-review","testing-suite","docs-sync"],"blockers":[{"id":"B1","title":"Fresh governed broker proof is still not closed","why":"The canonical F1-E runner still fails fail-closed on missing real bindings (TA3000_STOCKSHARP_API_KEY, TA3000_FINAM_API_TOKEN), and the worker report contains non-empty deferred_work, which is an automatic block under the acceptance contract.","remediation":"Bind the real staging Finam base URL/JWT and StockSharp API key, rerun the governed F1-E proof until it produces a fresh successful staging-real bundle, and only then attempt the planned -> implemented transition."},{"id":"B2","title":"Broker-contour truth-source docs are inconsistent","why":"STATUS.md keeps StockSharp/QUIK/Finam real broker process in planned/blocked, while stack-conformance-baseline.md still states that the real broker process closure is implemented for the bounded staging-real contour. Acceptance rules require documentation closure with no stale operator or architecture guidance.","remediation":"Reconcile the truth-source and claim-control docs to one honest state. Keep implemented wording only after the successful governed F1-E bundle is accepted; otherwise downgrade the baseline wording back to the blocked/planned state."}],"rerun_checks":["python scripts/run_f1e_real_broker_process.py --output-root artifacts/f1/phase05/real-broker-process --dotnet \"C:/Program Files/dotnet/dotnet.exe\"","python -m pytest tests/process/test_run_f1e_real_broker_process.py -q","python -m pytest tests/app/contracts/test_phase5_real_broker_process_contracts.py -q","python -m pytest tests/app/integration/test_real_execution_staging_rollout.py -q","python scripts/validate_stack_conformance.py","python scripts/validate_docs_links.py --roots AGENTS.md docs","python scripts/run_loop_gate.py --from-git --git-ref HEAD"],"evidence_gaps":["The current worker attempt produced fail-closed secret_validation evidence, not a fresh successful staging-real broker proof bundle.","The current attempt did not produce the truth-source transition needed to move real_broker_process from planned to implemented."],"prohibited_findings":["Non-empty deferred_work is present in the worker report.","Claim-control documentation still contains an implemented broker-process statement while the current truth source remains planned/blocked."]}
> END_PHASE_ACCEPTANCE_JSON

### C008: Phase-01 worker patch delivered: added production-universe data pipeline runner with fail-closed gates (duplicates/ma...

- Candidate id: `candidate-fe29ba6188`
- Suggested node: `delivery-gates`
- Suggested type: `problem`
- Suggested priority: `p1`
- Confidence: `medium`
- Source: `rollout-2026-04-08T13-59-36-019d6cbf-675e-71a3-9b66-96f97ada33f7.jsonl`
- Query: `trading advisor outdated task worktree not merged`

Excerpt:

> BEGIN_PHASE_WORKER_JSON
> {"status":"DONE","summary":"Phase-01 worker patch delivered: added production-universe data pipeline runner with fail-closed gates (duplicates/mandatory/calendar-roll), deterministic replay check, lineage artifact, and immutable production identity contract; added focused unit+integration tests and phase runbook; route kept explicit as worker:phase-only with governed session context.","route_signal":"worker:phase-only","files_touched":["C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/src/trading_advisor_3000/product_plane/data_plane/moex/production_phase01.py","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/src/trading_advisor_3000/product_plane/data_plane/moex/__init__.py","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/scripts/run_ta3000_production_phase01_pipeline.py","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/tests/product-plane/unit/test_production_phase01_pipeline.py","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/tests/product-plane/integration/test_production_phase01_pipeline_integration.py","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/docs/runbooks/app/ta3000-production-readiness-phase01-runbook.md","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/docs/runbooks/app/README.md","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/docs/runbooks/README.md","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/artifacts/codex/ta3000-production-readiness/phase-01/20260408T121000Z/phase01-production-universe-report.json","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/artifacts/codex/ta3000-production-readiness/phase-01/20260408T121000Z/production_universe_quality_gate_report.v1.json","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/artifacts/codex/ta3000-production-readiness/phase-01/20260408T121000Z/production_universe_lineage_report.v1.json","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/artifacts/codex/ta3000-production-readiness/phase-01/release/20260408T121000Z/production_universe_identity_contract.v1.json"],"checks_run":["python -m pytest tests/product-plane/unit/test_production_phase01_pipeline.py tests/product-plane/integration/test_production_phase01_pipeline_integration.py -q","python scripts/run_ta3000_production_phase01_pipeline.py --phase01-root artifacts/codex/moex-phase01 --phase01-run-id 20260408T120000Z --run-id 20260408T121000Z","python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none"],"remaining_risks":["Loop gate is currently blocked by pre-existing phase-planning contract violations in ta3000-production-readiness contract/phase docs (owner-phase mapping and contract field requirements); this was not remediated in this phase-only worker patch.","Proof run used a local deterministic phase01 fixture bundle under artifacts/codex/moex-phase01/20260408T120000Z; live MOEX fetch was not executed in this worker attempt."],"assumptions":[],"skips":[],"fallbacks":[],"deferred_work":[],"evidence_contract":{"surfaces":["production_universe_data_pipeline_surface"],"proof_class":"integration","artifact_paths":["C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/artifacts/codex/ta3000-production-readiness/phase-01/20260408T121000Z/phase01-production-universe-report.json","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/artifacts/codex/ta3000-production-readiness/phase-01/20260408T121000Z/production_universe_quality_gate_report.v1.json","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/artifacts/codex/ta3000-production-readiness/phase-01/20260408T121000Z/production_universe_lineage_report.v1.json","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/artifacts/codex/ta3000-production-readiness/phase-01/release/20260408T121000Z/production_universe_identity_contract.v1.json"],"checks":["python -m pytest tests/product-plane/unit/test_production_phase01_pipeline.py tests/product-plane/integration/test_production_phase01_pipeline_integration.py -q","python scripts/run_ta3000_production_phase01_pipeline.py --phase01-root artifacts/codex/moex-phase01 --phase01-run-id 20260408T120000Z --run-id 20260408T121000Z","python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none"],"real_bindings":["C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/artifacts/codex/moex-phase01/20260408T120000Z/coverage-report.pass2.json","C:/Users/Admin/.codex/worktrees/0663/trading advisor 3000/artifacts/codex/moex-phase01/20260408T120000Z/raw-ingest-report.pass2.json","https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRM6/candleborders.json","moex_iss","production_universe_lineage_report.v1#58933ec59dd860dbfb692f821bbd4adab07c5eb59441b122ee57683acc8d4d4f","production_universe_quality_gate_report.v1#852fc0e73ad0e4df3bb9980c1fa7ac0e1dd9502daaf349d3f34d03b693941d0d"]}}
> END_PHASE_WORKER_JSON

### C009: Phase10 evidence still claims a checklist artifact that does not exist

- Candidate id: `candidate-34921bc380`
- Suggested node: `delivery-gates`
- Suggested type: `problem`
- Suggested priority: `p1`
- Confidence: `low`
- Source: `rollout-2026-03-30T19-35-35-019d3f99-c6a5-71f3-83c6-3ed8c4077a92.jsonl`
- Query: `trading advisor missing proof acceptance blocker`

Excerpt:

> BEGIN_PHASE_ACCEPTANCE_JSON
> {"verdict":"BLOCKED","summary":"Validator hardening and targeted checks are good, but F1-A still leaves a stale evidence claim: the phase10 re-acceptance report says a new checklist exists while that artifact is absent and the evidence pack still references it.","route_signal":"acceptance:governed-phase-route","used_skills":["phase-acceptance-governor","architecture-review","testing-suite","docs-sync"],"blockers":[{"id":"B1","title":"Phase10 evidence still claims a checklist artifact that does not exist","why":"The current phase scope explicitly includes honest alignment of the phase10 report and acceptance evidence claims. The report says a new Phase 10 re-acceptance checklist was regenerated, but docs/checklists/app/phase10-reacceptance-checklist.md is absent, and the evidence pack still cites gate commands that reference that missing file. This is missing evidence plus stale documentation, which is a hard block under the acceptance contract.","remediation":"Either restore the Phase 10 re-acceptance checklist as a real governed artifact with matching evidence, or remove/update every current-scope reference that presents it as existing proof, including the phase10 report and evidence pack. Then rerun the phase checks and resubmit for acceptance."}],"rerun_checks":["rg -n \"phase10-reacceptance-checklist\" docs artifacts","python scripts/validate_stack_conformance.py","python -m pytest tests/process/test_validate_stack_conformance.py -q","python scripts/validate_docs_links.py --roots docs/architecture/app artifacts/acceptance/f1 docs/codex/modules","python scripts/run_loop_gate.py --from-git --git-ref HEAD"],"evidence_gaps":["docs/checklists/app/phase10-reacceptance-checklist.md is still claimed as regenerated/used evidence, but the artifact is missing from the repository."],"prohibited_findings":["Evidence is declared in current-scope acceptance artifacts but the referenced artifact does not exist."]}
> END_PHASE_ACCEPTANCE_JSON

### C010: There is also acceptance-surface drift. The phase brief requires STATUS, registry, docs, and runbooks to reflect the...

- Candidate id: `candidate-fbe943caa4`
- Suggested node: `delivery-gates`
- Suggested type: `problem`
- Suggested priority: `p1`
- Confidence: `low`
- Source: `rollout-2026-03-31T23-42-41-019d45a2-5b34-7b83-8482-729144ea2e9e.jsonl`
- Query: `trading advisor open question unknown state`

Excerpt:

> There is also acceptance-surface drift. The phase brief requires STATUS, registry, docs, and runbooks to reflect the same scope, but STATUS and the stack registry say implemented, while the closure doc and runbook still say blocked.
