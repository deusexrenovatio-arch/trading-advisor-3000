# Trading Advisor 3000 — приёмка фаз до F1 и ТЗ на закрытие F1

Дата пакета: 2026-03-30

## Что внутри
- `01_phase_acceptance_verdict.md` — итоговая приёмка фаз G0, G1, D1, D2, D3, R1, R2, E1, S1.
- `02_detailed_phase_findings.md` — детальная аргументация по фазам и cross-phase расхождениям.
- `03_f1_full_closure_TZ.md` — техническое задание на полноценное закрытие F1 без самообмана.
- `04_anti_self_deception_controls.md` — правила доказательства, отрицательные тесты и fail-closed контуры.
- `05_source_evidence_manifest.md` — манифест исходных project docs и evidence surfaces, по которым делалась приёмка.
- `06_phase_status_matrix.csv` и `07_phase_status_matrix.json` — машинно-читаемая сводка.

## Как трактуется скоуп
Упоминание `F1` относится к линии stack-conformance remediation. Поэтому в этой приёмке разобраны именно remediation phases:
`G0`, `G1`, `D1`, `D2`, `D3`, `R1`, `R2`, `E1`, `S1`.
Сама фаза `F1` здесь не принимается; вместо этого дан отдельный closure-spec на её честное закрытие.

## Краткий итог
Приняты: `D1`, `D2`, `D3`, `R1`  
Принята с оговорками: `E1`  
Не приняты в текущем состоянии дерева: `G0`, `G1`, `R2`, `S1`

Ключевой вывод: remediation действительно продвинула фундамент — реальные Delta/Spark/Dagster/FastAPI/Postgres/.NET surfaces теперь есть. Но governance/claim system всё ещё позволяет документам объявлять closure там, где truth-source, registry, stack spec и ADR set друг с другом не согласованы. Пока это не исправлено, F1 нельзя честно закрыть.
