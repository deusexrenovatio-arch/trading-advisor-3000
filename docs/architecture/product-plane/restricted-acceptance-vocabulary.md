# Restricted Acceptance Vocabulary

## Purpose
Keep acceptance language aligned with currently proven evidence and prevent closure overclaiming.

## Allowed Terms
Use these terms for product-plane documentation:

| Term | Use when | Required qualifier |
| --- | --- | --- |
| `implemented` | Code/tests/docs exist for a working slice. | Clarify slice scope if not full system. |
| `baseline accepted` | A bounded MVP/scaffold slice is evidenced. | State that this is not full target closure. |
| `partial` | Capability exists with explicit gaps. | Name the remaining gaps. |
| `planned` | Capability is specified but not landed. | Point to phase/backlog/spec reference. |
| `not accepted` | Closure claim is intentionally blocked. | Reference current truth source. |

## Restricted Terms
Do not use the terms below unless a dedicated acceptance phase explicitly proves them against the current contract:
- `full DoD`
- `full acceptance`
- `final closure`
- `production ready`
- `live ready`
- `release ready`

## Mandatory Phrase For Historical Checklists
When older checklist sections contain strong closure language, normalize to:

`Baseline evidence snapshot (historical); full closure is not accepted in current truth source.`

## Source-Of-Truth Reminder
- [STATUS.md](docs/architecture/product-plane/STATUS.md) governs current product-plane reality.
- Historical phase notes and checklists are supportive evidence only.
