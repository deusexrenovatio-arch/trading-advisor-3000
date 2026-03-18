# Skills Roadmap

## Purpose
Track deferred and optional skill classes outside runtime catalog.

Runtime source of truth is local `.cursor/skills/*`.
`docs/agent/skills-catalog.md` is generated mirror and must not contain deferred items.

## Class Policy

| Class | Runtime in baseline shell | Rollout gate |
| --- | --- | --- |
| `KEEP_CORE` | yes | baseline |
| `KEEP_OPTIONAL` | no | explicit phase approval |
| `DEFER_STACK` | no | stack adoption proof |
| `EXCLUDE_DOMAIN_INITIAL` | no | explicit non-baseline decision |

## Deferred Buckets

### KEEP_OPTIONAL
- Add only when the owning surface exists and has acceptance tests.
- Must include routing policy update and validator coverage.

### DEFER_STACK
- Add only when corresponding stack is present in source tree and CI lanes.
- Must include stack-specific contract and rollback plan.

### EXCLUDE_DOMAIN_INITIAL
- Excluded from baseline shell by policy.
- Reconsider only under explicit business/domain scope decision.

## Evidence Required For Any Deferred Promotion
1. Source tree shows real target surface.
2. Routing policy updated for new trigger class.
3. Validator and tests added for lifecycle control.
4. Generated catalog synced from local runtime skills.
