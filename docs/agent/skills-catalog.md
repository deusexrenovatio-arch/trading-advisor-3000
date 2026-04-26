# Skills Catalog

<!-- generated-by: scripts/sync_skills_catalog.py -->
<!-- source-of-truth: .codex/skills/*/SKILL.md -->
<!-- generated-contract: do not edit manually; run python scripts/sync_skills_catalog.py -->
<!-- catalog-sha256: 77735f56f00823d263d203c0580b45e8043cec7884e7a1ebae6514f2a2fb3545 -->

| skill_id | classification | wave | status | scope | owner_surface | routing_triggers | source | hot_context_policy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ta3000-data-plane-proof` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 authoritative data-plane proof and materialization evidence | `CTX-DATA` | data-plane proof; D:/TA3000-data; Delta proof; _delta_log; row counts; canonical bars; research materialization; real prod tables | `repo_local` | `cold-by-default` |
| `ta3000-governed-package-intake` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 governed package intake and route integrity | `CTX-ORCHESTRATION` | governed package intake; governed flow; package intake; technical intake; product intake; continue route; intake gate | `repo_local` | `cold-by-default` |
| `ta3000-product-surface-naming-cleanup` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 active product-surface naming cleanup | `CTX-DOMAIN` | product surface naming; active surface; phase labels; debug labels; capability names; naming cleanup | `repo_local` | `cold-by-default` |

## Generation
- Generate: `python scripts/sync_skills_catalog.py`
- Check drift: `python scripts/sync_skills_catalog.py --check`
