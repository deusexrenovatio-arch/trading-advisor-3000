# Phase 9 Gap Analysis

## Summary

The current repo already closes scaffold, contracts, replay baseline, controlled transport,
and operational proving. The remaining gap is mostly about real external-system closure:
named data routes, real Telegram operating contour, Postgres-by-default battle runs,
and optional real broker canary evidence.

## Gap table

| Integration surface | Current repo reality | Needed for Phase 9A | Needed for Phase 9B | Priority |
| --- | --- | --- | --- | --- |
| `MOEX` historical data | seams only | freeze `MOEX` as historical source, document bootstrap and dataset/version evidence | same | critical |
| `QUIK` live feed | route semantics exist, live-feed closure not frozen | freeze `QUIK` as primary live feed, document freshness and session checks | keep live-feed and execution roles separate | critical |
| `Telegram` | lifecycle contracts and publisher logic exist | real shadow/advisory destinations, restart-safe lifecycle evidence | same | critical |
| `PostgreSQL` | store and migrations exist | battle-run mode must default to Postgres | same | high |
| `HTTP sidecar gateway` | real contract + staging stub exist | no extra closure required for 9A beyond keeping contract visible | readiness path for real sidecar proving | medium for 9A / critical for 9B |
| `StockSharp` sidecar | placeholder only | no direct acceptance claim in 9A | actual process or pinned external package | critical for 9B |
| `Finam` route | contract only | out of scope for 9A acceptance | canary evidence and reconciliation closure | critical for 9B |
| `Prometheus / Loki / Grafana` | observability stack exists | battle-run evidence snapshots | canary metrics/log snapshots | high |
| `MCP` | support bundle exists | battle-run inspection notes only | same | medium |

## Key engineering gaps

1. Historical and live data sources are not yet frozen as concrete named integrations in the docs.
2. `QUIK` needs an explicit split:
   - `Phase 9A`: primary live market feed.
   - `Phase 9B`: execution route hop behind `StockSharp`.
3. Battle-run mode still needs a documentation contract that says `PostgreSQL` is mandatory.
4. Telegram acceptance currently proves runtime behavior, but not yet the real external operating contour.
5. Real broker closure cannot be claimed until `StockSharp` delivery is real and `Finam` canary evidence exists.

## Recommended closure order

### Tier A
1. Freeze `MOEX` historical source and `QUIK` live feed.
2. Freeze pilot universe and session/roll rules.
3. Close one production strategy slice on real data.
4. Close `Telegram + PostgreSQL` battle-run contour.
5. Collect observability-backed evidence package.

### Tier B
1. Freeze actual `StockSharp` delivery mode.
2. Reuse current HTTP gateway contract for readiness proving.
3. Run Stage 1 connectivity.
4. Run Stage 2 canary.
5. Run Stage 3 controlled batch only if owner enables it.

## Explicit no-go claims

Phase 9 docs must not claim any of the following:

- `QUIK` live feed closure proves broker execution closure.
- `StockSharp -> QUIK -> Finam` is ready because only the wire contract exists.
- `Phase 9A` requires real broker canary.
- battle runs are acceptable without `PostgreSQL`.
- `MCP` is part of the product runtime path.
