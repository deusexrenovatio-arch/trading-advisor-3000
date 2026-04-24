# MOEX Nightly v2 Architecture

## Goal
Describe the target nightly architecture for incremental historical refresh without full multi-year rebuilds.

## Target Shape
- Python owns source-aware raw delta refresh from the current governed baseline watermark.
- Spark recomputes canonical outputs only for the affected scope.
- Dagster owns orchestration, scheduling, and route visibility.
- Live broker execution remains a separate boundary and is not used as the authoritative source for historical nightly refresh.

## Migration Direction
1. Stop treating nightly refresh as a full-history rebuild candidate flow.
2. Promote bounded raw delta refresh as the default historical ingest mode.
3. Keep canonical recompute scoped to changed windows and governed acceptance evidence.
4. Preserve a clear separation between historical data refresh and live execution transport.

## Constraints
- Do not rewrite accepted historical evidence.
- Do not claim runtime or broker closures that are still partial.
- Keep the pinned baseline authoritative until the replacement route is accepted.
