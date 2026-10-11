# Moscow clock and aggregate-date delivery

Change surface: product-plane. Solution class: staged. Spark owns canonical
aggregation, admission and research joins; Delta owns durable output and
readback. Python computes affected-key metadata in the existing canonical repair helper.

Intraday canonical bars now use the Moscow civil clock: quarter hours at
:00/:15/:30/:45, H1 at HH:00 and H4 at 00/04/08/12/16/20 Moscow. For a source
minute at 2026-04-02T10:01Z, the H4 key becomes 09:00Z (12:00 Moscow), replacing
the session-relative 10:00Z key. Python affected keys use the same grid.

Valid partial session-edge buckets remain canonical. A source spanning outside
its nominal target bucket fails closed. Adjacent intervals admit each source
once; provenance keeps the final contributing interval. Research marks partial
or cross-session nominal buckets as boundary_risk. Interior bars remain regular.

D1/W1 research session_date uses the Moscow date of provenance start in both
native and PIT active-front contours. After a closed Monday, W1 session_date
can be Tuesday while its weekly bucket label remains Monday. Aggregate bucket
labels, prices and missing monetary-input semantics remain unchanged.

## Delivery boundaries

- Retain pinned official session evidence, explicit closed dates and historical economics.
- Retain the existing CF 09:00-23:50 Moscow policy. H4 08:00 Moscow can exist in canonical/native while excluded from CF.
- New intraday labels change canonical/invalidation keys. Do not append to or cut over into old-clock primary; promotion requires the accepted rebuilt staging root/snapshot and downstream verification.
- UTC bar-close year remains separate from Moscow business dates. The physical fixture verifies UTC key/provenance/year invariants; yearly writer defaults and partition readiness belong to a separate delivery group.
- No data-root switch, production mutation, strategy or money-methodology change is included.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: old-grid append; silent session-relative keys; discarded valid edge bars; double source contribution; synthetic upstream closure
- Closure Evidence: focused native Spark/Delta clock/date/key readback regressions and local gates; not full target closure of canonical dataset/downstream research or runtime-ready surface
- Shortcut Waiver: none

## Self-review

Moved contract: intraday canonical key identity plus aggregate exchange-date
and boundary-risk attribution. Old session-relative keys, UTC aggregate-date
joins and duplicate adjacent-session contributions are forbidden. Unit and
native regressions cover grid/parity, source spans, edge admission, both
research contours and W1 provenance. Physical Delta readback verifies keys and
UTC close-year behavior across Moscow New Year. Residual risk: fixture proof
does not establish full accepted-data coverage or production promotion. Direct
D1/W1 affected-key span parity in the repair helper is a pre-existing separate
issue. Its public selector uses raw 1m; the current Dagster historical route uses
its own Spark/interval publish scope. Direct D1/W1 helper metadata is outside
this intraday change; aggregate bucket labels are retained here.
