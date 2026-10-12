# Canonical session delivery

Change surface: product-plane. This staged delivery reconciles the accepted
canonical-session implementation with current main through an isolated patch.
Delta owns pinned input tables, Spark owns canonical transforms and joins, and
Dagster owns the existing route.

Scope: frozen session inputs, explicit closed dates, separation of session data
from monetary-input acceptance, and retained-history weekly session coverage.
The first four canonical-session commits apply cleanly to main; subsequent
economics, clock, OI and research changes remain separate delivery groups.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: silent calendar fabrication; synthetic upstream closure; treating price acceptance as monetary acceptance
- Closure Evidence: focused sessions/closed dates/weekly coverage native regressions and local gates; not full target closure of canonical dataset/downstream research or runtime-ready surface
- Shortcut Waiver: none

## Self-review

Contract changes: frozen verified session inputs and explicit closed dates
determine canonical calendar coverage; price acceptance and monetary acceptance
remain distinct. Forbidden old behavior: requiring an open interval for a
verified closed date or inventing calendar coverage from bars alone. Existing
canonical unit and Spark regressions cover these cases and weekly coverage.
Residual risk: this isolated group does not deliver the remaining canonical
overlays, publish Data/research snapshots, or switch the production data root.
