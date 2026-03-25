# Data Integration Closure Passport

## Purpose
- Define what counts as real closure from data integration to a downstream research or runtime-ready surface.

## Target
- Real upstream output reaches a canonical dataset and a downstream research or runtime-ready surface.
- Evidence is executable, not just descriptive.

## Staged
- The patch closes a bounded sub-step but explicitly says the contour is still partial.
- The target shape stays intact and the missing downstream handoff is named.

## Forbidden Green Paths
- fixture path or sample artifact presented as the upstream boundary;
- scaffold-only downstream handoff presented as full closure;
- synthetic upstream data accepted as real contour evidence.

## Required Evidence
- an integration test covering the upstream handoff;
- evidence of canonical dataset output;
- evidence of downstream research or runtime-ready consumption.

## Re-Acceptance Triggers
- upstream boundary changes;
- canonical contract or schema changes;
- downstream handoff changes.
