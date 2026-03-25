# Runtime Publication Closure Passport

## Purpose
- Define what counts as real closure from runtime output to durable publication.

## Target
- Runtime output reaches the durable store or publication contour through the intended path.
- Evidence shows the end-to-end publication path, not only smoke or manifest artifacts.

## Staged
- The patch preserves the target publication shape but explicitly says the contour is still partial.
- The missing durable or publication step is named and bounded.

## Forbidden Green Paths
- synthetic publication path or sample artifact accepted as full closure;
- smoke-only evidence accepted as publication proof;
- scaffold-only publication flow presented as durable closure.

## Required Evidence
- evidence of runtime output generation;
- evidence of the durable store or publication contour;
- end-to-end publication evidence.

## Re-Acceptance Triggers
- runtime output shape changes;
- durable storage changes;
- publication path changes.
