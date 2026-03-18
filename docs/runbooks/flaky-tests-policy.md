# Flaky Tests Policy

## Scope
Policy for unstable tests across process, architecture, and application suites.

## Rules
1. No silent ignore of flaky behavior.
2. Quarantine requires explicit metadata and owner.
3. Retries must be bounded and visible.
4. Quarantine has expiry date and exit criteria.

## Required Quarantine Metadata
- owner
- first_seen
- expiry_date
- tracking_issue
- exit_criteria

## CI Behavior
- Exceeding retry budget is a blocking failure.
- Expired quarantine entries are blocking failures.
- Quarantined tests remain visible in reports until resolved.

## Validation
- `python scripts/validate_flaky_policy.py` (when configured)
