---
name: dependency-and-license-audit
description: Audit dependencies for vulnerability and license policy compliance.
classification: KEEP_CORE
wave: WAVE_2
status: ACTIVE
owner_surface: CTX-OPS
scope: dependency risk and license governance
routing_triggers:
  - "dependency audit"
  - "license audit"
  - "supply chain"
  - "vulnerability"
  - "dependency upgrade"
  - "security advisory"
---

# Dependency And License Audit

## Purpose
Audit dependencies for vulnerability and license policy compliance.

## Trigger Patterns
- "new package was added"
- "security advisory is open"
- "license policy check failed"
- "transitive dependency risk"

## Capabilities
- Build dependency inventory by direct and transitive scope with explicit risk ownership.
- Evaluate CVE severity against exploitability in current runtime context.
- Enforce allow/deny license policy and flag legal-review exceptions explicitly.
- Produce remediation choices: pin, patch, replace, isolate, or defer with compensating controls.

## Workflow
1. Capture dependency delta (added, upgraded, removed) and target runtime surface.
2. Run vulnerability and license checks for both direct and transitive graph.
3. Rank findings by exploitability, blast radius, and release impact.
4. Apply least-risk remediation and document any temporary exception with expiration criteria.
5. Re-run checks and include evidence in PR summary before closure.

## Integration
- Pair with `secrets-and-config-hardening` when dependency updates affect credential handling.
- Pair with `ci-bootstrap` when adding automated dependency-policy checks into lanes.
- Pair with `verification-before-completion` to block "done" until audit evidence is explicit.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_no_tracked_secrets.py`
- `python scripts/validate_skills.py --strict`

## Boundaries

This skill should NOT:
- approve risky packages only because CVE score is low without context analysis.
- hide unresolved legal/security exceptions inside generic "tech debt" notes.
