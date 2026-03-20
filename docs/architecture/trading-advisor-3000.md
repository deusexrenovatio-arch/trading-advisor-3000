# Trading Advisor 3000 Architecture Narrative

## Purpose
Trading Advisor 3000 uses an AI delivery shell as the control plane for how work is requested, executed, validated, and closed.

The shell governs process quality and repository state.
It does not implement trading business behavior inside shell surfaces.

## System split
1. Control Plane:
   governance policy, lifecycle scripts, validators, gates, plans, memory, and reporting.
2. Application Plane:
   isolated product/application plane under app paths, with code, contracts, tests, and app-specific docs.

## Design decisions
1. PR-only mainline with explicit emergency override variables.
2. Pointer-shim session handoff with durable task notes.
3. Context routing with bounded cards and high-risk handling.
4. Surface-aware gates:
   loop for fast feedback, PR for closeout confidence, nightly for hygiene and telemetry.
5. Item-per-file canonical state for plans and memory, with compatibility outputs for tooling.

## Boundaries
1. No imported exchange logic, strategy code, signal generation, or market business rules.
2. No legacy gate aliases.
3. No cold-context bulk loading by default.

## Operational objective
Enable repeatable Codex-first delivery where every task leaves measurable traces in:
1. task artifacts,
2. plans and memory ledgers,
3. gate evidence,
4. governance reports.

