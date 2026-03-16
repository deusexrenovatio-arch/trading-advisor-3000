# Glossary

- **AI delivery shell**: process/governance control plane for implementation work.
- **Hot context**: minimal files required for day-to-day execution.
- **Pointer-shim handoff**: compact `docs/session_handoff.md` that points to active task note.
- **Loop gate**: fast local validation lane (`run_loop_gate.py`).
- **PR gate**: closeout lane that includes loop gate plus additional checks.
- **Nightly gate**: deep hygiene lane and reporting generation.
- **Canonical state**: item-per-file registry under `plans/items/` and `memory/*/`.
- **Compatibility output**: generated aggregate files (`plans/PLANS.yaml`, `memory/agent_memory.yaml`).
