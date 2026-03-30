from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from phase_tz_compiler import compile_phase_plan  # noqa: E402


def test_phase_tz_compiler_extracts_phase_order_objective_and_gates(tmp_path: Path) -> None:
    source = tmp_path / "F1_TZ.md"
    source.write_text(
        """# F1

### Phase F1-A - Truth Source
**Objective:** Align truth sources honestly.

**Acceptance gate**
- Truth sources agree.
- Validator fails closed.

**Disprover**
- Reinsert false claim and confirm validation fails.

### Phase F1-B - Runtime Closure
**Objective:** Close runtime contour with real evidence.

**Acceptance gate**
- Runtime contour reaches terminal state.

**Disprover**
- Remove one runtime artifact and confirm the phase fails.
""",
        encoding="utf-8",
    )

    compiled = compile_phase_plan(source)

    assert [item.phase_id for item in compiled.phases] == ["F1-A", "F1-B"]
    assert compiled.phases[0].objective == "Align truth sources honestly."
    assert compiled.phases[0].acceptance_gate == ("Truth sources agree.", "Validator fails closed.")
    assert compiled.phases[1].disprover == ("Remove one runtime artifact and confirm the phase fails.",)
