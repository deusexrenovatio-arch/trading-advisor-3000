from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from collect_project_map_candidates import candidates_from_hits, parse_mempalace_search, render_report  # noqa: E402


MEMORY_OUTPUT = """
============================================================
  Results for: "trading advisor blocked unresolved proof gap"
============================================================

  [1] codex_chats / problem
      Source: rollout-2026-04-01T00-19-13-demo.jsonl
      Match:  0.516

      BLOCKED. Governed route evidence is present, but the required staging-real proof is still missing.

  ----------------------------------------------------------------
  [2] codex_chats / decision
      Source: rollout-2026-04-01T00-19-13-demo.jsonl
      Match:  0.516

      BLOCKED. Governed route evidence is present, but the required staging-real proof is still missing.

  ----------------------------------------------------------------
  [3] codex_chats / problem
      Source: rollout-2026-04-02T00-00-00-demo.jsonl
      Match:  0.431

      Unknown runtime state needs verification before claiming closure.
"""


def test_parse_mempalace_search_extracts_hits() -> None:
    hits = parse_mempalace_search(MEMORY_OUTPUT, query="demo query")

    assert len(hits) == 3
    assert hits[0].wing == "codex_chats"
    assert hits[0].room == "problem"
    assert hits[0].source == "rollout-2026-04-01T00-19-13-demo.jsonl"
    assert "staging-real proof" in hits[0].excerpt


def test_candidates_are_deduplicated_and_classified() -> None:
    hits = parse_mempalace_search(MEMORY_OUTPUT, query="demo query")

    candidates = candidates_from_hits(hits)

    assert len(candidates) == 2
    assert candidates[0].item_type == "problem"
    assert candidates[0].suggested_priority == "p1"
    assert candidates[0].suggested_node == "delivery-gates"
    assert candidates[1].item_type == "risk"
    assert candidates[1].suggested_priority == "p2"
    assert candidates[1].suggested_node == "runtime-plane"


def test_render_report_marks_candidates_as_non_authoritative() -> None:
    candidates = candidates_from_hits(parse_mempalace_search(MEMORY_OUTPUT, query="demo query"))

    report = render_report(candidates)

    assert "This is an inbox, not current project truth" in report
    assert "MemPalace is advisory recall" in report
    assert "C001:" in report
    assert "Suggested priority: `p1`" in report


def test_candidates_drop_session_and_context_noise() -> None:
    hits = parse_mempalace_search(
        """
  [1] codex_chats / problem
      Source: rollout-session.jsonl
      Match:  0.600

      {"timestamp":"2026-04-09T09:51:31.348Z","type":"session_meta","payload":{"id":"demo"}}

  [2] codex_chats / problem
      Source: rollout-ide.jsonl
      Match:  0.600

      > # Context from my IDE setup:

  [3] codex_chats / problem
      Source: rollout-real.jsonl
      Match:  0.600

      Real acceptance blocker remains unresolved.
""",
        query="demo query",
    )

    candidates = candidates_from_hits(hits)

    assert len(candidates) == 1
    assert candidates[0].source == "rollout-real.jsonl"
