"""Completed Research handoff must enforce separate physical budgets before Spark."""

from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.research import native_observations as owner


class ForbiddenSpark:
    def __getattr__(self, name):
        pytest.fail("rejected Research compute scope reached Spark: " + name)


@pytest.mark.parametrize(
    "field,value", [("row_count", 20001), ("file_count", 129), ("physical_bytes", 67108865)]
)
def test_research_compute_rejects_budgets_before_spark(monkeypatch, field, value):
    reference = {"row_count": 2, "file_count": 1}
    if field != "physical_bytes":
        reference[field] = value
    graph = {"context": {"layer": "L0"}, "manifest": {"tables": {"research_bar_views": reference}}}
    reader = object.__new__(owner.NativeResearchSnapshot)
    reader.root = Path("/research-trial")
    reader._completion_json = "{}"
    monkeypatch.setattr(reader, "verify", lambda: graph)
    table = SimpleNamespace(get_add_actions=lambda: pa.table({"size_bytes": [67108865]}))
    monkeypatch.setattr(owner, "open_pinned_table", lambda _: table)
    with pytest.raises(SchemaValidationError, match="compute.*budget"):
        reader.read_spark(ForbiddenSpark())
