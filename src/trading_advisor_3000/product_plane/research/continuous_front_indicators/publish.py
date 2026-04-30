from __future__ import annotations


BLOCKER_CHECK_GROUPS = {
    "adapter_authorization",
    "anti_bypass",
    "formula_sample",
    "input_projection",
    "lineage",
    "pandas_ta_parity",
    "prefix_invariance",
    "rule_coverage",
    "schema",
}


def publish_status_from_qc(qc_rows: list[dict[str, object]]) -> str:
    for row in qc_rows:
        if str(row.get("severity")) == "blocker" and str(row.get("status")) != "pass":
            return "quarantined"
    return "accepted"


def fail_count(qc_rows: list[dict[str, object]], check_group: str) -> int:
    return sum(
        1
        for row in qc_rows
        if str(row.get("check_group")) == check_group and str(row.get("severity")) == "blocker" and str(row.get("status")) != "pass"
    )
