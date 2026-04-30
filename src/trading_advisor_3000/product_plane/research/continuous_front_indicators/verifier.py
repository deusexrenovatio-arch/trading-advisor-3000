from __future__ import annotations

from .qc import qc_observation
from .rules import IndicatorRollRule


def verify_rule_coverage(
    *,
    run_id: str,
    output_columns: set[str],
    rules: tuple[IndicatorRollRule, ...],
    output_family: str,
) -> dict[str, object]:
    covered = {rule.output_column for rule in rules if rule.output_family == output_family}
    missing = sorted(output_columns - covered)
    duplicates = sorted(
        column
        for column in covered
        if sum(1 for rule in rules if rule.output_family == output_family and rule.output_column == column) > 1
    )
    status = "fail" if missing or duplicates else "pass"
    return qc_observation(
        run_id=run_id,
        check_id=f"{output_family}_rule_coverage",
        check_group="rule_coverage",
        severity="blocker",
        status=status,
        entity_key=output_family,
        observed_value={"missing": missing, "duplicates": duplicates},
        expected_value="exactly_one_rule_per_output_column",
        sample_rows={"missing": missing, "duplicates": duplicates},
    )


def verify_input_projection_identity(*, run_id: str, rows: list[dict[str, object]]) -> dict[str, object]:
    failures: list[dict[str, object]] = []
    for row in rows:
        offset = float(row["cumulative_additive_offset"])
        checks = {
            "open0": float(row["native_open"]) - offset,
            "high0": float(row["native_high"]) - offset,
            "low0": float(row["native_low"]) - offset,
            "close0": float(row["native_close"]) - offset,
        }
        for column, expected in checks.items():
            observed = float(row[column])
            if abs(observed - expected) > 1e-9:
                failures.append(
                    {
                        "ts": row["ts"],
                        "column": column,
                        "observed": observed,
                        "expected": expected,
                    }
                )
    return qc_observation(
        run_id=run_id,
        check_id="cf_input_projection_identity",
        check_group="input_projection",
        severity="blocker",
        status="fail" if failures else "pass",
        entity_key="cf_indicator_input_frame",
        observed_value=len(failures),
        expected_value=0,
        sample_rows=failures[:20],
    )
