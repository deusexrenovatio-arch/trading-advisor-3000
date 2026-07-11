from __future__ import annotations

import json
import math
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    delta_table_version,
    has_delta_log,
    read_filtered_delta_table_rows,
)

PUBLISHED_REUSE_MAX_SOURCE_AGE_DAYS = 3
REQUIRED_POSITIVE_FIELDS = (
    "min_step",
    "lot_volume",
    "fx_rate_to_rub",
    "mr1",
    "last_settle_price",
    "step_price_rub",
    "margin_required_estimate",
)


class EconomicsSourceUnavailable(RuntimeError):
    def __init__(
        self,
        *,
        target_session_date: str,
        required_contract_ids: Sequence[str],
        missing_sources: Sequence[str],
    ) -> None:
        self.target_session_date = date.fromisoformat(str(target_session_date)[:10]).isoformat()
        self.required_contract_ids = tuple(
            sorted(
                {
                    str(contract_id).strip()
                    for contract_id in required_contract_ids
                    if str(contract_id).strip()
                }
            )
        )
        self.missing_sources = tuple(
            dict.fromkeys(str(source).strip() for source in missing_sources if str(source).strip())
        )
        source_labels = {
            "indicative_fx": "no indicative FX rows",
            "rms_limits": "no RMS limits rows",
            "rms_staticparams": "no RMS staticparams rows",
        }
        missing_text = ", ".join(
            source_labels.get(source, source) for source in self.missing_sources
        )
        super().__init__(
            "MOEX economics raw refresh produced "
            f"{missing_text} for {self.target_session_date}; "
            "refusing to write partial economics raw tables"
        )


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _try_parse_utc_timestamp(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day, tzinfo=UTC)
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _is_positive_finite_number(value: object) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0


def _published_contract_economics_coverage(
    *,
    canonical_economics_root: Path,
    source_error: EconomicsSourceUnavailable,
) -> dict[str, object]:
    table_path = canonical_economics_root / "canonical_contract_economics.delta"
    required_contract_ids = source_error.required_contract_ids
    target_session_date = date.fromisoformat(source_error.target_session_date)
    base_report: dict[str, object] = {
        "target_session_date": target_session_date.isoformat(),
        "required_contracts": len(required_contract_ids),
        "covered_contracts": 0,
        "missing_contract_ids": list(required_contract_ids),
        "invalid_contracts": {},
        "table_path": table_path.as_posix(),
        "delta_version": None,
        "max_source_age_days": PUBLISHED_REUSE_MAX_SOURCE_AGE_DAYS,
        "economics_session_dates": [],
        "effective_session_dates": [],
    }
    if not required_contract_ids:
        return {**base_report, "status": "BLOCKED", "blocked_reason": "no_required_contracts"}
    if not has_delta_log(table_path):
        return {
            **base_report,
            "status": "BLOCKED",
            "blocked_reason": "canonical_contract_economics_missing",
        }

    rows = read_filtered_delta_table_rows(
        table_path,
        filters=[
            ("contract_id", "in", list(required_contract_ids)),
            ("effective_session_date", "<=", target_session_date.isoformat()),
        ],
        columns=[
            "contract_id",
            "economics_session_date",
            "effective_session_date",
            "effective_from_ts",
            "effective_to_ts",
            *REQUIRED_POSITIVE_FIELDS,
            "model_quality",
        ],
    )
    target_at_utc = datetime(
        target_session_date.year,
        target_session_date.month,
        target_session_date.day,
        tzinfo=UTC,
    )
    active_rows_by_contract: dict[str, list[dict[str, object]]] = {
        contract_id: [] for contract_id in required_contract_ids
    }
    for row in rows:
        contract_id = str(row.get("contract_id") or "").strip()
        if contract_id not in active_rows_by_contract:
            continue
        effective_from = _try_parse_utc_timestamp(row.get("effective_from_ts"))
        effective_to = _try_parse_utc_timestamp(row.get("effective_to_ts"))
        if effective_from is None or effective_from > target_at_utc:
            continue
        if effective_to is not None and effective_to <= target_at_utc:
            continue
        active_rows_by_contract[contract_id].append(row)

    missing_contract_ids: list[str] = []
    invalid_contracts: dict[str, list[str]] = {}
    covered_rows: list[dict[str, object]] = []
    source_age_days: list[int] = []
    for contract_id in required_contract_ids:
        candidates = active_rows_by_contract[contract_id]
        if not candidates:
            missing_contract_ids.append(contract_id)
            continue
        if len(candidates) != 1:
            invalid_contracts[contract_id] = [f"ambiguous_active_intervals={len(candidates)}"]
            continue
        row = candidates[0]
        invalid_fields = [
            field_name
            for field_name in REQUIRED_POSITIVE_FIELDS
            if not _is_positive_finite_number(row.get(field_name))
        ]
        if str(row.get("model_quality") or "").strip().lower() == "unavailable":
            invalid_fields.append("model_quality")
        try:
            economics_session_date = date.fromisoformat(
                str(row.get("economics_session_date") or "")[:10]
            )
        except ValueError:
            invalid_fields.append("economics_session_date")
        else:
            source_age = (target_session_date - economics_session_date).days
            if source_age < 0 or source_age > PUBLISHED_REUSE_MAX_SOURCE_AGE_DAYS:
                invalid_fields.append(f"source_age_days={source_age}")
            else:
                source_age_days.append(source_age)
        if invalid_fields:
            invalid_contracts[contract_id] = sorted(set(invalid_fields))
            continue
        covered_rows.append(row)

    coverage_status = "PASS" if not missing_contract_ids and not invalid_contracts else "BLOCKED"
    return {
        **base_report,
        "status": coverage_status,
        "blocked_reason": "" if coverage_status == "PASS" else "coverage_gap",
        "covered_contracts": len(covered_rows),
        "missing_contract_ids": missing_contract_ids,
        "invalid_contracts": invalid_contracts,
        "delta_version": delta_table_version(table_path),
        "economics_session_dates": sorted(
            {str(row.get("economics_session_date") or "")[:10] for row in covered_rows}
        ),
        "effective_session_dates": sorted(
            {str(row.get("effective_session_date") or "")[:10] for row in covered_rows}
        ),
        "source_age_days_max": max(source_age_days) if source_age_days else None,
    }


def reuse_published_economics_after_source_unavailable(
    *,
    canonical_economics_root: Path,
    evidence_dir: Path,
    run_id: str,
    source_error: EconomicsSourceUnavailable,
) -> dict[str, object]:
    coverage = _published_contract_economics_coverage(
        canonical_economics_root=canonical_economics_root,
        source_error=source_error,
    )
    coverage_passed = coverage["status"] == "PASS"
    report_path = evidence_dir / "contract-economics-report.json"
    report: dict[str, object] = {
        "status": "PASS-NOOP" if coverage_passed else "BLOCKED",
        "mode": "baseline_update_economics_refresh",
        "run_id": run_id,
        "refresh_status": "DEFERRED",
        "skipped_reason": "published_economics_covers_target" if coverage_passed else "",
        "blocked_reason": "" if coverage_passed else "published_economics_coverage_failed",
        "source_unavailable": {
            "error_type": type(source_error).__name__,
            "error_message": str(source_error),
            "target_session_date": source_error.target_session_date,
            "missing_sources": list(source_error.missing_sources),
            "required_contract_ids": list(source_error.required_contract_ids),
        },
        "published_coverage": coverage,
        "row_counts": {
            "canonical_contract_economics": int(coverage["covered_contracts"]),
        },
        "missing_economics_rows": 0
        if coverage_passed
        else (len(coverage["missing_contract_ids"]) + len(coverage["invalid_contracts"])),
        "defaulted_radius_rows": 0,
        "official_margin_dominates_rows": 0,
        "formula_margin_dominates_rows": 0,
        "affected_downstream_partitions": [],
        "report_path": report_path.as_posix(),
        "generated_at_utc": _utc_now_iso(),
    }
    _write_json(report_path, report)
    if not coverage_passed:
        missing = ",".join(str(item) for item in coverage["missing_contract_ids"])
        invalid = ",".join(str(item) for item in coverage["invalid_contracts"])
        details = "; ".join(
            item
            for item in (
                f"missing={missing}" if missing else "",
                f"invalid={invalid}" if invalid else "",
                f"reason={coverage['blocked_reason']}",
            )
            if item
        )
        raise RuntimeError(
            f"{source_error}; published economics coverage failed: {details}"
        ) from source_error
    return report
