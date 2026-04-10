from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from result_quality import result_quality_label


INTAKE_QUALITY_DIMENSIONS = (
    "scope_clarity",
    "ambiguity_resolution",
    "workflow_quality",
    "acceptance_readiness",
)


@dataclass
class IntakeQualityDimension:
    score: int
    summary: str


@dataclass
class IntakeQualitySummary:
    status: str
    overall_score: int | None
    score_label: str
    dimensions: dict[str, IntakeQualityDimension]
    strengths: list[str]
    gaps: list[str]
    scored_by: str
    reason: str = ""


def _clamp_score(value: int) -> int:
    return max(0, min(100, int(value)))


def _normalize_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    return out


def _normalize_score(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"`{field_name}` must be an integer score between 0 and 100")
    try:
        score = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"`{field_name}` must be an integer score between 0 and 100") from exc
    if score < 0 or score > 100:
        raise ValueError(f"`{field_name}` must be between 0 and 100")
    return _clamp_score(score)


def normalize_intake_quality_payload(value: Any) -> IntakeQualitySummary | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("intake payload `intake_quality` must be an object when present")

    dimensions_source = value
    if isinstance(value.get("dimensions"), dict):
        dimensions_source = value["dimensions"]

    dimensions: dict[str, IntakeQualityDimension] = {}
    scores: list[int] = []
    for dimension_name in INTAKE_QUALITY_DIMENSIONS:
        raw_dimension = dimensions_source.get(dimension_name)
        if not isinstance(raw_dimension, dict):
            raise ValueError(
                "intake payload `intake_quality` must include object sections for: "
                + ", ".join(INTAKE_QUALITY_DIMENSIONS)
            )
        score = _normalize_score(raw_dimension.get("score"), field_name=f"intake_quality.{dimension_name}.score")
        summary = str(raw_dimension.get("summary", "")).strip()
        if not summary:
            raise ValueError(f"`intake_quality.{dimension_name}.summary` is required")
        dimensions[dimension_name] = IntakeQualityDimension(score=score, summary=summary)
        scores.append(score)

    overall_score_raw = value.get("overall_score")
    overall_score = (
        _normalize_score(overall_score_raw, field_name="intake_quality.overall_score")
        if overall_score_raw is not None
        else _clamp_score(round(sum(scores) / len(scores)))
    )
    scored_by = str(value.get("scored_by", "")).strip() or "intake-lane-llm"
    status = str(value.get("status", "")).strip() or "scored"
    reason = str(value.get("reason", "")).strip()

    return IntakeQualitySummary(
        status=status,
        overall_score=overall_score,
        score_label=result_quality_label(overall_score),
        dimensions=dimensions,
        strengths=_normalize_string_list(value.get("strengths", [])),
        gaps=_normalize_string_list(value.get("gaps", [])),
        scored_by=scored_by,
        reason=reason,
    )


def unscored_intake_quality_summary(reason: str) -> IntakeQualitySummary:
    return IntakeQualitySummary(
        status="unscored",
        overall_score=None,
        score_label="unscored",
        dimensions={},
        strengths=[],
        gaps=[],
        scored_by="none",
        reason=str(reason or "").strip() or "Intake quality was not scored.",
    )


def intake_quality_to_dict(summary: IntakeQualitySummary | None) -> dict[str, Any] | None:
    if summary is None:
        return None
    return asdict(summary)


def build_intake_gate_quality_summary(
    *,
    technical: IntakeQualitySummary | None,
    product: IntakeQualitySummary | None,
    severity_counts: dict[str, int],
) -> dict[str, Any]:
    if technical is None or product is None:
        return intake_quality_to_dict(
            unscored_intake_quality_summary(
                "One or more intake lanes did not provide intake_quality; gate score remains observational only."
            )
        ) or {}
    if technical.overall_score is None or product.overall_score is None:
        return intake_quality_to_dict(
            unscored_intake_quality_summary("One or more intake lanes produced an unscored intake quality summary.")
        ) or {}

    average_score = round((technical.overall_score + product.overall_score) / 2)
    lane_score_delta = abs(technical.overall_score - product.overall_score)
    blocker_penalty = (
        int(severity_counts.get("P0", 0)) * 20
        + int(severity_counts.get("P1", 0)) * 10
        + int(severity_counts.get("P2", 0)) * 3
        + min(12, lane_score_delta // 3)
    )
    intake_gate_score = _clamp_score(average_score - blocker_penalty)

    gaps: list[str] = []
    if severity_counts.get("P0", 0):
        gaps.append("Critical intake blockers remain unresolved.")
    if severity_counts.get("P1", 0):
        gaps.append("High-priority intake blockers still reduce readiness.")
    if lane_score_delta >= 15:
        gaps.append("Technical and product lanes disagree materially on readiness and need reconciliation.")

    strengths: list[str] = []
    if technical.overall_score >= 80 and product.overall_score >= 80:
        strengths.append("Both intake lanes see the package as structurally ready for governed materialization.")
    if lane_score_delta <= 7:
        strengths.append("Technical and product lanes are well aligned on intake quality.")

    return {
        "status": "scored",
        "intake_gate_score": intake_gate_score,
        "score_label": result_quality_label(intake_gate_score),
        "technical_intake_score": technical.overall_score,
        "product_intake_score": product.overall_score,
        "lane_score_delta": lane_score_delta,
        "blocker_penalty": blocker_penalty,
        "scored_by": "python-intake-gate",
        "strengths": strengths,
        "gaps": gaps,
    }
