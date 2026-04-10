from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


RESULT_QUALITY_DIMENSIONS = (
    "requirements_alignment",
    "documentation_quality",
    "implementation_quality",
    "testing_quality",
)
RESULT_QUALITY_LABEL_THRESHOLDS = (
    (90, "excellent"),
    (80, "strong"),
    (65, "watch"),
    (50, "fragile"),
    (0, "critical"),
)


@dataclass
class ResultQualityDimension:
    score: int
    summary: str


@dataclass
class ResultQualitySummary:
    status: str
    overall_score: int | None
    score_label: str
    dimensions: dict[str, ResultQualityDimension]
    strengths: list[str]
    gaps: list[str]
    scored_by: str
    reason: str = ""


def _clamp_score(value: int) -> int:
    return max(0, min(100, int(value)))


def result_quality_label(score: int | None) -> str:
    if score is None:
        return "unscored"
    for threshold, label in RESULT_QUALITY_LABEL_THRESHOLDS:
        if score >= threshold:
            return label
    return "critical"


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


def normalize_result_quality_payload(value: Any) -> ResultQualitySummary | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("acceptance payload `result_quality` must be an object when present")

    dimensions_source = value
    if isinstance(value.get("dimensions"), dict):
        dimensions_source = value["dimensions"]

    dimensions: dict[str, ResultQualityDimension] = {}
    scores: list[int] = []
    for dimension_name in RESULT_QUALITY_DIMENSIONS:
        raw_dimension = dimensions_source.get(dimension_name)
        if not isinstance(raw_dimension, dict):
            raise ValueError(
                "acceptance payload `result_quality` must include object sections for: "
                + ", ".join(RESULT_QUALITY_DIMENSIONS)
            )
        score = _normalize_score(raw_dimension.get("score"), field_name=f"result_quality.{dimension_name}.score")
        summary = str(raw_dimension.get("summary", "")).strip()
        if not summary:
            raise ValueError(f"`result_quality.{dimension_name}.summary` is required")
        dimensions[dimension_name] = ResultQualityDimension(score=score, summary=summary)
        scores.append(score)

    overall_score_raw = value.get("overall_score")
    overall_score = (
        _normalize_score(overall_score_raw, field_name="result_quality.overall_score")
        if overall_score_raw is not None
        else _clamp_score(round(sum(scores) / len(scores)))
    )
    status = str(value.get("status", "")).strip() or "scored"
    scored_by = str(value.get("scored_by", "")).strip() or "acceptor-llm"
    reason = str(value.get("reason", "")).strip()

    return ResultQualitySummary(
        status=status,
        overall_score=overall_score,
        score_label=result_quality_label(overall_score),
        dimensions=dimensions,
        strengths=_normalize_string_list(value.get("strengths", [])),
        gaps=_normalize_string_list(value.get("gaps", [])),
        scored_by=scored_by,
        reason=reason,
    )


def unscored_result_quality_summary(reason: str) -> ResultQualitySummary:
    return ResultQualitySummary(
        status="unscored",
        overall_score=None,
        score_label="unscored",
        dimensions={},
        strengths=[],
        gaps=[],
        scored_by="none",
        reason=str(reason or "").strip() or "Result quality was not scored.",
    )


def result_quality_to_dict(summary: ResultQualitySummary | None) -> dict[str, Any] | None:
    if summary is None:
        return None
    return asdict(summary)
