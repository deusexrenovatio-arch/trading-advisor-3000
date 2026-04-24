from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log


DEFAULT_MOEX_BASELINE_ID = "baseline-4y-current"
DEFAULT_LOCAL_T3_DATA_ROOT = Path("D:/TA3000-data/trading-advisor-3000-nightly")


@dataclass(frozen=True)
class CanonicalStorageBinding:
    baseline_id: str
    storage_mode: str
    data_root: Path
    canonical_root: Path
    canonical_bars_path: Path
    canonical_bar_provenance_path: Path | None
    derived_root: Path | None
    features_root: Path | None
    indicators_root: Path | None
    manifest_path: Path | None
    source: str

    def to_dict(self) -> dict[str, object]:
        return {
            "baseline_id": self.baseline_id,
            "storage_mode": self.storage_mode,
            "data_root": self.data_root.as_posix(),
            "canonical_root": self.canonical_root.as_posix(),
            "canonical_bars_path": self.canonical_bars_path.as_posix(),
            "canonical_bar_provenance_path": (
                self.canonical_bar_provenance_path.as_posix()
                if self.canonical_bar_provenance_path is not None
                else None
            ),
            "derived_root": self.derived_root.as_posix() if self.derived_root is not None else None,
            "features_root": self.features_root.as_posix() if self.features_root is not None else None,
            "indicators_root": self.indicators_root.as_posix() if self.indicators_root is not None else None,
            "manifest_path": self.manifest_path.as_posix() if self.manifest_path is not None else None,
            "source": self.source,
        }


def _path_from_text(value: object) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    return Path(text).expanduser().resolve()


def _manifest_candidate_from_data_root(data_root: Path, baseline_id: str) -> Path:
    return data_root / "canonical" / "moex" / baseline_id / "baseline-manifest.json"


def _binding_from_direct_path(path: Path, *, source: str) -> CanonicalStorageBinding:
    canonical_bars_path = path.resolve()
    if not has_delta_log(canonical_bars_path):
        raise RuntimeError(f"T3 canonical bars delta is missing `_delta_log`: {canonical_bars_path.as_posix()}")
    canonical_root = canonical_bars_path.parent
    data_root = canonical_root.parents[2] if len(canonical_root.parents) >= 3 else canonical_root
    provenance = canonical_root / "canonical_bar_provenance.delta"
    derived_root = data_root / "derived" / "moex"
    return CanonicalStorageBinding(
        baseline_id=canonical_root.name or "direct-canonical-bars",
        storage_mode="direct-delta",
        data_root=data_root,
        canonical_root=canonical_root,
        canonical_bars_path=canonical_bars_path,
        canonical_bar_provenance_path=provenance if has_delta_log(provenance) else None,
        derived_root=derived_root if derived_root.exists() else None,
        features_root=(derived_root / "features") if (derived_root / "features").exists() else None,
        indicators_root=(derived_root / "indicators") if (derived_root / "indicators").exists() else None,
        manifest_path=None,
        source=source,
    )


def load_canonical_storage_binding(manifest_path: Path) -> CanonicalStorageBinding:
    resolved_manifest = manifest_path.resolve()
    payload = json.loads(resolved_manifest.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"T3 baseline manifest must be a JSON object: {resolved_manifest.as_posix()}")

    baseline_paths = payload.get("baseline_paths")
    storage_layout = payload.get("storage_layout")
    if not isinstance(baseline_paths, dict):
        raise RuntimeError(f"T3 baseline manifest is missing `baseline_paths`: {resolved_manifest.as_posix()}")
    if not isinstance(storage_layout, dict):
        storage_layout = {}

    canonical_bars_path = _path_from_text(baseline_paths.get("canonical_bars"))
    if canonical_bars_path is None:
        raise RuntimeError(f"T3 baseline manifest is missing `baseline_paths.canonical_bars`: {resolved_manifest.as_posix()}")
    if not has_delta_log(canonical_bars_path):
        raise RuntimeError(f"T3 canonical bars delta is missing `_delta_log`: {canonical_bars_path.as_posix()}")

    canonical_root = _path_from_text(storage_layout.get("canonical_root")) or canonical_bars_path.parent
    data_root = _path_from_text(payload.get("data_root")) or canonical_root.parents[2]
    provenance = _path_from_text(baseline_paths.get("canonical_bar_provenance"))
    derived_root = _path_from_text(storage_layout.get("derived_root"))
    features_root = _path_from_text(storage_layout.get("features_root"))
    indicators_root = _path_from_text(storage_layout.get("indicators_root"))

    return CanonicalStorageBinding(
        baseline_id=str(payload.get("baseline_id") or canonical_root.name or DEFAULT_MOEX_BASELINE_ID),
        storage_mode=str(payload.get("storage_mode") or "materialized-baseline"),
        data_root=data_root,
        canonical_root=canonical_root,
        canonical_bars_path=canonical_bars_path,
        canonical_bar_provenance_path=provenance if provenance is not None and has_delta_log(provenance) else provenance,
        derived_root=derived_root,
        features_root=features_root,
        indicators_root=indicators_root,
        manifest_path=resolved_manifest,
        source="baseline-manifest",
    )


def resolve_moex_t3_storage(
    *,
    canonical_bars_path: Path | None = None,
    baseline_manifest_path: Path | None = None,
    canonical_root: Path | None = None,
    data_root: Path | None = None,
    baseline_id: str = DEFAULT_MOEX_BASELINE_ID,
) -> CanonicalStorageBinding:
    searched: list[str] = []

    if canonical_bars_path is not None:
        searched.append(canonical_bars_path.resolve().as_posix())
        return _binding_from_direct_path(canonical_bars_path, source="explicit-canonical-bars")

    manifest_candidates: list[tuple[str, Path]] = []
    if baseline_manifest_path is not None:
        manifest_candidates.append(("explicit-baseline-manifest", baseline_manifest_path.resolve()))

    env_manifest = _path_from_text(os.environ.get("TA3000_MOEX_BASELINE_MANIFEST"))
    if env_manifest is not None:
        manifest_candidates.append(("env:TA3000_MOEX_BASELINE_MANIFEST", env_manifest))

    if canonical_root is not None:
        manifest_candidates.append(("explicit-canonical-root", canonical_root.resolve() / "baseline-manifest.json"))

    env_data_root = _path_from_text(os.environ.get("TA3000_DATA_ROOT"))
    if env_data_root is not None:
        manifest_candidates.append(("env:TA3000_DATA_ROOT", _manifest_candidate_from_data_root(env_data_root, baseline_id)))

    if data_root is not None:
        manifest_candidates.append(("explicit-data-root", _manifest_candidate_from_data_root(data_root.resolve(), baseline_id)))

    manifest_candidates.append(
        (
            "default-local-t3-data-root",
            _manifest_candidate_from_data_root(DEFAULT_LOCAL_T3_DATA_ROOT, baseline_id),
        )
    )

    for source, candidate in manifest_candidates:
        searched.append(candidate.as_posix())
        if candidate.exists():
            binding = load_canonical_storage_binding(candidate)
            return CanonicalStorageBinding(
                baseline_id=binding.baseline_id,
                storage_mode=binding.storage_mode,
                data_root=binding.data_root,
                canonical_root=binding.canonical_root,
                canonical_bars_path=binding.canonical_bars_path,
                canonical_bar_provenance_path=binding.canonical_bar_provenance_path,
                derived_root=binding.derived_root,
                features_root=binding.features_root,
                indicators_root=binding.indicators_root,
                manifest_path=binding.manifest_path,
                source=source,
            )

    if canonical_root is not None:
        direct_path = canonical_root.resolve() / "canonical_bars.delta"
        searched.append(direct_path.as_posix())
        if has_delta_log(direct_path):
            return _binding_from_direct_path(direct_path, source="explicit-canonical-root")

    searched_text = "; ".join(searched)
    raise RuntimeError(f"cannot resolve MOEX T3 canonical storage; searched: {searched_text}")
