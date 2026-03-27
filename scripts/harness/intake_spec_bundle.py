from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import shutil
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from pydantic import ValidationError

try:
    from .models import ArtifactManifestEntryModel, SpecManifestModel
    from .schema_registry import load_schema_catalog, resolve_schema_dir
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import ArtifactManifestEntryModel, SpecManifestModel
    from scripts.harness.schema_registry import load_schema_catalog, resolve_schema_dir


SUPPORTED_TEXT_SUFFIXES = {".md", ".txt", ".rst", ".json", ".yaml", ".yml", ".csv"}
MAX_MEMBER_SIZE_BYTES = 5 * 1024 * 1024


class IntakeError(RuntimeError):
    """Base error for zip intake failures."""


class UnsupportedArtifactError(IntakeError):
    """Raised when a bundle contains a file type outside supported intake surface."""


@dataclass(frozen=True)
class IntakeResult:
    run_id: str
    run_root: Path
    manifest_path: Path
    text_cache_path: Path
    workspace_root: Path
    artifact_count: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _default_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run-{stamp}"


def _assert_supported_zip(path: Path) -> None:
    if not path.exists():
        raise IntakeError(f"input zip not found: {path}")
    if not path.is_file():
        raise IntakeError(f"input path is not a file: {path}")
    if path.suffix.lower() != ".zip":
        raise IntakeError(f"input must be .zip archive: {path}")
    if not zipfile.is_zipfile(path):
        raise IntakeError(f"input archive is corrupted or not a zip: {path}")


def _normalize_member_path(raw_name: str) -> PurePosixPath:
    member = PurePosixPath(raw_name)
    if not member.parts:
        raise IntakeError("zip member path is empty")
    if member.is_absolute():
        raise IntakeError(f"zip contains absolute path: {raw_name}")
    if ".." in member.parts:
        raise IntakeError(f"zip contains unsafe traversal path: {raw_name}")
    if any(part in {"", "."} for part in member.parts):
        raise IntakeError(f"zip contains unsupported relative segment: {raw_name}")
    if ":" in member.parts[0]:
        raise IntakeError(f"zip contains drive-qualified path: {raw_name}")
    return member


def _zip_member_is_symlink(info: zipfile.ZipInfo) -> bool:
    mode = info.external_attr >> 16
    return (mode & 0o170000) == 0o120000


def _artifact_id(index: int, sha256: str) -> str:
    return f"artifact-{index:04d}-{sha256[:12]}"


def _media_type(path: PurePosixPath) -> str:
    guessed, _encoding = mimetypes.guess_type(path.name)
    return guessed or "application/octet-stream"


def _decode_text(raw_bytes: bytes) -> str:
    encodings = ("utf-8-sig", "utf-8", "cp1251")
    for encoding in encodings:
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise IntakeError("text extraction failed: unsupported encoding")


def _write_json(path: Path, payload: dict[str, object] | list[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_bundle_intake(
    *,
    input_zip: Path,
    registry_root: Path,
    run_id: str | None = None,
) -> IntakeResult:
    _assert_supported_zip(input_zip)
    run_id = (run_id or _default_run_id()).strip()
    if not run_id:
        raise IntakeError("run_id must be non-empty")

    # Validate schema catalog early so intake fails before mutating state.
    load_schema_catalog(resolve_schema_dir())

    intake_root = registry_root / "intake"
    run_root = intake_root / run_id
    if run_root.exists():
        raise IntakeError(f"run_id already exists: {run_root}")

    workspace_root = run_root / "workspace"
    text_cache_root = run_root / "text_cache"
    manifest_path = run_root / "spec_manifest.json"
    text_cache_path = run_root / "extracted_text_cache.json"
    seen_member_paths: set[PurePosixPath] = set()

    intake_root.mkdir(parents=True, exist_ok=True)
    run_root.mkdir(parents=True, exist_ok=False)

    try:
        artifacts: list[ArtifactManifestEntryModel] = []
        text_cache_entries: list[dict[str, object]] = []

        with zipfile.ZipFile(input_zip) as archive:
            members = [info for info in archive.infolist() if not info.is_dir()]
            for index, info in enumerate(members, start=1):
                if info.flag_bits & 0x1:
                    raise IntakeError(f"encrypted archive members are not supported: {info.filename}")
                if _zip_member_is_symlink(info):
                    raise IntakeError(f"symlink member is not allowed: {info.filename}")
                if info.file_size > MAX_MEMBER_SIZE_BYTES:
                    raise IntakeError(
                        f"member is too large ({info.file_size} bytes): {info.filename} "
                        f"(limit={MAX_MEMBER_SIZE_BYTES})"
                    )

                relative_member = _normalize_member_path(info.filename)
                if relative_member in seen_member_paths:
                    raise IntakeError(f"duplicate member path in archive: {relative_member.as_posix()}")
                seen_member_paths.add(relative_member)

                suffix = relative_member.suffix.lower()
                if suffix not in SUPPORTED_TEXT_SUFFIXES:
                    raise UnsupportedArtifactError(
                        f"unsupported file type `{suffix or '<none>'}` in archive member: {relative_member.as_posix()}"
                    )

                raw_bytes = archive.read(info)
                sha256 = hashlib.sha256(raw_bytes).hexdigest()
                artifact_id = _artifact_id(index, sha256)

                workspace_target = workspace_root / Path(*relative_member.parts)
                workspace_target.parent.mkdir(parents=True, exist_ok=True)
                workspace_target.write_bytes(raw_bytes)

                extracted_text = _decode_text(raw_bytes)
                text_cache_file = text_cache_root / f"{artifact_id}.txt"
                text_cache_file.parent.mkdir(parents=True, exist_ok=True)
                text_cache_file.write_text(extracted_text, encoding="utf-8")

                artifacts.append(
                    ArtifactManifestEntryModel(
                        artifact_id=artifact_id,
                        path=workspace_target.relative_to(run_root).as_posix(),
                        sha256=sha256,
                        size_bytes=len(raw_bytes),
                        media_type=_media_type(relative_member),
                        extraction_status="extracted",
                        source_kind="zip",
                    )
                )
                text_cache_entries.append(
                    {
                        "artifact_id": artifact_id,
                        "path": workspace_target.relative_to(run_root).as_posix(),
                        "text_path": text_cache_file.relative_to(run_root).as_posix(),
                        "sha256": sha256,
                    }
                )

        manifest = SpecManifestModel(run_id=run_id, generated_at=_utc_now(), artifacts=artifacts)
        _write_json(manifest_path, manifest.model_dump(mode="json"))
        _write_json(
            text_cache_path,
            {
                "run_id": run_id,
                "generated_at": _utc_now(),
                "entries": text_cache_entries,
            },
        )

        return IntakeResult(
            run_id=run_id,
            run_root=run_root,
            manifest_path=manifest_path,
            text_cache_path=text_cache_path,
            workspace_root=workspace_root,
            artifact_count=len(artifacts),
        )
    except (zipfile.BadZipFile, KeyError) as exc:
        shutil.rmtree(run_root, ignore_errors=True)
        raise IntakeError(f"failed to read zip archive: {exc}") from exc
    except ValidationError as exc:
        shutil.rmtree(run_root, ignore_errors=True)
        raise IntakeError(f"manifest validation failed: {exc}") from exc
    except Exception:
        shutil.rmtree(run_root, ignore_errors=True)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Secure intake for phase-driven harness spec bundles.")
    parser.add_argument("--input-zip", required=True, help="Path to source .zip bundle.")
    parser.add_argument("--run-id", required=False, help="Explicit harness run id.")
    parser.add_argument(
        "--registry-root",
        default="registry",
        help="Root path for canonical registry artifacts (default: registry).",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        result = run_bundle_intake(
            input_zip=Path(args.input_zip).resolve(),
            registry_root=Path(args.registry_root).resolve(),
            run_id=args.run_id,
        )
    except IntakeError as exc:
        raise SystemExit(str(exc)) from exc

    payload = {
        "run_id": result.run_id,
        "run_root": result.run_root.as_posix(),
        "spec_manifest": result.manifest_path.as_posix(),
        "extracted_text_cache": result.text_cache_path.as_posix(),
        "artifact_count": result.artifact_count,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
