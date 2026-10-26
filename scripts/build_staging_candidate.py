"""Build an immutable integration candidate without checking out feature branches."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

ROOTS = ("src/", "scripts/", "configs/", "deployment/")
DEPENDENCIES = {"pyproject.toml", "uv.lock", "poetry.lock", "setup.py", "setup.cfg"}


def git(root: Path, *args: str, data: str | None = None) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        input=data,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        check=False,
    )
    if result.returncode:
        raise ValueError(f"Git {args[0]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def resolve(root: Path, ref: str) -> str:
    return git(root, "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}")


def relevant(path: str, *, configuration_only: bool = False) -> bool:
    if path.endswith((".md", ".rst")):
        return False
    roots = ("configs/", "deployment/") if configuration_only else ROOTS
    return path.startswith(roots) or path in DEPENDENCIES or path.startswith("requirements")


def normalized_hash(value: bytes) -> str:
    try:
        value.decode("utf-8")
        value = value.replace(b"\r\n", b"\n")
    except UnicodeDecodeError:
        pass
    return hashlib.sha256(value).hexdigest()


def manifest_digest(manifest: dict[str, str]) -> str:
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def execution_manifest(root: Path, ref: str, *, configuration_only: bool = False) -> dict[str, str]:
    commit = resolve(root, ref)
    entries = git(root, "ls-tree", "-rz", commit).split("\0")
    manifest = {}
    for entry in filter(None, entries):
        metadata, path = entry.split("\t", 1)
        if not relevant(path, configuration_only=configuration_only):
            continue
        mode, kind, blob = metadata.split()
        if kind != "blob" or mode == "120000":
            raise ValueError(f"Executable closure cannot contain links/submodules: {path}")
        content = subprocess.run(
            ["git", "cat-file", "blob", blob], cwd=root, capture_output=True, check=True
        ).stdout
        manifest[path] = normalized_hash(content)
    if not manifest:
        raise ValueError("Empty executable/configuration closure")
    return manifest


def executable_fingerprint(root: Path, ref: str) -> str:
    return manifest_digest(execution_manifest(root, ref))


def config_fingerprint(root: Path, ref: str) -> str:
    return manifest_digest(execution_manifest(root, ref, configuration_only=True))


def verify_deployed_files(root: Path, manifest: dict[str, str]) -> str:
    root = root.resolve()
    for name, expected in manifest.items():
        path = root / name
        if not path.resolve().is_relative_to(root) or path.is_symlink():
            raise ValueError(f"Unsafe deployed path: {name}")
        if not path.is_file() or normalized_hash(path.read_bytes()) != expected:
            raise ValueError(f"Deployed code differs: {name}")
    for directory in ("src", "scripts", "configs", "deployment"):
        for path in (root / directory).rglob("*"):
            name = path.relative_to(root).as_posix()
            if "__pycache__" in path.parts or ".egg-info/" in name or path.suffix == ".pyc":
                continue
            if path.is_file() and relevant(name) and name not in manifest:
                raise ValueError(f"Unpinned deployed input: {name}")
    return manifest_digest(manifest)


def build_candidate(
    root: Path, *, base_ref: str, branch_refs: list[str], candidate_ref: str, image_digest: str
) -> dict:
    if git(root, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("Candidate construction requires a clean worktree")
    if not candidate_ref.startswith("refs/heads/"):
        raise ValueError("Candidate ref must be a refs/heads/ integration branch")
    if candidate_ref in {"refs/heads/main", "refs/heads/ta3000-production"}:
        raise ValueError("Candidate cannot replace a mainline/runtime branch")
    git(root, "check-ref-format", candidate_ref)
    if not branch_refs:
        raise ValueError("At least one branch head is required")
    base = resolve(root, base_ref)
    heads = [resolve(root, ref) for ref in branch_refs]
    if len(set(heads)) != len(heads):
        raise ValueError("Duplicate branch heads")
    current = base
    for head in heads:
        tree = git(root, "merge-tree", "--write-tree", current, head).splitlines()[0]
        current = git(
            root,
            "-c",
            "user.name=Staging Candidate",
            "-c",
            "user.email=staging@localhost",
            "commit-tree",
            tree,
            "-p",
            current,
            "-p",
            head,
            data="Integration candidate for staging acceptance\n",
        )
    tree = git(root, "rev-parse", f"{current}^{{tree}}")
    candidate = {
        "base_sha": base,
        "head_sha": current,
        "tree_sha": tree,
        "branch_heads": dict(zip(branch_refs, heads, strict=True)),
        "executable_sha256": executable_fingerprint(root, current),
        "config_sha256": config_fingerprint(root, current),
        "image_digest": image_digest,
    }
    if len(image_digest) != 71 or not image_digest.startswith("sha256:"):
        raise ValueError("Runtime image must use a sha256 digest")
    try:
        int(image_digest[7:], 16)
    except ValueError as exc:
        raise ValueError("Invalid runtime image digest") from exc
    # Compare-and-create: a prior candidate/feature ref can never be overwritten.
    git(root, "update-ref", candidate_ref, current, "0" * len(current))
    return {
        "schema_version": "staging_candidate.v1",
        "candidate_ref": candidate_ref,
        "candidate": candidate,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base-ref", default="origin/main")
    parser.add_argument("--branch-head", action="append", required=True)
    parser.add_argument("--candidate-ref", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = build_candidate(
            args.repo,
            base_ref=args.base_ref,
            branch_refs=args.branch_head,
            candidate_ref=args.candidate_ref,
            image_digest=args.image_digest,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result))
        return 0
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        print(f"staging candidate: BLOCKED ({exc})")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
