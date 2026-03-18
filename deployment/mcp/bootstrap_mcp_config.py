from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_SOURCE = Path("deployment/mcp/config.template.toml")
DEFAULT_TARGET = Path(".codex/config.toml")


def run(*, source: Path, target: Path, force: bool) -> int:
    if not source.exists():
        raise FileNotFoundError(f"source config template not found: {source.as_posix()}")
    if target.exists() and not force:
        raise FileExistsError(
            f"target config already exists: {target.as_posix()} "
            "(use --force to overwrite)"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"mcp config bootstrap completed: {target.as_posix()}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap project-scoped MCP config from template.")
    parser.add_argument("--source", default=DEFAULT_SOURCE.as_posix())
    parser.add_argument("--target", default=DEFAULT_TARGET.as_posix())
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    raise SystemExit(
        run(
            source=Path(args.source),
            target=Path(args.target),
            force=args.force,
        )
    )


if __name__ == "__main__":
    main()
