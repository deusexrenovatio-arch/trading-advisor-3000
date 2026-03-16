from __future__ import annotations

import json

from .app import build_app_metadata


def main() -> int:
    print(json.dumps(build_app_metadata(), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
