"""Create a new immutable SYNTHETIC Delta input fixture; never run Research."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from trading_advisor_3000.product_plane.common.staging_test_fixture import (  # noqa: E402
    prepare_fixture,
    verify_fixture,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, required=True, help="New fixture directory")
    parser.add_argument("--verify-sha256", help="Verify an existing fixture without writes")
    args = parser.parse_args()
    result = (
        verify_fixture(args.path, args.verify_sha256)
        if args.verify_sha256
        else prepare_fixture(args.path)
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
