from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

try:
    import psycopg
except ImportError as exc:  # pragma: no cover - optional dependency path
    raise SystemExit("psycopg is required to apply app migrations") from exc


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "src" / "trading_advisor_3000" / "migrations"


def _migration_files() -> list[Path]:
    return sorted(path for path in MIGRATIONS_DIR.glob("[0-9][0-9][0-9][0-9]_*.sql"))


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def apply_migrations(*, dsn: str) -> list[str]:
    applied_now: list[str] = []
    with psycopg.connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.schema_migrations (
                    version TEXT PRIMARY KEY,
                    checksum TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute("SELECT version, checksum FROM public.schema_migrations ORDER BY version")
            applied = {str(version): str(checksum) for version, checksum in cur.fetchall()}

            for path in _migration_files():
                version = path.name
                checksum = _checksum(path)
                known_checksum = applied.get(version)
                if known_checksum is not None:
                    if known_checksum != checksum:
                        raise RuntimeError(
                            f"migration checksum drift detected for {version}: {known_checksum} != {checksum}"
                        )
                    continue

                cur.execute(path.read_text(encoding="utf-8"))
                cur.execute(
                    "INSERT INTO public.schema_migrations (version, checksum) VALUES (%s, %s)",
                    (version, checksum),
                )
                applied_now.append(version)

    return applied_now


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply Trading Advisor 3000 app migrations.")
    parser.add_argument(
        "--dsn",
        default=os.environ.get("TA3000_APP_DSN", ""),
        help="PostgreSQL DSN. Defaults to TA3000_APP_DSN.",
    )
    args = parser.parse_args()

    dsn = str(args.dsn).strip()
    if not dsn:
        raise SystemExit("missing DSN: pass --dsn or set TA3000_APP_DSN")

    applied_now = apply_migrations(dsn=dsn)
    if applied_now:
        print("\n".join(applied_now))
    else:
        print("up-to-date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
