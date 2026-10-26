"""Initialize only the isolated Dagster quota database on the existing PG service."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

DATABASE = "ta3000_staging_quota"
OWNER = "ta3000_quota_owner"
WRITER = "ta3000_quota_writer"
CONTAINER = "ta3000-journal-dev-20261012"


def admin_sql(sql: str, database: str = "postgres") -> str:
    command = [
        "docker",
        "exec",
        "-i",
        CONTAINER,
        "sh",
        "-c",
        'psql -U "$POSTGRES_USER" -d "$1" -v ON_ERROR_STOP=1 -At',
        "--",
        database,
    ]
    result = subprocess.run(command, input=sql, text=True, capture_output=True, timeout=30)
    if result.returncode:
        # PostgreSQL errors can repeat SQL containing a newly generated password.
        raise RuntimeError("Protected PostgreSQL initialization command failed")
    return result.stdout.strip()


def write_new(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)


def host_url(value: str) -> str:
    from sqlalchemy.engine import make_url

    url = make_url(value)
    if url.database != DATABASE or url.username not in {OWNER, WRITER}:
        raise ValueError("Quota credentials identify a different database or role")
    return url.set(host="127.0.0.1").render_as_string(hide_password=False)


def bootstrap(quota_root: Path, private_root: Path, *, apply: bool) -> dict:
    quota_root, private_root = quota_root.resolve(), private_root.resolve()
    if quota_root.is_relative_to(private_root) or private_root.is_relative_to(quota_root):
        raise ValueError("Private owner credentials must be outside mounted quota configuration")
    config_path, owner_path = quota_root / "storage.json", private_root / "quota-owner.json"
    existence = json.loads(
        admin_sql(
            "SELECT json_build_object("
            f"'database',EXISTS(SELECT FROM pg_database WHERE datname='{DATABASE}'),"
            f"'owner',EXISTS(SELECT FROM pg_roles WHERE rolname='{OWNER}'),"
            f"'writer',EXISTS(SELECT FROM pg_roles WHERE rolname='{WRITER}'))::text;"
        )
    )
    if any(existence.values()):
        if not all(existence.values()) or not config_path.is_file() or not owner_path.is_file():
            raise ValueError("Quota resources exist without the complete protected bootstrap state")
        return {
            "status": "EXISTS",
            "database": DATABASE,
            "role": WRITER,
            "quota_config": str(config_path),
            "owner_config": str(owner_path),
        }
    if config_path.exists() or owner_path.exists():
        raise ValueError("Protected credential files already exist; refusing to replace them")
    if not apply:
        return {"status": "PLANNED", "database": DATABASE, "roles": [OWNER, WRITER]}
    from dagster_postgres.run_storage import PostgresRunStorage

    owner_password, writer_password = secrets.token_hex(32), secrets.token_hex(32)
    owner_url = f"postgresql+psycopg2://{OWNER}:{owner_password}@127.0.0.1:55432/{DATABASE}"
    writer_url = (
        f"postgresql+psycopg2://{WRITER}:{writer_password}@host.docker.internal:55432/{DATABASE}"
    )
    from sqlalchemy import create_engine

    create_engine(owner_url).dispose()  # Select the installed DBAPI before any DDL.
    # Save protected recovery credentials before creating resources. Never rotate an
    # existing role or drop/recreate a database to recover a partial bootstrap.
    write_new(owner_path, {"schema_version": "staging_quota_owner.v1", "postgres_url": owner_url})
    write_new(
        config_path,
        {
            "schema_version": "staging_quota_storage.v1",
            "backend": "postgres",
            "postgres_url": writer_url,
            "expected_database": DATABASE,
            "expected_role": WRITER,
        },
    )
    admin_sql(
        f"CREATE ROLE {OWNER} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE "
        f"NOREPLICATION NOBYPASSRLS PASSWORD '{owner_password}';\n"
        f"CREATE ROLE {WRITER} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE "
        f"NOREPLICATION NOBYPASSRLS PASSWORD '{writer_password}';\n"
        f"CREATE DATABASE {DATABASE} OWNER {OWNER} TEMPLATE template0;\n"
        f"REVOKE ALL ON DATABASE {DATABASE} FROM PUBLIC;\n"
        f"GRANT CONNECT ON DATABASE {DATABASE} TO {OWNER},{WRITER};"
    )
    admin_sql(
        "REVOKE ALL ON SCHEMA public FROM PUBLIC;\n"
        f"ALTER SCHEMA public OWNER TO {OWNER};\n"
        f"GRANT USAGE ON SCHEMA public TO {WRITER};",
        DATABASE,
    )
    storage = PostgresRunStorage(owner_url, should_autocreate_tables=True)
    storage.dispose()
    admin_sql(
        f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {WRITER};\n"
        f"GRANT INSERT ON public.runs,public.run_tags TO {WRITER};\n"
        f"GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO {WRITER};\n"
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {OWNER} IN SCHEMA public "
        f"GRANT SELECT ON TABLES TO {WRITER};\n"
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {OWNER} IN SCHEMA public "
        f"GRANT USAGE,SELECT ON SEQUENCES TO {WRITER};",
        DATABASE,
    )
    return {
        "status": "CREATED",
        "database": DATABASE,
        "role": WRITER,
        "quota_config": str(config_path),
        "owner_config": str(owner_path),
    }


def verify_native(config_path: Path) -> dict:
    from dagster import DagsterRun, DagsterRunStatus, RunsFilter
    from dagster._core.errors import DagsterRunAlreadyExists
    from dagster_postgres.run_storage import PostgresRunStorage
    from sqlalchemy import create_engine
    from sqlalchemy.exc import DBAPIError

    config = json.loads(config_path.read_text("utf-8"))
    if (
        config.get("expected_database") != DATABASE
        or config.get("expected_role") != WRITER
        or config.get("backend") != "postgres"
    ):
        raise ValueError("Unexpected quota storage identity")
    url = host_url(config["postgres_url"])
    store = PostgresRunStorage(url, should_autocreate_tables=False)
    engine = create_engine(url)
    other, collision = uuid4().hex, uuid4().hex
    store.add_run(
        DagsterRun(
            job_name="ta3000_staging_control",
            run_id=other,
            status=DagsterRunStatus.SUCCESS,
            run_config={"task_id": "quota-proof-other-task"},
            tags={"ta3000/staging_control": "reservation"},
        )
    )
    denied = {}
    statements = {
        "delete_another_task": f"DELETE FROM public.runs WHERE run_id='{other}'",
        "update_another_task": f"UPDATE public.runs SET run_body=run_body WHERE run_id='{other}'",
        "truncate": "TRUNCATE public.runs CASCADE",
        "create_table": "CREATE TABLE public.quota_forbidden_probe (id integer)",
        "create_temp": "CREATE TEMP TABLE quota_forbidden_temp (id integer)",
        "alter_owner": f"ALTER TABLE public.runs OWNER TO {WRITER}",
        "delete_tags": f"DELETE FROM public.run_tags WHERE run_id='{other}'",
        "create_schema": "CREATE SCHEMA quota_forbidden_schema",
        "grant_membership": f"GRANT {OWNER} TO {WRITER}",
        "sequence_setval": "SELECT setval('public.runs_id_seq',"
        "(SELECT last_value FROM public.runs_id_seq),"
        "(SELECT is_called FROM public.runs_id_seq))",
    }
    try:
        with engine.connect() as connection:
            identity = connection.exec_driver_sql("SELECT current_database(),current_user").one()
            if tuple(identity) != (DATABASE, WRITER):
                raise ValueError("Actual quota role/database differs")
            connection.rollback()
            for name, sql in statements.items():
                transaction = connection.begin()
                try:
                    connection.exec_driver_sql(sql)
                except DBAPIError as exc:
                    code = getattr(exc.orig, "pgcode", None)
                    denied[name] = code == "42501"
                else:
                    denied[name] = False
                finally:
                    transaction.rollback()  # Negative proof never mutates even on a bad grant.

        def reserve(_):
            local = PostgresRunStorage(url, should_autocreate_tables=False)
            try:
                local.add_run(
                    DagsterRun(
                        job_name="ta3000_staging_control",
                        run_id=collision,
                        status=DagsterRunStatus.SUCCESS,
                        run_config={"task_id": "quota-proof-race"},
                        tags={"ta3000/staging_control": "reservation"},
                    )
                )
                return True
            except DagsterRunAlreadyExists:
                return False
            finally:
                local.dispose()

        with ThreadPoolExecutor(max_workers=8) as pool:
            winners = sum(pool.map(reserve, range(8)))
        preserved = store.get_runs(filters=RunsFilter(run_ids=[other]), limit=1)
        if not all(denied.values()) or winners != 1 or len(preserved) != 1:
            raise ValueError("Quota append-only/CAS proof failed")
        return {
            "status": "PASS",
            "database": DATABASE,
            "role": WRITER,
            "denied": denied,
            "concurrent_attempts": 8,
            "atomic_winners": winners,
            "another_task_preserved": True,
            "preserved_run_ids": [other, collision],
        }
    finally:
        engine.dispose()
        store.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quota-root", type=Path, required=True)
    parser.add_argument("--private-root", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--proof-output", type=Path)
    args = parser.parse_args()
    try:
        result = bootstrap(args.quota_root, args.private_root, apply=args.apply)
        if args.verify:
            result["native_proof"] = verify_native(args.quota_root / "storage.json")
        if args.proof_output:
            args.proof_output.parent.mkdir(parents=True, exist_ok=True)
            args.proof_output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:
        # DSNs/passwords and SQL are intentionally absent from public diagnostics.
        print(f"Quota bootstrap BLOCKED ({type(exc).__name__})")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
