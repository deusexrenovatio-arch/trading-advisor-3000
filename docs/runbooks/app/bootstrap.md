# App Bootstrap Runbook

## Goal
Bring up the product-plane development baseline with one reproducible dependency path.

## Local Python Setup
1. Create and activate a Python 3.11 virtual environment.
2. Install dependencies from the repository root:

```bash
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

## Baseline Validation
Run the minimum acceptance checks:

```bash
python -m pytest tests/app -q
python scripts/run_loop_gate.py --from-git --git-ref HEAD
python scripts/run_pr_gate.py --from-git --git-ref HEAD
```

## Postgres Runtime State Bootstrap
1. Start a PostgreSQL instance.
2. Apply app migrations:

```bash
python scripts/apply_app_migrations.py --dsn postgresql://postgres:postgres@127.0.0.1:5432/ta3000
```

## Optional Docker Profiles
- `deployment/docker/production-like/docker-compose.production-like.yml`
- `deployment/docker/staging-gateway/docker-compose.staging-gateway.yml`

These profiles are for proving and staging-style validation, not proof of live production closure.
