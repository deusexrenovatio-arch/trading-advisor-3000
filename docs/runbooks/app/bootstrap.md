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

## Telegram Publication Binding (F1-B)
`TA3000_TELEGRAM_CHANNEL` is a mandatory runtime binding for publication contour closure.

Required bindings before publication evidence:
- `TA3000_TELEGRAM_BOT_TOKEN` (real bot credential)
- `TA3000_TELEGRAM_CHANNEL` (real publication chat/channel id or alias)

Fail-closed behavior:
- Runtime bootstrap fails if `TA3000_TELEGRAM_CHANNEL` is missing.
- Phase-02 publication evidence is not accepted if chat/channel binding is passed only via `--publication-channel`.

Governed no-override evidence command:

```bash
python scripts/build_phase02_publication_evidence.py \
  --attempt "<run-id>/attempt-<NN>" \
  --output "artifacts/codex/orchestration/<run-id>/attempt-<NN>/publication-message-lifecycle-evidence.json" \
  --fail-if-not-live-real
```

This command must run with `TA3000_TELEGRAM_CHANNEL` already configured in the runtime environment.

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
