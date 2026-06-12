@echo off
setlocal enabledelayedexpansion

if "%PROD_REPO%"=="" set "PROD_REPO=D:\TA3000-production"
if "%DATA_ROOT%"=="" set "DATA_ROOT=D:\TA3000-data\trading-advisor-3000-nightly"
if "%TEST_STAGING_ROOT%"=="" set "TEST_STAGING_ROOT=D:\TA3000-data\trading-advisor-3000-verification"
if "%LOGDIR%"=="" set "LOGDIR=D:\TA3000-data\logs"
if "%LOGFILE%"=="" set "LOGFILE=%LOGDIR%\ta3000-production-nightly.log"
if "%TA3000_DAGSTER_PORT%"=="" set "TA3000_DAGSTER_PORT=3000"

set "COMPOSE_BASE=%PROD_REPO%\deployment\docker\dagster-staging\docker-compose.dagster-staging.yml"
set "COMPOSE_PRODUCT=%PROD_REPO%\deployment\docker\dagster-staging\docker-compose.dagster-product-main-bind.yml"
set "TA3000_PRODUCT_MAIN_WORKTREE=%PROD_REPO%"
set "TA3000_MOEX_HISTORICAL_DATA_ROOT_HOST=%DATA_ROOT%"
set "TA3000_MOEX_PRODUCT_STAGING_ROOT_HOST=%DATA_ROOT%"
set "TA3000_MOEX_TEST_STAGING_ROOT_HOST=%TEST_STAGING_ROOT%"

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

echo [%DATE% %TIME%] TA3000 product staging bootstrap start >> "%LOGFILE%"
echo [%DATE% %TIME%] PROD_REPO=%PROD_REPO% >> "%LOGFILE%"
echo [%DATE% %TIME%] DATA_ROOT=%DATA_ROOT% >> "%LOGFILE%"

if not exist "%PROD_REPO%\.git" (
  echo [%DATE% %TIME%] ERROR: production checkout missing at "%PROD_REPO%" >> "%LOGFILE%"
  exit /b 1
)

if not exist "%DATA_ROOT%" mkdir "%DATA_ROOT%"

if exist "%DATA_ROOT%\.git" (
  echo [%DATE% %TIME%] ERROR: data root must not be a git checkout: "%DATA_ROOT%" >> "%LOGFILE%"
  exit /b 1
)

if not exist "%COMPOSE_BASE%" (
  echo [%DATE% %TIME%] ERROR: compose base file missing: "%COMPOSE_BASE%" >> "%LOGFILE%"
  exit /b 1
)

if not exist "%COMPOSE_PRODUCT%" (
  echo [%DATE% %TIME%] ERROR: compose product bind file missing: "%COMPOSE_PRODUCT%" >> "%LOGFILE%"
  exit /b 1
)

where docker >nul 2>nul
if errorlevel 1 (
  echo [%DATE% %TIME%] ERROR: docker CLI is not available on PATH >> "%LOGFILE%"
  exit /b 1
)

echo [%DATE% %TIME%] Starting product staging containers via Docker Compose >> "%LOGFILE%"
docker compose -p dagster-product-staging -f "%COMPOSE_BASE%" -f "%COMPOSE_PRODUCT%" up -d --build >> "%LOGFILE%" 2>&1 || goto :fail

set "WEB_HEALTH="
for /L %%I in (1,1,60) do (
  for /f "usebackq tokens=*" %%H in (`docker inspect -f "{{.State.Health.Status}}" ta3000-dagster-webserver 2^>nul`) do set "WEB_HEALTH=%%H"
  if /I "!WEB_HEALTH!"=="healthy" goto :web_healthy
  timeout /t 5 /nobreak >nul
)

echo [%DATE% %TIME%] ERROR: ta3000-dagster-webserver did not become healthy; last health=!WEB_HEALTH! >> "%LOGFILE%"
exit /b 1

:web_healthy
set "DAEMON_RUNNING="
for /f "usebackq tokens=*" %%R in (`docker inspect -f "{{.State.Running}}" ta3000-dagster-daemon 2^>nul`) do set "DAEMON_RUNNING=%%R"
if /I not "!DAEMON_RUNNING!"=="true" (
  echo [%DATE% %TIME%] ERROR: ta3000-dagster-daemon is not running >> "%LOGFILE%"
  exit /b 1
)

docker exec ta3000-dagster-webserver python -c "from pathlib import Path; assert Path('/workspace').exists(); assert Path('/ta3000-data/moex-historical').exists(); print('product staging mounts ok')" >> "%LOGFILE%" 2>&1 || goto :fail

docker exec ta3000-dagster-webserver python -c "import trading_advisor_3000.dagster_defs as d; repo=d.product_plane_definitions.get_repository_def(); schedules={s.name:s for s in repo.schedule_defs}; schedule=schedules['moex_baseline_daily_update_schedule']; repo.get_job('moex_baseline_update_job'); assert schedule.cron_schedule == '0 2 * * *'; assert getattr(schedule.default_status, 'value', str(schedule.default_status)) == 'RUNNING'; print('Dagster daemon owns moex_baseline_daily_update_schedule')" >> "%LOGFILE%" 2>&1 || goto :fail

echo [%DATE% %TIME%] TA3000 product staging bootstrap success; Dagster daemon owns nightly schedule >> "%LOGFILE%"
exit /b 0

:fail
set "ERR=!ERRORLEVEL!"
echo [%DATE% %TIME%] TA3000 product staging bootstrap FAILED with code !ERR! >> "%LOGFILE%"
exit /b !ERR!
