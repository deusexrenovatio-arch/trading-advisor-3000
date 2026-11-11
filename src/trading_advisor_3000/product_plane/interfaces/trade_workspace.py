"""Local manual Journal API, independent of market/model and RuntimeStack."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import psycopg
from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from trading_advisor_3000.product_plane.contracts.trade_workspace import (
    ChangeTrade,
    CreateTrade,
    Receipt,
    Snapshot,
    Status,
)
from trading_advisor_3000.product_plane.domain.journal.policy import JournalError
from trading_advisor_3000.product_plane.domain.journal.store import PostgresJournal

PREFIX = "/api/trade-workspace/v1"


def create_app(journal: PostgresJournal | None = None) -> FastAPI:
    app = FastAPI(title="TA3000 Manual Journal", version="1")
    dsn = os.environ.get("TA3000_JOURNAL_DSN")
    app.state.journal = journal or (PostgresJournal(dsn) if dsn else None)

    @app.get("/", include_in_schema=False)
    def home():
        return RedirectResponse("/journal/")

    frontend = Path(__file__).parent / "frontend" / "dist"

    @app.get("/journal/", include_in_schema=False)
    def journal_ui():
        if not (frontend / "index.html").is_file():
            return JSONResponse(status_code=503, content={"code": "frontend_not_built"})
        return FileResponse(frontend / "index.html", headers={"Cache-Control": "no-cache"})

    if (frontend / "assets").is_dir():
        app.mount(
            "/journal/assets", StaticFiles(directory=frontend / "assets"), name="journal_assets"
        )

    def storage() -> PostgresJournal:
        if app.state.journal is None:
            raise JournalError("storage_unconfigured", "Journal storage is not configured", 503)
        return app.state.journal

    @app.exception_handler(JournalError)
    def journal_error(request: Request, exc: JournalError):
        return JSONResponse(
            status_code=exc.status,
            content={
                "code": exc.code,
                "message": exc.message,
                "field_errors": [],
                "retryable": exc.status == 503,
                "correlation_id": None,
            },
        )

    @app.exception_handler(psycopg.Error)
    def storage_error(request: Request, exc: psycopg.Error):
        return JSONResponse(
            status_code=503,
            content={
                "code": "storage_unavailable",
                "message": "Journal storage is unavailable",
                "field_errors": [],
                "retryable": True,
                "correlation_id": None,
            },
        )

    @app.exception_handler(RequestValidationError)
    def invalid_input(request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content={
                "code": "invalid_input",
                "message": "Request failed validation",
                "field_errors": [
                    {"field": ".".join(map(str, error["loc"])), "message": error["msg"]}
                    for error in exc.errors()
                ],
                "retryable": False,
                "correlation_id": None,
            },
        )

    @app.post(PREFIX + "/trades", response_model=Receipt, status_code=201)
    def create_trade(command: CreateTrade, store: PostgresJournal = Depends(storage)):
        return store.create_trade(command)

    @app.post(PREFIX + "/trades/{trade_id}/changes", response_model=Receipt)
    def change_trade(
        trade_id: str, command: ChangeTrade, store: PostgresJournal = Depends(storage)
    ):
        return store.apply_change(trade_id, command)

    @app.get(PREFIX + "/commands/{command_id}", response_model=Receipt)
    def receipt(command_id: str, store: PostgresJournal = Depends(storage)):
        return store.get_receipt(command_id)

    @app.get(PREFIX + "/trades")
    def list_trades(
        scope: Literal["current", "archive", "all"] = "current",
        query: str = Query(default="", max_length=100),
        limit: int = Query(default=25, ge=1, le=100),
        cursor: str | None = Query(default=None, max_length=2048),
        status: Status | None = None,
        store: PostgresJournal = Depends(storage),
    ):
        return store.list_trades(
            scope=scope, query=query, limit=limit, cursor=cursor, status=status
        )

    @app.get(PREFIX + "/trades/{trade_id}", response_model=Snapshot)
    def get_trade(
        trade_id: str,
        revision: int | None = Query(default=None, ge=1),
        store: PostgresJournal = Depends(storage),
    ):
        return store.get_trade(trade_id, revision)

    @app.get(PREFIX + "/trades/{trade_id}/timeline")
    def timeline(
        trade_id: str,
        limit: int = Query(default=25, ge=1, le=100),
        cursor: str | None = Query(default=None, max_length=2048),
        order: Literal["asc", "desc"] = "asc",
        store: PostgresJournal = Depends(storage),
    ):
        return store.get_timeline(trade_id, limit=limit, cursor=cursor, order=order)

    @app.get("/health")
    def health():
        return {"service": "manual_journal", "status": "running"}

    @app.get("/ready")
    def ready(store: PostgresJournal = Depends(storage)):
        store.check_ready()
        return {"service": "manual_journal", "status": "ready"}

    return app


app = create_app()
