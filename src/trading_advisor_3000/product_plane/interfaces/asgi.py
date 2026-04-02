from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Mapping

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from trading_advisor_3000.product_plane.contracts import DecisionCandidate
from trading_advisor_3000.product_plane.interfaces.api import RuntimeAPI
from trading_advisor_3000.product_plane.runtime import RuntimeBootstrapResult, build_runtime_stack_from_env


class ReplayCandidatesRequest(BaseModel):
    candidates: list[dict[str, object]] = Field(min_length=1)


class CloseSignalRequest(BaseModel):
    signal_id: str
    closed_at: str
    reason_code: str


class CancelSignalRequest(BaseModel):
    signal_id: str
    canceled_at: str
    reason_code: str


def _runtime_api(request: Request) -> RuntimeAPI:
    runtime_api = getattr(request.app.state, "runtime_api", None)
    if not isinstance(runtime_api, RuntimeAPI):  # pragma: no cover - defensive path
        raise HTTPException(status_code=503, detail="runtime_api_not_initialized")
    return runtime_api


def _bootstrap_result(request: Request) -> RuntimeBootstrapResult:
    result = getattr(request.app.state, "runtime_bootstrap", None)
    if not isinstance(result, RuntimeBootstrapResult):  # pragma: no cover - defensive path
        raise HTTPException(status_code=503, detail="runtime_bootstrap_not_initialized")
    return result


def _parse_candidates(payloads: list[dict[str, object]]) -> list[DecisionCandidate]:
    candidates: list[DecisionCandidate] = []
    for payload in payloads:
        try:
            candidates.append(DecisionCandidate.from_dict(payload))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"invalid decision candidate payload: {exc}") from exc
    return candidates


def create_app(*, env: Mapping[str, str] | None = None) -> FastAPI:
    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        bootstrap = build_runtime_stack_from_env(env)
        app.state.runtime_bootstrap = bootstrap
        app.state.runtime_api = RuntimeAPI(runtime_stack=bootstrap.runtime_stack)
        yield

    app = FastAPI(
        title="Trading Advisor 3000 Runtime API",
        version="0.1.0",
        lifespan=_lifespan,
    )

    @app.get("/health")
    def health(request: Request) -> dict[str, object]:
        bootstrap = _bootstrap_result(request)
        return {
            "status": "ok",
            **bootstrap.config.to_dict(),
        }

    @app.get("/ready")
    def ready(request: Request) -> dict[str, object]:
        bootstrap = _bootstrap_result(request)
        return {
            "ready": True,
            **bootstrap.config.to_dict(),
        }

    @app.post("/runtime/replay-candidates")
    def replay_candidates(payload: ReplayCandidatesRequest, request: Request) -> dict[str, object]:
        runtime_api = _runtime_api(request)
        candidates = _parse_candidates(list(payload.candidates))
        try:
            return runtime_api.replay_candidates(candidates)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/runtime/close-signal")
    def close_signal(payload: CloseSignalRequest, request: Request) -> dict[str, object]:
        runtime_api = _runtime_api(request)
        try:
            return runtime_api.close_signal(
                signal_id=payload.signal_id,
                closed_at=payload.closed_at,
                reason_code=payload.reason_code,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/runtime/cancel-signal")
    def cancel_signal(payload: CancelSignalRequest, request: Request) -> dict[str, object]:
        runtime_api = _runtime_api(request)
        try:
            return runtime_api.cancel_signal(
                signal_id=payload.signal_id,
                canceled_at=payload.canceled_at,
                reason_code=payload.reason_code,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/runtime/signal-events")
    def list_signal_events(request: Request) -> list[dict[str, object]]:
        return _runtime_api(request).list_signal_events()

    @app.get("/runtime/strategy-registry")
    def list_strategy_registry(request: Request) -> list[dict[str, object]]:
        return _runtime_api(request).list_strategy_registry()

    return app


app = create_app()
