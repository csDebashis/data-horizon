"""Small analytics API. Service keys stay between the API and the UI server."""

from __future__ import annotations

import logging
import math
import os
from pathlib import Path
import secrets
import re
from typing import Annotated, Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path as APIPath, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field
from starlette.types import ASGIApp, Receive, Scope, Send

from .alert_scope import (
    alert_matches,
    alert_source_status,
    fresh_observations,
    retained_alert,
    selection_contexts,
)
from .contracts import utcnow

LOGGER = logging.getLogger(__name__)
IDENTIFIER = r"^[A-Za-z0-9_.:-]+$"
RouteID = Annotated[str, Query(min_length=1, max_length=32, pattern=IDENTIFIER)]
StopID = Annotated[str, Query(min_length=1, max_length=64, pattern=IDENTIFIER)]
bearer = HTTPBearer(auto_error=False)


class RequestBodyLimitMiddleware:
    """Bound received bytes before JSON parsing or authentication can run."""

    def __init__(self, app: ASGIApp, max_bytes: int = 16_384):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        lengths = [value for name, value in scope.get("headers", []) if name.lower() == b"content-length"]
        if lengths:
            if len(lengths) != 1 or not re.fullmatch(rb"[0-9]+", lengths[0]):
                await JSONResponse(status_code=400, content={"detail": "Invalid content length"})(
                    scope, receive, send
                )
                return
            digits = lengths[0].lstrip(b"0") or b"0"
            if len(digits) > len(str(self.max_bytes)) or int(digits) > self.max_bytes:
                await JSONResponse(status_code=413, content={"detail": "Request body is too large"})(
                    scope, receive, send
                )
                return

        # A missing or understated length never bypasses the received-byte cap.
        # Do not retain the chunk that would take the bounded buffer over it.
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            if len(body) + len(chunk) > self.max_bytes:
                await JSONResponse(status_code=413, content={"detail": "Request body is too large"})(
                    scope, receive, send
                )
                return
            body.extend(chunk)
            if not message.get("more_body", False):
                break
        bounded_body = bytes(body)
        del body
        delivered = False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bounded_body, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


class ScenarioRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    route_id: str = Field(min_length=1, max_length=32, pattern=IDENTIFIER)
    stop_id: str = Field(min_length=1, max_length=64, pattern=IDENTIFIER)
    delay_seconds: int = Field(ge=0, le=7200)
    horizon_minutes: int = Field(default=30, ge=1, le=60)
    limit: int = Field(default=20, ge=1, le=100)


def _secret(name: str) -> str:
    """Read operator-supplied secrets without including values in diagnostics."""
    filename = os.getenv(f"{name}_FILE")
    if filename:
        try:
            return Path(filename).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise RuntimeError(f"Cannot read {name} secret file") from exc
    return os.getenv(name, "").strip()


def create_app(store: Any | None = None) -> FastAPI:
    """Build an app; inject a store for isolated contract tests."""
    production = os.getenv("APP_ENV", "development").lower() == "production"
    token = _secret("API_TOKEN")
    public_token = _secret("PUBLIC_API_TOKEN")
    if production and len(token) < 32:
        raise RuntimeError("Production requires API_TOKEN_FILE or API_TOKEN with at least 32 characters")
    if production and len(public_token) < 32:
        raise RuntimeError("Production requires a private PUBLIC_API_TOKEN with at least 32 characters")

    if token and public_token and secrets.compare_digest(token.encode(), public_token.encode()):
        raise RuntimeError("API_TOKEN and PUBLIC_API_TOKEN must be different")

    app = FastAPI(
        title="Data Horizon API",
        version="1.0.0",
        description="Public transit analytics. Time values and source freshness accompany every result.",
        docs_url=None if production else "/docs",
        redoc_url=None if production else "/redoc",
        openapi_url=None if production else "/openapi.json",
    )
    app.state.store = store

    @app.exception_handler(RequestValidationError)
    async def safe_validation_error(request, error):
        # Return the problem location without echoing submitted values.
        details = [
            {key: item[key] for key in ("type", "loc", "msg") if key in item} for item in error.errors()
        ]
        return JSONResponse(status_code=422, content={"detail": details})

    origins = set()
    for address in (
        os.getenv("DASHBOARD_PUBLIC_URL", "http://127.0.0.1:8501"),
        os.getenv("PUBLIC_API_BASE_URL", "http://127.0.0.1:8000"),
    ):
        parsed = urlsplit(address)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username:
            raise RuntimeError("Public application URLs must be valid HTTP(S) origins")
        if production and parsed.scheme != "https":
            raise RuntimeError("Production public application URLs require HTTPS")
        origins.add(f"{parsed.scheme}://{parsed.netloc}")
    if not production:
        origins.update({"http://localhost:8501", "http://localhost:8000"})
    app.add_middleware(
        CORSMiddleware,
        allow_origins=sorted(origins),
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )
    app.add_middleware(RequestBodyLimitMiddleware)

    def database() -> Any:
        if app.state.store is None:
            from data_horizon.store import Store

            kwargs: dict[str, Any] = {}
            if production:
                kwargs["ensure_schema"] = False
            try:
                app.state.store = Store(**kwargs)
            except Exception:
                LOGGER.warning("serving_store_unavailable")
                raise HTTPException(
                    503,
                    detail={
                        "code": "STORE_UNAVAILABLE",
                        "message": "Analytics storage is unavailable. Please try again shortly.",
                    },
                ) from None
        return app.state.store

    def call(method: str, **kwargs: Any) -> Any:
        try:
            return getattr(database(), method)(**kwargs)
        except HTTPException:
            raise
        except Exception as exc:
            # SQL errors and connection strings may contain credentials: log type only.
            LOGGER.warning("serving_query_failed operation=%s error_type=%s", method, type(exc).__name__)
            raise HTTPException(
                503,
                detail={
                    "code": "DEPENDENCY_UNAVAILABLE",
                    "message": "This information is temporarily unavailable. Please try again shortly.",
                },
            ) from None

    def authenticate(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)] = None,
    ) -> str:
        """Accept only server-held keys; browser cookies never grant access."""
        supplied = credentials.credentials if credentials else ""
        if token and secrets.compare_digest(supplied.encode(), token.encode()):
            return "analytics"
        if public_token and secrets.compare_digest(supplied.encode(), public_token.encode()):
            return "public"
        raise HTTPException(401, detail="Service key required", headers={"WWW-Authenticate": "Bearer"})

    def require_analytics(access: str = Depends(authenticate)) -> str:
        if access != "analytics":
            raise HTTPException(403, detail="This endpoint requires the analytics service key")
        return access

    def envelope(data: Any, *, status: dict[str, Any] | None = None) -> dict[str, Any]:
        status = call("status_snapshot") if status is None else status
        return {
            "data": data,
            "meta": {
                "mode": status.get("mode", os.getenv("DATA_MODE", "live")),
                "as_of": status.get("as_of"),
                "freshness_seconds": status.get("freshness_seconds"),
                "stale_after_seconds": getattr(getattr(database(), "settings", None), "stale_seconds", 120),
                "stale": bool(status.get("stale", True)),
                "time_basis": status.get("time_basis", "mixed; inspect each record"),
            },
        }

    def require_route(route_id: str) -> None:
        if not any(str(row.get("route_id")) == route_id for row in call("routes")):
            raise HTTPException(404, detail="Route not found")

    def validate_query(request: Request) -> None:
        route = request.scope.get("route")
        allowed = {field.alias for field in getattr(getattr(route, "dependant", None), "query_params", [])}
        unknown = sorted(set(request.query_params) - allowed)
        if unknown:
            raise HTTPException(422, detail={"code": "UNSUPPORTED_QUERY_PARAMETER", "parameters": unknown})

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            origin = request.headers.get("origin")
            if origin and origin not in origins:
                return JSONResponse(status_code=403, content={"detail": "Untrusted request origin"})
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/health/live", tags=["Health"])
    def live():
        return {"status": "alive", "service": "data-horizon-api"}

    @app.get("/health/ready", tags=["Health"])
    def ready():
        status = call("status_snapshot")
        return {"status": "ready", "data_status": "stale" if status.get("stale", True) else "fresh"}

    router = APIRouter(prefix="/api/v1", dependencies=[Depends(authenticate), Depends(validate_query)])

    @router.get("/status", tags=["Transit information"])
    def public_status():
        state = call("status_snapshot")
        return envelope(
            {key: state.get(key) for key in ("mode", "as_of", "freshness_seconds", "stale", "time_basis")}
            | {
                "city": "New York City",
                "stale_after_seconds": getattr(getattr(database(), "settings", None), "stale_seconds", 120),
                "routes": [row["route_id"] for row in call("routes")],
            },
            status=state,
        )

    @router.get("/routes", tags=["Network"])
    def routes(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0, le=100000)):
        return envelope(call("routes")[offset : offset + limit])

    @router.get("/stops", tags=["Network"])
    def stops(
        route_id: RouteID | None = None,
        limit: int = Query(500, ge=1, le=2000),
        offset: int = Query(0, ge=0, le=100000),
    ):
        if route_id:
            require_route(route_id)
        return envelope(call("stops", route_id=route_id)[offset : offset + limit])

    @router.get("/network/health", tags=["Analytics"])
    def network_health():
        data = dict(call("network_health"))
        catalogue = call("stops")
        data["stop_count"] = len({str(stop["stop_id"]) for stop in catalogue})
        if isinstance(data.get("routes"), list):
            data["routes"] = [
                {
                    **route,
                    "reading_entity_count": route.get("stop_count"),
                    "stop_count": len({str(stop["stop_id"]) for stop in catalogue
                                       if str(route.get("route_id")) in stop.get("route_ids", [])}),
                }
                for route in data["routes"]
            ]
        return envelope(data)

    @router.get("/routes/{route_id}/coverage", tags=["Analytics"], dependencies=[Depends(require_analytics)])
    def coverage(
        route_id: Annotated[str, APIPath(min_length=1, max_length=32, pattern=IDENTIFIER)],
        limit: int = Query(2000, ge=1, le=2000),
    ):
        from .route_coverage import route_coverage

        require_route(route_id)
        static = call("get_metadata", key="static", default={}) if callable(getattr(database(), "get_metadata", None)) else {}
        stale_after_seconds = getattr(getattr(database(), "settings", None), "stale_seconds", 120)
        result = route_coverage(
            route_id, call("stops", route_id=route_id), call("latest_metrics"),
            now=utcnow(), stale_after_seconds=stale_after_seconds, limit=limit, static=static,
        )
        return envelope(result)

    @router.get(
        "/routes/{route_id}/metrics", tags=["Analytics"], dependencies=[Depends(require_analytics)]
    )
    def metrics(
        route_id: Annotated[str, APIPath(min_length=1, max_length=32, pattern=IDENTIFIER)],
        stop_id: StopID | None = None,
        limit: int = Query(300, ge=1, le=2000),
    ):
        require_route(route_id)
        filters: dict[str, Any] = {"route_id": route_id, "limit": limit}
        if stop_id:
            if not any(str(row.get("stop_id")) == stop_id for row in call("stops", route_id=route_id)):
                raise HTTPException(404, detail="Stop not found on this route")
            filters["stop_id"] = stop_id
        return envelope(call("metrics", **filters))

    @router.get("/alerts", tags=["Analytics"])
    def alerts(route_id: RouteID | None = None, limit: int = Query(100, ge=1, le=1000)):
        rows = call("alerts")
        if route_id:
            require_route(route_id)
        stale_after_seconds = getattr(getattr(database(), "settings", None), "stale_seconds", 120)
        observations = call("latest_metrics") if callable(getattr(database(), "latest_metrics", None)) else []
        contexts = selection_contexts(
            call("routes"),
            call("stops", route_id=route_id),
            route_id=route_id,
            observations=fresh_observations(observations, utcnow(), stale_after_seconds),
        )
        state = call("status_snapshot")
        mode = state.get("mode", os.getenv("DATA_MODE", "live"))
        source_status = alert_source_status(
            mode,
            state.get("sources"),
            stale_after_seconds=stale_after_seconds,
            fixture_as_of=state.get("as_of"),
        )
        rows = [
            retained_alert(row, source_status)
            for row in rows
            if any(alert_matches(row, context) for context in contexts)
        ]
        return {
            "data": rows[:limit],
            "meta": {
                "mode": mode,
                "time_basis": state.get("time_basis", "mixed; inspect each record"),
                **source_status,
            },
        }

    @router.get("/anomalies", tags=["Analytics"], dependencies=[Depends(require_analytics)])
    def anomalies(route_id: RouteID | None = None, limit: int = Query(100, ge=1, le=1000)):
        rows = call("anomalies")
        if route_id:
            require_route(route_id)
            rows = [row for row in rows if str(row.get("route_id")) == route_id]
        return envelope(rows[:limit])

    @router.get("/pipeline/status", tags=["Operations"], dependencies=[Depends(require_analytics)])
    def pipeline_status():
        state = dict(call("pipeline_status"))
        return envelope(state, status=state)

    @router.post("/what-if", tags=["Scenarios"], dependencies=[Depends(require_analytics)])
    def what_if(body: ScenarioRequest):
        require_route(body.route_id)
        edges = call("graph_edges")
        if not edges:
            raise HTTPException(503, detail="The network graph is not available yet")
        from data_horizon.graph import what_if as evaluate

        try:
            result = evaluate(edges, **body.model_dump())
        except (ValueError, LookupError):
            raise HTTPException(404, detail="Scenario origin was not found in the network graph") from None
        return envelope(result)

    @app.get(
        "/metrics",
        response_class=PlainTextResponse,
        dependencies=[Depends(require_analytics)],
        tags=["Operations"],
    )
    def operational_metrics():
        status = call("pipeline_status")
        values = {"data_horizon_source_stale": int(bool(status.get("stale", True)))}
        if status.get("freshness_seconds") is not None:
            values["data_horizon_freshness_seconds"] = max(0.0, float(status["freshness_seconds"]))
        for name in ("events", "metrics", "quarantine", "batches"):
            values[f"data_horizon_{name}_total"] = int((status.get("counts") or {}).get(name, 0))
        streaming = status.get("streaming") or {}
        for field in ("input_rows_per_second", "processed_rows_per_second", "batch_duration_ms", "kafka_lag"):
            value = streaming.get(field)
            try:
                numeric = float(value) if value is not None and not isinstance(value, bool) else None
            except (TypeError, ValueError, OverflowError):
                numeric = None
            if numeric is not None and math.isfinite(numeric) and numeric >= 0:
                values[f"data_horizon_{field}"] = numeric
        gauges = {
            f"data_horizon_{field}"
            for field in (
                "input_rows_per_second",
                "processed_rows_per_second",
                "batch_duration_ms",
                "kafka_lag",
            )
        }
        lines = []
        for name, value in values.items():
            if name in gauges:
                lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")
        return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    app.include_router(router)
    return app


app = create_app()


def main(argv=None):
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="Data Horizon HTTP API service")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    uvicorn.run("data_horizon.api:app", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
