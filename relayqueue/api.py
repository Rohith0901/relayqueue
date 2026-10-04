"""HTTP API (FastAPI).

Authentication: ``Authorization: Bearer rq_…`` (or ``X-API-Key``). Every
resource belongs to the account that owns the key; other accounts' resources
return 404. Account creation is an admin operation guarded by
``RELAYQUEUE_ADMIN_TOKEN`` (or done with ``python -m relayqueue create-account``).
"""

from __future__ import annotations

import hmac
import sqlite3
from importlib import resources
from typing import Any, Iterator, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from . import metrics, store
from .config import Settings, get_settings
from .db import connect, init_db
from .urlguard import UnsafeURL, validate_url


class AccountIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class ApiKeyIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class EndpointIn(BaseModel):
    url: str = Field(max_length=2048)
    description: str | None = Field(default=None, max_length=500)
    event_types: list[str] = Field(default_factory=lambda: ["*"], max_length=100)


class EndpointPatch(BaseModel):
    url: str | None = Field(default=None, max_length=2048)
    description: str | None = Field(default=None, max_length=500)
    event_types: list[str] | None = Field(default=None, max_length=100)
    active: bool | None = None


class EventIn(BaseModel):
    type: str = Field(min_length=1, max_length=200)
    data: Any = Field(default_factory=dict)


class ReplayIn(BaseModel):
    reason: str | None = Field(default=None, max_length=500)


class BulkReplayIn(ReplayIn):
    status: Literal["failed", "succeeded"] = "failed"
    endpoint_id: str | None = None
    limit: int = Field(default=100, ge=1, le=1000)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = (settings or get_settings()).validate()
    init_db(settings.db_path)
    app = FastAPI(title="RelayQueue", version="0.3.0",
                  description="Reliable webhook delivery: persistence, retries, signing, replay.")

    @app.exception_handler(store.NotFound)
    async def _not_found(_: Request, exc: store.NotFound):
        return JSONResponse({"detail": "not found"}, status_code=404)

    @app.exception_handler(UnsafeURL)
    async def _unsafe(_: Request, exc: UnsafeURL):
        return JSONResponse({"detail": f"destination URL rejected: {exc}"}, status_code=422)

    def db() -> Iterator[sqlite3.Connection]:
        conn = connect(settings.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def principal(
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None),
        conn: sqlite3.Connection = Depends(db),
    ) -> store.Principal:
        key = x_api_key
        if authorization and authorization.lower().startswith("bearer "):
            key = authorization[7:].strip()
        p = store.authenticate(conn, key) if key else None
        if p is None:
            raise HTTPException(401, "missing or invalid API key",
                                headers={"WWW-Authenticate": "Bearer"})
        return p

    def admin(x_admin_token: str | None = Header(default=None)) -> None:
        if not settings.admin_token:
            raise HTTPException(403, "admin API disabled (set RELAYQUEUE_ADMIN_TOKEN)")
        if not x_admin_token or not hmac.compare_digest(x_admin_token, settings.admin_token):
            raise HTTPException(401, "invalid admin token")

    def check_url(url: str) -> str:
        return validate_url(url, allow_private=settings.allow_private_networks)

    # ------------------------------------------------------------------ misc

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> str:
        return resources.files("relayqueue").joinpath("static/dashboard.html").read_text()

    @app.get("/healthz")
    def healthz(conn=Depends(db)) -> dict:
        conn.execute("SELECT 1").fetchone()
        return {"ok": True}

    # ------------------------------------------------------------------ accounts

    @app.post("/v1/admin/accounts", status_code=201, dependencies=[Depends(admin)])
    def create_account(body: AccountIn, conn=Depends(db)) -> dict:
        acct, key = store.create_account(conn, body.name)
        acct["api_key"]["key"] = key
        return acct

    @app.get("/v1/account")
    def whoami(p: store.Principal = Depends(principal)) -> dict:
        return {"account_id": p.account_id, "account_name": p.account_name,
                "api_key": {"id": p.key_id, "name": p.key_name, "prefix": p.key_prefix}}

    @app.post("/v1/api-keys", status_code=201)
    def create_key(body: ApiKeyIn, p=Depends(principal), conn=Depends(db)) -> dict:
        meta, key = store.create_api_key(conn, p.account_id, body.name)
        return {**meta, "key": key}

    @app.get("/v1/api-keys")
    def list_keys(p=Depends(principal), conn=Depends(db)) -> list[dict]:
        return store.list_api_keys(conn, p.account_id)

    @app.delete("/v1/api-keys/{key_id}", status_code=204)
    def revoke_key(key_id: str, p=Depends(principal), conn=Depends(db)) -> Response:
        store.revoke_api_key(conn, p.account_id, key_id)
        return Response(status_code=204)

    # ------------------------------------------------------------------ endpoints

    @app.post("/v1/endpoints", status_code=201)
    def create_endpoint(body: EndpointIn, p=Depends(principal), conn=Depends(db)) -> dict:
        """Register a destination. The signing secret is returned once."""
        return store.create_endpoint(conn, p.account_id, check_url(body.url), body.description,
                                     body.event_types)

    @app.get("/v1/endpoints")
    def list_endpoints(p=Depends(principal), conn=Depends(db)) -> list[dict]:
        return store.list_endpoints(conn, p.account_id)

    @app.get("/v1/endpoints/{endpoint_id}")
    def get_endpoint(endpoint_id: str, p=Depends(principal), conn=Depends(db)) -> dict:
        return store.get_endpoint(conn, p.account_id, endpoint_id)

    @app.patch("/v1/endpoints/{endpoint_id}")
    def patch_endpoint(endpoint_id: str, body: EndpointPatch, p=Depends(principal), conn=Depends(db)) -> dict:
        changes = body.model_dump(exclude_unset=True)
        if changes.get("url"):
            changes["url"] = check_url(changes["url"])
        return store.update_endpoint(conn, p.account_id, endpoint_id, **changes)

    @app.get("/v1/endpoints/{endpoint_id}/secret")
    def endpoint_secret(endpoint_id: str, p=Depends(principal), conn=Depends(db)) -> dict:
        return store.get_endpoint_secret(conn, p.account_id, endpoint_id)

    @app.post("/v1/endpoints/{endpoint_id}/rotate-secret")
    def rotate_secret(endpoint_id: str, p=Depends(principal), conn=Depends(db)) -> dict:
        """New secret; the old one keeps signing alongside it for the grace period."""
        return store.rotate_secret(conn, p.account_id, endpoint_id, settings.rotation_grace_seconds)

    # ------------------------------------------------------------------ events

    @app.post("/v1/events", status_code=202)
    def create_event(
        body: EventIn,
        response: Response,
        idempotency_key: str | None = Header(default=None, max_length=255),
        p=Depends(principal),
        conn=Depends(db),
    ) -> dict:
        """Accept an event. Send an ``Idempotency-Key`` header to make retries safe."""
        try:
            evt, created = store.create_event(conn, p.account_id, body.type, body.data,
                                              settings.max_attempts, idempotency_key)
        except store.IdempotencyConflict:
            raise HTTPException(409, "Idempotency-Key was already used with a different payload")
        if not created:
            response.status_code = 200
            response.headers["Idempotent-Replayed"] = "true"
        return evt

    @app.get("/v1/events")
    def list_events(type: str | None = None, limit: int = 100, p=Depends(principal), conn=Depends(db)):
        return store.list_events(conn, p.account_id, type, min(limit, 1000))

    @app.get("/v1/events/{event_id}")
    def get_event(event_id: str, p=Depends(principal), conn=Depends(db)) -> dict:
        return store.get_event(conn, p.account_id, event_id)

    @app.post("/v1/events/{event_id}/replay")
    def replay_event(event_id: str, body: ReplayIn, p=Depends(principal), conn=Depends(db)) -> dict:
        """Replay every finished delivery of an event (same event id, same body)."""
        store.get_event(conn, p.account_id, event_id)
        out = []
        for status in store.TERMINAL_STATUSES:
            out += store.replay_many(conn, p.account_id, p, body.reason, settings.max_attempts,
                                     status=status, event_id=event_id)
        return {"replayed": len(out), "replays": out}

    # ------------------------------------------------------------------ deliveries

    @app.get("/v1/deliveries")
    def list_deliveries(status: str | None = None, endpoint_id: str | None = None,
                        event_id: str | None = None, limit: int = 100,
                        p=Depends(principal), conn=Depends(db)) -> list[dict]:
        return store.list_deliveries(conn, p.account_id, status, endpoint_id, event_id, min(limit, 1000))

    @app.get("/v1/failed-deliveries")
    def failed_deliveries(endpoint_id: str | None = None, limit: int = 100,
                          p=Depends(principal), conn=Depends(db)) -> list[dict]:
        """Deliveries that permanently failed or ran out of attempts."""
        return store.list_deliveries(conn, p.account_id, "failed", endpoint_id, None, min(limit, 1000))

    @app.get("/v1/deliveries/{delivery_id}")
    def get_delivery(delivery_id: str, p=Depends(principal), conn=Depends(db)) -> dict:
        """Delivery with its full attempt history and replay audit trail."""
        return store.get_delivery(conn, p.account_id, delivery_id)

    @app.post("/v1/deliveries/{delivery_id}/replay", status_code=202)
    def replay_delivery(delivery_id: str, body: ReplayIn, p=Depends(principal), conn=Depends(db)) -> dict:
        try:
            return store.replay_delivery(conn, p.account_id, delivery_id, p, body.reason,
                                         settings.max_attempts)
        except store.ReplayConflict as exc:
            raise HTTPException(409, str(exc))

    @app.post("/v1/deliveries/replay", status_code=202)
    def replay_bulk(body: BulkReplayIn, p=Depends(principal), conn=Depends(db)) -> dict:
        out = store.replay_many(conn, p.account_id, p, body.reason, settings.max_attempts,
                                status=body.status, endpoint_id=body.endpoint_id, limit=body.limit)
        return {"replayed": len(out), "replays": out}

    @app.get("/v1/replays")
    def list_replays(limit: int = 100, p=Depends(principal), conn=Depends(db)) -> list[dict]:
        """Audit trail of manual replays for this account."""
        return store.list_replays(conn, p.account_id, min(limit, 1000))

    @app.get("/v1/stats")
    def get_stats(window: float | None = None, p=Depends(principal), conn=Depends(db)) -> dict:
        return metrics.stats(conn, p.account_id, window)

    return app
