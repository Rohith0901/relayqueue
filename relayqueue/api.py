"""HTTP API (FastAPI)."""

from __future__ import annotations

import sqlite3
from typing import Any, Iterator

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from . import store
from .config import Settings, get_settings
from .db import connect, init_db


class EndpointIn(BaseModel):
    url: str
    description: str | None = None


class EventIn(BaseModel):
    type: str = Field(min_length=1, max_length=200)
    data: Any = Field(default_factory=dict)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    init_db(settings.db_path)
    app = FastAPI(title="RelayQueue", version="0.1.0")

    def db() -> Iterator[sqlite3.Connection]:
        conn = connect(settings.db_path)
        try:
            yield conn
        finally:
            conn.close()

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True}

    @app.post("/v1/endpoints", status_code=201)
    def create_endpoint(body: EndpointIn, conn=Depends(db)) -> dict:
        return store.create_endpoint(conn, body.url, body.description)

    @app.get("/v1/endpoints")
    def list_endpoints(conn=Depends(db)) -> list[dict]:
        return store.list_endpoints(conn)

    @app.post("/v1/events", status_code=202)
    def create_event(body: EventIn, conn=Depends(db)) -> dict:
        return store.create_event(conn, body.type, body.data)

    @app.get("/v1/events/{event_id}")
    def get_event(event_id: str, conn=Depends(db)) -> dict:
        evt = store.get_event(conn, event_id)
        if not evt:
            raise HTTPException(404, "event not found")
        return evt

    @app.get("/v1/deliveries")
    def list_deliveries(status: str | None = None, limit: int = 100, conn=Depends(db)) -> list[dict]:
        return store.list_deliveries(conn, status, min(limit, 1000))

    @app.get("/v1/deliveries/{delivery_id}")
    def get_delivery(delivery_id: str, conn=Depends(db)) -> dict:
        d = store.get_delivery(conn, delivery_id)
        if not d:
            raise HTTPException(404, "delivery not found")
        return d

    return app
