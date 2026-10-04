"""Data access: every state transition of an event / delivery lives here."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .db import transaction
from .ids import new_id


def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds")


# --------------------------------------------------------------------------- endpoints

def create_endpoint(conn: sqlite3.Connection, url: str, description: str | None = None) -> dict:
    ep_id = new_id("ep")
    now = time.time()
    with transaction(conn):
        conn.execute(
            "INSERT INTO endpoints (id, url, description, created_at) VALUES (?, ?, ?, ?)",
            (ep_id, url, description, now),
        )
    return get_endpoint(conn, ep_id)


def get_endpoint(conn: sqlite3.Connection, ep_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM endpoints WHERE id = ?", (ep_id,)).fetchone()
    return _endpoint(row) if row else None


def list_endpoints(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM endpoints ORDER BY created_at").fetchall()
    return [_endpoint(r) for r in rows]


def _endpoint(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "url": row["url"],
        "description": row["description"],
        "created_at": iso(row["created_at"]),
    }


# --------------------------------------------------------------------------- events

def create_event(conn: sqlite3.Connection, event_type: str, data: Any) -> dict:
    """Persist an event and fan it out to every endpoint in ONE transaction.

    Either the event and all of its deliveries are durable, or none are – there
    is no window in which an accepted event has no delivery rows.
    """
    now = time.time()
    evt_id = new_id("evt")
    body = json.dumps(
        {"id": evt_id, "type": event_type, "created_at": iso(now), "data": data},
        separators=(",", ":"),
    )
    with transaction(conn):
        conn.execute(
            "INSERT INTO events (id, type, body, created_at) VALUES (?, ?, ?, ?)",
            (evt_id, event_type, body, now),
        )
        endpoints = conn.execute("SELECT id FROM endpoints").fetchall()
        for ep in endpoints:
            conn.execute(
                "INSERT INTO deliveries (id, event_id, endpoint_id, status, created_at, updated_at)"
                " VALUES (?, ?, ?, 'pending', ?, ?)",
                (new_id("dlv"), evt_id, ep["id"], now, now),
            )
    return get_event(conn, evt_id)


def get_event(conn: sqlite3.Connection, evt_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM events WHERE id = ?", (evt_id,)).fetchone()
    if not row:
        return None
    deliveries = conn.execute(
        "SELECT * FROM deliveries WHERE event_id = ? ORDER BY created_at", (evt_id,)
    ).fetchall()
    return {
        "id": row["id"],
        "type": row["type"],
        "payload": json.loads(row["body"])["data"],
        "created_at": iso(row["created_at"]),
        "deliveries": [_delivery(d) for d in deliveries],
    }


# --------------------------------------------------------------------------- deliveries

def _delivery(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "event_id": row["event_id"],
        "endpoint_id": row["endpoint_id"],
        "status": row["status"],
        "attempts": row["attempts"],
        "last_status_code": row["last_status_code"],
        "last_error": row["last_error"],
        "created_at": iso(row["created_at"]),
        "updated_at": iso(row["updated_at"]),
        "delivered_at": iso(row["delivered_at"]),
    }


def list_deliveries(conn: sqlite3.Connection, status: str | None = None, limit: int = 100) -> list[dict]:
    sql, args = "SELECT * FROM deliveries", []
    if status:
        sql += " WHERE status = ?"
        args.append(status)
    sql += " ORDER BY created_at DESC LIMIT ?"
    args.append(limit)
    return [_delivery(r) for r in conn.execute(sql, args).fetchall()]


def get_delivery(conn: sqlite3.Connection, dlv_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM deliveries WHERE id = ?", (dlv_id,)).fetchone()
    if not row:
        return None
    out = _delivery(row)
    out["attempt_log"] = [
        {
            "attempt_number": a["attempt_number"],
            "worker_id": a["worker_id"],
            "started_at": iso(a["started_at"]),
            "duration_ms": a["duration_ms"],
            "status_code": a["status_code"],
            "outcome": a["outcome"],
            "error": a["error"],
        }
        for a in conn.execute(
            "SELECT * FROM attempts WHERE delivery_id = ? ORDER BY id", (dlv_id,)
        ).fetchall()
    ]
    return out


@dataclass
class Job:
    delivery_id: str
    event_id: str
    event_type: str
    url: str
    body: str
    attempt_number: int


def claim(conn: sqlite3.Connection, worker_id: str, limit: int) -> list[Job]:
    """Atomically move up to ``limit`` pending deliveries to ``in_flight``."""
    now = time.time()
    with transaction(conn):
        rows = conn.execute(
            """
            SELECT d.id, d.event_id, d.attempts, e.type, e.body, ep.url
              FROM deliveries d
              JOIN events e     ON e.id = d.event_id
              JOIN endpoints ep ON ep.id = d.endpoint_id
             WHERE d.status = 'pending'
             ORDER BY d.created_at
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
        jobs = []
        for r in rows:
            conn.execute(
                "UPDATE deliveries SET status='in_flight', attempts=attempts+1, updated_at=? WHERE id=?",
                (now, r["id"]),
            )
            jobs.append(Job(r["id"], r["event_id"], r["type"], r["url"], r["body"], r["attempts"] + 1))
    return jobs


def record_result(
    conn: sqlite3.Connection,
    job: Job,
    worker_id: str,
    started_at: float,
    duration_ms: float,
    status_code: int | None,
    error: str | None,
    response_body: str | None,
) -> str:
    ok = status_code is not None and 200 <= status_code < 300
    outcome = "success" if ok else "failed"
    now = time.time()
    with transaction(conn):
        conn.execute(
            "INSERT INTO attempts (delivery_id, attempt_number, worker_id, started_at, duration_ms,"
            " status_code, outcome, error, response_body) VALUES (?,?,?,?,?,?,?,?,?)",
            (job.delivery_id, job.attempt_number, worker_id, started_at, duration_ms,
             status_code, outcome, error, response_body),
        )
        conn.execute(
            "UPDATE deliveries SET status=?, last_status_code=?, last_error=?, updated_at=?,"
            " delivered_at=? WHERE id=?",
            ("succeeded" if ok else "failed", status_code, error, now, now if ok else None,
             job.delivery_id),
        )
    return outcome
