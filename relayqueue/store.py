"""Data access: every state transition of an event / delivery lives here."""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .db import transaction
from .ids import new_id

ACTIVE_STATUSES = ("pending", "retrying", "in_flight")
TERMINAL_STATUSES = ("succeeded", "failed")


class IdempotencyConflict(Exception):
    """The idempotency key was already used with a different request body."""


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

def _request_hash(event_type: str, data: Any) -> str:
    canonical = json.dumps({"type": event_type, "data": data}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def create_event(
    conn: sqlite3.Connection,
    event_type: str,
    data: Any,
    max_attempts: int,
    idempotency_key: str | None = None,
) -> tuple[dict, bool]:
    """Persist an event and fan it out to every endpoint in ONE transaction.

    Returns ``(event, created)``. When ``idempotency_key`` was already used
    with the same payload the original event is returned with
    ``created=False`` – the producer can safely retry its POST after a
    timeout without creating a second event.
    """
    now = time.time()
    req_hash = _request_hash(event_type, data)
    with transaction(conn):
        if idempotency_key is not None:
            existing = conn.execute(
                "SELECT id, request_hash FROM events WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if existing:
                if existing["request_hash"] != req_hash:
                    raise IdempotencyConflict(idempotency_key)
                evt_id, created = existing["id"], False
            else:
                evt_id, created = None, True
        else:
            evt_id, created = None, True

        if created:
            evt_id = new_id("evt")
            body = json.dumps(
                {"id": evt_id, "type": event_type, "created_at": iso(now), "data": data},
                separators=(",", ":"),
            )
            conn.execute(
                "INSERT INTO events (id, type, body, idempotency_key, request_hash, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (evt_id, event_type, body, idempotency_key, req_hash, now),
            )
            for ep in conn.execute("SELECT id FROM endpoints").fetchall():
                conn.execute(
                    "INSERT INTO deliveries (id, event_id, endpoint_id, status, max_attempts,"
                    " next_attempt_at, created_at, updated_at)"
                    " VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)",
                    (new_id("dlv"), evt_id, ep["id"], max_attempts, now, now, now),
                )
    return get_event(conn, evt_id), created


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
        "idempotency_key": row["idempotency_key"],
        "created_at": iso(row["created_at"]),
        "deliveries": [_delivery(d) for d in deliveries],
    }


# --------------------------------------------------------------------------- deliveries

def _delivery(row: sqlite3.Row) -> dict:
    keys = row.keys()
    out = {
        "id": row["id"],
        "event_id": row["event_id"],
        "endpoint_id": row["endpoint_id"],
        "status": row["status"],
        "attempts": row["attempts"],
        "max_attempts": row["max_attempts"],
        "next_attempt_at": iso(row["next_attempt_at"]) if row["status"] in ("pending", "retrying") else None,
        "last_status_code": row["last_status_code"],
        "last_error": row["last_error"],
        "created_at": iso(row["created_at"]),
        "updated_at": iso(row["updated_at"]),
        "delivered_at": iso(row["delivered_at"]),
    }
    if "event_type" in keys:
        out["event_type"] = row["event_type"]
    if "endpoint_url" in keys:
        out["endpoint_url"] = row["endpoint_url"]
    return out


def list_deliveries(
    conn: sqlite3.Connection,
    status: str | None = None,
    endpoint_id: str | None = None,
    event_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    sql = (
        "SELECT d.*, e.type AS event_type, ep.url AS endpoint_url FROM deliveries d"
        " JOIN events e ON e.id = d.event_id JOIN endpoints ep ON ep.id = d.endpoint_id WHERE 1=1"
    )
    args: list[Any] = []
    for col, val in (("d.status", status), ("d.endpoint_id", endpoint_id), ("d.event_id", event_id)):
        if val:
            sql += f" AND {col} = ?"
            args.append(val)
    sql += " ORDER BY d.updated_at DESC LIMIT ?"
    args.append(limit)
    return [_delivery(r) for r in conn.execute(sql, args).fetchall()]


def get_delivery(conn: sqlite3.Connection, dlv_id: str) -> dict | None:
    row = conn.execute(
        "SELECT d.*, e.type AS event_type, ep.url AS endpoint_url FROM deliveries d"
        " JOIN events e ON e.id = d.event_id JOIN endpoints ep ON ep.id = d.endpoint_id"
        " WHERE d.id = ?",
        (dlv_id,),
    ).fetchone()
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
            "response_body": a["response_body"],
        }
        for a in conn.execute(
            "SELECT * FROM attempts WHERE delivery_id = ? ORDER BY id", (dlv_id,)
        ).fetchall()
    ]
    return out


# --------------------------------------------------------------------------- worker side

@dataclass
class Job:
    delivery_id: str
    event_id: str
    event_type: str
    endpoint_id: str
    url: str
    body: str
    attempt_number: int
    max_attempts: int
    lease_token: str


def claim(conn: sqlite3.Connection, worker_id: str, limit: int, lease_seconds: float) -> list[Job]:
    """Atomically claim up to ``limit`` due deliveries for ``worker_id``.

    A claim is a lease: ``lease_expires_at`` is set ``lease_seconds`` in the
    future. If the worker crashes, the lease simply expires and the delivery
    becomes claimable again – no coordinator or heartbeat protocol is needed.

    The attempt counter is incremented *at claim time*, so a payload that
    crashes every worker that touches it still exhausts its attempt budget
    instead of looping forever.
    """
    now = time.time()
    jobs: list[Job] = []
    with transaction(conn):
        rows = conn.execute(
            """
            SELECT d.id, d.event_id, d.endpoint_id, d.status, d.attempts, d.max_attempts,
                   d.lease_owner, d.lease_expires_at, e.type, e.body, ep.url
              FROM deliveries d
              JOIN events e     ON e.id = d.event_id
              JOIN endpoints ep ON ep.id = d.endpoint_id
             WHERE (d.status IN ('pending', 'retrying') AND d.next_attempt_at <= :now)
                OR (d.status = 'in_flight' AND d.lease_expires_at <= :now)
             ORDER BY d.next_attempt_at
             LIMIT :limit
            """,
            {"now": now, "limit": limit},
        ).fetchall()
        for r in rows:
            if r["status"] == "in_flight":
                # The previous owner crashed or stalled: record the lost attempt.
                conn.execute(
                    "INSERT INTO lease_recoveries (delivery_id, previous_owner, lease_expired_at,"
                    " reclaimed_at, reclaimed_by) VALUES (?, ?, ?, ?, ?)",
                    (r["id"], r["lease_owner"], r["lease_expires_at"], now, worker_id),
                )
                conn.execute(
                    "INSERT INTO attempts (delivery_id, attempt_number, worker_id, started_at,"
                    " outcome, error) VALUES (?, ?, ?, ?, 'abandoned', ?)",
                    (r["id"], r["attempts"], r["lease_owner"], r["lease_expires_at"],
                     "claim expired before a result was recorded (worker crashed or stalled)"),
                )
                if r["attempts"] >= r["max_attempts"]:
                    conn.execute(
                        "UPDATE deliveries SET status='failed', lease_owner=NULL, lease_token=NULL,"
                        " lease_expires_at=NULL, last_error=?, updated_at=? WHERE id=?",
                        ("attempts exhausted (last claim expired)", now, r["id"]),
                    )
                    continue
            token = secrets.token_hex(8)
            conn.execute(
                "UPDATE deliveries SET status='in_flight', attempts=attempts+1, lease_owner=?,"
                " lease_token=?, lease_expires_at=?, updated_at=? WHERE id=?",
                (worker_id, token, now + lease_seconds, now, r["id"]),
            )
            jobs.append(Job(
                delivery_id=r["id"], event_id=r["event_id"], event_type=r["type"],
                endpoint_id=r["endpoint_id"], url=r["url"], body=r["body"],
                attempt_number=r["attempts"] + 1, max_attempts=r["max_attempts"], lease_token=token,
            ))
    return jobs


@dataclass
class Result:
    outcome: str                  # success | retry | failed
    status_code: int | None
    error: str | None
    response_body: str | None
    started_at: float
    duration_ms: float
    retry_at: float | None = None


def record_result(conn: sqlite3.Connection, job: Job, worker_id: str, res: Result) -> str:
    """Write an attempt and transition the delivery.

    The update is fenced on ``lease_token``: if this worker's claim expired and
    another worker took the delivery over, the late result is logged as
    ``stale`` but does not overwrite the newer state.
    """
    now = time.time()
    with transaction(conn):
        if res.outcome == "success":
            cur = conn.execute(
                "UPDATE deliveries SET status='succeeded', delivered_at=?, last_status_code=?,"
                " last_error=NULL, lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL,"
                " updated_at=? WHERE id=? AND lease_token=? AND status='in_flight'",
                (now, res.status_code, now, job.delivery_id, job.lease_token),
            )
        elif res.outcome == "retry":
            cur = conn.execute(
                "UPDATE deliveries SET status='retrying', next_attempt_at=?, last_status_code=?,"
                " last_error=?, lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL,"
                " updated_at=? WHERE id=? AND lease_token=? AND status='in_flight'",
                (res.retry_at, res.status_code, res.error, now, job.delivery_id, job.lease_token),
            )
        else:
            cur = conn.execute(
                "UPDATE deliveries SET status='failed', last_status_code=?, last_error=?,"
                " lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL, updated_at=?"
                " WHERE id=? AND lease_token=? AND status='in_flight'",
                (res.status_code, res.error, now, job.delivery_id, job.lease_token),
            )
        outcome = res.outcome if cur.rowcount == 1 else "stale"
        conn.execute(
            "INSERT INTO attempts (delivery_id, attempt_number, worker_id, started_at, duration_ms,"
            " status_code, outcome, error, response_body) VALUES (?,?,?,?,?,?,?,?,?)",
            (job.delivery_id, job.attempt_number, worker_id, res.started_at, res.duration_ms,
             res.status_code, outcome, res.error, res.response_body),
        )
    return outcome


def heartbeat(conn: sqlite3.Connection, worker_id: str, pid: int, hostname: str,
              processed: int, stopped: bool = False) -> None:
    now = time.time()
    with transaction(conn):
        conn.execute(
            "INSERT INTO workers (worker_id, pid, hostname, started_at, last_seen_at, processed)"
            " VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(worker_id) DO UPDATE SET"
            " last_seen_at=excluded.last_seen_at, processed=excluded.processed,"
            " pid=excluded.pid, stopped_at=?",
            (worker_id, pid, hostname, now, now, processed, now if stopped else None),
        )
