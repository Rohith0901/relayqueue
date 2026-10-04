"""Data access: every state transition of an event / delivery lives here.

Every read and write that a client can trigger is scoped by ``account_id``:
resources owned by another account are indistinguishable from resources that
do not exist.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import secrets
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .db import transaction
from .ids import new_id
from .signing import generate_secret

ACTIVE_STATUSES = ("pending", "retrying", "in_flight")
TERMINAL_STATUSES = ("succeeded", "failed")
API_KEY_PREFIX = "rq_"


class NotFound(Exception):
    pass


class IdempotencyConflict(Exception):
    """The idempotency key was already used with a different request body."""


class ReplayConflict(Exception):
    """The delivery is still scheduled or in flight; nothing to replay yet."""


def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds")


# --------------------------------------------------------------------------- accounts & keys

def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def create_account(conn: sqlite3.Connection, name: str) -> tuple[dict, str]:
    """Create an account and its first API key. The plaintext key is returned once."""
    acct_id = new_id("acct")
    now = time.time()
    with transaction(conn):
        conn.execute("INSERT INTO accounts (id, name, created_at) VALUES (?, ?, ?)", (acct_id, name, now))
        key_meta, plain = _insert_api_key(conn, acct_id, "default")
    return {"id": acct_id, "name": name, "created_at": iso(now), "api_key": key_meta}, plain


def _insert_api_key(conn: sqlite3.Connection, account_id: str, name: str) -> tuple[dict, str]:
    plain = API_KEY_PREFIX + secrets.token_urlsafe(32)
    key_id = new_id("key")
    now = time.time()
    conn.execute(
        "INSERT INTO api_keys (id, account_id, name, prefix, key_hash, created_at) VALUES (?,?,?,?,?,?)",
        (key_id, account_id, name, plain[:10], _hash_key(plain), now),
    )
    return {"id": key_id, "name": name, "prefix": plain[:10], "created_at": iso(now)}, plain


def create_api_key(conn: sqlite3.Connection, account_id: str, name: str) -> tuple[dict, str]:
    with transaction(conn):
        return _insert_api_key(conn, account_id, name)


def list_api_keys(conn: sqlite3.Connection, account_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM api_keys WHERE account_id = ? ORDER BY created_at", (account_id,)
    ).fetchall()
    return [{"id": r["id"], "name": r["name"], "prefix": r["prefix"], "created_at": iso(r["created_at"]),
             "last_used_at": iso(r["last_used_at"]), "revoked_at": iso(r["revoked_at"])} for r in rows]


def revoke_api_key(conn: sqlite3.Connection, account_id: str, key_id: str) -> None:
    with transaction(conn):
        cur = conn.execute(
            "UPDATE api_keys SET revoked_at = ? WHERE id = ? AND account_id = ? AND revoked_at IS NULL",
            (time.time(), key_id, account_id),
        )
    if cur.rowcount != 1:
        raise NotFound(key_id)


@dataclass(frozen=True)
class Principal:
    account_id: str
    account_name: str
    key_id: str
    key_name: str
    key_prefix: str

    @property
    def actor(self) -> str:
        return f"api_key:{self.key_name} ({self.key_prefix}…)"


def authenticate(conn: sqlite3.Connection, key: str) -> Principal | None:
    if not key or not key.startswith(API_KEY_PREFIX):
        return None
    row = conn.execute(
        "SELECT k.*, a.name AS account_name FROM api_keys k JOIN accounts a ON a.id = k.account_id"
        " WHERE k.key_hash = ? AND k.revoked_at IS NULL",
        (_hash_key(key),),
    ).fetchone()
    if not row:
        return None
    now = time.time()
    if not row["last_used_at"] or now - row["last_used_at"] > 60:  # throttle writes
        try:
            conn.execute("UPDATE api_keys SET last_used_at = ? WHERE id = ?", (now, row["id"]))
        except sqlite3.OperationalError:
            pass
    return Principal(row["account_id"], row["account_name"], row["id"], row["name"], row["prefix"])


# --------------------------------------------------------------------------- endpoints

def create_endpoint(
    conn: sqlite3.Connection,
    account_id: str,
    url: str,
    description: str | None = None,
    event_types: list[str] | None = None,
) -> dict:
    ep_id = new_id("ep")
    now = time.time()
    secret = generate_secret()
    with transaction(conn):
        conn.execute(
            "INSERT INTO endpoints (id, account_id, url, description, event_types, secret,"
            " created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ep_id, account_id, url, description, json.dumps(event_types or ["*"]), secret, now, now),
        )
    out = get_endpoint(conn, account_id, ep_id)
    out["secret"] = secret  # shown once at creation
    return out


def _endpoint_row(conn: sqlite3.Connection, account_id: str, ep_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM endpoints WHERE id = ? AND account_id = ?", (ep_id, account_id)
    ).fetchone()
    if not row:
        raise NotFound(ep_id)
    return row


def get_endpoint(conn: sqlite3.Connection, account_id: str, ep_id: str) -> dict:
    row = _endpoint_row(conn, account_id, ep_id)
    out = _endpoint(row)
    stats = conn.execute(
        "SELECT status, COUNT(*) AS n FROM deliveries WHERE endpoint_id = ? GROUP BY status", (ep_id,)
    ).fetchall()
    out["delivery_counts"] = {r["status"]: r["n"] for r in stats}
    return out


def get_endpoint_secret(conn: sqlite3.Connection, account_id: str, ep_id: str) -> dict:
    row = _endpoint_row(conn, account_id, ep_id)
    out = {"secret": row["secret"]}
    if row["previous_secret"] and (row["previous_secret_expires_at"] or 0) > time.time():
        out["previous_secret"] = row["previous_secret"]
        out["previous_secret_expires_at"] = iso(row["previous_secret_expires_at"])
    return out


def list_endpoints(conn: sqlite3.Connection, account_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM endpoints WHERE account_id = ? ORDER BY created_at", (account_id,)
    ).fetchall()
    return [_endpoint(r) for r in rows]


def update_endpoint(conn: sqlite3.Connection, account_id: str, ep_id: str, **changes: Any) -> dict:
    allowed = {"url", "description", "event_types", "active"}
    sets, args = [], []
    for k, v in changes.items():
        if k not in allowed or v is None:
            continue
        if k == "event_types":
            v = json.dumps(v)
        if k == "active":
            v = 1 if v else 0
        sets.append(f"{k} = ?")
        args.append(v)
    with transaction(conn):
        _endpoint_row(conn, account_id, ep_id)
        if sets:
            conn.execute(
                f"UPDATE endpoints SET {', '.join(sets)}, updated_at = ? WHERE id = ? AND account_id = ?",
                (*args, time.time(), ep_id, account_id),
            )
    return get_endpoint(conn, account_id, ep_id)


def rotate_secret(conn: sqlite3.Connection, account_id: str, ep_id: str, grace_seconds: float) -> dict:
    """Issue a new secret. The old one keeps signing (alongside) for ``grace_seconds``."""
    new_secret = generate_secret()
    now = time.time()
    with transaction(conn):
        _endpoint_row(conn, account_id, ep_id)
        conn.execute(
            "UPDATE endpoints SET previous_secret = secret, previous_secret_expires_at = ?,"
            " secret = ?, updated_at = ? WHERE id = ? AND account_id = ?",
            (now + grace_seconds, new_secret, now, ep_id, account_id),
        )
    return get_endpoint_secret(conn, account_id, ep_id)


def _endpoint(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "url": row["url"],
        "description": row["description"],
        "event_types": json.loads(row["event_types"]),
        "active": bool(row["active"]),
        "created_at": iso(row["created_at"]),
        "updated_at": iso(row["updated_at"]),
    }


def _matches(patterns: list[str], event_type: str) -> bool:
    return any(p == "*" or fnmatch.fnmatchcase(event_type, p) for p in patterns)


# --------------------------------------------------------------------------- events

def _request_hash(event_type: str, data: Any) -> str:
    canonical = json.dumps({"type": event_type, "data": data}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def create_event(
    conn: sqlite3.Connection,
    account_id: str,
    event_type: str,
    data: Any,
    max_attempts: int,
    idempotency_key: str | None = None,
) -> tuple[dict, bool]:
    """Persist an event and fan it out to matching endpoints in ONE transaction.

    Returns ``(event, created)``. When ``idempotency_key`` was already used
    (by this account) with the same payload, the original event is returned
    with ``created=False`` – producers can safely retry after a timeout.
    """
    now = time.time()
    req_hash = _request_hash(event_type, data)
    with transaction(conn):
        if idempotency_key is not None:
            existing = conn.execute(
                "SELECT id, request_hash FROM events WHERE account_id = ? AND idempotency_key = ?",
                (account_id, idempotency_key),
            ).fetchone()
            if existing:
                if existing["request_hash"] != req_hash:
                    raise IdempotencyConflict(idempotency_key)
                return get_event(conn, account_id, existing["id"]), False

        evt_id = new_id("evt")
        body = json.dumps(
            {"id": evt_id, "type": event_type, "created_at": iso(now), "data": data},
            separators=(",", ":"),
        )
        conn.execute(
            "INSERT INTO events (id, account_id, type, body, idempotency_key, request_hash, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (evt_id, account_id, event_type, body, idempotency_key, req_hash, now),
        )
        endpoints = conn.execute(
            "SELECT id, event_types FROM endpoints WHERE account_id = ? AND active = 1", (account_id,)
        ).fetchall()
        for ep in endpoints:
            if not _matches(json.loads(ep["event_types"]), event_type):
                continue
            conn.execute(
                "INSERT INTO deliveries (id, account_id, event_id, endpoint_id, status, max_attempts,"
                " next_attempt_at, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?)",
                (new_id("dlv"), account_id, evt_id, ep["id"], max_attempts, now, now, now),
            )
    return get_event(conn, account_id, evt_id), True


def get_event(conn: sqlite3.Connection, account_id: str, evt_id: str) -> dict:
    row = conn.execute(
        "SELECT * FROM events WHERE id = ? AND account_id = ?", (evt_id, account_id)
    ).fetchone()
    if not row:
        raise NotFound(evt_id)
    out = _event(row)
    out["deliveries"] = list_deliveries(conn, account_id, event_id=evt_id, limit=1000)
    return out


def list_events(conn: sqlite3.Connection, account_id: str, event_type: str | None = None,
                limit: int = 100) -> list[dict]:
    sql, args = "SELECT * FROM events WHERE account_id = ?", [account_id]
    if event_type:
        sql += " AND type = ?"
        args.append(event_type)
    sql += " ORDER BY created_at DESC LIMIT ?"
    args.append(limit)
    return [_event(r) for r in conn.execute(sql, args).fetchall()]


def _event(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "type": row["type"],
        "payload": json.loads(row["body"])["data"],
        "idempotency_key": row["idempotency_key"],
        "created_at": iso(row["created_at"]),
    }


# --------------------------------------------------------------------------- deliveries

_DELIVERY_SELECT = (
    "SELECT d.*, e.type AS event_type, e.created_at AS event_created_at, ep.url AS endpoint_url"
    " FROM deliveries d JOIN events e ON e.id = d.event_id JOIN endpoints ep ON ep.id = d.endpoint_id"
)


def _delivery(row: sqlite3.Row) -> dict:
    scheduled = row["status"] in ("pending", "retrying")
    latency = None
    if row["delivered_at"]:
        latency = round((row["delivered_at"] - row["event_created_at"]) * 1000, 1)
    return {
        "id": row["id"],
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "endpoint_id": row["endpoint_id"],
        "endpoint_url": row["endpoint_url"],
        "status": row["status"],
        "attempts": row["attempts"],
        "max_attempts": row["max_attempts"],
        "next_attempt_at": iso(row["next_attempt_at"]) if scheduled else None,
        "last_status_code": row["last_status_code"],
        "last_error": row["last_error"],
        "replay_count": row["replay_count"],
        "created_at": iso(row["created_at"]),
        "updated_at": iso(row["updated_at"]),
        "delivered_at": iso(row["delivered_at"]),
        "latency_ms": latency,
    }


def list_deliveries(
    conn: sqlite3.Connection,
    account_id: str,
    status: str | None = None,
    endpoint_id: str | None = None,
    event_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    sql = _DELIVERY_SELECT + " WHERE d.account_id = ?"
    args: list[Any] = [account_id]
    for col, val in (("d.status", status), ("d.endpoint_id", endpoint_id), ("d.event_id", event_id)):
        if val:
            sql += f" AND {col} = ?"
            args.append(val)
    sql += " ORDER BY d.updated_at DESC LIMIT ?"
    args.append(limit)
    return [_delivery(r) for r in conn.execute(sql, args).fetchall()]


def get_delivery(conn: sqlite3.Connection, account_id: str, dlv_id: str) -> dict:
    row = conn.execute(_DELIVERY_SELECT + " WHERE d.id = ? AND d.account_id = ?",
                       (dlv_id, account_id)).fetchone()
    if not row:
        raise NotFound(dlv_id)
    out = _delivery(row)
    out["attempt_log"] = [
        {
            "attempt_number": a["attempt_number"],
            "replay_id": a["replay_id"],
            "worker_id": a["worker_id"],
            "started_at": iso(a["started_at"]),
            "duration_ms": a["duration_ms"],
            "status_code": a["status_code"],
            "outcome": a["outcome"],
            "error": a["error"],
            "response_body": a["response_body"],
        }
        for a in conn.execute("SELECT * FROM attempts WHERE delivery_id = ? ORDER BY id", (dlv_id,))
    ]
    out["replays"] = [
        _replay(r) for r in conn.execute(
            "SELECT * FROM replays WHERE delivery_id = ? ORDER BY created_at", (dlv_id,))
    ]
    return out


# --------------------------------------------------------------------------- replay

def _replay(r: sqlite3.Row) -> dict:
    return {
        "id": r["id"],
        "delivery_id": r["delivery_id"],
        "actor": r["actor"],
        "reason": r["reason"],
        "previous_status": r["previous_status"],
        "previous_error": r["previous_error"],
        "previous_attempts": r["previous_attempts"],
        "created_at": iso(r["created_at"]),
    }


def _replay_locked(conn, account_id, dlv_id, principal: Principal | None, actor: str,
                   reason: str | None, max_attempts: int) -> dict:
    row = conn.execute(
        "SELECT * FROM deliveries WHERE id = ? AND account_id = ?", (dlv_id, account_id)
    ).fetchone()
    if not row:
        raise NotFound(dlv_id)
    if row["status"] not in TERMINAL_STATUSES:
        raise ReplayConflict(f"delivery is {row['status']}; replay is only possible once it has finished")
    now = time.time()
    replay_id = new_id("rpl")
    conn.execute(
        "INSERT INTO replays (id, account_id, delivery_id, api_key_id, actor, reason, previous_status,"
        " previous_error, previous_attempts, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (replay_id, account_id, dlv_id, principal.key_id if principal else None, actor, reason,
         row["status"], row["last_error"], row["attempts"], now),
    )
    conn.execute(
        "UPDATE deliveries SET status='pending', attempts=0, max_attempts=?, next_attempt_at=?,"
        " replay_count=replay_count+1, current_replay_id=?, delivered_at=NULL, updated_at=?"
        " WHERE id = ?",
        (max_attempts, now, replay_id, now, dlv_id),
    )
    return {"id": replay_id, "delivery_id": dlv_id, "actor": actor, "reason": reason,
            "previous_status": row["status"], "created_at": iso(now)}


def replay_delivery(conn: sqlite3.Connection, account_id: str, dlv_id: str,
                    principal: Principal | None, reason: str | None, max_attempts: int,
                    actor: str | None = None) -> dict:
    """Reschedule a finished delivery with a fresh attempt budget.

    The receiver gets the *same* event id (``webhook-id``) and identical body,
    so a receiver that deduplicates on the id stays correct.
    """
    actor = actor or (principal.actor if principal else "system")
    with transaction(conn):
        return _replay_locked(conn, account_id, dlv_id, principal, actor, reason, max_attempts)


def replay_many(conn: sqlite3.Connection, account_id: str, principal: Principal | None,
                reason: str | None, max_attempts: int, status: str = "failed",
                endpoint_id: str | None = None, event_id: str | None = None,
                limit: int = 1000) -> list[dict]:
    if status not in TERMINAL_STATUSES:
        raise ValueError("status must be 'failed' or 'succeeded'")
    actor = principal.actor if principal else "system"
    sql, args = "SELECT id FROM deliveries WHERE account_id = ? AND status = ?", [account_id, status]
    if endpoint_id:
        sql += " AND endpoint_id = ?"
        args.append(endpoint_id)
    if event_id:
        sql += " AND event_id = ?"
        args.append(event_id)
    sql += " ORDER BY created_at LIMIT ?"
    args.append(limit)
    with transaction(conn):
        ids = [r["id"] for r in conn.execute(sql, args).fetchall()]
        return [_replay_locked(conn, account_id, i, principal, actor, reason, max_attempts) for i in ids]


def list_replays(conn: sqlite3.Connection, account_id: str, limit: int = 100) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM replays WHERE account_id = ? ORDER BY created_at DESC LIMIT ?", (account_id, limit)
    ).fetchall()
    return [_replay(r) for r in rows]


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
    secrets: list[str] = field(default_factory=list)
    replay_id: str | None = None


def claim(conn: sqlite3.Connection, worker_id: str, limit: int, lease_seconds: float) -> list[Job]:
    """Atomically claim up to ``limit`` due deliveries for ``worker_id``.

    A claim is a lease: ``lease_expires_at`` is set ``lease_seconds`` in the
    future. If the worker crashes, the lease simply expires and the delivery
    becomes claimable again – no coordinator is needed.

    The attempt counter is incremented *at claim time*, so a payload that
    crashes every worker that touches it still exhausts its attempt budget
    instead of looping forever. Deliveries of paused (inactive) endpoints wait.
    """
    now = time.time()
    jobs: list[Job] = []
    with transaction(conn):
        rows = conn.execute(
            """
            SELECT d.id, d.event_id, d.endpoint_id, d.status, d.attempts, d.max_attempts,
                   d.lease_owner, d.lease_expires_at, d.current_replay_id,
                   e.type, e.body, ep.url, ep.secret, ep.previous_secret,
                   ep.previous_secret_expires_at
              FROM deliveries d
              JOIN events e     ON e.id = d.event_id
              JOIN endpoints ep ON ep.id = d.endpoint_id
             WHERE ep.active = 1
               AND ((d.status IN ('pending', 'retrying') AND d.next_attempt_at <= :now)
                 OR (d.status = 'in_flight' AND d.lease_expires_at <= :now))
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
                    "INSERT INTO attempts (delivery_id, attempt_number, replay_id, worker_id, started_at,"
                    " outcome, error) VALUES (?, ?, ?, ?, ?, 'abandoned', ?)",
                    (r["id"], r["attempts"], r["current_replay_id"], r["lease_owner"], r["lease_expires_at"],
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
            keys = [r["secret"]]
            if r["previous_secret"] and (r["previous_secret_expires_at"] or 0) > now:
                keys.append(r["previous_secret"])
            jobs.append(Job(
                delivery_id=r["id"], event_id=r["event_id"], event_type=r["type"],
                endpoint_id=r["endpoint_id"], url=r["url"], body=r["body"],
                attempt_number=r["attempts"] + 1, max_attempts=r["max_attempts"], lease_token=token,
                secrets=keys, replay_id=r["current_replay_id"],
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
    clear = "lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL"
    fence = "WHERE id=? AND lease_token=? AND status='in_flight'"
    with transaction(conn):
        if res.outcome == "success":
            cur = conn.execute(
                f"UPDATE deliveries SET status='succeeded', delivered_at=?, last_status_code=?,"
                f" last_error=NULL, {clear}, updated_at=? {fence}",
                (now, res.status_code, now, job.delivery_id, job.lease_token),
            )
        elif res.outcome == "retry":
            cur = conn.execute(
                f"UPDATE deliveries SET status='retrying', next_attempt_at=?, last_status_code=?,"
                f" last_error=?, {clear}, updated_at=? {fence}",
                (res.retry_at, res.status_code, res.error, now, job.delivery_id, job.lease_token),
            )
        else:
            cur = conn.execute(
                f"UPDATE deliveries SET status='failed', last_status_code=?, last_error=?,"
                f" {clear}, updated_at=? {fence}",
                (res.status_code, res.error, now, job.delivery_id, job.lease_token),
            )
        outcome = res.outcome if cur.rowcount == 1 else "stale"
        conn.execute(
            "INSERT INTO attempts (delivery_id, attempt_number, replay_id, worker_id, started_at,"
            " duration_ms, status_code, outcome, error, response_body) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (job.delivery_id, job.attempt_number, job.replay_id, worker_id, res.started_at,
             res.duration_ms, res.status_code, outcome, res.error, res.response_body),
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
