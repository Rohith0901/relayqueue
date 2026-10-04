"""Delivery statistics: success rate, latency, retries, recovery."""

from __future__ import annotations

import sqlite3
import time
from typing import Any, Sequence


def percentile(values: Sequence[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return round(s[lo] + (s[hi] - s[lo]) * (k - lo), 1)


def _dist(values: Sequence[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": round(max(values), 1) if values else None,
        "mean": round(sum(values) / len(values), 1) if values else None,
    }


def stats(conn: sqlite3.Connection, account_id: str | None = None,
          window_seconds: float | None = None, timeline_seconds: int = 300,
          bucket_seconds: int = 10) -> dict[str, Any]:
    """Compute delivery statistics, optionally scoped to one account / time window."""
    now = time.time()
    since = now - window_seconds if window_seconds else 0
    acct_d = " AND d.account_id = :acct" if account_id else ""
    params = {"acct": account_id, "since": since, "now": now}

    counts = {r["status"]: r["n"] for r in conn.execute(
        f"SELECT d.status, COUNT(*) n FROM deliveries d WHERE d.created_at >= :since{acct_d}"
        " GROUP BY d.status", params)}
    succeeded, failed = counts.get("succeeded", 0), counts.get("failed", 0)
    total = sum(counts.values())

    # End-to-end latency: event accepted -> receiver acknowledged (first-time deliveries only).
    latencies = [r[0] * 1000 for r in conn.execute(
        f"SELECT d.delivered_at - e.created_at FROM deliveries d JOIN events e ON e.id = d.event_id"
        f" WHERE d.status='succeeded' AND d.replay_count = 0 AND d.created_at >= :since{acct_d}",
        params)]

    attempt_rows = conn.execute(
        f"SELECT a.outcome, a.duration_ms, a.attempt_number, a.status_code FROM attempts a"
        f" JOIN deliveries d ON d.id = a.delivery_id WHERE a.started_at >= :since{acct_d}",
        params).fetchall()
    outcomes: dict[str, int] = {}
    durations = []
    for r in attempt_rows:
        outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1
        if r["duration_ms"] is not None:
            durations.append(r["duration_ms"])

    first_try = conn.execute(
        f"SELECT COUNT(*) FROM deliveries d WHERE d.status='succeeded' AND d.attempts = 1"
        f" AND d.replay_count = 0 AND d.created_at >= :since{acct_d}"
        f" AND NOT EXISTS (SELECT 1 FROM attempts a WHERE a.delivery_id = d.id AND a.outcome != 'success')",
        params).fetchone()[0]

    queue = conn.execute(
        f"""SELECT
              SUM(CASE WHEN d.status IN ('pending','retrying') AND d.next_attempt_at <= :now THEN 1 ELSE 0 END) due,
              SUM(CASE WHEN d.status = 'retrying' AND d.next_attempt_at > :now THEN 1 ELSE 0 END) scheduled,
              SUM(CASE WHEN d.status = 'in_flight' THEN 1 ELSE 0 END) in_flight,
              MIN(CASE WHEN d.status IN ('pending','retrying') THEN d.created_at END) oldest
            FROM deliveries d WHERE 1=1{acct_d}""", params).fetchone()

    recov = conn.execute(
        f"SELECT r.reclaimed_at - r.lease_expired_at AS delay FROM lease_recoveries r"
        f" JOIN deliveries d ON d.id = r.delivery_id WHERE r.reclaimed_at >= :since{acct_d}",
        params).fetchall()

    # Timeline for the dashboard: attempt outcomes per bucket over the last few minutes.
    t0 = now - timeline_seconds
    buckets = [{"t": int(t0 + i * bucket_seconds), "success": 0, "retry": 0, "failed": 0, "abandoned": 0}
               for i in range(timeline_seconds // bucket_seconds)]
    for r in conn.execute(
            f"SELECT a.started_at, a.outcome FROM attempts a JOIN deliveries d ON d.id = a.delivery_id"
            f" WHERE a.started_at >= :t0{acct_d}", {**params, "t0": t0}):
        i = int((r["started_at"] - t0) // bucket_seconds)
        if 0 <= i < len(buckets) and r["outcome"] in buckets[i]:
            buckets[i][r["outcome"]] += 1

    workers = conn.execute("SELECT * FROM workers ORDER BY started_at DESC LIMIT 50").fetchall()
    alive = [w for w in workers if w["stopped_at"] is None and now - w["last_seen_at"] < 15]

    return {
        "generated_at": now,
        "window_seconds": window_seconds,
        "deliveries": {"total": total, **{k: counts.get(k, 0) for k in
                                          ("pending", "in_flight", "retrying", "succeeded", "failed")}},
        "success_rate": round(succeeded / (succeeded + failed), 4) if succeeded + failed else None,
        "first_attempt_success_rate": round(first_try / succeeded, 4) if succeeded else None,
        "end_to_end_latency_ms": _dist(latencies),
        "attempt_duration_ms": _dist(durations),
        "attempts": {"total": len(attempt_rows), **outcomes},
        "queue": {
            "due_now": queue["due"] or 0,
            "scheduled_retries": queue["scheduled"] or 0,
            "in_flight": queue["in_flight"] or 0,
            "oldest_pending_age_s": round(now - queue["oldest"], 1) if queue["oldest"] else None,
        },
        "recovery": {
            "expired_claims_reclaimed": len(recov),
            "reclaim_delay_after_expiry_ms": _dist([r["delay"] * 1000 for r in recov]),
        },
        "workers": {
            "alive": len(alive),
            "known": [{"worker_id": w["worker_id"], "alive": w in alive, "processed": w["processed"],
                       "last_seen_s_ago": round(now - w["last_seen_at"], 1)} for w in workers[:12]],
        },
        "timeline": {"bucket_seconds": bucket_seconds, "buckets": buckets},
    }
