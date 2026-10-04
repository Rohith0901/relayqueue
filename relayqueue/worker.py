"""Delivery worker: claims due deliveries, POSTs them, schedules retries.

Many workers (processes or hosts) can run against the same database. Each
claim is a time-limited lease, so a worker that is killed mid-delivery simply
stops renewing nothing – its claims expire and another worker picks them up.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import time

import httpx

from . import store
from .config import Settings, get_settings
from .db import connect, init_db
from .ids import new_id
from .retry import backoff_delay, is_retryable_status, is_success, parse_retry_after

log = logging.getLogger("relayqueue.worker")


def classify(
    job: store.Job,
    settings: Settings,
    status: int | None,
    error: str | None,
    retry_after: str | None = None,
    now: float | None = None,
) -> tuple[str, str | None, float | None]:
    """Decide what happens after an attempt: ``(outcome, error, retry_at)``.

    * 2xx                                   -> success
    * timeout / connection error / 5xx /
      408, 409, 425, 429                    -> temporary: retry with backoff
    * any other status (4xx, 3xx)           -> permanent: fail immediately
    * temporary but out of attempts         -> failed (attempts exhausted)
    """
    now = time.time() if now is None else now
    if status is not None and is_success(status):
        return "success", None, None

    temporary = status is None or is_retryable_status(status)
    reason = error if status is None else f"HTTP {status}"
    if not temporary:
        return "failed", f"permanent failure: {reason}", None
    if job.attempt_number >= job.max_attempts:
        return "failed", f"attempts exhausted ({job.attempt_number}/{job.max_attempts}): {reason}", None

    delay = backoff_delay(job.attempt_number, settings.backoff_base, settings.backoff_max)
    hinted = parse_retry_after(retry_after, settings.backoff_max)
    if hinted is not None:
        delay = max(delay, hinted)
    return "retry", f"temporary failure: {reason}", now + delay


class Worker:
    def __init__(self, settings: Settings | None = None, worker_id: str | None = None):
        self.settings = settings or get_settings()
        self.worker_id = worker_id or f"wkr-{socket.gethostname()}-{os.getpid()}-{new_id('x')[-6:]}"
        self.conn = connect(self.settings.db_path)
        self._stop = asyncio.Event()
        self._inflight: set[asyncio.Task] = set()
        self.processed = 0

    def stop(self) -> None:
        self._stop.set()

    def _beat(self, stopped: bool = False) -> None:
        store.heartbeat(self.conn, self.worker_id, os.getpid(), socket.gethostname(),
                        self.processed, stopped)

    async def run(self) -> None:
        s = self.settings
        log.info("worker %s started (concurrency=%d, lease=%.0fs)",
                 self.worker_id, s.worker_concurrency, s.lease_seconds)
        await asyncio.to_thread(self._beat)
        last_beat = time.monotonic()
        timeout = httpx.Timeout(s.request_timeout)
        limits = httpx.Limits(max_connections=s.worker_concurrency * 2)
        async with httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=False,
                                     trust_env=False) as client:
            while not self._stop.is_set():
                free = s.worker_concurrency - len(self._inflight)
                jobs = []
                if free > 0:
                    jobs = await asyncio.to_thread(
                        store.claim, self.conn, self.worker_id, free, s.lease_seconds
                    )
                for job in jobs:
                    t = asyncio.create_task(self._deliver(client, job))
                    self._inflight.add(t)
                    t.add_done_callback(self._inflight.discard)
                if time.monotonic() - last_beat >= s.heartbeat_interval:
                    await asyncio.to_thread(self._beat)
                    last_beat = time.monotonic()
                if not jobs:
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=s.poll_interval)
                    except asyncio.TimeoutError:
                        pass
            # Graceful shutdown: stop claiming, let in-flight deliveries finish.
            if self._inflight:
                log.info("draining %d in-flight deliveries", len(self._inflight))
                await asyncio.gather(*self._inflight, return_exceptions=True)
        await asyncio.to_thread(self._beat, True)
        log.info("worker %s stopped after %d attempts", self.worker_id, self.processed)

    def build_headers(self, job: store.Job) -> dict[str, str]:
        return {
            "content-type": "application/json",
            "user-agent": "RelayQueue/0.2",
            "x-relayqueue-event-id": job.event_id,
            "x-relayqueue-event-type": job.event_type,
            "x-relayqueue-delivery-id": job.delivery_id,
            "x-relayqueue-attempt": str(job.attempt_number),
        }

    async def _deliver(self, client: httpx.AsyncClient, job: store.Job) -> None:
        headers = self.build_headers(job)
        started = time.time()
        t0 = time.perf_counter()
        status = err = resp_body = retry_after = None
        try:
            resp = await client.post(job.url, content=job.body.encode(), headers=headers)
            status, resp_body = resp.status_code, resp.text[:1024]
            retry_after = resp.headers.get("retry-after")
        except httpx.TimeoutException as exc:
            err = f"timeout after {self.settings.request_timeout:g}s ({type(exc).__name__})"
        except httpx.HTTPError as exc:
            err = f"{type(exc).__name__}: {exc}"[:500]
        duration_ms = (time.perf_counter() - t0) * 1000

        outcome, reason, retry_at = classify(job, self.settings, status, err, retry_after)
        result = store.Result(outcome, status, reason, resp_body, started, duration_ms, retry_at)
        try:
            recorded = await asyncio.to_thread(store.record_result, self.conn, job, self.worker_id, result)
        except Exception:  # noqa: BLE001 - lease expiry will recover the delivery
            log.exception("failed to record result for %s; claim will expire and be retried",
                          job.delivery_id)
            return
        self.processed += 1
        log.info("%s attempt=%d/%d -> %s (%s) %.0fms", job.delivery_id, job.attempt_number,
                 job.max_attempts, recorded, status or err, duration_ms)


def main() -> None:
    logging.basicConfig(level=os.environ.get("RELAYQUEUE_LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    init_db(settings.db_path)
    worker = Worker(settings, worker_id=os.environ.get("RELAYQUEUE_WORKER_ID"))

    async def _run() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, worker.stop)
        await worker.run()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
