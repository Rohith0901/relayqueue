"""Delivery worker: claims pending deliveries and POSTs them to endpoints."""

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

log = logging.getLogger("relayqueue.worker")


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

    async def run(self) -> None:
        s = self.settings
        log.info("worker %s started (concurrency=%d)", self.worker_id, s.worker_concurrency)
        async with httpx.AsyncClient(timeout=s.request_timeout, follow_redirects=False) as client:
            while not self._stop.is_set():
                free = s.worker_concurrency - len(self._inflight)
                jobs = []
                if free > 0:
                    jobs = await asyncio.to_thread(store.claim, self.conn, self.worker_id, free)
                for job in jobs:
                    t = asyncio.create_task(self._deliver(client, job))
                    self._inflight.add(t)
                    t.add_done_callback(self._inflight.discard)
                if not jobs:
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=s.poll_interval)
                    except asyncio.TimeoutError:
                        pass
            # graceful shutdown: let in-flight deliveries finish and be recorded
            if self._inflight:
                await asyncio.gather(*self._inflight, return_exceptions=True)
        log.info("worker %s stopped after %d deliveries", self.worker_id, self.processed)

    async def _deliver(self, client: httpx.AsyncClient, job: store.Job) -> None:
        headers = {
            "content-type": "application/json",
            "user-agent": "RelayQueue/0.1",
            "x-relayqueue-event-id": job.event_id,
            "x-relayqueue-delivery-id": job.delivery_id,
            "x-relayqueue-attempt": str(job.attempt_number),
        }
        started = time.time()
        t0 = time.perf_counter()
        status = err = resp_body = None
        try:
            resp = await client.post(job.url, content=job.body.encode(), headers=headers)
            status, resp_body = resp.status_code, resp.text[:1024]
        except httpx.HTTPError as exc:
            err = f"{type(exc).__name__}: {exc}"[:500]
        duration_ms = (time.perf_counter() - t0) * 1000
        outcome = await asyncio.to_thread(
            store.record_result, self.conn, job, self.worker_id, started, duration_ms,
            status, err, resp_body,
        )
        self.processed += 1
        log.info("%s attempt=%d -> %s (%s) %.0fms", job.delivery_id, job.attempt_number,
                 outcome, status or err, duration_ms)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
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
