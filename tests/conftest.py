from __future__ import annotations

import asyncio
import threading
import time
from http.server import ThreadingHTTPServer

import pytest

from relayqueue.config import Settings
from relayqueue.db import connect, init_db


@pytest.fixture
def settings(tmp_path) -> Settings:
    s = Settings(db_path=str(tmp_path / "rq.db"), request_timeout=2.0, poll_interval=0.02,
                 worker_concurrency=8)
    init_db(s.db_path)
    return s


@pytest.fixture
def conn(settings):
    c = connect(settings.db_path)
    yield c
    c.close()


@pytest.fixture
def receiver():
    """Demo receiver running in a background thread on a free port."""
    from demo import receiver as rcv

    rcv.RECEIVED.clear()
    srv = ThreadingHTTPServer(("127.0.0.1", 0), rcv.Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    srv.base_url = f"http://127.0.0.1:{srv.server_address[1]}"
    srv.module = rcv
    yield srv
    srv.shutdown()
    srv.server_close()


def run_worker_until(worker, predicate, timeout: float = 10.0) -> None:
    """Run ``worker`` in an event loop until ``predicate()`` is true."""

    async def _main():
        task = asyncio.create_task(worker.run())
        deadline = time.time() + timeout
        while time.time() < deadline:
            if await asyncio.to_thread(predicate):
                break
            await asyncio.sleep(0.05)
        worker.stop()
        await task

    asyncio.run(_main())
