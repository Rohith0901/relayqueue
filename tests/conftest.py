from __future__ import annotations

import asyncio
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from relayqueue.config import Settings
from relayqueue.db import connect, init_db


@pytest.fixture
def settings(tmp_path) -> Settings:
    s = Settings(db_path=str(tmp_path / "rq.db"), request_timeout=1.0, poll_interval=0.02,
                 worker_concurrency=8, lease_seconds=3.0, max_attempts=4,
                 backoff_base=0.05, backoff_max=0.2, heartbeat_interval=0.5)
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


class Scripted:
    """HTTP server that answers each request according to a script.

    Script entries: an int status code, ``("sleep", seconds)`` or
    ``("status", code, {headers})``. The last entry repeats forever.
    """

    def __init__(self, script):
        self.script = list(script)
        self.calls: list[dict] = []
        self.lock = threading.Lock()
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("content-length") or 0))
                with outer.lock:
                    idx = len(outer.calls)
                    outer.calls.append({"headers": dict(self.headers), "body": body.decode()})
                    step = outer.script[min(idx, len(outer.script) - 1)]
                headers = {}
                if isinstance(step, tuple) and step[0] == "sleep":
                    time.sleep(step[1])
                    code = 200
                elif isinstance(step, tuple):
                    _, code, headers = step
                else:
                    code = step
                self.send_response(code)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("content-length", "2")
                self.end_headers()
                try:
                    self.wfile.write(b"ok")
                except OSError:
                    pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.srv.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}/hook"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()


@pytest.fixture
def scripted():
    servers = []

    def make(*script):
        s = Scripted(script)
        servers.append(s)
        return s

    yield make
    for s in servers:
        s.close()
