"""Demo webhook receiver.

POST any path  -> records the webhook and answers 200
GET  /         -> HTML page listing received events (auto-refreshes)
GET  /events   -> the same data as JSON

    python demo/receiver.py --port 9000
"""

from __future__ import annotations

import argparse
import html
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOCK = threading.Lock()
RECEIVED: list[dict] = []


class Handler(BaseHTTPRequestHandler):
    server_version = "RelayQueueDemoReceiver/0.1"

    def log_message(self, fmt, *args):  # keep the console quiet
        pass

    def _send(self, code: int, body: str, ctype: str = "application/json") -> None:
        data = body.encode()
        self.send_response(code)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/events":
            with LOCK:
                self._send(200, json.dumps(RECEIVED))
            return
        if self.path == "/":
            self._send(200, render_page(), "text/html; charset=utf-8")
            return
        self._send(404, '{"error":"not found"}')

    def do_POST(self):
        length = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(length).decode()
        try:
            payload = json.loads(raw)
        except ValueError:
            payload = raw
        record = {
            "received_at": time.time(),
            "path": self.path,
            "event_id": self.headers.get("x-relayqueue-event-id"),
            "delivery_id": self.headers.get("x-relayqueue-delivery-id"),
            "attempt": self.headers.get("x-relayqueue-attempt"),
            "payload": payload,
        }
        with LOCK:
            RECEIVED.append(record)
        self._send(200, '{"received":true}')


def render_page() -> str:
    with LOCK:
        rows = list(reversed(RECEIVED[-200:]))
    trs = "".join(
        "<tr><td>{t}</td><td><code>{p}</code></td><td><code>{e}</code></td><td>{a}</td>"
        "<td><pre>{body}</pre></td></tr>".format(
            t=time.strftime("%H:%M:%S", time.localtime(r["received_at"])),
            p=html.escape(r["path"]),
            e=html.escape(str(r["event_id"])),
            a=html.escape(str(r["attempt"])),
            body=html.escape(json.dumps(r["payload"], indent=1)[:600]),
        )
        for r in rows
    )
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="2"><title>Demo receiver</title>
<style>body{{font:14px system-ui,sans-serif;margin:24px;color:#222}}
table{{border-collapse:collapse;width:100%}}td,th{{border-bottom:1px solid #ddd;padding:6px;text-align:left;vertical-align:top}}
pre{{margin:0;font-size:12px;white-space:pre-wrap}}</style></head><body>
<h1>Demo receiver</h1><p>{len(RECEIVED)} webhooks received</p>
<table><tr><th>Time</th><th>Path</th><th>Event</th><th>Attempt</th><th>Payload</th></tr>{trs}</table>
</body></html>"""


ThreadingHTTPServer.request_queue_size = 256  # default backlog (5) drops connections under load
ThreadingHTTPServer.daemon_threads = True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    args = ap.parse_args()
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"demo receiver listening on http://{args.host}:{args.port}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
