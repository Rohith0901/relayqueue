"""Command line entry point: ``python -m relayqueue {api,worker,init-db}``."""

from __future__ import annotations

import argparse
import os


def main() -> None:
    parser = argparse.ArgumentParser(prog="relayqueue")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_api = sub.add_parser("api", help="run the HTTP API")
    p_api.add_argument("--host", default="127.0.0.1")
    p_api.add_argument("--port", type=int, default=8000)

    p_worker = sub.add_parser("worker", help="run a delivery worker")
    p_worker.add_argument("--worker-id", default=None)

    sub.add_parser("init-db", help="create the database schema")

    args = parser.parse_args()

    if args.cmd == "api":
        import uvicorn

        from .api import create_app

        uvicorn.run(create_app(), host=args.host, port=args.port, log_level="warning")
    elif args.cmd == "worker":
        if args.worker_id:
            os.environ["RELAYQUEUE_WORKER_ID"] = args.worker_id
        from .worker import main as worker_main

        worker_main()
    elif args.cmd == "init-db":
        from .config import get_settings
        from .db import init_db

        init_db(get_settings().db_path)
        print("schema ready")


if __name__ == "__main__":
    main()
