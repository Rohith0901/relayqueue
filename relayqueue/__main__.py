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

    p_acct = sub.add_parser("create-account", help="create an account and print its API key")
    p_acct.add_argument("name")

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
    elif args.cmd == "create-account":
        import json

        from . import store
        from .config import get_settings
        from .db import connect, init_db

        path = get_settings().db_path
        init_db(path)
        acct, key = store.create_account(connect(path), args.name)
        acct["api_key"]["key"] = key
        print(json.dumps(acct, indent=2))
        print("\nStore this API key now - it is not shown again.")


if __name__ == "__main__":
    main()
