from __future__ import annotations

#Entry point for the telemetry server.

#Run with:
    #python -m server


#Entry point for the telemetry server.

import argparse
import asyncio
import os

import yaml
from aiohttp import web

from server.storage    import Storage
from server.tcp_ingest import configure as configure_tcp, start_tcp_server
from server.rest_api   import build_app

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config", "sensors.yaml")


def _load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


async def main(config_path: str) -> None:
    cfg       = _load_config(config_path)
    srv_cfg   = cfg.get("server", {})
    host      = srv_cfg.get("host",      "127.0.0.1")
    tcp_port  = srv_cfg.get("port",      9000)
    http_port = srv_cfg.get("http_port", 8080)

    db_cfg = cfg.get("database", {})
    db_path = db_cfg.get("path", "telemetry.db")

    storage = Storage(db_path)
    await storage.init()

    print(f"[Server] Storage initialised ({db_path})")
    

    configure_tcp(storage)

    tcp_server = await start_tcp_server(host, tcp_port)

    app    = build_app(storage)
    runner = web.AppRunner(app)
    await runner.setup()
    site   = web.TCPSite(runner, host, http_port)
    await site.start()
    print(f"[REST] Listening on http://{host}:{http_port}")

    async with tcp_server:
        await tcp_server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()
    try:
        asyncio.run(main(args.config))
    except KeyboardInterrupt:
        print("\n[Server] Shutting down")
