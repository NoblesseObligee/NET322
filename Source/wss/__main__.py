from __future__ import annotations
#Entry point for the WebSocket live-feed server.

#Run with:
    #python -m wss



import argparse
import asyncio
import os

import websockets
import yaml

from server.storage  import Storage
from wss.broadcaster import Broadcaster
from wss.handler     import live, set_broadcaster

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config", "sensors.yaml")
POLL_INTERVAL  = 1.0


def _load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


async def _poll_and_broadcast(broadcaster: Broadcaster, storage: Storage) -> None:
    last_id = await storage.latest_reading_id()
    print(f"[WS] Broadcaster starting from reading id={last_id}")
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        try:
            rows = await storage.readings_after(last_id)
        except Exception as exc:
            print(f"[WS] DB poll error: {exc}")
            continue
        for row in rows:
            last_id = max(last_id, row["id"])
            frame = {
                "sensor_id": row["sensor_id"],
                "sensor_type":      row["sensor_type"],
                "value":     row["value"],
                "unit":      row["unit"],
                "ts":        row["timestamp"],
                "location":  row["location"],
            }
            await broadcaster.publish(frame)


async def main(config_path: str) -> None:
    cfg     = _load_config(config_path)
    srv_cfg = cfg.get("server", {})
    host    = srv_cfg.get("host",    "127.0.0.1")
    ws_port = srv_cfg.get("ws_port", 8081)

    broadcaster = Broadcaster()
    set_broadcaster(broadcaster)

    storage = Storage()
    await storage.init()

    poll_task = asyncio.create_task(_poll_and_broadcast(broadcaster, storage))

    ws_server = await websockets.serve(live, host, ws_port)
    print(f"[WS] WebSocket live feed on  ws://{host}:{ws_port}/live")

    try:
        await asyncio.gather(poll_task, ws_server.wait_closed())
    finally:
        poll_task.cancel()
        ws_server.close()
        await ws_server.wait_closed()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()
    try:
        asyncio.run(main(args.config))
    except KeyboardInterrupt:
        print("\n[WS] Shutting down")
