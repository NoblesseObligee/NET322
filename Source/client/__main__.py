from __future__ import annotations

#Entry point for the sensor simulator.

#Run with:
#python -m client --config config/sensors.yaml

#Entry point for the sensor simulator.


import argparse
import asyncio
import os

import yaml

from client.simulator import SensorSimulator

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config", "sensors.yaml")


def _load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    cfg     = _load_config(args.config)
    srv_cfg = cfg.get("server", {})
    host    = srv_cfg.get("host", "127.0.0.1")
    port    = int(srv_cfg.get("port", 9000))

    sensors = cfg.get("sensors", [])
    if not sensors:
        print("[Client] No sensors defined in config — exiting")
        return

    print(f"[Client] Starting {len(sensors)} sensor(s) → {host}:{port}")

    tasks = [
        asyncio.create_task(
            SensorSimulator(
                sensor_id        = s["id"],
                sensor_type      = s["type"],
                interval_seconds = float(s["interval_seconds"]),
                host             = host,
                port             = port,
                location         = s.get("location", ""),
                unit             = s.get("unit", ""),
                min_value        = s.get("min_value"),
                max_value        = s.get("max_value"),
            ).run(),
            name=s["id"],
        )
        for s in sensors
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Client] Shutting down")
