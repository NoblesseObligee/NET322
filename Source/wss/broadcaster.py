from __future__ import annotations
"""
Tracks connected WebSocket clients and dispatches readings to them.

Owns the set of live clients, their subscription filters, and a way for
producers (the telemetry server) to publish a new reading.
"""
"""Broadcaster — fans out readings to WebSocket clients."""


import asyncio
import json
from typing import Any


class Broadcaster:
    def __init__(self) -> None:
        self._clients: list[dict] = []

    def register(self, ws, subscriptions: set[str] | None = None) -> None:
        self._clients.append({"ws": ws, "subscriptions": subscriptions})
        print(f"[WS] Client registered  (total={len(self._clients)})")

    def unregister(self, ws) -> None:
        self._clients = [c for c in self._clients if c["ws"] is not ws]
        print(f"[WS] Client unregistered (total={len(self._clients)})")

    def update_subscriptions(self, ws, subscriptions: set[str] | None) -> None:
        for entry in self._clients:
            if entry["ws"] is ws:
                entry["subscriptions"] = subscriptions
                return

    async def publish(self, reading: dict[str, Any]) -> None:
        payload = json.dumps(reading, default=str)
        dead: list[dict] = []
        for entry in list(self._clients):
            subs = entry["subscriptions"]
            if subs is not None and reading.get("sensor_id") not in subs:
                continue
            try:
                await entry["ws"].send(payload)
            except Exception:
                dead.append(entry)
        for entry in dead:
            if entry in self._clients:
                self._clients.remove(entry)
