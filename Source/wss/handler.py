from __future__ import annotations

import json
from wss.broadcaster import Broadcaster

_broadcaster: Broadcaster | None = None


def set_broadcaster(b: Broadcaster) -> None:
    global _broadcaster
    _broadcaster = b


def _parse_subscription(raw: str) -> set[str] | None:
    try:
        obj = json.loads(raw)
        sensors = obj.get("sensors", "*")

        # ✔ explicit meaning: None = ALL sensors
        if sensors == "*" or sensors == ["*"]:
            return None

        if isinstance(sensors, list):
            return set(sensors)

        return None
    except Exception:
        return None


async def live(websocket, path: str = "/live") -> None:
    if _broadcaster is None:
        raise RuntimeError("Broadcaster not initialized")

    # default: subscribe to ALL
    _broadcaster.register(websocket, subscriptions=None)

    try:
        async for raw in websocket:
            subs = _parse_subscription(raw)

            _broadcaster.update_subscriptions(websocket, subs)

            print(f"[WS] Subscription → {subs if subs else 'ALL'}")

    except Exception as e:
        print(f"[WS] Client error: {e}")

    finally:
        _broadcaster.unregister(websocket)
