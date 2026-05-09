from __future__ import annotations

"""
Asynchronous TCP listener for sensor connections.

Sensors connect over TCP and stream Protobuf-encoded readings. This module:
  - Accepts connections concurrently with asyncio.start_server.
  - Frames and decodes each Protobuf message from the byte stream.
  - Hands decoded readings to the storage layer (and optionally to a
    broadcaster so the WebSocket /live feed can push them).
  - Tolerates disconnects and malformed messages without crashing the server.

Framing convention: 4-byte big-endian length prefix followed by the Protobuf
payload of that length. Adjust if your design uses a different framing scheme.
"""
"""
Async TCP listener for sensor connections.
"""
import asyncio
import struct
import time

from proto import telemetry_pb2
from server.storage import Storage

_storage: Storage | None = None
_broadcast_queue: asyncio.Queue | None = None


def configure(storage: Storage, broadcast_queue: asyncio.Queue | None = None) -> None:
    global _storage, _broadcast_queue
    _storage = storage
    _broadcast_queue = broadcast_queue


async def _read_frame(reader: asyncio.StreamReader) -> bytes:
    header = await reader.readexactly(4)
    length = struct.unpack(">I", header)[0]
    return await reader.readexactly(length)


async def handle_sensor(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    print(f"[TCP] Sensor connected: {peer}")

    try:
        while True:
            try:
                payload = await _read_frame(reader)
            except asyncio.IncompleteReadError:
                break

           
            msg = telemetry_pb2.SensorReading()

            try:
                msg.ParseFromString(payload)
            except Exception as exc:
                print(f"[TCP] {peer}: malformed protobuf — {exc}")
                continue
                
            if _storage is None:
                print("[TCP] Storage not initialized")
                return

            reading = {
             "sensor_id": msg.sensor_id,
             "sensor_type": msg.sensor_type,  
             "value": msg.value, 
             "unit": msg.unit,
             "timestamp": msg.timestamp or int(time.time()),
             "location": msg.location,
           }
            # store safely
            try:
                await _storage.add_reading(reading)
            except Exception as exc:
                print(f"[TCP] storage error — {exc}")
                continue

            print(f"[TCP] {reading['sensor_id']} -> {reading['value']:.2f}")

            # broadcast live feed
            if _broadcast_queue:
                try:
                    _broadcast_queue.put_nowait(reading)
                except asyncio.QueueFull:
                    pass

    finally:
        print(f"[TCP] Sensor disconnected: {peer}")
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def start_tcp_server(host: str, port: int) -> asyncio.AbstractServer:
    server = await asyncio.start_server(handle_sensor, host, port)
    addr = server.sockets[0].getsockname()
    print(f"[TCP] Listening on {addr[0]}:{addr[1]}")
    return server
