from __future__ import annotations

#Single-sensor simulation logic.

#Each simulated sensor:
  #Connects to the telemetry server over TCP.
  #Generates plausible readings on its configured interval.
  #Encodes each reading as a Protobuf message and writes a length-prefixed
  #frame on the socket.
  #Reconnects with backoff after transient network failures.






import asyncio
import random
import struct
import time

from proto import telemetry_pb2


def _encode_frame(msg) -> bytes:
    payload = msg.SerializeToString()
    return struct.pack(">I", len(payload)) + payload


class SensorSimulator:
    _DEFAULTS: dict[str, tuple[float, float, str]] = {
        "temperature":   (15.0, 40.0,      "°C"),
        "humidity":      (30.0, 95.0,      "%"),
        "soil_moisture": (10.0, 90.0,      "%"),
        "light":         (0.0,  120_000.0, "lux"),
    }

    def __init__(
        self,
        sensor_id: str,
        sensor_type: str,
        interval_seconds: float,
        host: str,
        port: int,
        *,
        location: str = "",
        unit: str = "",
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> None:
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.interval_seconds = interval_seconds
        self.host = host
        self.port = port
        self.location = location

        default_min, default_max, default_unit = self._DEFAULTS.get(
            sensor_type, (0.0, 100.0, "")
        )

        self.unit = unit or default_unit
        self.min_value = min_value if min_value is not None else default_min
        self.max_value = max_value if max_value is not None else default_max

        span = self.max_value - self.min_value
        self._current_value = random.uniform(
            self.min_value + span * 0.2,
            self.max_value - span * 0.2,
        )

    async def run(self) -> None:
        backoff = 2.0
        reader = writer = None

        while True:
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
                print(f"[{self.sensor_id}] Connected to {self.host}:{self.port}")

                backoff = 2.0  # reset backoff after success

                while True:
                    reading = self._generate_reading()
                    frame = _encode_frame(reading)

                    writer.write(frame)
                    await writer.drain()

                    print(f"[{self.sensor_id}] {reading.value:.2f} {reading.unit}")

                    await asyncio.sleep(self.interval_seconds)

            except asyncio.CancelledError:
                raise

            except (ConnectionRefusedError, OSError) as exc:
                print(f"[{self.sensor_id}] Connection error: {exc}. Retrying in {backoff:.0f}s...")

            except Exception as exc:
                print(f"[{self.sensor_id}] Unexpected error: {exc}")

            finally:
                if writer:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    def _generate_reading(self) -> telemetry_pb2.SensorReading:
        span = self.max_value - self.min_value
        delta = random.gauss(0, span * 0.025)

        self._current_value = max(
            self.min_value,
            min(self.max_value, self._current_value + delta),
        )

        return telemetry_pb2.SensorReading(
            sensor_id=self.sensor_id,
            sensor_type=self.sensor_type,   # MUST match .proto
            value=round(self._current_value, 2),
            unit=self.unit,
            timestamp=int(time.time()),
            location=self.location,
        )
