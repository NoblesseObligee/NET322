from __future__ import annotations

#Storage layer for sensors and readings.

#The backing store is an implementation detail (in-memory dict, SQLite,
#something else). The interface below is what the rest of the server uses.


import time
from typing import Iterable, Optional

import aiosqlite

DB_PATH = "telemetry.db"

_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sensors (
    sensor_id     TEXT    PRIMARY KEY,
    sensor_type   TEXT    NOT NULL,
    location      TEXT    NOT NULL DEFAULT '',
    unit          TEXT    NOT NULL DEFAULT '',
    registered_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS readings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id   TEXT    NOT NULL REFERENCES sensors(sensor_id) ON DELETE CASCADE,
    sensor_type TEXT    NOT NULL,
    value       REAL    NOT NULL,
    unit        TEXT    NOT NULL DEFAULT '',
    timestamp   INTEGER NOT NULL,
    location    TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_readings_sensor_ts
    ON readings (sensor_id, timestamp);
"""

def _rows_to_dicts(description, rows) -> list[dict]:
    cols = [d[0] for d in description]
    return [dict(zip(cols, row)) for row in rows]


class Storage:
    def __init__(self, db_path: str = DB_PATH) -> None:
        self._path = db_path

    async def init(self) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.executescript(_DDL)
            await db.commit()

    async def add_sensor(self, sensor: dict) -> None:
        now = sensor.get("registered_at", int(time.time()))
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                """
                INSERT INTO sensors (
                    sensor_id,
                    sensor_type,
                    location,
                    unit,
                    registered_at
                 )
                VALUES (
                    :sensor_id, 
                    :sensor_type, 
                    :location, 
                    :unit, 
                    :registered_at
                  )
                
                ON CONFLICT(sensor_id) DO UPDATE SET
                    sensor_type    = excluded.sensor_type,
                    location = excluded.location,
                    unit     = excluded.unit
                """,
                {
                    "sensor_id":     sensor["sensor_id"],
                    "sensor_type":          sensor.get("sensor_type", "unknown"),
                    "location":      sensor.get("location", ""),
                    "unit":          sensor.get("unit", ""),
                    "registered_at": now,
                },
            )
            await db.commit()

    async def remove_sensor(self, sensor_id: str) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.execute("DELETE FROM sensors WHERE sensor_id = ?", (sensor_id,))
            await db.commit()

    async def list_sensors(self) -> list[dict]:
        async with aiosqlite.connect(self._path) as db:
            async with db.execute("SELECT * FROM sensors ORDER BY registered_at") as cur:
                rows = await cur.fetchall()
                return _rows_to_dicts(cur.description, rows)

    async def get_sensor(self, sensor_id: str) -> dict | None:
        async with aiosqlite.connect(self._path) as db:
            async with db.execute(
                "SELECT * FROM sensors WHERE sensor_id = ?", (sensor_id,)
            ) as cur:
                row = await cur.fetchone()
                if row is None:
                    return None
                return dict(zip([d[0] for d in cur.description], row))

    async def add_reading(self, reading: dict) -> int:
        existing = await self.get_sensor(reading["sensor_id"])
        if existing is None:
            await self.add_sensor({
                "sensor_id": reading["sensor_id"],
                "sensor_type":      reading.get("sensor_type", "unknown"),
                "location":  reading.get("location", ""),
                "unit":      reading.get("unit", ""),
            })
        async with aiosqlite.connect(self._path) as db:
            cur = await db.execute(
    """
    INSERT INTO readings (sensor_id, sensor_type, value, unit, timestamp, location)
    VALUES (:sensor_id, :sensor_type, :value, :unit, :timestamp, :location)
    """,
    {
        "sensor_id": reading["sensor_id"],
        "sensor_type": reading.get("sensor_type", "unknown"),
        "value": reading["value"],
        "unit": reading.get("unit", ""),
        "timestamp": reading.get("timestamp", int(time.time())),
        "location": reading.get("location", ""),
    },
)
            await db.commit()
            return cur.lastrowid

    async def get_readings(
        self,
        sensor_id: str,
        from_ts: Optional[float] = None,
        to_ts: Optional[float] = None,
    ) -> list[dict]:
        frm = int(from_ts) if from_ts is not None else 0
        to  = int(to_ts)   if to_ts   is not None else int(time.time()) + 86400
        async with aiosqlite.connect(self._path) as db:
            async with db.execute(
                """
                SELECT * FROM readings
                WHERE sensor_id = ? AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp
                """,
                (sensor_id, frm, to),
            ) as cur:
                rows = await cur.fetchall()
                return _rows_to_dicts(cur.description, rows)

    async def latest_reading_id(self) -> int:
        async with aiosqlite.connect(self._path) as db:
            async with db.execute("SELECT COALESCE(MAX(id), 0) FROM readings") as cur:
                row = await cur.fetchone()
                return row[0] if row else 0

    async def readings_after(self, last_id: int) -> list[dict]:
        async with aiosqlite.connect(self._path) as db:
            async with db.execute(
                """
                SELECT r.id, r.sensor_id, r.sensor_type, r.value, r.unit,
                       r.timestamp, r.location
                FROM readings r
                WHERE r.id > ?
                ORDER BY r.id
                """,
                (last_id,),
            ) as cur:
                rows = await cur.fetchall()
                return _rows_to_dicts(cur.description, rows)
               
