from __future__ import annotations
"""REST API for the telemetry server.

Endpoints:
    GET    /sensors                       list registered sensors
    GET    /sensors/{id}/readings         historical readings  (?from=&to=)
    POST   /sensors                       register a new sensor
    DELETE /sensors/{id}                  remove a sensor

Content negotiation:
    Server-driven via the `Accept` header. Supported media types:
      application/json, application/xml, application/yaml.
    Delegates to server.serialization.

Sessions:
    A cookie identifies the client session — set on first response, read
    on subsequent requests.
"""
"""REST API for the telemetry server."""


import time
import uuid

from aiohttp import web

from server.serialization import negotiate, serialize
from server.storage import Storage

SESSION_COOKIE = "session_id"
STORAGE_KEY    = "storage"


def _make_response(request: web.Request, payload, status: int = 200) -> web.Response:
    media_type = negotiate(request)
    body       = serialize(payload, media_type)
    resp = web.Response(status=status, body=body, content_type=media_type)
    resp.headers["Content-Type"] = media_type
    sid = request.cookies.get(SESSION_COOKIE) or str(uuid.uuid4())
    resp.set_cookie(SESSION_COOKIE, sid, max_age=86400*7, httponly=True, samesite="Lax")
    return resp


def _storage(request: web.Request) -> Storage:
    return request.app[STORAGE_KEY]


async def list_sensors(request: web.Request) -> web.Response:
    sensors = await _storage(request).list_sensors()
    return _make_response(request, {"sensors": sensors})


async def get_readings(request: web.Request) -> web.Response:
    sensor_id = request.match_info["id"]
    sensor = await _storage(request).get_sensor(sensor_id)
    if sensor is None:
        return _make_response(request, {"error": "Sensor not found"}, status=404)
    params = request.rel_url.query
    try:
        from_ts = float(params["from"]) if "from" in params else None
        to_ts   = float(params["to"])   if "to"   in params else None
    except ValueError:
        return _make_response(request, {"error": "'from' and 'to' must be numeric"}, status=400)
    readings = await _storage(request).get_readings(sensor_id, from_ts, to_ts)
    return _make_response(request, {"sensor_id": sensor_id, "readings": readings})


async def register_sensor(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:
        return _make_response(request, {"error": "Invalid JSON body"}, status=400)
    required = {"sensor_id", "sensor_type", "location", "unit"}
    missing  = required - body.keys()
    if missing:
        return _make_response(request, {"error": f"Missing fields: {', '.join(sorted(missing))}"}, status=422)
    body["registered_at"] = int(time.time())
    await _storage(request).add_sensor(body)
    sensor = await _storage(request).get_sensor(body["sensor_id"])
    resp = _make_response(request, {"sensor": sensor}, status=201)
    resp.headers["Location"] = f"/sensors/{body['sensor_id']}"
    return resp


async def delete_sensor(request: web.Request) -> web.Response:
    sensor_id = request.match_info["id"]
    sensor = await _storage(request).get_sensor(sensor_id)
    if sensor is None:
        return _make_response(request, {"error": "Sensor not found"}, status=404)
    await _storage(request).remove_sensor(sensor_id)
    resp = web.Response(status=204)
    sid = request.cookies.get(SESSION_COOKIE) or str(uuid.uuid4())
    resp.set_cookie(SESSION_COOKIE, sid, max_age=86400*7, httponly=True, samesite="Lax")
    return resp


@web.middleware
async def session_cookie_middleware(request: web.Request, handler):
    sid = request.cookies.get(SESSION_COOKIE)
    request["session_id"] = sid or str(uuid.uuid4())
    response = await handler(request)
    if SESSION_COOKIE not in response.cookies:
        response.set_cookie(SESSION_COOKIE, request["session_id"],
                            max_age=86400*7, httponly=True, samesite="Lax")
    return response


@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        resp = web.Response()
    else:
        resp = await handler(request)
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,DELETE,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type,Accept"
    return resp


def build_app(storage: Storage) -> web.Application:
    app = web.Application(middlewares=[cors_middleware, session_cookie_middleware])
    app[STORAGE_KEY] = storage
    app.router.add_get   ("/sensors",               list_sensors)
    app.router.add_get   ("/sensors/{id}/readings", get_readings)
    app.router.add_post  ("/sensors",               register_sensor)
    app.router.add_delete("/sensors/{id}",          delete_sensor)
    app.router.add_static("/static", path="static", name="static")
    return app
