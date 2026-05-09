from __future__ import annotations
"""
Content negotiation for the REST API.

Maps the `Accept` header on a request to a serializer for the response.
Supported media types:
    application/json
    application/xml
    application/yaml   (also accepts text/yaml)

Falls back to JSON when no supported type matches.
"""
"""
Content negotiation for the REST API.
"""


import json
import xml.etree.ElementTree as ET
from typing import Any

import yaml
from aiohttp import web

_JSON = "application/json"
_XML  = "application/xml"
_YAML = "application/yaml"

_ALIASES: dict[str, str] = {
    "text/yaml": _YAML,
    "text/xml":  _XML,
    "*/*":       _JSON,
}


def negotiate(request: web.Request) -> str:
    accept = request.headers.get("Accept", "*/*")
    candidates: list[tuple[float, str]] = []
    for part in accept.split(","):
        segments = part.strip().split(";")
        mime = segments[0].strip().lower()
        q = 1.0
        for seg in segments[1:]:
            seg = seg.strip()
            if seg.startswith("q="):
                try:
                    q = float(seg[2:])
                except ValueError:
                    pass
        candidates.append((q, mime))

    candidates.sort(key=lambda t: -t[0])

    for _, mime in candidates:
        normalised = _ALIASES.get(mime, mime)
        if normalised in (_JSON, _XML, _YAML):
            return normalised
        if mime == "*/*":
            return _JSON
    return _JSON


def _dict_to_xml(parent: ET.Element, data: Any, item_tag: str = "item") -> None:
    if isinstance(data, dict):
        for key, val in data.items():
            tag = str(key).replace(" ", "_").replace("/", "_").replace("°", "deg")
            child = ET.SubElement(parent, tag)
            _dict_to_xml(child, val, item_tag)
    elif isinstance(data, list):
        for val in data:
            child = ET.SubElement(parent, item_tag)
            _dict_to_xml(child, val, item_tag)
    else:
        parent.text = "" if data is None else str(data)


def serialize(payload: Any, media_type: str) -> bytes:
    if media_type == _XML:
        root = ET.Element("response")
        _dict_to_xml(root, payload)
        xml_str = ET.tostring(root, encoding="unicode")
        return ('<?xml version="1.0" encoding="utf-8"?>\n' + xml_str).encode("utf-8")
    if media_type in (_YAML, "text/yaml"):
        return yaml.dump(payload, allow_unicode=True, default_flow_style=False).encode("utf-8")
    return json.dumps(payload, default=str, indent=2).encode("utf-8")
