"""Shared helpers: Confluent wire format + Schema Registry lookups.

Records use the standard Confluent wire format:
  byte 0     magic byte (0x00)
  bytes 1-4  big-endian schema ID
  bytes 5+   Avro binary body
That embedded schema ID is why Schema Registry migrations usually hurt -- and
why Shadowing preserving IDs matters: the bytes already on disk keep working.
"""
import io
import json
import os
import struct

import fastavro
import requests

CLUSTER = os.environ.get("CLUSTER_LABEL", "confluent").upper()
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "confluent-kafka:29092")
SR_URL = os.environ.get("SR_URL", "http://confluent-schema-registry:8081")
STATE_DIR = os.environ.get("STATE_DIR", "/state")


def _resolve_refs(sr_url, refs, named):
    for ref in refs or []:
        r = requests.get(f"{sr_url}/subjects/{ref['subject']}/versions/{ref['version']}", timeout=10)
        r.raise_for_status()
        data = r.json()
        _resolve_refs(sr_url, data.get("references"), named)
        fastavro.parse_schema(json.loads(data["schema"]), named_schemas=named)


def latest_schema(subject, sr_url=SR_URL):
    r = requests.get(f"{sr_url}/subjects/{subject}/versions/latest", timeout=10)
    r.raise_for_status()
    data = r.json()
    named = {}
    _resolve_refs(sr_url, data.get("references"), named)
    return data["id"], fastavro.parse_schema(json.loads(data["schema"]), named_schemas=named)


def schema_by_id(schema_id, sr_url=SR_URL):
    r = requests.get(f"{sr_url}/schemas/ids/{schema_id}", timeout=10)
    r.raise_for_status()
    data = r.json()
    named = {}
    _resolve_refs(sr_url, data.get("references"), named)
    return fastavro.parse_schema(json.loads(data["schema"]), named_schemas=named)


def encode(schema_id, parsed, record):
    buf = io.BytesIO()
    buf.write(struct.pack(">bI", 0, schema_id))
    fastavro.schemaless_writer(buf, parsed, record)
    return buf.getvalue()


def decode(raw, cache, sr_url=SR_URL):
    magic, schema_id = struct.unpack(">bI", raw[:5])
    if magic != 0:
        raise ValueError(f"unexpected magic byte {magic!r}")
    if schema_id not in cache:
        cache[schema_id] = schema_by_id(schema_id, sr_url)
    return schema_id, fastavro.schemaless_reader(io.BytesIO(raw[5:]), cache[schema_id])
