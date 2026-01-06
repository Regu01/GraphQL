import json
import time
from datetime import datetime, timezone

import requests
import pynautobot

# ======================
# CONFIG
# ======================
NAUTOBOT_URL = ""
NAUTOBOT_TOKEN = ""
NAUTOBOT_SSL_VERIFY = True

SPLUNK_HEC_URL = ""
SPLUNK_HEC_TOKEN = ""
SPLUNK_SSL_VERIFY = True
SPLUNK_INDEX = "nautobot"
SPLUNK_SOURCE = "nautobot"
SPLUNK_HOST = None

PAGE_SIZE = 200
BATCH_SIZE = 300
SPLUNK_TIMEOUT = 10
SPLUNK_RETRIES = 3
SPLUNK_RETRY_SLEEP = 2

HEADERS_SPLUNK = {
    "Authorization": f"Splunk {SPLUNK_HEC_TOKEN}",
    "Content-Type": "application/json",
}

# ======================
# Nautobot API
# ======================
nautobot = pynautobot.api(
    NAUTOBOT_URL,
    token=NAUTOBOT_TOKEN,
    verify=NAUTOBOT_SSL_VERIFY,
)


def normalize_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: normalize_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [normalize_value(v) for v in value]
    if hasattr(value, "serialize"):
        return normalize_value(value.serialize())
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def record_to_dict(record):
    if hasattr(record, "serialize"):
        raw = record.serialize()
    elif isinstance(record, dict):
        raw = record
    else:
        raw = record.__dict__
    return normalize_value(raw)


def splunk_event(sourcetype, data):
    event = {
        "time": datetime.now(timezone.utc).timestamp(),
        "index": SPLUNK_INDEX,
        "source": SPLUNK_SOURCE,
        "sourcetype": sourcetype,
        "event": data,
    }
    if SPLUNK_HOST:
        event["host"] = SPLUNK_HOST
    return event


def send_to_splunk(batch):
    payload = "\n".join(json.dumps(e) for e in batch)

    for attempt in range(1, SPLUNK_RETRIES + 1):
        try:
            response = requests.post(
                SPLUNK_HEC_URL,
                headers=HEADERS_SPLUNK,
                data=payload,
                timeout=SPLUNK_TIMEOUT,
                verify=SPLUNK_SSL_VERIFY,
            )

            if response.status_code == 200:
                return True

            print(f"[SPLUNK] HTTP {response.status_code}: {response.text[:500]}")

        except requests.exceptions.RequestException as exc:
            print(f"[SPLUNK] attempt {attempt} failed: {exc}")

        time.sleep(SPLUNK_RETRY_SLEEP)

    print("[ERROR] batch abandoned after retries")
    return False


def get_endpoint(group, attr):
    return getattr(group, attr, None)


def load_cache(endpoint, value_attr):
    cache = {}
    if endpoint is None:
        return cache
    for obj in endpoint.all():
        obj_id = getattr(obj, "id", None)
        if obj_id is None:
            continue
        value = getattr(obj, value_attr, None)
        if value is None and hasattr(obj, "name"):
            value = obj.name
        if value is None:
            continue
        cache[obj_id] = value
    return cache


def map_cached_value(value, cache):
    if value is None:
        return None
    if isinstance(value, list):
        return [map_cached_value(item, cache) for item in value]
    if isinstance(value, dict):
        obj_id = value.get("id")
        if obj_id is None:
            return value
        mapped = cache.get(obj_id)
        return mapped if mapped is not None else obj_id
    mapped = cache.get(value)
    return mapped if mapped is not None else value


def apply_blacklist(data, blacklist):
    if not blacklist:
        return data
    return {k: v for k, v in data.items() if k not in blacklist}


def replace_fields(data, fields_map, caches):
    for field, cache_key in fields_map.items():
        if field in data:
            data[field] = map_cached_value(data[field], caches.get(cache_key, {}))
    return data


def export_devices():
    caches = {
        "tags": load_cache(get_endpoint(nautobot.extras, "tags"), "name"),
        "statuses": load_cache(get_endpoint(nautobot.extras, "statuses"), "value"),
        "roles": load_cache(get_endpoint(nautobot.dcim, "device_roles"), "name"),
        "tenants": load_cache(get_endpoint(nautobot.tenancy, "tenants"), "name"),
        "locations": load_cache(get_endpoint(nautobot.dcim, "locations"), "name"),
        "dynamic_groups": load_cache(get_endpoint(nautobot.extras, "dynamic_groups"), "name"),
    }

    blacklist = set()

    blacklist = {
        "local_context_data", 
        "custom_fields", 
        "secrets"
        }

    fields_map = {
        "status": "statuses",
        "role": "roles",
        "device_role": "roles",
        "tenant": "tenants",
        "location": "locations",
        "tags": "tags",
        "dynamic_groups": "dynamic_groups",
    }

    offset = 0
    batch = []
    total = 0

    while True:
        devices = list(nautobot.dcim.devices.filter(limit=PAGE_SIZE, offset=offset))
        if not devices:
            break

        for d in devices:
            data = record_to_dict(d)
            data = replace_fields(data, fields_map, caches)
            data = apply_blacklist(data, blacklist)
            batch.append(splunk_event("nautobot:dcim:device", data))

            if len(batch) >= BATCH_SIZE:
                if send_to_splunk(batch):
                    total += len(batch)
                batch.clear()

        offset += len(devices)
        print(f"[DEVICES] fetched: {offset}")

    if batch:
        if send_to_splunk(batch):
            total += len(batch)

    print(f"[DONE] devices sent: {total}")


if __name__ == "__main__":
    export_devices()
