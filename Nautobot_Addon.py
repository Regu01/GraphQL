import json
import time
from datetime import datetime, timezone

import requests
import pynautobot


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
    payload = "\n".join(
        json.dumps(event, separators=(",", ":"), ensure_ascii=True)
        for event in batch
    )

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

    return False


def export_endpoint(name, endpoint, sourcetype):
    if endpoint is None:
        print(f"[SKIP] {name} not available")
        return True

    batch = []
    total = 0
    offset = 0

    while True:
        try:
            page = list(endpoint.filter(limit=PAGE_SIZE, offset=offset))
        except Exception:
            for record in endpoint.all():
                batch.append(splunk_event(sourcetype, record_to_dict(record)))
                if len(batch) >= BATCH_SIZE:
                    if not send_to_splunk(batch):
                        print(f"[ERROR] {name} batch failed")
                        return False
                    total += len(batch)
                    batch.clear()
            break

        if not page:
            break

        for record in page:
            batch.append(splunk_event(sourcetype, record_to_dict(record)))
            if len(batch) >= BATCH_SIZE:
                if not send_to_splunk(batch):
                    print(f"[ERROR] {name} batch failed")
                    return False
                total += len(batch)
                batch.clear()

        offset += len(page)
        print(f"[{name}] fetched {offset}")

    if batch:
        if not send_to_splunk(batch):
            print(f"[ERROR] {name} final batch failed")
            return False
        total += len(batch)

    print(f"[DONE] {name} sent {total}")
    return True


def get_endpoint(group, attr):
    return getattr(group, attr, None)


def main():
    targets = [
        ("devices", get_endpoint(nautobot.dcim, "devices"), "nautobot:dcim:device"),
        ("interfaces", get_endpoint(nautobot.dcim, "interfaces"), "nautobot:dcim:interface"),
        ("ip_addresses", get_endpoint(nautobot.ipam, "ip_addresses"), "nautobot:ipam:ip_address"),
        ("prefixes", get_endpoint(nautobot.ipam, "prefixes"), "nautobot:ipam:prefix"),
        ("vrfs", get_endpoint(nautobot.ipam, "vrfs"), "nautobot:ipam:vrf"),
        ("vlans", get_endpoint(nautobot.ipam, "vlans"), "nautobot:ipam:vlan"),
        ("tags", get_endpoint(nautobot.extras, "tags"), "nautobot:extras:tag"),
        ("custom_fields", get_endpoint(nautobot.extras, "custom_fields"), "nautobot:extras:custom_field"),
        ("statuses", get_endpoint(nautobot.extras, "statuses"), "nautobot:extras:status"),
    ]

    for name, endpoint, sourcetype in targets:
        if not export_endpoint(name, endpoint, sourcetype):
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
