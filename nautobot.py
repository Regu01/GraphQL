#!/usr/bin/env python3
import json

import requests

NAUTOBOT_GRAPHQL_URL = "https://nautobot.example.com/graphql/"
NAUTOBOT_TOKEN = "CHANGE_ME"
NAUTOBOT_VERIFY = True
NAUTOBOT_TIMEOUT = 30
NAUTOBOT_PAGE_SIZE = 500

SPLUNK_HEC_URL = "https://splunk.example.com:8088/services/collector"
SPLUNK_HEC_TOKEN = "CHANGE_ME"
SPLUNK_HEC_INDEX = "nautobot"
SPLUNK_HEC_SOURCETYPE = "nautobot:device"
SPLUNK_HEC_SOURCE = "nautobot"
SPLUNK_HEC_BATCH_SIZE = 200
SPLUNK_HEC_VERIFY = True
SPLUNK_HEC_TIMEOUT = 30

DEVICE_QUERY = """
query DeviceInventory($limit: Int!, $offset: Int!) {
  devices(limit: $limit, offset: $offset) {
    name
    serial
    asset_tag
    status { name }
    device_role { name }
    device_type { model manufacturer { name } }
    platform { name }
    site { name }
    location { name }
    rack { name }
    position
    face { label }
    tenant { name }
    cluster { name }
    primary_ip4 { address }
    primary_ip6 { address }
    tags { name }
    custom_fields
    local_context_data
    comments
  }
}
"""


def send_batch(session, events):
    if not events:
        return 0
    payload = "\n".join(json.dumps(event, ensure_ascii=True) for event in events)
    response = session.post(
        SPLUNK_HEC_URL,
        data=payload,
        headers={"Authorization": f"Splunk {SPLUNK_HEC_TOKEN}"},
        timeout=SPLUNK_HEC_TIMEOUT,
        verify=SPLUNK_HEC_VERIFY,
    )
    response.raise_for_status()
    return len(events)


def main():
    if "CHANGE_ME" in (NAUTOBOT_TOKEN, SPLUNK_HEC_TOKEN):
        print("Set tokens in nautobot.py before running.")
        return 2

    session = requests.Session()
    offset = 0
    sent = 0
    batch = []

    while True:
        response = session.post(
            NAUTOBOT_GRAPHQL_URL,
            json={"query": DEVICE_QUERY, "variables": {"limit": NAUTOBOT_PAGE_SIZE, "offset": offset}},
            headers={"Authorization": f"Token {NAUTOBOT_TOKEN}"},
            timeout=NAUTOBOT_TIMEOUT,
            verify=NAUTOBOT_VERIFY,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("errors"):
            raise RuntimeError(f"GraphQL errors: {data['errors']}")

        devices = data.get("data", {}).get("devices", [])
        if not devices:
            break

        for device in devices:
            event = {
                "index": SPLUNK_HEC_INDEX,
                "sourcetype": SPLUNK_HEC_SOURCETYPE,
                "source": SPLUNK_HEC_SOURCE,
                "event": device,
            }
            if device.get("name"):
                event["host"] = device["name"]
            batch.append(event)
            if len(batch) >= SPLUNK_HEC_BATCH_SIZE:
                sent += send_batch(session, batch)
                batch = []

        if len(devices) < NAUTOBOT_PAGE_SIZE:
            break
        offset += NAUTOBOT_PAGE_SIZE

    sent += send_batch(session, batch)
    print(f"Done. Sent {sent} devices.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
