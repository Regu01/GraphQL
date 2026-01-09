#!/usr/bin/env python3
import json
import os
import time

import requests

# Nautobot settings
NAUTOBOT_GRAPHQL_URL = "https://nautobot.example.com/api/graphql/"
NAUTOBOT_TOKEN = "CHANGE_ME"
NAUTOBOT_VERIFY = True
NAUTOBOT_TIMEOUT = 30
NAUTOBOT_PAGE_SIZE = 50
NAUTOBOT_MAX_RPS = 30

# Output and logging
OUTPUT_DIR = "output_splunk_nautobot"
LOG_FILE = os.path.join(OUTPUT_DIR, "nautobot_export.log")

# GraphQL queries
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
    created
    last_updated
  }
}
"""

# Export list (name, query)
EXPORTS = [
    ("devices", DEVICE_QUERY),
    ("ip_addresses", IP_ADDRESS_QUERY),
    ("prefixes", PREFIX_QUERY),
    ("vlans", VLAN_QUERY),
    ("vrfs", VRF_QUERY),
]


# Remove empty values to reduce output size
def prune_empty(value):
    if value is None:
        return None
    
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            cleaned_item = prune_empty(item)
            if cleaned_item is None:
                continue
            cleaned[key] = cleaned_item
        return cleaned or None
    
    if isinstance(value, list):
        cleaned_list = []
        for item in value:
            cleaned_item = prune_empty(item)
            if cleaned_item is None:
                continue
            cleaned_list.append(cleaned_item)
        return cleaned_list or None
    
    if isinstance(value, str) and value == "":
        return None
    return value


# Log to console and file
def log_message(log_fh, message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} {message}"
    print(line)
    log_fh.write(line + "\n")
    log_fh.flush()


# Export one object type to JSON
def export_items(session, name, query, log_fh):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f"{name}.json")
    offset = 0
    last_request = 0.0
    min_interval = 1.0 / float(max(1, NAUTOBOT_MAX_RPS))
    count = 0
    first = True

    log_message(log_fh, f"{name}: start export -> {output_path}")

    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write("[")
        while True:
            elapsed = time.monotonic() - last_request
            if last_request and elapsed < min_interval:
                time.sleep(min_interval - elapsed)

            response = session.post(
                NAUTOBOT_GRAPHQL_URL,
                json={"query": query, "variables": {"limit": NAUTOBOT_PAGE_SIZE, "offset": offset}},
                headers={"Authorization": f"Token {NAUTOBOT_TOKEN}"},
                timeout=NAUTOBOT_TIMEOUT,
                verify=NAUTOBOT_VERIFY,
            )
            last_request = time.monotonic()
            response.raise_for_status()
            data = response.json()

            if data.get("errors"):
                raise RuntimeError(f"Errors: {data['errors']}")

            items = data.get("data", {}).get(name, [])
            if not items:
                break

            log_message(log_fh, f"{name}: page offset {offset} items {len(items)}")
            for item in items:
                item = prune_empty(item)
                if not item:
                    continue
                if not first:
                    fh.write(",\n")
                fh.write(json.dumps(item, ensure_ascii=True))
                first = False
                count += 1

            if len(items) < NAUTOBOT_PAGE_SIZE:
                break
            offset += NAUTOBOT_PAGE_SIZE

        fh.write("]\n")

    log_message(log_fh, f"{name}: done {count} items -> {output_path}")
    return count



if __name__ == "__main__":
    session = requests.Session()
    total = 0
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(LOG_FILE, "a", encoding="utf-8") as log_fh:
        log_message(log_fh, "Run start")
        try:
            for name, query in EXPORTS:
                total += export_items(session, name, query, log_fh)
            log_message(log_fh, f"Run done. Wrote {total} items.")
        except Exception as exc:
            log_message(log_fh, f"Run failed: {exc}")
            raise
