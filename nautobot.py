#!/usr/bin/env python3
import asyncio
import functools
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

IP_ADDRESS_QUERY = """
query IpAddressInventory($limit: Int!, $offset: Int!) {
  ip_addresses(limit: $limit, offset: $offset) {
    address
    status { name }
    role { name }
    dns_name
    description
    tenant { name }
    vrf { name }
    tags { name }
    custom_fields
    created
    last_updated
    assigned_object {
      ... on Interface { name device { name } }
      ... on VMInterface { name virtual_machine { name } }
    }
  }
}
"""

PREFIX_QUERY = """
query PrefixInventory($limit: Int!, $offset: Int!) {
  prefixes(limit: $limit, offset: $offset) {
    prefix
    status { name }
    role { name }
    description
    is_pool
    site { name }
    tenant { name }
    vrf { name }
    vlan { name vid }
    tags { name }
    custom_fields
    created
    last_updated
  }
}
"""

VLAN_QUERY = """
query VlanInventory($limit: Int!, $offset: Int!) {
  vlans(limit: $limit, offset: $offset) {
    name
    vid
    status { name }
    role { name }
    description
    site { name }
    group { name }
    tenant { name }
    tags { name }
    custom_fields
    created
    last_updated
  }
}
"""

VRF_QUERY = """
query VrfInventory($limit: Int!, $offset: Int!) {
  vrfs(limit: $limit, offset: $offset) {
    name
    rd
    tenant { name }
    enforce_unique
    description
    tags { name }
    custom_fields
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
async def log_message(log_fh, log_lock, message):
    async with log_lock:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"{timestamp} {message}"
        print(line)
        log_fh.write(line + "\n")
        log_fh.flush()


# Global async rate limiter
class RateLimiter:
    def __init__(self, max_rps):
        self.min_interval = 1.0 / float(max(1, max_rps))
        self.lock = asyncio.Lock()
        self.last_request = 0.0

    async def wait(self):
        async with self.lock:
            elapsed = time.monotonic() - self.last_request
            if self.last_request and elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self.last_request = time.monotonic()


# Async Nautobot GraphQL call (throttled)
async def query_nautobot(session, limiter, query, variables):
    await limiter.wait()
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(
        None,
        functools.partial(
            session.post,
            NAUTOBOT_GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers={"Authorization": f"Token {NAUTOBOT_TOKEN}"},
            timeout=NAUTOBOT_TIMEOUT,
            verify=NAUTOBOT_VERIFY,
        ),
    )
    response.raise_for_status()
    data = response.json()
    if data.get("errors"):
        raise RuntimeError(f"Errors: {data['errors']}")
    return data


# Export one object type to JSON
async def export_items(name, query, log_fh, log_lock, limiter):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f"{name}.json")
    offset = 0
    count = 0
    first = True
    session = requests.Session()

    await log_message(log_fh, log_lock, f"{name}: start export -> {output_path}")
    try:
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write("[")
            while True:
                data = await query_nautobot(
                    session,
                    limiter,
                    query,
                    {"limit": NAUTOBOT_PAGE_SIZE, "offset": offset},
                )

                items = data.get("data", {}).get(name, [])
                if not items:
                    break

                await log_message(log_fh, log_lock, f"{name}: page offset {offset} items {len(items)}")
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
    finally:
        session.close()

    await log_message(log_fh, log_lock, f"{name}: done {count} items -> {output_path}")
    return count


async def main():
    if NAUTOBOT_TOKEN == "CHANGE_ME":
        print("Set NAUTOBOT_TOKEN in nautobot.py before running.")
        return 2

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    limiter = RateLimiter(NAUTOBOT_MAX_RPS)
    log_lock = asyncio.Lock()

    with open(LOG_FILE, "a", encoding="utf-8") as log_fh:
        await log_message(log_fh, log_lock, "Run start")
        try:
            tasks = [
                export_items(name, query, log_fh, log_lock, limiter)
                for name, query in EXPORTS
            ]
            results = await asyncio.gather(*tasks)
            total = sum(results)
            await log_message(log_fh, log_lock, f"Run done. Wrote {total} items.")
        except Exception as exc:
            await log_message(log_fh, log_lock, f"Run failed: {exc}")
            raise
    return 0


# Entrypoint
if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
