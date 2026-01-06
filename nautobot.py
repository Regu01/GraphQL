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

EXPORTS = [
    {
        "name": "devices",
        "query": DEVICE_QUERY,
        "root": "devices",
        "sourcetype": "nautobot:device",
        "host_key": "name",
    },
    {
        "name": "ip_addresses",
        "query": IP_ADDRESS_QUERY,
        "root": "ip_addresses",
        "sourcetype": "nautobot:ip_address",
    },
    {
        "name": "prefixes",
        "query": PREFIX_QUERY,
        "root": "prefixes",
        "sourcetype": "nautobot:prefix",
    },
    {
        "name": "vlans",
        "query": VLAN_QUERY,
        "root": "vlans",
        "sourcetype": "nautobot:vlan",
    },
    {
        "name": "vrfs",
        "query": VRF_QUERY,
        "root": "vrfs",
        "sourcetype": "nautobot:vrf",
    },
]

# Function to prune empty values from data structures
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

# Function to send a batch of events to Splunk HEC
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

# Main function to perform GraphQL queries and send data to Splunk HEC
def graphql(query, root, sourcetype, host_key=None):
    session = requests.Session()
    offset = 0
    sent = 0
    batch = []

    while True:
        response = session.post(
            NAUTOBOT_GRAPHQL_URL,
            json={"query": query, "variables": {"limit": NAUTOBOT_PAGE_SIZE, "offset": offset}},
            headers={"Authorization": f"Token {NAUTOBOT_TOKEN}"},
            timeout=NAUTOBOT_TIMEOUT,
            verify=NAUTOBOT_VERIFY,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("errors"):
            raise RuntimeError(f"Errors: {data['errors']}")

        items = data.get("data", {}).get(root, [])
        if not items:
            break

        for item in items:
            item = prune_empty(item)
            if not item:
                continue
            event = {
                "index": SPLUNK_HEC_INDEX,
                "sourcetype": sourcetype,
                "source": SPLUNK_HEC_SOURCE,
                "event": item,
            }
            if host_key and item.get(host_key):
                event["host"] = item[host_key]

            batch.append(event)

            if len(batch) >= SPLUNK_HEC_BATCH_SIZE:
                sent += send_batch(session, batch)
                batch = []

        if len(items) < NAUTOBOT_PAGE_SIZE:
            break
        offset += NAUTOBOT_PAGE_SIZE

    sent += send_batch(session, batch)
    print(f"Sent {sent} {root}.")
    return 0


if __name__ == "__main__":
    for export in EXPORTS:
        graphql(
            export["query"],
            export["root"],
            export["sourcetype"],
            export.get("host_key"),
        )
