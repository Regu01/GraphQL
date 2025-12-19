import os
import json
import time
import requests
import pynautobot
from datetime import datetime

# ======================
# CONFIG
# ======================
NAUTOBOT_URL = os.getenv("NAUTOBOT_URL")
NAUTOBOT_TOKEN = os.getenv("NAUTOBOT_TOKEN")

SPLUNK_HEC_URL = os.getenv("SPLUNK_HEC_URL")
SPLUNK_HEC_TOKEN = os.getenv("SPLUNK_HEC_TOKEN")
SPLUNK_INDEX = os.getenv("SPLUNK_INDEX", "nautobot")

PAGE_SIZE = 200
BATCH_SIZE = 300
SPLUNK_TIMEOUT = 10
SPLUNK_RETRIES = 3

HEADERS_SPLUNK = {
    "Authorization": f"Splunk {SPLUNK_HEC_TOKEN}",
    "Content-Type": "application/json"
}

# ======================
# Nautobot API
# ======================
nautobot = pynautobot.api(
    NAUTOBOT_URL,
    token=NAUTOBOT_TOKEN
)

# ======================
# Splunk Sender (sync)
# ======================
def send_to_splunk(batch):
    payload = "\n".join(json.dumps(e) for e in batch)

    for attempt in range(1, SPLUNK_RETRIES + 1):
        try:
            r = requests.post(
                SPLUNK_HEC_URL,
                headers=HEADERS_SPLUNK,
                data=payload,
                timeout=SPLUNK_TIMEOUT,
                verify=True
            )

            if r.status_code == 200:
                return True

            print(f"⚠️ Splunk HTTP {r.status_code}: {r.text}")

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Splunk attempt {attempt}: {e}")

        time.sleep(2)

    print("❌ Batch abandonné après retries")
    return False

# ======================
# Helper Splunk event
# ======================
def splunk_event(sourcetype, data):
    return {
        "time": datetime.utcnow().timestamp(),
        "index": SPLUNK_INDEX,
        "source": "nautobot",
        "sourcetype": sourcetype,
        "event": data
    }

# ======================
# EXPORT FUNCTIONS
# ======================
def export_devices():
    offset = 0
    batch = []
    total = 0

    while True:
        devices = nautobot.dcim.devices.filter(limit=PAGE_SIZE, offset=offset)
        if not devices:
            break

        for d in devices:
            batch.append(splunk_event("nautobot:device", {
                "id": d.id,
                "name": d.name,
                "status": d.status.value if d.status else None,
                "site": d.site.name if d.site else None,
                "role": d.device_role.name if d.device_role else None,
                "type": d.device_type.model if d.device_type else None,
                "platform": d.platform.name if d.platform else None,
                "serial": d.serial
            }))

            if len(batch) >= BATCH_SIZE:
                if send_to_splunk(batch):
                    total += len(batch)
                batch.clear()

        offset += PAGE_SIZE
        print(f"[DEVICES] traités: {offset}")

    if batch:
        if send_to_splunk(batch):
            total += len(batch)

    print(f"✅ Devices envoyés: {total}")

def export_interfaces():
    offset = 0
    batch = []
    total = 0

    while True:
        interfaces = nautobot.dcim.interfaces.filter(limit=PAGE_SIZE, offset=offset)
        if not interfaces:
            break

        for i in interfaces:
            batch.append(splunk_event("nautobot:interface", {
                "id": i.id,
                "name": i.name,
                "device": i.device.name if i.device else None,
                "type": i.type.value if i.type else None,
                "enabled": i.enabled,
                "mac_address": i.mac_address
            }))

            if len(batch) >= BATCH_SIZE:
                if send_to_splunk(batch):
                    total += len(batch)
                batch.clear()

        offset += PAGE_SIZE
        print(f"[INTERFACES] traités: {offset}")

    if batch:
        if send_to_splunk(batch):
            total += len(batch)

    print(f"✅ Interfaces envoyées: {total}")

def export_ip_addresses():
    offset = 0
    batch = []
    total = 0

    while True:
        ips = nautobot.ipam.ip_addresses.filter(limit=PAGE_SIZE, offset=offset)
        if not ips:
            break

        for ip in ips:
            batch.append(splunk_event("nautobot:ip", {
                "id": ip.id,
                "address": ip.address,
                "status": ip.status.value if ip.status else None,
                "tenant": ip.tenant.name if ip.tenant else None,
                "assigned_object": str(ip.assigned_object)
            }))

            if len(batch) >= BATCH_SIZE:
                if send_to_splunk(batch):
                    total += len(batch)
                batch.clear()

        offset += PAGE_SIZE
        print(f"[IPS] traités: {offset}")

    if batch:
        if send_to_splunk(batch):
            total += len(batch)

    print(f"✅ IPs envoyées: {total}")

def export_sites():
    batch = []
    total = 0

    for s in nautobot.dcim.sites.all():
        batch.append(splunk_event("nautobot:site", {
            "id": s.id,
            "name": s.name,
            "status": s.status.value if s.status else None,
            "region": s.region.name if s.region else None
        }))

        if len(batch) >= BATCH_SIZE:
            if send_to_splunk(batch):
                total += len(batch)
            batch.clear()

    if batch:
        if send_to_splunk(batch):
            total += len(batch)

    print(f"✅ Sites envoyés: {total}")

def export_device_types():
    batch = []
    total = 0

    for dt in nautobot.dcim.device_types.all():
        batch.append(splunk_event("nautobot:device_type", {
            "id": dt.id,
            "model": dt.model,
            "manufacturer": dt.manufacturer.name if dt.manufacturer else None
        }))

        if len(batch) >= BATCH_SIZE:
            if send_to_splunk(batch):
                total += len(batch)
            batch.clear()

    if batch:
        if send_to_splunk(batch):
            total += len(batch)

    print(f"✅ Device types envoyés: {total}")

# ======================
# MAIN
# ======================
if __name__ == "__main__":
    export_devices()
    export_interfaces()
    export_ip_addresses()
    export_sites()
    export_device_types()
