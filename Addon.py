import pynautobot
import requests
import json
import os
from datetime import datetime

# ======================
# Configuration
# ======================
NAUTOBOT_URL = os.getenv("NAUTOBOT_URL")
NAUTOBOT_TOKEN = os.getenv("NAUTOBOT_TOKEN")

SPLUNK_HEC_URL = os.getenv("SPLUNK_HEC_URL")
SPLUNK_HEC_TOKEN = os.getenv("SPLUNK_HEC_TOKEN")
SPLUNK_INDEX = os.getenv("SPLUNK_INDEX", "nautobot")

HEADERS_SPLUNK = {
    "Authorization": f"Splunk {SPLUNK_HEC_TOKEN}",
    "Content-Type": "application/json"
}

# ======================
# Connexion Nautobot
# ======================
nautobot = pynautobot.api(
    NAUTOBOT_URL,
    token=NAUTOBOT_TOKEN
)

# ======================
# Envoi vers Splunk
# ======================
def send_to_splunk(events):
    payload = "\n".join(json.dumps(event) for event in events)

    response = requests.post(
        SPLUNK_HEC_URL,
        headers=HEADERS_SPLUNK,
        data=payload,
        verify=False
    )

    if response.status_code != 200:
        print("❌ Erreur Splunk:", response.text)
    else:
        print(f"✅ {len(events)} devices envoyés à Splunk")

# ======================
# Export des devices
# ======================
def export_devices():
    events = []

    devices = nautobot.dcim.devices.all()

    for device in devices:
        event = {
            "time": datetime.utcnow().timestamp(),
            "index": SPLUNK_INDEX,
            "source": "nautobot",
            "sourcetype": "nautobot:device",
            "event": {
                "id": device.id,
                "name": device.name,
                "status": device.status.value if device.status else None,
                "role": device.device_role.name if device.device_role else None,
                "type": device.device_type.model if device.device_type else None,
                "manufacturer": (
                    device.device_type.manufacturer.name
                    if device.device_type and device.device_type.manufacturer
                    else None
                ),
                "site": device.site.name if device.site else None,
                "tenant": device.tenant.name if device.tenant else None,
                "platform": device.platform.name if device.platform else None,
                "serial": device.serial,
                "primary_ip": str(device.primary_ip) if device.primary_ip else None,
                "created": device.created,
                "last_updated": device.last_updated,
            }
        }

        events.append(event)

        # Envoi par batch (important pour Splunk)
        if len(events) >= 100:
            send_to_splunk(events)
            events = []

    # Envoi du reste
    if events:
        send_to_splunk(events)

# ======================
# Main
# ======================
if __name__ == "__main__":
    export_devices()
