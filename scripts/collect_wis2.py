import argparse
import ssl
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlopen

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from asos_parquet.wis2 import EccodesBufrDecoder, Wis2Collector, Wis2Notification

TOPIC = "cache/a/wis2/+/data/core/weather/surface-based-observations/synop/#"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--broker", default="globalbroker.meteo.fr")
    parser.add_argument("--port", type=int, default=8883)
    args = parser.parse_args()
    collector = Wis2Collector(args.output, EccodesBufrDecoder())
    client = mqtt.Client(CallbackAPIVersion.VERSION2)
    client.username_pw_set("everyone", "everyone")
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

    def on_connect(
        client: mqtt.Client,
        userdata: object,
        flags: object,
        reason_code: object,
        properties: object,
    ) -> None:
        client.subscribe(TOPIC, qos=1)

    def on_message(client: mqtt.Client, userdata: object, message: mqtt.MQTTMessage) -> None:
        parsed = Wis2Notification.parse(message.topic, message.payload)
        downloaded = None
        if parsed.content is None:
            with urlopen(parsed.canonical_url, timeout=60) as response:
                downloaded = response.read()
        result = collector.process(message.topic, message.payload, datetime.now(UTC), downloaded)
        print(f"{parsed.publisher} {parsed.data_id} {result.status}", flush=True)

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(args.broker, args.port, 60)
    client.loop_forever(retry_first_connection=True)


if __name__ == "__main__":
    main()
