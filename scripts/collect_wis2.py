import argparse
import ssl
import traceback
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from asos_parquet.wis2 import EccodesBufrDecoder, Wis2Collector

TOPIC = "cache/a/wis2/+/data/core/weather/surface-based-observations/synop/#"
MAX_PAYLOAD_BYTES = 32 * 1024 * 1024


def fetch_canonical(url: str) -> bytes:
    """Download a canonical BUFR object announced by an anonymous public broker.

    The URL is publisher-controlled, so the scheme is restricted (``urlopen`` would
    otherwise honour ``file://`` and ``ftp://``) and the read is capped.
    """
    if urlparse(url).scheme not in {"http", "https"}:
        raise ValueError(f"Refusing non-HTTP canonical URL: {url}")
    with urlopen(url, timeout=60) as response:
        data: bytes = response.read(MAX_PAYLOAD_BYTES + 1)
    if len(data) > MAX_PAYLOAD_BYTES:
        raise ValueError(f"Canonical object exceeds {MAX_PAYLOAD_BYTES} bytes: {url}")
    return data


def stable_client_id(root: Path) -> str:
    """Return a client id that survives restarts so the broker keeps the QoS 1 session."""
    path = root / "client_id"
    if not path.exists():
        path.write_text(f"asos-parquet-wis2-{uuid.uuid4().hex[:16]}\n")
    return path.read_text().strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--broker", default="globalbroker.meteo.fr")
    parser.add_argument("--port", type=int, default=8883)
    parser.add_argument("--client-id", default=None)
    args = parser.parse_args()
    collector = Wis2Collector(args.output, EccodesBufrDecoder())
    client_id = args.client_id or stable_client_id(args.output)
    # A random, clean-session client discards everything published while the collector is
    # restarting, which is exactly what the restart/outage drills need to measure.
    client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=False)
    client.username_pw_set("everyone", "everyone")
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

    def on_connect(
        client: mqtt.Client,
        userdata: object,
        flags: object,
        reason_code: object,
        properties: object,
    ) -> None:
        if getattr(reason_code, "is_failure", False):
            print(f"connect refused: {reason_code}", flush=True)
            return
        client.subscribe(TOPIC, qos=1)

    def on_message(client: mqtt.Client, userdata: object, message: mqtt.MQTTMessage) -> None:
        # paho re-raises callback exceptions and loop_forever only catches OSError, so an
        # unhandled error here would silently end the 30-day collection window.
        try:
            result = collector.process(
                message.topic, message.payload, datetime.now(UTC), fetch_canonical
            )
        except Exception:
            print(f"error {message.topic}\n{traceback.format_exc()}", flush=True)
            return
        print(f"{result.publisher} {result.data_id} {result.status}", flush=True)

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(args.broker, args.port, 60)
    client.loop_forever(retry_first_connection=True)


if __name__ == "__main__":
    main()
