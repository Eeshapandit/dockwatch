"""Step 7: poll the live feed and PUBLISH changes to Redpanda (Kafka).

Why put a broker between the API and the database?
  * Decoupling: if Postgres is down, events wait safely in the topic
    instead of being lost. The producer never has to know about the DB.
  * Replay: the topic keeps 7 days of events. A new consumer (or a bug fix)
    can re-read history from the start.
  * Fan out: later, other consumers (alerts, a live map) can read the same
    stream without touching this code.

Topics:
  gbfs.station_status       one message per REAL change (Step 6), key = station_id
  gbfs.station_information  full snapshot every hour, compacted, key = station_id
"""
import json
import logging
import os
import signal
import time

import requests
from confluent_kafka import Producer

from gbfs import changed_rows, clean_info_feed, clean_status_feed, remember

ROOT = os.getenv("GBFS_ROOT", "https://gbfs.citibikenyc.com/gbfs/gbfs.json")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC_STATUS = "gbfs.station_status"
TOPIC_INFO = "gbfs.station_information"
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
INFO_EVERY_SECONDS = 3600

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("producer")

running = True


def stop(*_):
    global running
    running = False


def to_json(row):
    """datetimes are not JSON, so send them as ISO 8601 strings."""
    return json.dumps({k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()})


def publish(producer, topic, rows):
    for row in rows:
        # The KEY decides the partition. Same station -> same partition -> its
        # events stay in order. Kafka only guarantees order within a partition.
        producer.produce(topic, key=row["station_id"], value=to_json(row))
        producer.poll(0)  # serve delivery callbacks without blocking
    producer.flush(10)  # wait until the broker has everything


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    session = requests.Session()
    discovery = session.get(ROOT, timeout=15).json()
    feeds = {f["name"]: f["url"] for f in discovery["data"]["en"]["feeds"]}

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,   # broker drops duplicates caused by our own retries
        "compression.type": "zstd",   # smaller batches on the wire
        "linger.ms": 50,              # wait up to 50ms to batch messages together
    })

    last_state = {}  # in memory: after a restart, the first cycle re-sends every station once
    next_info = 0.0
    while running:
        started = time.monotonic()
        try:
            if started >= next_info:
                info = clean_info_feed(session.get(feeds["station_information"], timeout=15).json())
                publish(producer, TOPIC_INFO, info)
                log.info("station_information: published %d stations", len(info))
                next_info = started + INFO_EVERY_SECONDS

            resp = session.get(feeds["station_status"], timeout=15)
            resp.raise_for_status()
            fetched = clean_status_feed(resp.json())
            rows = changed_rows(fetched, last_state)
            publish(producer, TOPIC_STATUS, rows)
            remember(rows, last_state)
            log.info("station_status: %d stations, %d changed, published", len(fetched), len(rows))
        except requests.RequestException as e:
            log.warning("fetch failed, will retry next cycle: %s", e)

        time.sleep(max(0, POLL_SECONDS - (time.monotonic() - started)))

    producer.flush(10)
    log.info("producer stopped")


if __name__ == "__main__":
    main()
