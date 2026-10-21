"""Step 8: read events from Redpanda and load them into Postgres.

Delivery guarantee, the important part:
  1. read a batch of messages
  2. write ALL of them to Postgres in ONE transaction
  3. only then commit the Kafka offsets ("I'm done up to here")

If we crash between 2 and 3, the batch is read again on restart
(at least once delivery). That replay is harmless because every insert
is idempotent (on conflict). At least once + idempotent sink = the
warehouse ends up exactly once.
"""
import json
import logging
import os
import signal

import psycopg
from confluent_kafka import Consumer, KafkaError

from db import DSN, INFO_UPSERT_SQL, STATUS_INSERT_SQL

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
SQL_BY_TOPIC = {
    "gbfs.station_status": STATUS_INSERT_SQL,
    "gbfs.station_information": INFO_UPSERT_SQL,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("consumer")

running = True


def stop(*_):
    global running
    running = False


def group_by_topic(messages):
    """Split a mixed batch into {topic: [row dicts]}, skipping error messages."""
    grouped = {}
    for m in messages:
        if m.error():
            if m.error().code() != KafkaError._PARTITION_EOF:
                log.error("kafka error: %s", m.error())
            continue
        grouped.setdefault(m.topic(), []).append(json.loads(m.value()))
    return grouped


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "dockwatch-loader",   # consumers in one group share the partitions
        "auto.offset.reset": "earliest",  # brand new group: start from the oldest message
        "enable.auto.commit": False,      # WE decide when a message counts as done
    })
    consumer.subscribe(list(SQL_BY_TOPIC))

    with psycopg.connect(DSN, autocommit=True) as conn:
        while running:
            messages = consumer.consume(num_messages=500, timeout=1.0)
            if not messages:
                continue
            grouped = group_by_topic(messages)
            with conn.transaction(), conn.cursor() as cur:
                for topic, rows in grouped.items():
                    cur.executemany(SQL_BY_TOPIC[topic], rows)
            consumer.commit(asynchronous=False)  # strictly AFTER the DB commit
            log.info("loaded %s", {t: len(r) for t, r in grouped.items()})

    consumer.close()
    log.info("consumer stopped")


if __name__ == "__main__":
    main()
