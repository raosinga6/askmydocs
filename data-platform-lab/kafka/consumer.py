"""Read lab.events and insert into Postgres raw.events."""

from __future__ import annotations

import argparse
import json
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer

TOPIC = "lab.events"
BOOTSTRAP = "localhost:19092"
GROUP_ID = "lab-consumer"
DSN = "host=localhost port=5432 dbname=lab user=lab password=lab"


def parse_ts(value: str) -> datetime:
    # ISO strings from producer; handle trailing Z
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def upsert_event(cur, event: dict) -> None:
    cur.execute(
        """
        INSERT INTO raw.events (event_id, event_type, occurred_at, user_id, payload)
        VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            event["event_id"],
            event["event_type"],
            parse_ts(event["occurred_at"]),
            event.get("user_id"),
            json.dumps(event.get("payload") or {}),
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-messages", type=int, default=50)
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    parser.add_argument("--from-beginning", action="store_true")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=args.bootstrap,
        group_id=GROUP_ID,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=10_000,
    )

    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    processed = 0

    try:
        with conn.cursor() as cur:
            for msg in consumer:
                upsert_event(cur, msg.value)
                conn.commit()
                processed += 1
                print(f"stored {msg.value['event_id']}")
                if processed >= args.max_messages:
                    break
    finally:
        consumer.close()
        conn.close()

    print(f"done: stored {processed} message(s)")


if __name__ == "__main__":
    main()
