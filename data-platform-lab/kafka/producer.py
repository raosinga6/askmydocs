"""Send sample JSON events to the lab.events topic."""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "lab.events"
BOOTSTRAP = "localhost:19092"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10, help="Number of events to send")
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    for i in range(args.count):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "page_view" if i % 2 == 0 else "signup",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "user_id": f"user_{i % 5}",
            "payload": {"source": "lab-producer", "seq": i},
        }
        producer.send(TOPIC, value=event)
        print(f"sent {event['event_id']} ({event['event_type']})")

    producer.flush()
    producer.close()
    print(f"done: {args.count} events -> {TOPIC}")


if __name__ == "__main__":
    main()
