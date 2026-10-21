#!/usr/bin/env python3
"""Publish every line of a JSONL file to a Kafka topic, one record per message.

Used twice in the pipeline:
  Producer 1 (VM1): dataset          -> topic "orders"
  Producer 2 (VM4): order_metrics.jsonl -> topic "order_metrics"

Lines are sent exactly as they appear in the file (malformed lines included),
so that cleaning stays Spark Job 1's responsibility.
"""
import argparse
import json
import sys
import time

from confluent_kafka import Producer


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", required=True, help="e.g. 10.147.17.2:9092,10.147.17.3:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--file", required=True, help="JSONL file to publish")
    return ap.parse_args()


def record_key(line):
    """Key by order_id when the line is valid JSON with one, so a given order always lands in the same partition."""
    try:
        obj = json.loads(line)
        if isinstance(obj, dict) and obj.get("order_id") is not None:
            return str(obj["order_id"]).encode("utf-8")
    except ValueError:
        pass
    return None


def main():
    args = parse_args()
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "client.id": f"producer-{args.topic}",
    })

    stats = {"delivered": 0, "failed": 0}
    per_partition = {}

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            print(f"Delivery failed: {err}", file=sys.stderr)
        else:
            stats["delivered"] += 1
            per_partition[msg.partition()] = per_partition.get(msg.partition(), 0) + 1

    sent = 0
    skipped_blank = 0
    start = time.time()
    with open(args.file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line.strip():
                skipped_blank += 1
                continue
            while True:
                try:
                    producer.produce(args.topic, value=line.encode("utf-8"), key=record_key(line), on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.5)
            sent += 1
            producer.poll(0)

    remaining = producer.flush(60)
    elapsed = time.time() - start

    print("=" * 60)
    print(f"Topic            : {args.topic}")
    print(f"Source file      : {args.file}")
    print(f"Lines sent       : {sent}")
    print(f"Blank lines skip : {skipped_blank}")
    print(f"Delivered (acked): {stats['delivered']}")
    print(f"Failed           : {stats['failed'] + remaining}")
    for p in sorted(per_partition):
        print(f"  partition {p}    : {per_partition[p]}")
    print(f"Time taken       : {elapsed:.1f}s")
    print("=" * 60)
    sys.exit(0 if stats["failed"] == 0 and remaining == 0 else 1)


if __name__ == "__main__":
    main()
