#!/usr/bin/env python3
"""Batch-consume a whole Kafka topic into a local JSONL file, then exit.

Used twice in the pipeline:
  Consumer 1 (VM4): topic "orders"        -> orders_landed.jsonl
  Consumer 2 (VM1): topic "order_metrics" -> order_metrics_landed.jsonl

The consumer reads every partition from the beginning up to the end offset
that existed when it started, writes each message value as one line, and
commits those offsets under --group so the group shows up with zero lag in
kafka-consumer-groups.sh. If the topic is still empty it waits (--wait) for
the upstream producer before giving up.
"""
import argparse
import sys
import time

from confluent_kafka import Consumer, KafkaException, TopicPartition, OFFSET_BEGINNING


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", required=True, help="e.g. 10.147.17.2:9092,10.147.17.3:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--group", required=True, help="consumer group id")
    ap.add_argument("--out", required=True, help="local JSONL file to write")
    ap.add_argument("--wait", type=int, default=300, help="seconds to wait for data if the topic is empty")
    return ap.parse_args()


def end_offsets(consumer, topic, partitions):
    lows, highs = {}, {}
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
        lows[p], highs[p] = low, high
    return lows, highs


def main():
    args = parse_args()
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "client.id": f"consumer-{args.topic}",
    })

    md = consumer.list_topics(args.topic, timeout=15)
    tmd = md.topics.get(args.topic)
    if tmd is None or tmd.error is not None:
        print(f"Topic '{args.topic}' not found: {tmd.error if tmd else 'missing'}", file=sys.stderr)
        sys.exit(1)
    partitions = sorted(tmd.partitions.keys())

    deadline = time.time() + args.wait
    while True:
        lows, highs = end_offsets(consumer, args.topic, partitions)
        if sum(highs[p] - lows[p] for p in partitions) > 0:
            break
        if time.time() > deadline:
            print(f"Topic '{args.topic}' is still empty after {args.wait}s; nothing to consume.", file=sys.stderr)
            sys.exit(1)
        print(f"Topic '{args.topic}' is empty, waiting for the producer...")
        time.sleep(5)

    # Wait until the producer has finished (end offsets stop moving) so the batch is complete.
    while True:
        time.sleep(5)
        lows2, highs2 = end_offsets(consumer, args.topic, partitions)
        if highs2 == highs:
            break
        print(f"Producer still writing ({sum(highs2.values())} messages so far), waiting...")
        lows, highs = lows2, highs2

    consumer.assign([TopicPartition(args.topic, p, OFFSET_BEGINNING) for p in partitions])
    remaining = {p for p in partitions if highs[p] > lows[p]}
    per_partition = {p: 0 for p in partitions}
    written = 0
    last_progress = time.time()

    with open(args.out, "w", encoding="utf-8") as out:
        while remaining:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_progress > 120:
                    print("No progress for 120s, stopping early.", file=sys.stderr)
                    break
                continue
            if msg.error():
                raise KafkaException(msg.error())
            p = msg.partition()
            if msg.offset() >= highs[p]:
                remaining.discard(p)
                continue
            value = msg.value()
            if value is not None:
                out.write(value.decode("utf-8", errors="replace").rstrip("\r\n") + "\n")
                written += 1
                per_partition[p] += 1
            last_progress = time.time()
            if msg.offset() >= highs[p] - 1:
                remaining.discard(p)

    consumer.commit(offsets=[TopicPartition(args.topic, p, highs[p]) for p in partitions], asynchronous=False)
    consumer.close()

    print("=" * 60)
    print(f"Topic            : {args.topic}")
    print(f"Consumer group   : {args.group}")
    print(f"Records written  : {written}")
    for p in partitions:
        print(f"  partition {p}    : {per_partition[p]} (end offset {highs[p]})")
    print(f"Output file      : {args.out}")
    print("=" * 60)


if __name__ == "__main__":
    main()
