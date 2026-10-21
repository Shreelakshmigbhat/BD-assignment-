#!/usr/bin/env python3
"""Independent plain-Python version of both Spark jobs, to cross-check Spark's output.

Usage: python3 reference_check.py <orders_landed.jsonl> <order_metrics.jsonl> <output_dir>
Exits non-zero if any value differs.
"""
import csv
import json
import math
import re
import sys
from collections import OrderedDict, defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

FIELDS = ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
          "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
          "expected_delivery_timestamp", "delivered_timestamp",
          "distance_km", "order_amount", "delivery_fee", "status"]
TS = FIELDS[5:10]


def ts(v):
    if not isinstance(v, str) or not re.match(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}", v.strip()):
        return None
    try:
        return datetime.fromisoformat(v.strip().replace("T", " "))
    except ValueError:
        return None


def num(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def r2(x):
    """Round half-up to 2 places on the decimal form of x, the way Spark's round() does."""
    if x is None:
        return None
    return float(Decimal(repr(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def f2(x):
    return f"{r2(x):.2f}"


def job1(lines):
    seen, out = set(), []
    for line in lines:
        try:
            o = json.loads(line)
        except ValueError:
            continue
        if not isinstance(o, dict):
            continue
        key = tuple(None if o.get(f) is None else str(o.get(f)) for f in FIELDS)
        if key in seen:
            continue
        seen.add(key)
        if any(o.get(f) is None or str(o.get(f)).strip() == "" for f in ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city", "status"]):
            continue
        t = {f: ts(o.get(f)) for f in TS}
        n = {f: num(o.get(f)) for f in ["distance_km", "order_amount", "delivery_fee"]}
        if None in t.values() or None in n.values():
            continue
        if n["distance_km"] <= 0 or n["order_amount"] <= 0 or str(o["status"]).strip() != "DELIVERED":
            continue
        mins = lambda a, b: (t[a] - t[b]).total_seconds() / 60.0
        prep = mins("picked_up_timestamp", "accepted_timestamp")
        dlv = mins("delivered_timestamp", "picked_up_timestamp")
        ful = mins("delivered_timestamp", "order_timestamp")
        delay = max(0.0, mins("delivered_timestamp", "expected_delivery_timestamp"))
        speed = n["distance_km"] / (dlv / 60.0) if dlv != 0 else None
        d = r2(delay)
        cat = "ON_TIME" if d == 0 else "SLIGHT_DELAY" if d <= 15 else "MODERATE_DELAY" if d <= 30 else "SEVERE_DELAY"
        out.append(OrderedDict([
            ("order_id", o["order_id"]), ("customer_id", o["customer_id"]), ("restaurant_id", o["restaurant_id"]),
            ("delivery_partner_id", o["delivery_partner_id"]), ("city", o["city"]),
            ("order_amount", r2(n["order_amount"])), ("delivery_fee", r2(n["delivery_fee"])), ("distance_km", r2(n["distance_km"])),
            ("preparation_time_min", r2(prep)), ("delivery_time_min", r2(dlv)), ("fulfillment_time_min", r2(ful)),
            ("delivery_speed_kmph", r2(speed)), ("delay_min", d),
            ("delivery_status", "ON_TIME" if d == 0 else "DELAYED"), ("delay_category", cat),
        ]))
    return sorted(out, key=lambda r: r["order_id"])


def job2(metrics):
    def summarise(key, cols):
        g = defaultdict(list)
        for m in metrics:
            g[m[key]].append(m)
        rows = []
        for k in sorted(g):
            ms = g[k]
            c = len(ms)
            delayed = sum(1 for m in ms if m["delivery_status"] == "DELAYED")
            vals = {
                key: k, "order_count": str(c),
                "avg_order_amount": f2(sum(m['order_amount'] for m in ms) / c),
                "avg_preparation_time_min": f2(sum(m['preparation_time_min'] for m in ms) / c),
                "avg_delivery_time_min": f2(sum(m['delivery_time_min'] for m in ms) / c),
                "delayed_order_count": str(delayed),
                "delay_rate_pct": f2(100.0 * delayed / c),
            }
            rows.append([vals[col] for col in cols])
        return rows
    return (summarise("restaurant_id", ["restaurant_id", "order_count", "avg_order_amount", "avg_preparation_time_min",
                                        "avg_delivery_time_min", "delayed_order_count", "delay_rate_pct"]),
            summarise("city", ["city", "order_count", "avg_order_amount", "avg_delivery_time_min",
                               "delayed_order_count", "delay_rate_pct"]))


def close(a, b):
    if isinstance(a, float) or isinstance(b, float):
        return a is not None and b is not None and abs(a - b) < 0.006
    return a == b


def main():
    landed, metrics_path, outdir = sys.argv[1:4]
    expected = job1(open(landed, encoding="utf-8").read().splitlines())
    got = [json.loads(l) for l in open(metrics_path, encoding="utf-8")]
    bad = 0
    if len(expected) != len(got):
        print(f"JOB1 record count differs: expected {len(expected)}, spark {len(got)}"); bad += 1
    for e, g in zip(expected, got):
        if list(e.keys()) != list(g.keys()):
            print("JOB1 column order differs", list(g.keys())); bad += 1; break
        for k in e:
            if not close(e[k], g[k]):
                print(f"JOB1 {e['order_id']} {k}: expected {e[k]}, spark {g[k]}"); bad += 1
    print(f"Job 1: {len(expected)} records checked")

    exp_r, exp_c = job2(expected)
    for name, exp in [("restaurant_summary.csv", exp_r), ("city_summary.csv", exp_c)]:
        rows = list(csv.reader(open(f"{outdir}/{name}", encoding="utf-8")))[1:]
        if rows != exp:
            print(f"JOB2 {name} differs"); bad += 1
            for a, b in zip(exp, rows):
                if a != b:
                    print("  expected", a, "\n  spark   ", b)
        print(f"Job 2: {name} {len(exp)} rows checked")
    print("ALL MATCH" if bad == 0 else f"{bad} MISMATCHES")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
