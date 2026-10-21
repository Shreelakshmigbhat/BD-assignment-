#!/usr/bin/env python3
"""Generate a synthetic orders file with every cleaning edge case, for testing only."""
import json
import random
import sys
from datetime import datetime, timedelta

random.seed(7)
out = sys.argv[1] if len(sys.argv) > 1 else "sample_orders.jsonl"
cities = ["Bengaluru", "Mumbai", "Delhi", "Chennai"]
base = datetime(2026, 3, 1, 11, 0, 0)


def order(i):
    t0 = base + timedelta(minutes=7 * i)
    acc = t0 + timedelta(minutes=random.randint(1, 5))
    pick = acc + timedelta(minutes=random.randint(8, 25))
    exp = t0 + timedelta(minutes=40)
    deliv = pick + timedelta(minutes=random.randint(10, 45))
    fmt = "%Y-%m-%dT%H:%M:%S" if i % 5 == 0 else "%Y-%m-%d %H:%M:%S"
    return {
        "order_id": f"ORD{i:05d}",
        "customer_id": f"CUST{random.randint(1, 60):03d}",
        "restaurant_id": f"REST{random.randint(1, 8):02d}",
        "delivery_partner_id": f"DP{random.randint(1, 20):02d}",
        "city": random.choice(cities),
        "order_timestamp": t0.strftime(fmt),
        "accepted_timestamp": acc.strftime(fmt),
        "picked_up_timestamp": pick.strftime(fmt),
        "expected_delivery_timestamp": exp.strftime(fmt),
        "delivered_timestamp": deliv.strftime(fmt),
        "distance_km": round(random.uniform(0.8, 12.0), 2),
        "order_amount": round(random.uniform(120, 1500), 2),
        "delivery_fee": round(random.choice([0, 19, 25, 39, 49]), 2),
        "status": "DELIVERED",
    }


lines = []
for i in range(1, 181):
    lines.append(json.dumps(order(i)))

bad = []
o = order(500); o["distance_km"] = 0; bad.append(json.dumps(o))
o = order(501); o["distance_km"] = -2.5; bad.append(json.dumps(o))
o = order(502); o["order_amount"] = 0; bad.append(json.dumps(o))
o = order(503); o["status"] = "CANCELLED"; bad.append(json.dumps(o))
o = order(504); o["delivered_timestamp"] = "2026-13-45 99:00:00"; bad.append(json.dumps(o))
o = order(505); o["picked_up_timestamp"] = "not-a-time"; bad.append(json.dumps(o))
o = order(506); del o["city"]; bad.append(json.dumps(o))
o = order(507); o["restaurant_id"] = ""; bad.append(json.dumps(o))
o = order(508); o["order_amount"] = "abc"; bad.append(json.dumps(o))
o = order(509); o["customer_id"] = None; bad.append(json.dumps(o))
bad.append('{"order_id": "ORD00510", "customer_id": "CUST001", ')          # truncated JSON
bad.append('this is not json at all')
bad.append('{"order_id": "ORD00511" "city": "Delhi"}')                        # missing comma

dups = [lines[3], lines[3], lines[10], lines[42]]                             # exact duplicates
def late_by(i, minutes):
    """Order i delivered `minutes` after its expected time (picked up 10 min before expected)."""
    o = order(i)
    exp = datetime.strptime(o["expected_delivery_timestamp"].replace("T", " "), "%Y-%m-%d %H:%M:%S")
    o["picked_up_timestamp"] = (exp - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    o["delivered_timestamp"] = (exp + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    return json.dumps(o)


edge = [late_by(601, 0), late_by(602, 15), late_by(603, 16), late_by(604, 30), late_by(606, 31)]
# 0 -> ON_TIME, 15 -> SLIGHT, 16 and 30 -> MODERATE, 31 -> SEVERE

allines = lines + bad + dups + edge
random.shuffle(allines)
with open(out, "w") as f:
    f.write("\n".join(allines) + "\n")
print(f"wrote {len(allines)} lines to {out}: 180 good + {len(edge)} edge, {len(bad)} bad, {len(dups)} duplicates")
