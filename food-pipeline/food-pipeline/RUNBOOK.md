# Distributed Food Ordering System — Runbook

Run the phases **in order**. `📸 SS-n` marks where to take a screenshot; keep the numbers so the report is easy to assemble.

| VM | Role | Runs |
|---|---|---|
| VM1 | Producer 1, Consumer 2, Spark Job 2 | `producer.py`, `consumer.py`, `spark_job2.py` |
| VM2 | KRaft controller + Broker 1 | `setup/vm2_setup.sh`, topic and test commands |
| VM3 | Broker 2 | `setup/vm3_setup.sh` |
| VM4 | Consumer 1, Spark Job 1, Producer 2 | `consumer.py`, `spark_job1.py`, `producer.py` |

**Pasting into the Ubuntu terminal:** use **Ctrl+Shift+V**. If you see `^[[200~` in front of a command, press Ctrl+U and paste again.

---

## Phase 0 — ZeroTier (all 4 VMs)

ZeroTier network ID: **`743993800f3cfc30`** (owner approves members at https://my.zerotier.com).

On **every VM**:
```bash
sudo zerotier-cli join 743993800f3cfc30
sudo zerotier-cli status        # send the 10-character node ID to the network owner
```
After the owner ticks **Auth** for every node:
```bash
sudo zerotier-cli listnetworks  # status OK + an IP in the last column
```
📸 **SS-1** (each VM): `sudo zerotier-cli listnetworks` showing `OK` and the IP.
📸 **SS-2** (owner): ZeroTier Central → Members page with all 4 nodes authorised.

Fill in and share with the team:

| VM | ZeroTier IP |
|---|---|
| VM1 | |
| VM2 | |
| VM3 | |
| VM4 | |

In **every terminal** you open from now on, set these first (with the real IPs):
```bash
export VM2_IP=10.x.x.x
export VM3_IP=10.x.x.x
export BOOTSTRAP=$VM2_IP:9092,$VM3_IP:9092
```

From each VM, ping the others:
```bash
ping -c 3 $VM2_IP
```

---

## Phase 1 — Get the code onto each VM (all 4 VMs)

Copy `food-pipeline.zip` into the VM (Google Drive / WhatsApp Web in the VM's browser, or `scp`), then:
```bash
cd ~ && unzip -o ~/Downloads/food-pipeline.zip -d ~ && ls ~/food-pipeline
```

---

## Phase 2 — Kafka cluster

### VM2 (do first)
```bash
bash ~/food-pipeline/setup/vm2_setup.sh $VM2_IP
```
It stops the VM's built-in single-node Kafka, backs up its config and data, writes the cluster config, formats storage and starts Kafka. At the end it prints the **CLUSTER ID**; send it and VM2's IP to the VM3 teammate.

📸 **SS-3** (VM2): `cat /opt/kafka/config/server.properties`
📸 **SS-4** (VM2): the quorum status printed at the end (`LeaderId: 1`) and the CLUSTER ID.

### VM3
```bash
bash ~/food-pipeline/setup/vm3_setup.sh $VM2_IP $VM3_IP <CLUSTER_ID_FROM_VM2>
```
📸 **SS-5** (VM3): `cat /opt/kafka/config/server.properties`
📸 **SS-6** (VM3): the last lines listing `id: 1` and `id: 2`.

### VM2 — confirm both brokers and create the topics
```bash
kafka-broker-api-versions.sh --bootstrap-server $BOOTSTRAP | grep -o '^[^ ]* (id: [0-9]*[^)]*)'
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status

kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic orders        --partitions 2 --replication-factor 2
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic order_metrics --partitions 2 --replication-factor 2
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic orders
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic order_metrics
```
📸 **SS-7** (VM2): broker list (ids 1 and 2) + quorum status.
📸 **SS-8** (VM2): both `--describe` outputs — `PartitionCount: 2`, `ReplicationFactor: 2`, `Replicas: 1,2` / `2,1`, `Isr` with both brokers.

---

## Phase 3 — Client setup (VM1 and VM4)

```bash
bash ~/food-pipeline/setup/client_setup.sh $VM2_IP $VM3_IP
```
Installs the Python Kafka client, stops the VM's own unused Kafka, and lists the cluster's topics.
📸 **SS-9** (VM1) and **SS-10** (VM4): both brokers `OK` and the topic list showing `orders` and `order_metrics`.

---

## Phase 4 — Stage 1: Order Enrichment

### VM1 — get the dataset
Download your team's dataset (A2 Dataset Download → enter team number) and put it at
`~/food-pipeline/data/orders_dataset.jsonl`, then check it:
```bash
cd ~/food-pipeline
wc -l data/orders_dataset.jsonl
head -3 data/orders_dataset.jsonl
```
If the file is **not** one JSON object per line (e.g. it is CSV or a single JSON array), stop and fix the format first.
📸 **SS-11** (VM1): `wc -l` + `head -3` of the dataset.

### VM1 — Producer 1 → `orders`
```bash
cd ~/food-pipeline
python3 producer.py --bootstrap $BOOTSTRAP --topic orders --file data/orders_dataset.jsonl
```
📸 **SS-12** (VM1): producer summary (lines sent = delivered, failed = 0, split across partitions 0 and 1).

> Run the producer **once**. Running it again appends a second copy to the topic. If you need to redo it, delete and recreate the topic on VM2 first.

### VM4 — Consumer 1 → `orders_landed.jsonl`
```bash
cd ~/food-pipeline
python3 consumer.py --bootstrap $BOOTSTRAP --topic orders --group vm4-orders-consumer --out data/orders_landed.jsonl
wc -l data/orders_landed.jsonl
```
📸 **SS-13** (VM4): consumer summary + `wc -l` (must equal SS-12's "Lines sent").

### VM4 — Spark Job 1 → `order_metrics.jsonl`
```bash
cd ~/food-pipeline
spark-submit spark_job1.py --input data/orders_landed.jsonl --output data/order_metrics.jsonl
wc -l data/order_metrics.jsonl
head -3 data/order_metrics.jsonl
```
(If `spark-submit` is not found: `/opt/spark/bin/spark-submit ...`)
📸 **SS-14** (VM4): the Spark Job 1 summary box (input / malformed / duplicates / invalid / valid).
📸 **SS-15** (VM4): `head -3` of `order_metrics.jsonl`.

### VM4 — Producer 2 → `order_metrics`
```bash
python3 producer.py --bootstrap $BOOTSTRAP --topic order_metrics --file data/order_metrics.jsonl
```
📸 **SS-16** (VM4): producer summary (delivered = valid records from SS-14).

---

## Phase 5 — Stage 2: Operations Analytics

### VM1 — Consumer 2 → `order_metrics_landed.jsonl`
```bash
cd ~/food-pipeline
python3 consumer.py --bootstrap $BOOTSTRAP --topic order_metrics --group vm1-order-metrics-consumer --out data/order_metrics_landed.jsonl
wc -l data/order_metrics_landed.jsonl
```
📸 **SS-17** (VM1): consumer summary + `wc -l`.

### VM1 — Spark Job 2 → CSVs
```bash
spark-submit spark_job2.py --input data/order_metrics_landed.jsonl --outdir output
cat output/city_summary.csv
head -15 output/restaurant_summary.csv
wc -l output/*.csv
```
📸 **SS-18** (VM1): Spark Job 2 summary box.
📸 **SS-19** (VM1): `city_summary.csv` and the top of `restaurant_summary.csv`.

### VM2 — consumer groups caught up
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group vm4-orders-consumer
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group vm1-order-metrics-consumer
```
📸 **SS-20** (VM2): both groups with `LAG 0` on partitions 0 and 1.

---

## Phase 6 — Broker failure test (stop VM3, not VM2)

### VM2 — before
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic orders
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic order_metrics
```
📸 **SS-21** (VM2): `Isr: 1,2` / `2,1` on every partition.

### VM3 — stop the broker
```bash
sudo systemctl stop kafka
sudo systemctl status kafka --no-pager | head -5
```
📸 **SS-22** (VM3): Kafka `inactive (dead)`.

### VM2 — during (use only VM2 as the bootstrap server)
Wait ~10 s, then:
```bash
kafka-topics.sh --bootstrap-server $VM2_IP:9092 --describe --topic orders
kafka-topics.sh --bootstrap-server $VM2_IP:9092 --describe --topic order_metrics
```
📸 **SS-23** (VM2): every partition now has `Leader: 1` and `Isr: 1` — data still served by VM2.

Prove the data is still fully readable (the counts must match SS-12 and SS-16):
```bash
kafka-console-consumer.sh --bootstrap-server $VM2_IP:9092 --topic orders --from-beginning --timeout-ms 20000 2>/dev/null | wc -l
kafka-console-consumer.sh --bootstrap-server $VM2_IP:9092 --topic order_metrics --from-beginning --timeout-ms 20000 2>/dev/null | wc -l
```
📸 **SS-24** (VM2): both counts.

### VM3 — start it again
```bash
sudo systemctl start kafka
```
### VM2 — after recovery (wait ~20 s)
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic orders
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic order_metrics
```
📸 **SS-25** (VM2): `Isr` back to both brokers.

---

## What to submit

| File | Where |
|---|---|
| `orders_landed.jsonl` | VM4 `~/food-pipeline/data/` |
| `order_metrics.jsonl` | VM4 `~/food-pipeline/data/` |
| `order_metrics_landed.jsonl` | VM1 `~/food-pipeline/data/` |
| `restaurant_summary.csv` | VM1 `~/food-pipeline/output/` |
| `city_summary.csv` | VM1 `~/food-pipeline/output/` |
| Code | `producer.py`, `consumer.py`, `spark_job1.py`, `spark_job2.py`, `setup/*.sh` |
| Screenshots | SS-1 … SS-25 |

---

## How the rules are implemented

**Spark Job 1** reads every field as text, then drops, in this order:
1. lines that are not valid JSON;
2. exact duplicates (all 14 fields identical) — kept once;
3. records with any field missing/empty, a number that doesn't parse (`distance_km`, `order_amount`, `delivery_fee`), or a timestamp that isn't a real date-time;
4. `distance_km <= 0`, `order_amount <= 0`, or `status` not `DELIVERED`.

Times are differences in minutes. `delay_min = max(0, delivered − expected)`; `ON_TIME` when 0, else `DELAYED`. Categories: 0 → `ON_TIME`, ≤15 → `SLIGHT_DELAY`, ≤30 → `MODERATE_DELAY`, >30 → `SEVERE_DELAY`. All computed numbers are rounded half-up to 2 decimals.

**Spark Job 2** drops exact duplicate records again, groups by `restaurant_id` and by `city`, and writes the averages and `delay_rate_pct = 100 × delayed / orders` to 2 decimals. `DELAYED` is taken from `delivery_status`.

**Local files only.** The course VM image sets Spark's default filesystem to HDFS and its default catalog to Iceberg; both jobs override this (`fs.defaultFS=file:///`) so nothing touches HDFS.

**Kafka.** VM2 is the only KRaft controller (`controller.quorum.voters=1@VM2:9093`); VM2 and VM3 are brokers. `min.insync.replicas=1` and replicated internal topics (`offsets.topic.replication.factor=2`) keep reads, writes and consumer groups working while VM3 is down.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `zerotier-cli status` says `OFFLINE` | VM has no internet: `ping 8.8.8.8`. In VMware use **NAT** with **Connected** ticked. |
| `listnetworks` says `ACCESS_DENIED` | Owner must tick **Auth** for that node in ZeroTier Central. |
| VM3 setup: "Cannot reach VM2:9093" | VM2's Kafka not running (`sudo systemctl status kafka` on VM2) or ZeroTier not `OK`. |
| Topic create fails: replication factor larger than available brokers | VM3 isn't up yet — check SS-6/SS-7. |
| Consumer prints "waiting for the producer" | Expected if the upstream producer hasn't run yet; it continues once data arrives. |
| Kafka won't start | `journalctl -u kafka -n 50 --no-pager` and `tail -50 /opt/kafka/logs/server.log` |
| Need to redo a stage | On VM2: `kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic orders`, recreate it, rerun from Producer 1. |
