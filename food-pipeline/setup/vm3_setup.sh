#!/usr/bin/env bash
# VM3: Kafka Broker 2 (node.id=2). VM2 is the only KRaft controller.
# Usage: bash vm3_setup.sh <VM2_ZEROTIER_IP> <VM3_ZEROTIER_IP> <CLUSTER_ID_FROM_VM2>
set -euo pipefail
VM2_IP=${1:?Usage: bash vm3_setup.sh <VM2_IP> <VM3_IP> <CLUSTER_ID>}
VM3_IP=${2:?Usage: bash vm3_setup.sh <VM2_IP> <VM3_IP> <CLUSTER_ID>}
CLUSTER_ID=${3:?Usage: bash vm3_setup.sh <VM2_IP> <VM3_IP> <CLUSTER_ID>}
CFG=/opt/kafka/config/server.properties
DATA=/data/kafka

echo ">> Checking that the controller on VM2 is reachable"
if ! timeout 5 bash -c "</dev/tcp/$VM2_IP/9093" 2>/dev/null; then
  echo "Cannot reach $VM2_IP:9093. Is VM2's Kafka running and are both VMs OK on ZeroTier?" >&2
  exit 1
fi

echo ">> Stopping the single-node Kafka that came with the VM"
sudo systemctl stop kafka || true

echo ">> Backing up old config and data"
STAMP=$(date +%Y%m%d-%H%M%S)
cp "$CFG" "$CFG.single-node.$STAMP.bak"
if [[ -d $DATA && -n "$(ls -A "$DATA" 2>/dev/null)" ]]; then
  sudo mv "$DATA" "$DATA.single-node.$STAMP.bak"
fi
sudo mkdir -p "$DATA" && sudo chown "$USER": "$DATA"

echo ">> Writing $CFG"
cat > "$CFG" <<EOF
# VM3 - broker 2
process.roles=broker
node.id=2
controller.quorum.voters=1@${VM2_IP}:9093
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://${VM3_IP}:9092
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=${DATA}

num.partitions=2
default.replication.factor=2
min.insync.replicas=1
auto.create.topics.enable=false
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=2
share.coordinator.state.topic.min.isr=1
log.retention.hours=168
EOF

echo ">> Formatting storage with cluster ID $CLUSTER_ID"
/opt/kafka/bin/kafka-storage.sh format -t "$CLUSTER_ID" -c "$CFG"

echo ">> Starting Kafka"
sudo systemctl start kafka
for i in $(seq 1 30); do
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$VM3_IP:9092" >/dev/null 2>&1 && break
  sleep 2
done

echo ">> Brokers in the cluster (expect id: 1 and id: 2)"
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$VM2_IP:9092,$VM3_IP:9092" | grep -o '^[^ ]* (id: [0-9]*[^)]*)'
