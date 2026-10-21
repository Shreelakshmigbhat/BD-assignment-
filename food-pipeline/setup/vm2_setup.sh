#!/usr/bin/env bash
# VM2: KRaft controller + Kafka Broker 1 (node.id=1).
# Usage: bash vm2_setup.sh <VM2_ZEROTIER_IP>
# Prints the cluster ID at the end -- send it to the VM3 teammate.
set -euo pipefail
VM2_IP=${1:?Usage: bash vm2_setup.sh <VM2_ZEROTIER_IP>}
CFG=/opt/kafka/config/server.properties
DATA=/data/kafka

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
# VM2 - KRaft controller + broker 1
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@${VM2_IP}:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://${VM2_IP}:9093
advertised.listeners=PLAINTEXT://${VM2_IP}:9092
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

if [[ -f ~/kafka_cluster_id.txt ]]; then
  CLUSTER_ID=$(cat ~/kafka_cluster_id.txt)
else
  CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
  echo "$CLUSTER_ID" > ~/kafka_cluster_id.txt
fi

echo ">> Formatting storage with cluster ID $CLUSTER_ID"
/opt/kafka/bin/kafka-storage.sh format -t "$CLUSTER_ID" -c "$CFG"

echo ">> Starting Kafka"
sudo systemctl start kafka
for i in $(seq 1 30); do
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$VM2_IP:9092" >/dev/null 2>&1 && break
  sleep 2
done

echo ">> Quorum status"
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$VM2_IP:9092" describe --status

echo
echo "================================================================"
echo " VM2 is up.  Send these to the VM3 teammate:"
echo "   VM2 IP     : $VM2_IP"
echo "   CLUSTER ID : $CLUSTER_ID"
echo "================================================================"
