#!/usr/bin/env bash
# VM1 and VM4: install the Python Kafka client and check the cluster is reachable.
# Usage: bash client_setup.sh <VM2_ZEROTIER_IP> <VM3_ZEROTIER_IP>
set -euo pipefail
VM2_IP=${1:?Usage: bash client_setup.sh <VM2_IP> <VM3_IP>}
VM3_IP=${2:?Usage: bash client_setup.sh <VM2_IP> <VM3_IP>}

echo ">> Installing python3-confluent-kafka"
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -yq python3-confluent-kafka
python3 -c "import confluent_kafka; print('confluent-kafka', confluent_kafka.version()[0])"

echo ">> Stopping this VM's own single-node Kafka (not part of the pipeline; frees memory)"
sudo systemctl stop kafka 2>/dev/null || true

mkdir -p ~/food-pipeline/data ~/food-pipeline/output

echo ">> Reaching the cluster"
for ip in "$VM2_IP" "$VM3_IP"; do
  if timeout 5 bash -c "</dev/tcp/$ip/9092" 2>/dev/null; then echo "  $ip:9092 OK"; else echo "  $ip:9092 NOT reachable"; fi
done
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$VM2_IP:9092,$VM3_IP:9092" --list
