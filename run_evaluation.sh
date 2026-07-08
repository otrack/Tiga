#!/bin/bash
set -e

TIGA_DIR="/home/otrack/Implementation/Tiga"
YCSB_DIR="/home/otrack/Implementation/YCSB"

cleanup() {
  echo ""
  echo "===================================================="
  echo "Cleanup: Printing container logs..."
  echo "===================================================="
  cd "$TIGA_DIR"
  docker compose logs --tail=100 || true

  echo ""
  echo "===================================================="
  echo "Cleanup: Stopping and removing Tiga containers..."
  echo "===================================================="
  docker compose down || true
}
trap cleanup EXIT INT TERM


PROTOCOL="${1:-tiga}"
echo "Selected protocol: $PROTOCOL"
export PROTOCOL

echo "===================================================="
echo "1. Rebuilding Docker Image for Tiga/Calvin/Detock..."
echo "===================================================="
cd "$TIGA_DIR"
docker build -t tiga-suite .

echo ""
echo "===================================================="
echo "2. Bootstrapping 3-node cluster via Compose..."
echo "===================================================="
docker compose down || true
docker compose up -d

echo "Waiting 3 seconds for cluster nodes to stabilize..."
sleep 3

echo ""
echo "===================================================="
echo "3. Compiling Java YCSB client binding..."
echo "===================================================="
cd "$YCSB_DIR"
mvn clean package -pl tiga -am -DskipTests

echo ""
echo "===================================================="
echo "4. Loading YCSB workload against cluster..."
echo "===================================================="
# Expose the native JNI library location to Java
export JAVA_OPTS="-Djava.library.path=$TIGA_DIR/bazel-bin/ycsb_jni"

./bin/ycsb load tiga \
  -P workloads/workloada \
  -p tiga.config="$TIGA_DIR/config-ycsb.yml" \
  -p tiga.mode="$PROTOCOL"

echo ""
echo "===================================================="
echo "5. Running YCSB workload against cluster..."
echo "===================================================="
./bin/ycsb run tiga \
  -P workloads/workloada \
  -p tiga.config="$TIGA_DIR/config-ycsb.yml" \
  -p tiga.mode="$PROTOCOL"
