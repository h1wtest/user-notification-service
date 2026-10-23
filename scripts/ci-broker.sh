#!/usr/bin/env bash
set -euo pipefail

# Test-only plaintext broker bound to loopback. Runtime clients always require
# TLS/SCRAM; no production configuration or existing cluster is modified.
image='docker.io/redpandadata/redpanda:v26.2.5@sha256:badca2d958f81da675c3055a9ecb6ebb590325582bcf16936148ffd65e96e920'
name="go-kafka-regression-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}-${RANDOM}"
port="${TEST_KAFKA_PORT:-29093}"
cleanup() {
  status=$?
  trap - EXIT
  if ! docker rm -fv "$name"; then
    echo "Unable to remove owned test broker $name" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

docker run --detach --name "$name" --publish "127.0.0.1:${port}:${port}" "$image" redpanda start --mode dev-container --smp 1 --memory 512M --reserve-memory 0M --overprovisioned --node-id 0 --check=false --kafka-addr "0.0.0.0:${port}" --advertise-kafka-addr "127.0.0.1:${port}"
ready=false
for _ in {1..60}; do
  if docker exec "$name" rpk -X "brokers=127.0.0.1:${port}" cluster info; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  docker logs "$name"
  echo 'Disposable Kafka did not become ready' >&2
  exit 1
fi
docker exec "$name" rpk -X "brokers=127.0.0.1:${port}" topic create identity.events.v1 --partitions 6
docker exec "$name" rpk -X "brokers=127.0.0.1:${port}" topic create notification.commands.v1 --partitions 6
docker exec "$name" rpk -X "brokers=127.0.0.1:${port}" topic create platform.dlq.v1 --partitions 6
export TEST_KAFKA_BROKERS="127.0.0.1:${port}"
go test -race ./...
