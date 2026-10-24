#!/usr/bin/env bash
set -euo pipefail
umask 077

# Isolated test-only TLS/SCRAM broker; runtime security is not overridden.
image='docker.io/redpandadata/redpanda:v26.2.5@sha256:badca2d958f81da675c3055a9ecb6ebb590325582bcf16936148ffd65e96e920'
root="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/go-kafka-regression.XXXXXXXX")"
name="${root##*/}"
port="${TEST_KAFKA_PORT:-29093}"
cleanup() {
  status=$?
  trap - EXIT
  if ! docker rm -fv "$name"; then
    echo "Unable to remove owned test broker $name" >&2
  fi
  rm -rf -- "$root"
  exit "$status"
}
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$root/ca.key" -out "$root/ca.crt" -subj /CN=notification-ci-ca -days 1 >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$root/broker.key" -out "$root/broker.csr" -subj /CN=localhost >/dev/null 2>&1
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > "$root/extensions"
openssl x509 -req -in "$root/broker.csr" -CA "$root/ca.crt" -CAkey "$root/ca.key" -CAcreateserial -out "$root/broker.crt" -days 1 -extfile "$root/extensions" >/dev/null 2>&1
password="$(openssl rand -hex 32)"
if [[ "${GITHUB_ACTIONS:-}" == true ]]; then
  printf '::add-mask::%s\n' "$password"
fi
cat > "$root/redpanda.yaml" <<YAML
redpanda:
  data_directory: /var/lib/redpanda/data
  node_id: 0
  seed_servers: []
  rpc_server: {address: 0.0.0.0, port: 33145}
  advertised_rpc_api: {address: localhost, port: 33145}
  kafka_api:
    - {address: 0.0.0.0, port: ${port}, name: secure, authentication_method: sasl}
  advertised_kafka_api:
    - {address: 127.0.0.1, port: ${port}, name: secure}
  kafka_api_tls:
    - name: secure
      enabled: true
      cert_file: /tls/broker.crt
      key_file: /tls/broker.key
      truststore_file: /tls/ca.crt
      require_client_auth: false
  admin: [{address: 0.0.0.0, port: 9644}]
  enable_sasl: true
  superusers: [notification-ci]
  developer_mode: true
YAML

docker run --detach --name "$name" --user 0:0 --publish "127.0.0.1:${port}:${port}" --volume "$root:/tls:ro" --entrypoint redpanda "$image" --redpanda-cfg /tls/redpanda.yaml --smp 1 --memory 512M --reserve-memory 0M --overprovisioned
ready=false
for _ in {1..60}; do
  if docker exec "$name" rpk cluster health -X admin.hosts=localhost:9644 >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  docker logs "$name"
  echo 'Disposable TLS Kafka did not become ready' >&2
  exit 1
fi
docker exec "$name" rpk security user create notification-ci --password "$password" --mechanism SCRAM-SHA-256 -X admin.hosts=localhost:9644
for topic in identity.events.v1 notification.commands.v1 platform.dlq.v1 identity.events.v2 notification.commands.v2 platform.dlq.v2; do
  docker exec "$name" rpk topic create "$topic" --partitions 6 -X "brokers=127.0.0.1:${port}" -X tls.enabled=true -X tls.ca=/tls/ca.crt -X user=notification-ci -X "pass=$password" -X sasl.mechanism=SCRAM-SHA-256
done
export TEST_KAFKA_BROKERS="127.0.0.1:${port}"
export TEST_KAFKA_CA="$root/ca.crt"
export TEST_KAFKA_USERNAME=notification-ci
export TEST_KAFKA_PASSWORD="$password"
go test -race -v ./...
