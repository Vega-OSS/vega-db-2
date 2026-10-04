#!/usr/bin/env bash
set -euo pipefail

echo "=== Building and starting VegaDB container ==="
docker compose down -v 2>/dev/null || true
docker compose up -d --build

echo "=== Waiting for container to become healthy ==="
for i in $(seq 1 30); do
  status=$(docker inspect --format '{{.State.Health.Status}}' vegadb 2>/dev/null || echo "starting")
  if [ "$status" = "healthy" ]; then
    echo "Container is healthy!"
    break
  fi
  echo "Current status: $status (waiting... $i/30)"
  sleep 1
done

final_status=$(docker inspect --format '{{.State.Health.Status}}' vegadb 2>/dev/null || echo "failed")
if [ "$final_status" != "healthy" ]; then
  echo "ERROR: Container failed to become healthy. Logs:"
  docker logs vegadb
  exit 1
fi

echo "=== Testing REST PUT and GET ==="
curl -s -f -X PUT --data-binary 'hello' localhost:8080/v1/kv/greeting
val=$(curl -s -f localhost:8080/v1/kv/greeting)
if [ "$val" != "hello" ]; then
  echo "ERROR: Expected 'hello', got '$val'"
  exit 1
fi
echo "REST PUT/GET succeeded: $val"

echo "=== Testing gRPC via vegactl inside container ==="
docker exec vegadb /vegactl --addr 127.0.0.1:50051 put foo bar
grpc_val=$(docker exec vegadb /vegactl --addr 127.0.0.1:50051 get foo)
if [ "$grpc_val" != "bar" ]; then
  echo "ERROR: Expected 'bar', got '$grpc_val'"
  exit 1
fi
echo "gRPC PUT/GET succeeded: $grpc_val"

echo "=== Testing persistence across restart ==="
docker compose restart
for i in $(seq 1 30); do
  status=$(docker inspect --format '{{.State.Health.Status}}' vegadb 2>/dev/null || echo "starting")
  if [ "$status" = "healthy" ]; then
    echo "Container restarted and healthy!"
    break
  fi
  sleep 1
done

persisted_val=$(curl -s -f localhost:8080/v1/kv/greeting)
if [ "$persisted_val" != "hello" ]; then
  echo "ERROR: Expected 'hello' after restart, got '$persisted_val'"
  exit 1
fi
echo "Persistence verification succeeded: greeting = $persisted_val"

echo "=== Testing graceful stop ==="
docker compose down -v
echo "=== Docker smoke test passed! ==="
