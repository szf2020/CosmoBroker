#!/usr/bin/env bash
set -e

# 1. Setup servers
docker stop nats-bench-diag > /dev/null 2>&1 || true
docker rm nats-bench-diag > /dev/null 2>&1 || true
docker run -d --name nats-bench-diag -p 4261:4222 nats:latest

pkill -f CosmoBroker.Server || true
dotnet run --project CosmoBroker.Server -c Release --no-build -- 4262 8262 > cb_diag.log 2>&1 &
CB_PID=$!

echo "Warming up servers (20s)..."
sleep 20

echo ">>> ATTEMPT 1 (Cold Start) <<<"
dotnet run --project CosmoBroker.Benchmarks -c Release --no-build -- \
  --label "CosmoBroker-Attempt1" --url nats://localhost:4262 \
  --count 10000 --payload 64 --publishers 1 --latency 100 --sub-pending 1000000

echo ""
echo ">>> ATTEMPT 2 (Warm Start) <<<"
dotnet run --project CosmoBroker.Benchmarks -c Release --no-build -- \
  --label "CosmoBroker-Attempt2" --url nats://localhost:4262 \
  --count 10000 --payload 64 --publishers 1 --latency 100 --sub-pending 1000000

echo ""
echo ">>> ATTEMPT 3 (Fully Warm) <<<"
dotnet run --project CosmoBroker.Benchmarks -c Release --no-build -- \
  --label "CosmoBroker-Attempt3" --url nats://localhost:4262 \
  --count 10000 --payload 64 --publishers 1 --latency 100 --sub-pending 1000000

# Cleanup
kill $CB_PID
docker stop nats-bench-diag > /dev/null 2>&1
