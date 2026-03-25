#!/usr/bin/env bash
set -euo pipefail

# --- 1. Start nats-server (Reference) ---
docker stop nats-bench-final > /dev/null 2>&1 || true
docker rm nats-bench-final > /dev/null 2>&1 || true
docker run -d --name nats-bench-final -p 4225:4222 nats:latest

# --- 2. Start CosmoBroker (Optimized) ---
pkill -f CosmoBroker.Server || true
dotnet run --project CosmoBroker.Server -c Release --no-build -- 4226 8226 > cb_bench.log 2>&1 &
CB_PID=$!

# Wait 20s for both to be fully ready
sleep 20

# --- 3. Run CosmoBroker Benchmark ---
dotnet run --project CosmoBroker.Benchmarks -c Release -- \
  --label CosmoBroker.Client --url nats://localhost:4226 \
  --count 100000 --payload 128 --publishers 1 --latency 1000

# --- 4. Run nats-server Benchmark (using CosmoBroker.Client SDK) ---
dotnet run --project CosmoBroker.Benchmarks -c Release -- \
  --label "CosmoBroker.Client (nats-server)" --url nats://localhost:4225 \
  --count 100000 --payload 128 --publishers 1 --latency 1000

# --- Cleanup ---
kill $CB_PID || true
docker stop nats-bench-final > /dev/null 2>&1 || true
docker rm nats-bench-final > /dev/null 2>&1 || true
