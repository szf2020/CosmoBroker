#!/usr/bin/env bash
set -e

run_step() {
  COUNT=$1
  PAYLOAD=$2
  echo ">>> STEP: Count=$COUNT, Payload=$PAYLOAD bytes <<<"
  
  # CosmoBroker
  dotnet run --project CosmoBroker.Benchmarks -c Release --no-build -- \
    --label "CosmoBroker" --url nats://localhost:4260 \
    --count $COUNT --payload $PAYLOAD --publishers 1 --latency 200 --sub-pending 1000000
    
  echo ""
  
  # nats-server
  dotnet run --project CosmoBroker.Benchmarks -c Release --no-build -- \
    --label "nats-server" --url nats://localhost:4259 \
    --count $COUNT --payload $PAYLOAD --publishers 1 --latency 200 --sub-pending 1000000
  echo "---------------------------------------------------------"
}

# 1. Setup servers
docker stop nats-bench-gradual > /dev/null 2>&1 || true
docker rm nats-bench-gradual > /dev/null 2>&1 || true
docker run -d --name nats-bench-gradual -p 4259:4222 nats:latest

pkill -f CosmoBroker.Server || true
dotnet run --project CosmoBroker.Server -c Release --no-build -- 4260 8260 > cb_gradual.log 2>&1 &
CB_PID=$!

echo "Warming up servers (20s)..."
sleep 20

# 2. Run gradual steps
run_step 10000 64
run_step 50000 256
run_step 100000 512
run_step 250000 1024
run_step 500000 2048

# 3. Cleanup
kill $CB_PID
docker stop nats-bench-gradual > /dev/null 2>&1
