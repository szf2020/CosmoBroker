#!/bin/sh
set -eu

: "${COSMOBROKER_PORT:=4222}"
: "${COSMOBROKER_MONITOR_PORT:=8222}"
: "${COSMOBROKER_ENABLE_NATS:=true}"
: "${COSMOBROKER_ENABLE_AMQP:=false}"
: "${COSMOBROKER_MANAGEMENT_PORT:=9091}"

if [ "${COSMOBROKER_ENABLE_AMQP}" = "true" ] && [ -z "${COSMOBROKER_AMQP_PORT:-}" ]; then
  COSMOBROKER_AMQP_PORT=5672
fi

export COSMOBROKER_PORT
export COSMOBROKER_MONITOR_PORT
export COSMOBROKER_ENABLE_NATS
export COSMOBROKER_ENABLE_AMQP
export COSMOBROKER_AMQP_PORT="${COSMOBROKER_AMQP_PORT:-0}"
export COSMOBROKER_MANAGEMENT_PORT
export COSMOBROKER_MONITOR_URL="${COSMOBROKER_MONITOR_URL:-http://127.0.0.1:${COSMOBROKER_MONITOR_PORT}}"

broker_pid=""
management_pid=""

shutdown() {
  if [ -n "${management_pid}" ] && kill -0 "${management_pid}" 2>/dev/null; then
    kill "${management_pid}" 2>/dev/null || true
    wait "${management_pid}" 2>/dev/null || true
  fi

  if [ -n "${broker_pid}" ] && kill -0 "${broker_pid}" 2>/dev/null; then
    kill "${broker_pid}" 2>/dev/null || true
    wait "${broker_pid}" 2>/dev/null || true
  fi
}

trap shutdown INT TERM

dotnet /app/server/CosmoBroker.Server.dll &
broker_pid=$!

for _ in $(seq 1 30); do
  if echo "" | nc -w 1 127.0.0.1 "${COSMOBROKER_MONITOR_PORT}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

dotnet /app/management/CosmoBroker.Management.dll &
management_pid=$!

while true; do
  if ! kill -0 "${broker_pid}" 2>/dev/null; then
    wait "${broker_pid}" || true
    shutdown
    exit 1
  fi

  if ! kill -0 "${management_pid}" 2>/dev/null; then
    wait "${management_pid}" || true
    shutdown
    exit 1
  fi

  sleep 1
done
