#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COSMO_PORT="${COSMO_PORT:-4522}"
COSMO_MONITOR_PORT="${COSMO_MONITOR_PORT:-8522}"
COSMO_AMQP_PORT="${COSMO_AMQP_PORT:-5679}"
RABBIT_AMQP_PORT="${RABBIT_AMQP_PORT:-5672}"
COUNT="${COUNT:-1000}"
PAYLOAD="${PAYLOAD:-128}"
LATENCY="${LATENCY:-100}"
TIMEOUT_MS="${TIMEOUT_MS:-3000}"
REPORT_PATH="${REPORT_PATH:-$ROOT_DIR/benchmarks/amqp-compare-report.txt}"
DEFAULT_RELEASE_DLL="$ROOT_DIR/CosmoBroker.Server/bin/Release/net10.0/CosmoBroker.Server.dll"
DEFAULT_DEBUG_DLL="$ROOT_DIR/CosmoBroker.Server/bin/Debug/net10.0/CosmoBroker.Server.dll"
SERVER_DLL="${SERVER_DLL:-}"

if [[ -z "$SERVER_DLL" ]]; then
  if [[ -f "$DEFAULT_RELEASE_DLL" ]]; then
    SERVER_DLL="$DEFAULT_RELEASE_DLL"
  else
    SERVER_DLL="$DEFAULT_DEBUG_DLL"
  fi
fi

mkdir -p "$(dirname "$REPORT_PATH")"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_for_port() {
  local host="$1"
  local port="$2"
  for _ in $(seq 1 100); do
    if python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket, sys
host = sys.argv[1]
port = int(sys.argv[2])
with socket.socket() as s:
    s.settimeout(0.2)
    s.connect((host, port))
PY
    then
      return 0
    fi
    sleep 0.1
  done
  echo "Timed out waiting for ${host}:${port}" >&2
  return 1
}

echo "Starting CosmoBroker for AMQP comparison..."
if [[ ! -f "$SERVER_DLL" ]]; then
  echo "CosmoBroker server binary not found: $SERVER_DLL" >&2
  exit 1
fi

dotnet "$SERVER_DLL" \
  "$COSMO_PORT" "$COSMO_MONITOR_PORT" "$COSMO_AMQP_PORT" \
  >"$ROOT_DIR/benchmarks/cosmobroker-amqp-compare.log" 2>&1 &
SERVER_PID=$!

wait_for_port 127.0.0.1 "$COSMO_AMQP_PORT"
wait_for_port 127.0.0.1 "$RABBIT_AMQP_PORT"

echo "Running RabbitMQ.Client comparison..."
dotnet run --project "$ROOT_DIR/CosmoBroker.Benchmarks/CosmoBroker.Benchmarks.csproj" -- \
  --mode compare-amqp \
  --cosmo-url "amqp://guest:guest@127.0.0.1:${COSMO_AMQP_PORT}/" \
  --rabbit-url "amqp://guest:guest@127.0.0.1:${RABBIT_AMQP_PORT}/" \
  --count "$COUNT" \
  --payload "$PAYLOAD" \
  --latency "$LATENCY" \
  --timeout "$TIMEOUT_MS" | tee "$REPORT_PATH"

echo "Report written to $REPORT_PATH"
