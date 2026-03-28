# Benchmarking and AMQP Comparison

This repository ships a helper script that runs RabbitMQ.Client's AMQP compliance suite against both
CosmoBroker and a stock RabbitMQ server, then emits a concise comparison report plus the raw scenario
log. Use it whenever you want to confirm feature parity or capture performance regressions after large
changes.

## Latest AMQP comparison (v1.3.0)

| Metric | CosmoBroker (amqp://localhost:5679) | RabbitMQ 3.11 (amqp://localhost:5672) | Notes |
| --- | --- | --- | --- |
| Throughput (best of two runs) | 595,451 msg/sec | 1,221,449 msg/sec | Both runs built from the same machine; RabbitMQ yields higher throughput while CosmoBroker maintains lower latencies.
| Drop rate | 0.00% | 0.00% | Neither broker lost messages in this run. |
| Latency (average) | 0.141 ms | 0.297 ms | CosmoBroker is roughly half the latency of RabbitMQ on the same hardware. |
| Latency (p95) | 0.150 ms | 0.263 ms | Tight p95 values demonstrate stable delivery; CosmoBroker holds a lighter tail. |
| AMQP scenarios | 16 passed | 16 passed | All scenario comparisons succeeded with identical behavior (see report for details). |

The comparison details, including per-scenario diff snippets such as `basic_publish_get`, `tx_commit_roundtrip`,
`exclusive_queue_other_connection`, and the final `Anomalies` line, are recorded under
[`benchmarks/amqp-compare-report.txt`](../benchmarks/amqp-compare-report.txt).

## Reproducing the comparison

1. Build the release binaries so the benchmark script can start the latest server:

```bash
dotnet build CosmoBroker.Server/CosmoBroker.Server.csproj -c Release
```

2. Ensure a RabbitMQ server is reachable on `localhost:5672`. The fastest way locally is to use
   the bundled helper script with Docker:

```bash
docker run --rm -d --name cosmobroker-bench-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

3. Run the comparison script (it will automatically start CosmoBroker on `5679`, wait for both AMQP endpoints,
   and stream the benchmark results to `benchmarks/amqp-compare-report.txt`).

```bash
bash benchmarks/compare_amqp.sh
```

4. When the script completes, the human-readable report lives in
   `benchmarks/amqp-compare-report.txt` and the raw server log is in
   `benchmarks/cosmobroker-amqp-compare.log`. Review both if your PR touches RabbitMQ or streaming code.

5. Stop the RabbitMQ container when you are finished:

```bash
docker stop cosmobroker-bench-rabbitmq
```

If you would rather capture an explicit environment file, set the following variables before running the
script:

```bash
COUNT=1000 PAYLOAD=128 LATENCY=100 TIMEOUT_MS=3000 REPORT_PATH=benchmarks/amqp-compare-report.txt bash benchmarks/compare_amqp.sh
```

The workflow (`.github/workflows/ci.yml`) now runs this script, so each pull request can report regressions
before any tag is promoted.
