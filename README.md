# CosmoBroker

**CosmoBroker** is a high-performance, NATS-compatible distributed messaging engine built for .NET 10. It leverages `System.IO.Pipelines` and `Span<T>` to provide a zero-copy, ultra-low-latency messaging backbone that matches the official NATS feature set while adding native SQL-backed persistence and deep .NET ecosystem integration.

---

## 🏆 Performance: CosmoBroker vs. Official NATS

CosmoBroker is highly optimized for throughput and latency. In head-to-head benchmarks against the official `nats-server` (using the same client SDK on both sides), CosmoBroker delivers higher throughput and consistently **3× lower latency** at every percentile.

### Benchmark Results (March 26, 2026)
*Test environment: Apple M-series, 1 publisher, 100,000 messages, 128B payload, nats-server running in Docker.*

Both clients were run against both servers to give a complete and fair picture.

#### CosmoBroker.Client (native SDK) vs both servers

| Server | Throughput | P50 RTT | P95 RTT | P99 RTT |
| :--- | ---: | ---: | ---: | ---: |
| **CosmoBroker** | **1,150,483 msg/sec** | **0.059 ms** | **0.103 ms** | **0.138 ms** |
| nats-server | 853,585 msg/sec | 0.172 ms | 0.267 ms | 0.436 ms |
| **Advantage** | **+35%** | **2.9×** | **2.6×** | **3.2×** |

#### NATS.Client.Core (official SDK) vs both servers — apples-to-apples

| Server | Throughput | P50 RTT | P95 RTT | P99 RTT |
| :--- | ---: | ---: | ---: | ---: |
| **CosmoBroker** | **646,579 msg/sec** | **0.053 ms** | **0.106 ms** | **0.132 ms** |
| nats-server | 579,567 msg/sec | 0.174 ms | 0.272 ms | 0.405 ms |
| **Advantage** | **+11.6%** | **3.3×** | **2.6×** | **3.1×** |

**Key takeaways:**
- **Latency is a server characteristic.** CosmoBroker delivers ~3× lower RTT regardless of which client is used. This is due to inline socket completions, batch-flush coalescing, and the zero-copy dispatch pipeline.
- **Throughput gap reflects client overhead.** With the same `NATS.Client.Core` on both sides, CosmoBroker is +11.6% faster in raw message rate. The larger gap seen with `CosmoBroker.Client` (+35%) reflects the native client's own optimizations (direct `PipeWriter` writes, allocation-free span matching).
- **`CosmoBroker.Client` is ~1.8× faster** than `NATS.Client.Core` against the same CosmoBroker server (1,150k vs 647k msg/sec), making it the best choice for .NET applications.

---

## ⚖️ Feature Parity: CosmoBroker vs. NATS Server

| Feature Area | CosmoBroker | NATS Server | Notes |
| :--- | :--- | :--- | :--- |
| **Core Messaging** | | | |
| Pub/Sub & Request/Reply | ✅ Supported | ✅ Supported | |
| Queue Groups | ✅ Supported | ✅ Supported | |
| **JetStream (Persistence)** | | | |
| Streams & Consumers | ✅ Supported | ✅ Supported | CosmoBroker uses SQLite/SQL for persistence. |
| Mirroring / Sourcing | ✅ Supported | ✅ Supported | Data replication for HA and aggregation. |
| **Authentication** | | | |
| Token / Simple / TLS | ✅ Supported | ✅ Supported | |
| JWT / Accounts | ✅ Supported | ✅ Supported | |
| SQL-backed Auth | ✅ Supported | ❌ (Indirect) | CosmoBroker has a native `SqlAuthenticator`. |
| **Topologies** | | | |
| Clustering (Mesh) | ✅ Supported | ✅ Supported | |
| Leafnodes | ✅ Supported | ✅ Supported | Edge-to-cloud topology. |
| Gateways (Supercluster)| ✅ Supported | ✅ Supported | |
| **Protocols** | | | |
| NATS Protocol | ✅ Supported | ✅ Supported | |
| MQTT 3.1.1 | ✅ Supported | ✅ Supported | CosmoBroker acts as an MQTT bridge parser. |
| WebSockets | ✅ Supported | ✅ Supported | |

---

## 🚀 Features & Code Samples

CosmoBroker is fully compatible with standard NATS clients. The examples below use the official `NATS.Client.Core` package for C#.

### 1. Core Messaging (Pub/Sub, Request/Reply, Queue Groups)

**Publish / Subscribe:**
```csharp
await using var nats = new NatsConnection();
await nats.ConnectAsync();

// Subscriber
var sub = Task.Run(async () => {
    await foreach (var msg in nats.SubscribeAsync<string>("events.orders.*")) {
        Console.WriteLine($"Received: {msg.Data} on {msg.Subject}");
    }
});

// Publisher
await nats.PublishAsync("events.orders.created", "Order #1234");
```

**Request / Reply:**
```csharp
// Responder
var responder = Task.Run(async () => {
    await foreach (var msg in nats.SubscribeAsync<string>("services.time")) {
        await msg.ReplyAsync(DateTime.UtcNow.ToString());
    }
});

// Requester
var reply = await nats.RequestAsync<string, string>("services.time", "get");
Console.WriteLine($"Server time is: {reply.Data}");
```

**Queue Groups (Load Balancing):**
Messages sent to `jobs.process` are distributed evenly among members of the `worker-group`.
```csharp
await foreach (var msg in nats.SubscribeAsync<string>("jobs.process", queueGroup: "worker-group")) {
    Console.WriteLine($"Worker A processing: {msg.Data}");
}
```

### 2. JetStream (Persistence, Streams, Mirrors, Sourcing)

**Creating a Stream & Publishing:**
```csharp
var js = new JetStreamService(topicTree, repo);
js.CreateStream(new StreamConfig {
    Name = "ORDERS",
    Subjects = new List<string> { "orders.*" }
});

// Publish a durable message
await nats.PublishAsync("orders.new", "Order Data");
```

**Stream Mirroring (1:1 Copy):**
Mirroring creates an exact, read-only replica of another stream. Perfect for disaster recovery or geographic locality.
```csharp
js.CreateStream(new StreamConfig {
    Name = "ORDERS_MIRROR",
    Mirror = new StreamSource { Name = "ORDERS" }
});
// ORDERS_MIRROR automatically receives all data published to ORDERS.
```

**Stream Sourcing (Many-to-One Aggregation):**
Sourcing pulls data from multiple streams into one.
```csharp
js.CreateStream(new StreamConfig {
    Name = "ALL_METRICS",
    Sources = new List<StreamSource> {
        new StreamSource { Name = "US_METRICS" },
        new StreamSource { Name = "EU_METRICS" }
    }
});
```

### 3. Topologies (Clustering & Leafnodes)

CosmoBroker supports linking servers together to form resilient meshes or edge networks.

**Clustering (Full Mesh):**
Connect equal servers to share the load.
```csharp
var cluster = new ClusterManager(server, topicTree);
cluster.AddPeer(new IPEndPoint(IPAddress.Parse("10.0.0.2"), 4222));
await cluster.StartAsync(cts.Token);
```

**Leafnodes (Hub and Spoke):**
Extend a central cluster to edge locations securely.
```csharp
var leafnodes = new LeafnodeManager(server, topicTree);
// Connect this local broker to a remote cloud NATS hub
leafnodes.AddRemote("nats://cloud-hub.example.com:7422");
```

### 4. Multi-Protocol Sniffing (MQTT & WebSockets)

CosmoBroker detects the incoming protocol on the *same port*. You can connect a standard MQTT client directly to CosmoBroker.

```bash
# Using standard mosquitto_pub to publish to CosmoBroker via MQTT
mosquitto_pub -h localhost -p 4222 -t "sensors/temp" -m "22.5"

# A NATS client can receive that same message
nats sub "sensors.temp"
```

### 5. Authentication & Security

CosmoBroker supports Simple Auth, JWT/NKEYs, X.509 TLS Certificates, and native SQL Auth.

**Native SQL Authentication:**
Manage users directly in your database.
```csharp
var auth = new SqlAuthenticator("Data Source=broker.db;");
var broker = new BrokerServer(port: 4222, authenticator: auth);
```

**Traffic Shaping (Subject Mapping):**
```csharp
// Securely sandbox a tenant by forcing their traffic into a prefix
var mapping = new SubjectMapping { SourcePattern = "api.v1" };
mapping.Destinations.Add(new MapDestination { Subject = "tenantA.api.v1", Weight = 1.0 });
account.Mappings.AddMapping(mapping);
```

---

## 🛠 Getting Started

### Basic Setup (Standalone)

```csharp
using CosmoBroker;

// Start the broker with default settings (port 4222, monitor 8222)
var broker = new BrokerServer(port: 4222);
await broker.StartAsync();

Console.WriteLine("CosmoBroker is running. Connect with any NATS client!");
```

### Config File + SQLite JetStream
Set `COSMOBROKER_CONFIG` to point at a config file and `COSMOBROKER_REPO` to enable SQLite persistence.

Example `broker.conf`:
```
port: 4222
jetstream {
  batch_size: 256
  batch_delay_ms: 1
}
tls {
  cert: "server.pfx"
  password: "password"
}
auth {
  type: "sql"
}
```

Run from CLI:
```bash
COSMOBROKER_CONFIG=broker.conf COSMOBROKER_REPO="Data Source=broker.db;" dotnet run --project CosmoBroker.Server -c Release
```

---

## 🏗 Architecture & Tuning

| Component | Responsibility |
| :--- | :--- |
| `BrokerServer` | Orchestrates listeners, clustering, and monitoring. |
| `BrokerConnection` | High-performance `System.IO.Pipelines` handler with zero-copy protocol sniffing. |
| `TopicTree` | Lock-free Trie structure for fast, zero-allocation subject matching. |
| `JetStreamService` | Manages durable streams, mirrors, and consumer state. |
| `MessageRepository`| Native SQLite/SQL engine for persisting streams. |

### v1.1.2+ Ultra-Performance Architecture

**Server hot-path optimizations:**
- **Zero-allocation message dispatch**: Subjects and subscription metadata flow through `ReadOnlySpan<T>` end-to-end. No heap allocations on the publish/subscribe critical path.
- **Batch `FlushAsync` coalescing**: A `[ThreadStatic]` batch context groups all subscriber pipe flushes triggered within a single `ProcessPipeAsync` drain cycle into one flush per subscriber, reducing pipe lock acquisitions from O(messages) to O(1) per drain.
- **SubEntry state carrier**: The `Sublist` stores the `Subscription` object directly in `SubEntry.State`, eliminating a `ConcurrentDictionary` lookup per message delivery on the fast path.
- **Scatter-gather socket sends**: Multi-segment pipe buffers are sent with a single `socket.Send(IList<ArraySegment<byte>>)` syscall instead of N sequential `await socket.SendAsync` calls.
- **Inline socket completions**: `DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS=1` is set in the Docker image, running epoll callbacks directly on the I/O thread and eliminating one thread-pool dispatch per received message (~15–30% throughput gain in containers).
- **Thread pool pre-warming**: `ThreadPool.SetMinThreads(cpus × 8, cpus × 4)` at startup prevents the 500 ms/thread ramp-up latency under connection bursts.
- **Tiered PGO**: `<TieredPGO>true</TieredPGO>` recompiles hot loops using runtime call-count profiles after ~30 s of operation, yielding 10–20% additional throughput on steady-state workloads.
- **`SustainedLowLatency` GC mode**: Minimises Gen2 collection frequency, reducing tail latency on hot connections.

**Correctness fixes (this release):**
- Sublist entries are now cleaned up on connection close, preventing stale delivery to dead connections.
- `MaxMsgs` auto-unsubscribe is guarded by `Interlocked.CompareExchange`, preventing double-unsubscription under concurrent delivery.
- `ReceivedMsgs` incremented atomically via `Interlocked.Increment`.
- `TryMatchLiteral` returns snapshot copies of internal lists so callers iterate safely outside the read lock.
- WebSocket and MQTT connections now respect the configured `IAuthenticator` instead of auto-authenticating unconditionally.
- `EnterLameDuckMode` is idempotent under concurrent callers via `Interlocked.CompareExchange`.
- JetStream API responses use the scoped reply-to address so account-prefixed subscribers receive replies correctly.

**Client SDK (`CosmoBroker.Client`) optimizations:**
- Direct `PipeWriter` frame construction — no intermediate string or byte-array allocations per publish.
- Allocation-free wildcard subject matching via span-based token iteration (no `string.Split`).
- Semaphore fast-path: lock acquired synchronously in the uncontested case, avoiding async state machine allocation.
- `UNSUB` on unsubscribe uses a bounded 5-second timeout token, preventing indefinite blocking during connection teardown.
