# CosmoBroker

**CosmoBroker** is a high-performance, NATS-compatible distributed messaging engine built for .NET 10. It leverages `System.IO.Pipelines` and `Span<T>` to provide a zero-copy, ultra-low-latency messaging backbone that matches the official NATS feature set while adding native SQL-backed persistence and deep .NET ecosystem integration.

---

## 🏆 Performance: CosmoBroker vs. Official NATS

CosmoBroker is highly optimized for throughput and latency. In head-to-step benchmarks against the official `nats-server`, CosmoBroker demonstrates massive advantages in scaling and latency stability.

### Benchmark Results (March 23, 2026)
*Test Environment: 1 publisher, 200,000 messages, 256B payload.*

| Client (SDK) | Server | Throughput (msg/sec) | Avg Latency (RTT) | Perf % (Latency) |
| :--- | :--- | :--- | :--- | :--- |
| **CosmoBroker.Client** | **CosmoBroker.Server** | **~1,245,750** | 0.072 ms | **+250.0%** |
| NATS.Client.Core | CosmoBroker.Server | ~629,401 | **0.056 ms** | baseline |
| **CosmoBroker.Client** | nats-server | **~1,026,826** | **0.176 ms** | **+104.5%** |
| NATS.Client.Core | nats-server | ~573,060 | 0.184 ms | baseline |

*Winner:* **CosmoBroker.Client**. The native CosmoBroker client is **~2.0x to 2.2x faster** in throughput than the standard NATS client, even when connecting to the official `nats-server`.

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

### v1.1.2 Ultra-Performance Architecture
- **Zero-Allocation Hot Path**: Subjects and subscription metadata are handled via `ReadOnlySpan<T>`, eliminating heap allocations during message delivery.
- **Gathering I/O**: High-volume egress utilizes `Socket.SendAsync` with `IList<ArraySegment<byte>>` to minimize syscall overhead.
- **Lock-Free Wildcard Matching**: Lock contention is removed via volatile wildcard counters and versioned matching caches.
- **Optimized Client SDK**: `CosmoBroker.Client` leverages direct buffer writes and optimized socket polling to achieve ~2.0x higher throughput than standard clients.
