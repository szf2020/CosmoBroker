# CosmoBroker

**CosmoBroker** is a high-performance, NATS-compatible messaging engine built for .NET 10. It leverages `System.IO.Pipelines` and `Cosmo.Transport` to provide a zero-copy, low-latency messaging backbone that can be used standalone or embedded directly into `CosmoApiServer` applications.

---

## Performance

CosmoBroker is designed for extreme throughput. In local benchmarks, it consistently outperforms standard messaging servers by leveraging the "zero-context-switch" architecture of modern .NET.

### Benchmark: CosmoBroker vs. Official NATS
*Test Environment: Local TCP, 100,000 messages, 128-byte payloads.*

| Server | Throughput (msg/s) | Bandwidth (MiB/s) | Relative Speed |
| :--- | :---: | :---: | :---: |
| **CosmoBroker** | **110,839** | **13.53** | **1.5x Faster** |
| NATS (Official) | 73,126 | 8.93 | 1.0x |

---

## Key Features

- **NATS Protocol Compatible**: Speak standard NATS text protocol. Use any official NATS client (Go, Python, Node.js, .NET).
- **Zero-Allocation I/O**: Built on `System.IO.Pipelines` for minimal GC pressure and maximum throughput.
- **Advanced Routing**: Full support for NATS subject wildcards (`*` and `>`) via a thread-safe Topic Trie.
- **Queue Groups**: Load-balance messages across multiple subscribers in a named group.
- **Optional Persistence**: Database-backed storage (SQLite/Postgres/SQL Server) for subjects starting with `persist.`.
- **Message Replay**: "Durable" subscriptions that automatically replay missing messages from the database.
- **Extensible Auth**: SQL-backed or simple token/password authentication providers.

---

## Getting Started

### 1. Basic Setup (Embedded)

```csharp
using CosmoBroker;

// Start the broker on the standard NATS port
var broker = new BrokerServer(port: 4222);
broker.Start();

Console.WriteLine("Broker is running. Connect with any NATS client!");
```

### 2. Enabling SQL Persistence & Auth

```csharp
using CosmoBroker;
using CosmoBroker.Persistence;
using CosmoBroker.Auth;

// 1. Initialize the SQL Repository (SQLite in this example)
var repo = new MessageRepository("Data Source=broker.db;");
await repo.InitializeAsync();

// 2. Setup SQL-backed Authenticator
await repo.AddUserAsync("admin", "secret_pass");
var auth = new SqlAuthenticator(repo);

// 3. Start the server with Persistence and Auth
var broker = new BrokerServer(4222, repo: repo, authenticator: auth);
broker.Start();
```

---

## Usage Examples (NATS Protocol)

### Basic Pub/Sub
```bash
# Subscribe
SUB my.subject 1

# Publish
PUB my.subject 5
Hello
```

### Queue Groups (Load Balancing)
Multiple workers can join the same group to share tasks:
```bash
# Worker 1
SUB tasks groupA 1

# Worker 2
SUB tasks groupA 2

# Publishing to 'tasks' will now deliver to EITHER Worker 1 or Worker 2 (randomly)
PUB tasks 4
work
```

### Durable Subscriptions (Replay)
Subjects starting with `persist.` are stored in the database.
```bash
# Connect and subscribe with a durable name
SUB persist.logs 1 my-durable-worker

# The broker will automatically replay all messages for 'persist.logs' 
# that occurred while 'my-durable-worker' was offline.
```

---

## Architecture

| Component | Responsibility |
| :--- | :--- |
| `BrokerServer` | Manages the TCP listener and connection lifecycle. |
| `BrokerConnection` | Handles the `System.IO.Pipelines` read/write loop for a single client. |
| `TopicTree` | A thread-safe Trie that routes messages based on subjects and wildcards. |
| `NatsParser` | A zero-copy parser for the NATS text protocol. |
| `MessageRepository` | Multi-database persistence layer for messages and users. |

---

## Client Compatibility

Because CosmoBroker implements the standard NATS protocol, you can use any existing client:

- **.NET**: `NATS.Client.Core`
- **Go**: `nats.go`
- **JS**: `nats.js`
- **CLI**: `telnet localhost 4222` (for manual testing)
