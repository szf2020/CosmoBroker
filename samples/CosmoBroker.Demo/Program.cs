using System;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using CosmoBroker;

Console.WriteLine("=== CosmoBroker High-Performance Demo ===");

// 1. Start the Broker Server on port 4222 with SQLite persistence
var repo = new CosmoBroker.Persistence.MessageRepository("Data Source=broker.db;");
await repo.InitializeAsync();

var broker = new BrokerServer(4222, repo);
broker.Start();

// Give the server a second to initialize
await Task.Delay(500);

// 2. Start a subscriber task
var subTask = Task.Run(async () => {
    using var client = new TcpClient("localhost", 4222);
    using var stream = client.GetStream();
    var utf8NoBom = new UTF8Encoding(false);
    var reader = new StreamReader(stream, utf8NoBom);
    var writer = new StreamWriter(stream, utf8NoBom) { AutoFlush = true };

    // Read initial INFO block from broker
    string info = await reader.ReadLineAsync();
    Console.WriteLine($"[Subscriber] Handshake: {info}");

    // Send CONNECT and SUB commands
    await writer.WriteLineAsync("CONNECT {}");
    await writer.WriteLineAsync("SUB news.tech 1");
    Console.WriteLine("[Subscriber] Subscribed to 'news.tech' with SID 1");

    // Listen for messages
    while (true)
    {
        string line = await reader.ReadLineAsync();
        if (line == null) break;
        
        if (line.StartsWith("MSG"))
        {
            Console.WriteLine($"[Subscriber] Received Command: {line}");
            // NATS protocol: MSG <subject> <sid> <bytes>\r\n<payload>\r\n
            string payload = await reader.ReadLineAsync();
            Console.WriteLine($"[Subscriber] Received Payload: {payload}");
        }
    }
});

// 3. Start a publisher task
var pubTask = Task.Run(async () => {
    await Task.Delay(1000); // Wait for sub to be ready

    using var client = new TcpClient("localhost", 4222);
    using var stream = client.GetStream();
    var utf8NoBom = new UTF8Encoding(false);
    var writer = new StreamWriter(stream, utf8NoBom) { AutoFlush = true };

    Console.WriteLine("[Publisher] Sending messages to 'news.tech' (Non-persistent)...");
    
    string[] messages = {
        "CosmoBroker is lightning fast!",
        "This message is ephemeral.",
    };

    foreach (var msg in messages)
    {
        byte[] payloadBytes = Encoding.UTF8.GetBytes(msg);
        await writer.WriteAsync($"PUB news.tech {payloadBytes.Length}\r\n");
        await writer.WriteAsync(msg + "\r\n");
        await writer.FlushAsync();
        Console.WriteLine($"[Publisher] Sent ephemeral: {msg}");
        await Task.Delay(100);
    }

    Console.WriteLine("[Publisher] Sending messages to 'persist.logs' (Persistent)...");
    string[] pMessages = {
        "Persistent message 1",
        "Persistent message 2"
    };

    foreach (var msg in pMessages)
    {
        byte[] payloadBytes = Encoding.UTF8.GetBytes(msg);
        await writer.WriteAsync($"PUB persist.logs {payloadBytes.Length}\r\n");
        await writer.WriteAsync(msg + "\r\n");
        await writer.FlushAsync();
        Console.WriteLine($"[Publisher] Sent persistent: {msg}");
        await Task.Delay(100);
    }
});

// 4. Start a durable subscriber task LATER to test replay
var durableTask = Task.Run(async () => {
    await Task.Delay(3000); // Wait until publisher is done
    
    Console.WriteLine("[DurableSub] Connecting to replay 'persist.logs'...");
    using var client = new TcpClient("localhost", 4222);
    using var stream = client.GetStream();
    var utf8NoBom = new UTF8Encoding(false);
    var reader = new StreamReader(stream, utf8NoBom);
    var writer = new StreamWriter(stream, utf8NoBom) { AutoFlush = true };

    await reader.ReadLineAsync(); // Handshake
    await writer.WriteLineAsync("SUB persist.logs 1 my-durable-worker");
    Console.WriteLine("[DurableSub] Subscribed with durability 'my-durable-worker'");

    while (true)
    {
        string line = await reader.ReadLineAsync();
        if (line == null) break;
        if (line.StartsWith("MSG"))
        {
            string payload = await reader.ReadLineAsync();
            Console.WriteLine($"[DurableSub] Replayed/Received: {payload}");
        }
    }
});

// Run for a bit longer then exit
Console.WriteLine("Waiting for tasks to complete...");
await pubTask;
await Task.Delay(5000); // Give durable sub time to replay

Console.WriteLine("Demo complete. Disposing broker...");
await broker.DisposeAsync();
