using System;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using CosmoBroker;

Console.WriteLine("=== CosmoBroker Load Balancer (Queue Groups) Demo ===");

// 1. Start the Broker
var broker = new BrokerServer(4222);
broker.Start();
await Task.Delay(500);

// 2. Start 3 subscribers in the SAME queue group: "workers"
var subTasks = new List<Task>();
for (int i = 1; i <= 3; i++)
{
    int id = i;
    subTasks.Add(Task.Run(async () => {
        using var client = new TcpClient("localhost", 4222);
        using var stream = client.GetStream();
        var utf8NoBom = new UTF8Encoding(false);
        var reader = new StreamReader(stream, utf8NoBom);
        var writer = new StreamWriter(stream, utf8NoBom) { AutoFlush = true };

        await reader.ReadLineAsync(); // Handshake
        await writer.WriteLineAsync($"SUB tasks workers {id}");
        Console.WriteLine($"[Worker {id}] Joined group 'workers' on subject 'tasks'");

        while (true)
        {
            string line = await reader.ReadLineAsync();
            if (line == null) break;
            if (line.StartsWith("MSG"))
            {
                string payload = await reader.ReadLineAsync();
                Console.WriteLine($"[Worker {id}] Received: {payload}");
            }
        }
    }));
}

// 3. Start 1 individual subscriber (should receive ALL messages)
var individualTask = Task.Run(async () => {
    using var client = new TcpClient("localhost", 4222);
    using var stream = client.GetStream();
    var utf8NoBom = new UTF8Encoding(false);
    var reader = new StreamReader(stream, utf8NoBom);
    var writer = new StreamWriter(stream, utf8NoBom) { AutoFlush = true };

    await reader.ReadLineAsync();
    await writer.WriteLineAsync("SUB tasks 99");
    Console.WriteLine("[Monitor] Watching all 'tasks'");

    while (true)
    {
        string line = await reader.ReadLineAsync();
        if (line == null) break;
        if (line.StartsWith("MSG"))
        {
            string payload = await reader.ReadLineAsync();
            Console.WriteLine($"[Monitor] Received: {payload}");
        }
    }
});

// 4. Publisher sends 6 tasks
var pubTask = Task.Run(async () => {
    await Task.Delay(1000); // Wait for subs
    using var client = new TcpClient("localhost", 4222);
    using var stream = client.GetStream();
    var utf8NoBom = new UTF8Encoding(false);
    var writer = new StreamWriter(stream, utf8NoBom) { AutoFlush = true };

    for (int i = 1; i <= 6; i++)
    {
        string msg = $"Job-{i}";
        byte[] bytes = Encoding.UTF8.GetBytes(msg);
        await writer.WriteAsync($"PUB tasks {bytes.Length}\r\n{msg}\r\n");
        await writer.FlushAsync();
        Console.WriteLine($"[Publisher] Dispatched: {msg}");
        await Task.Delay(200);
    }
});

await Task.WhenAny(pubTask, Task.Delay(5000));
Console.WriteLine("Demo complete. Disposing broker...");
await broker.DisposeAsync();
