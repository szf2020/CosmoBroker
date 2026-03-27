using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker.Client;
using CosmoBroker.Persistence;
using Xunit;

namespace CosmoBroker.Client.Tests;

public class RabbitMQPersistenceTests
{
    [Fact]
    public async Task DurableQueueAndMessage_ShouldSurviveRestart()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"cosmobroker-rmq-{Guid.NewGuid():N}.db");
        string connectionString = $"Data Source={dbPath};";
        int port1 = 4271;
        int mon1 = 8271;

        var repo1 = new MessageRepository(connectionString);
        await using (var server1 = new BrokerServer(port: port1, repo: repo1, monitorPort: mon1))
        {
            using var cts = new CancellationTokenSource();
            await server1.StartAsync(cts.Token);
            await Task.Delay(150);

            await using var control = new CosmoClient(new CosmoClientOptions { Url = $"nats://127.0.0.1:{port1}" });
            await control.ConnectAsync();

            await RmqRequestAsync(control, "$RMQ.EXCHANGE.DECLARE",
                new { name = "persist.x", type = "Direct", durable = true });
            await RmqRequestAsync(control, "$RMQ.QUEUE.DECLARE",
                new { name = "persist.q", durable = true });
            await RmqRequestAsync(control, "$RMQ.QUEUE.BIND",
                new { exchange = "persist.x", queue = "persist.q", routingKey = "persist" });

            await control.PublishAsync("$RMQ.PUBLISH.persist.x.persist", Encoding.UTF8.GetBytes("before-restart"));
            await control.PingAsync();
            await Task.Delay(100);
        }

        int port2 = 4272;
        int mon2 = 8272;

        var repo2 = new MessageRepository(connectionString);
        await using (var server2 = new BrokerServer(port: port2, repo: repo2, monitorPort: mon2))
        {
            using var cts = new CancellationTokenSource();
            await server2.StartAsync(cts.Token);
            await Task.Delay(150);

            await using var control = new CosmoClient(new CosmoClientOptions { Url = $"nats://127.0.0.1:{port2}" });
            await using var consumer = new CosmoClient(new CosmoClientOptions { Url = $"nats://127.0.0.1:{port2}" });
            await control.ConnectAsync();
            await consumer.ConnectAsync();

            string replySubject = $"_rmq.persist.{Guid.NewGuid():N}";
            var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var subCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            _ = Task.Run(async () =>
            {
                await foreach (var msg in consumer.SubscribeAsync(replySubject, ct: subCts.Token))
                {
                    delivered.TrySetResult(msg.GetStringData());
                    break;
                }
            });

            await Task.Delay(150);
            await RmqRequestAsync(control, "$RMQ.CONSUME.persist.q",
                new { replySubject, autoAck = true });

            var raw = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var doc = JsonDocument.Parse(raw);
            var body = Encoding.UTF8.GetString(Convert.FromBase64String(
                doc.RootElement.GetProperty("body").GetString()!));

            Assert.Equal("before-restart", body);
        }

        try { File.Delete(dbPath); } catch { }
    }

    private static async Task<JsonDocument> RmqRequestAsync(CosmoClient client, string subject, object requestBody)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(requestBody);
        var reply = await client.RequestAsync(subject, payload, TimeSpan.FromSeconds(5));
        var doc = JsonDocument.Parse(reply.GetStringData());
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        return doc;
    }
}
