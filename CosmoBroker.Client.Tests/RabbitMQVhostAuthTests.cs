using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker.Auth;
using CosmoBroker.Client;
using CosmoBroker.Persistence;
using Xunit;

namespace CosmoBroker.Client.Tests;

public class RabbitMQVhostAuthTests
{
    [Fact]
    public async Task Vhosts_ShouldIsolateSameNamedQueues()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"cosmobroker-rmq-auth-{Guid.NewGuid():N}.db");
        string connectionString = $"Data Source={dbPath};";
        int port = 4273;
        int monitorPort = 8273;

        var repo = new MessageRepository(connectionString);
        await repo.InitializeAsync();
        await repo.AddUserAsync("alice", "secret");
        await repo.AddUserAsync("bob", "secret");
        await repo.SaveRabbitPermissionsAsync("alice", "tenant-a", new[] { "tenant-a" }, new[] { "exchange:*", "queue:*" }, new[] { "exchange:*" }, new[] { "queue:*" });
        await repo.SaveRabbitPermissionsAsync("bob", "tenant-b", new[] { "tenant-b" }, new[] { "exchange:*", "queue:*" }, new[] { "exchange:*" }, new[] { "queue:*" });

        await using var server = new BrokerServer(port: port, repo: repo, authenticator: new SqlAuthenticator(repo), monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(150);

        await using var alice = await ConnectAsync(port, "alice");
        await using var aliceConsumer = await ConnectAsync(port, "alice");
        await using var bob = await ConnectAsync(port, "bob");
        await using var bobConsumer = await ConnectAsync(port, "bob");

        await DeclareRouteAsync(alice, "tenant-a", "shared.x", "shared.q", "shared");
        await DeclareRouteAsync(bob, "tenant-b", "shared.x", "shared.q", "shared");

        string aliceReply = $"_rmq.vhost.alice.{Guid.NewGuid():N}";
        string bobReply = $"_rmq.vhost.bob.{Guid.NewGuid():N}";
        var aliceDelivered = SubscribeOneAsync(aliceConsumer, aliceReply);
        var bobDelivered = SubscribeOneAsync(bobConsumer, bobReply);

        await Task.Delay(150);
        await RmqRequestAsync(alice, "$RMQ.CONSUME.shared.q", new { vhost = "tenant-a", replySubject = aliceReply, autoAck = true });
        await RmqRequestAsync(bob, "$RMQ.CONSUME.shared.q", new { vhost = "tenant-b", replySubject = bobReply, autoAck = true });

        await RmqRequestAsync(alice, "$RMQ.PUBLISH.shared.x.shared", new { vhost = "tenant-a", body = Convert.ToBase64String(Encoding.UTF8.GetBytes("from-a")) });
        await RmqRequestAsync(bob, "$RMQ.PUBLISH.shared.x.shared", new { vhost = "tenant-b", body = Convert.ToBase64String(Encoding.UTF8.GetBytes("from-b")) });

        string aliceRaw = await aliceDelivered.WaitAsync(TimeSpan.FromSeconds(5));
        string bobRaw = await bobDelivered.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal("from-a", ReadBody(aliceRaw));
        Assert.Equal("from-b", ReadBody(bobRaw));
    }

    [Fact]
    public async Task RabbitPermissions_ShouldEnforceVhostAndResourceAccess()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"cosmobroker-rmq-perms-{Guid.NewGuid():N}.db");
        string connectionString = $"Data Source={dbPath};";
        int port = 4274;
        int monitorPort = 8274;

        var repo = new MessageRepository(connectionString);
        await repo.InitializeAsync();
        await repo.AddUserAsync("alice", "secret");
        await repo.SaveRabbitPermissionsAsync(
            "alice",
            "tenant-a",
            new[] { "tenant-a" },
            new[] { "exchange:*", "queue:*" },
            new[] { "exchange:allowed.*" },
            new[] { "queue:allowed.*" });

        await using var server = new BrokerServer(port: port, repo: repo, authenticator: new SqlAuthenticator(repo), monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(150);

        await using var alice = await ConnectAsync(port, "alice");

        var deniedVhost = await RmqRequestAsyncRaw(alice, "$RMQ.EXCHANGE.DECLARE", new { vhost = "tenant-b", name = "allowed.x", type = "Direct", durable = true });
        Assert.False(deniedVhost.RootElement.GetProperty("ok").GetBoolean());
        Assert.Contains("tenant-b", deniedVhost.RootElement.GetProperty("error").GetString(), StringComparison.OrdinalIgnoreCase);

        await RmqRequestAsync(alice, "$RMQ.EXCHANGE.DECLARE", new { vhost = "tenant-a", name = "blocked.x", type = "Direct", durable = true });
        await RmqRequestAsync(alice, "$RMQ.QUEUE.DECLARE", new { vhost = "tenant-a", name = "blocked.q", durable = true });
        await RmqRequestAsync(alice, "$RMQ.QUEUE.BIND", new { vhost = "tenant-a", exchange = "blocked.x", queue = "blocked.q", routingKey = "blocked" });

        var deniedPublish = await RmqRequestAsyncRaw(alice, "$RMQ.PUBLISH.blocked.x.blocked", new { vhost = "tenant-a", body = Convert.ToBase64String("nope"u8.ToArray()) });
        Assert.False(deniedPublish.RootElement.GetProperty("ok").GetBoolean());
        Assert.Contains("publish", deniedPublish.RootElement.GetProperty("error").GetString(), StringComparison.OrdinalIgnoreCase);

        var deniedConsume = await RmqRequestAsyncRaw(alice, "$RMQ.CONSUME.blocked.q", new { vhost = "tenant-a", replySubject = "_rmq.denied", autoAck = true });
        Assert.False(deniedConsume.RootElement.GetProperty("ok").GetBoolean());
        Assert.Contains("consume", deniedConsume.RootElement.GetProperty("error").GetString(), StringComparison.OrdinalIgnoreCase);
    }

    private static async Task DeclareRouteAsync(CosmoClient client, string vhost, string exchange, string queue, string routingKey)
    {
        await RmqRequestAsync(client, "$RMQ.EXCHANGE.DECLARE", new { vhost, name = exchange, type = "Direct", durable = true });
        await RmqRequestAsync(client, "$RMQ.QUEUE.DECLARE", new { vhost, name = queue, durable = true });
        await RmqRequestAsync(client, "$RMQ.QUEUE.BIND", new { vhost, exchange, queue, routingKey });
    }

    private static async Task<CosmoClient> ConnectAsync(int port, string username)
    {
        var client = new CosmoClient(new CosmoClientOptions
        {
            Url = $"nats://127.0.0.1:{port}",
            Username = username,
            Password = "secret"
        });
        await client.ConnectAsync();
        return client;
    }

    private static async Task<string> SubscribeOneAsync(CosmoClient client, string subject)
    {
        var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        _ = Task.Run(async () =>
        {
            await foreach (var msg in client.SubscribeAsync(subject, ct: subCts.Token))
            {
                tcs.TrySetResult(msg.GetStringData());
                break;
            }
        });

        return await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static async Task<JsonDocument> RmqRequestAsync(CosmoClient client, string subject, object requestBody)
    {
        var doc = await RmqRequestAsyncRaw(client, subject, requestBody);
        if (!doc.RootElement.TryGetProperty("ok", out var ok))
            throw new Xunit.Sdk.XunitException($"Unexpected RMQ response: {doc.RootElement}");
        Assert.True(ok.GetBoolean(), doc.RootElement.TryGetProperty("error", out var error) ? error.GetString() : doc.RootElement.ToString());
        return doc;
    }

    private static async Task<JsonDocument> RmqRequestAsyncRaw(CosmoClient client, string subject, object requestBody)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(requestBody);
        var reply = await client.RequestAsync(subject, payload, TimeSpan.FromSeconds(5));
        return JsonDocument.Parse(reply.GetStringData());
    }

    private static string ReadBody(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return Encoding.UTF8.GetString(Convert.FromBase64String(doc.RootElement.GetProperty("body").GetString()!));
    }
}
