using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker;
using CosmoBroker.Client;
using CosmoBroker.RabbitMQ;
using Xunit;

namespace CosmoBroker.Client.Tests;

/// <summary>
/// Integration tests for RabbitMQ-style features in CosmoBroker.
/// All tests communicate via the $RMQ.* NATS subject namespace.
/// </summary>
public class RabbitMQTests : IAsyncDisposable
{
    private readonly BrokerServer _server;
    private readonly CancellationTokenSource _cts = new();
    private const int Port = 4260;
    private readonly List<CosmoClient> _ownedClients = new();

    public RabbitMQTests()
    {
        _server = new BrokerServer(port: Port, monitorPort: 8260);
        _ = _server.StartAsync(_cts.Token);
    }

    private async Task<CosmoClient> ConnectedClientAsync()
    {
        await Task.Delay(120); // let server start
        var client = new CosmoClient(new CosmoClientOptions { Url = $"nats://localhost:{Port}" });
        await client.ConnectAsync();
        _ownedClients.Add(client);
        return client;
    }

    // Helper: send a $RMQ.* request and get the JSON response
    private async Task<JsonDocument> RmqRequestAsync(
        CosmoClient client, string subject, object requestBody)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(requestBody);
        var reply = await client.RequestAsync(subject, payload, TimeSpan.FromSeconds(3));
        return JsonDocument.Parse(reply.GetStringData());
    }

    // ── 1. Direct exchange routes to the correct queue ───────────────────────

    [Fact]
    public async Task Exchange_Direct_RoutesToCorrectQueue()
    {
        var controlClient = await ConnectedClientAsync();
        var consumerClient = await ConnectedClientAsync();

        // Subscribe to our push-delivery subject before registering the consumer
        var deliveredMsg = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        string replySubj = $"_rmq_test.{Guid.NewGuid():N}";

        _ = Task.Run(async () =>
        {
            await foreach (var msg in consumerClient.SubscribeAsync(replySubj, ct: subCts.Token))
            {
                deliveredMsg.TrySetResult(msg.GetStringData());
                break;
            }
        });

        await Task.Delay(100); // let sub reach server

        // Declare exchange
        await RmqRequestAsync(controlClient, "$RMQ.EXCHANGE.DECLARE",
            new { name = "test.direct", type = "Direct", durable = true });

        // Declare queue
        await RmqRequestAsync(controlClient, "$RMQ.QUEUE.DECLARE",
            new { name = "q.orders", durable = true });

        // Bind
        await RmqRequestAsync(controlClient, "$RMQ.QUEUE.BIND",
            new { exchange = "test.direct", queue = "q.orders", routingKey = "orders" });

        // Register consumer with push-delivery subject
        await RmqRequestAsync(controlClient, "$RMQ.CONSUME.q.orders",
            new { replySubject = replySubj, prefetchCount = 10 });

        // Publish a message
        await RmqRequestAsync(controlClient, "$RMQ.PUBLISH.test.direct.orders",
            new { body = Convert.ToBase64String(Encoding.UTF8.GetBytes("Order#1")), priority = 0 });

        var delivered = await deliveredMsg.Task;
        var doc = JsonDocument.Parse(delivered);
        var body = Encoding.UTF8.GetString(Convert.FromBase64String(
            doc.RootElement.GetProperty("body").GetString()!));

        Assert.Equal("Order#1", body);
        Assert.Equal("orders", doc.RootElement.GetProperty("routing_key").GetString());
    }

    // ── 2. Fanout broadcasts to all bound queues ──────────────────────────────

    [Fact]
    public async Task Exchange_Fanout_BroadcastsToAllBoundQueues()
    {
        var controlClient = await ConnectedClientAsync();

        await RmqRequestAsync(controlClient, "$RMQ.EXCHANGE.DECLARE",
            new { name = "test.fanout", type = "Fanout", durable = true });

        var received = new int[3];
        var tcsList = new TaskCompletionSource<bool>[3];
        for (int i = 0; i < 3; i++)
        {
            var idx = i;
            tcsList[i] = new(TaskCreationOptions.RunContinuationsAsynchronously);
            string qName      = $"fanout.q{i}";
            string replySubj  = $"_rmqfan.{i}.{Guid.NewGuid():N}";
            var consumerClient = await ConnectedClientAsync();

            await RmqRequestAsync(controlClient, "$RMQ.QUEUE.DECLARE", new { name = qName, durable = true });
            await RmqRequestAsync(controlClient, "$RMQ.QUEUE.BIND",
                new { exchange = "test.fanout", queue = qName, routingKey = "" });

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            _ = Task.Run(async () =>
            {
                await foreach (var _ in consumerClient.SubscribeAsync(replySubj, ct: cts.Token))
                { received[idx]++; tcsList[idx].TrySetResult(true); break; }
            });
            await Task.Delay(60);
            await RmqRequestAsync(controlClient, $"$RMQ.CONSUME.{qName}",
                new { replySubject = replySubj, prefetchCount = 1 });
        }

        await Task.Delay(150);

        await RmqRequestAsync(controlClient, "$RMQ.PUBLISH.test.fanout.",
            new { body = Convert.ToBase64String("broadcast"u8.ToArray()) });

        await Task.WhenAll(
            tcsList[0].Task.WaitAsync(TimeSpan.FromSeconds(5)),
            tcsList[1].Task.WaitAsync(TimeSpan.FromSeconds(5)),
            tcsList[2].Task.WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.Equal(1, received[0]);
        Assert.Equal(1, received[1]);
        Assert.Equal(1, received[2]);
    }

    // ── 3. Topic exchange wildcard routing ────────────────────────────────────

    [Fact]
    public async Task Exchange_Topic_WildcardRouting()
    {
        var controlClient = await ConnectedClientAsync();
        var consumerClient = await ConnectedClientAsync();

        await RmqRequestAsync(controlClient, "$RMQ.EXCHANGE.DECLARE",
            new { name = "test.topic", type = "Topic", durable = true });
        await RmqRequestAsync(controlClient, "$RMQ.QUEUE.DECLARE", new { name = "logs.all", durable = true });
        await RmqRequestAsync(controlClient, "$RMQ.QUEUE.BIND",
            new { exchange = "test.topic", queue = "logs.all", routingKey = "logs.#" });

        var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        string replySubj = $"_rmqtopic.{Guid.NewGuid():N}";
        var subCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        _ = Task.Run(async () =>
        {
            await foreach (var m in consumerClient.SubscribeAsync(replySubj, ct: subCts.Token))
            { tcs.TrySetResult(m.GetStringData()); break; }
        });
        await Task.Delay(100);
        await RmqRequestAsync(controlClient, "$RMQ.CONSUME.logs.all",
            new { replySubject = replySubj, prefetchCount = 1 });
        await Task.Delay(50);

        // Publish to "logs.info.db" — matches "logs.#"
        await RmqRequestAsync(controlClient, "$RMQ.PUBLISH.test.topic.logs.info.db",
            new { body = Convert.ToBase64String("db warning"u8.ToArray()) });

        var raw = await tcs.Task;
        var rk = JsonDocument.Parse(raw).RootElement.GetProperty("routing_key").GetString();
        Assert.Equal("logs.info.db", rk);
    }

    // ── 4. Ack removes message from unacked tracker ───────────────────────────

    [Fact]
    public async Task Ack_RemovesMessageFromUnacked()
    {
        // Direct test of ExchangeManager (unit-level, no network roundtrip)
        var mgr = new ExchangeManager();
        mgr.DeclareExchange("x", ExchangeType.Direct);
        var q = mgr.DeclareQueue("q.ack");
        mgr.Bind("x", "q.ack", "key");

        ulong? receivedTag = null;
        mgr.Consume("q.ack", "ctag1", msg => receivedTag = msg.DeliveryTag);

        mgr.Publish("x", "key", "hello"u8.ToArray());
        await Task.Delay(50);

        Assert.NotNull(receivedTag);
        Assert.Single(q.Unacked);

        mgr.Ack("q.ack", receivedTag!.Value);
        Assert.Empty(q.Unacked);
    }

    // ── 5. Nack with requeue redelivers the message ───────────────────────────

    [Fact]
    public async Task Nack_Requeue_RedeliversMessage()
    {
        var mgr = new ExchangeManager();
        mgr.DeclareExchange("x", ExchangeType.Direct);
        mgr.DeclareQueue("q.nack");
        mgr.Bind("x", "q.nack", "rk");

        int deliveries = 0;
        ulong? lastTag = null;
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        mgr.Consume("q.nack", "ct", msg =>
        {
            lastTag = msg.DeliveryTag;
            deliveries++;
            if (deliveries >= 2) tcs.TrySetResult(true);
        });

        mgr.Publish("x", "rk", "payload"u8.ToArray());
        await Task.Delay(50);

        Assert.Equal(1, deliveries);
        Assert.NotNull(lastTag);

        // Nack with requeue — should redeliver
        mgr.Nack("q.nack", lastTag!.Value, requeue: true);

        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.True(deliveries >= 2);
    }

    // ── 6. Nack without requeue dead-letters when DLX is configured ───────────

    [Fact]
    public async Task Nack_NoRequeue_DeadLetters_WhenDlxConfigured()
    {
        var mgr = new ExchangeManager();
        mgr.DeclareExchange("main.x",  ExchangeType.Direct);
        mgr.DeclareExchange("dlx",     ExchangeType.Fanout);

        var dlq = mgr.DeclareQueue("dead.letter.q");
        mgr.Bind("dlx", "dead.letter.q", "");

        var mainQ = mgr.DeclareQueue("main.q", new RabbitQueueArgs
        {
            Durable = true,
            DeadLetterExchange = "dlx"
        });
        mgr.Bind("main.x", "main.q", "work");

        ulong? mainTag = null;
        mgr.Consume("main.q", "main.ct", msg => mainTag = msg.DeliveryTag);

        var dlqTcs = new TaskCompletionSource<RabbitMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        mgr.Consume("dead.letter.q", "dlq.ct", msg => dlqTcs.TrySetResult(msg));

        mgr.Publish("main.x", "work", "important"u8.ToArray());
        await Task.Delay(50);

        Assert.NotNull(mainTag);
        Assert.Empty(dlq.Unacked);

        // Nack without requeue → DLX
        mgr.Nack("main.q", mainTag!.Value, requeue: false);

        var deadMsg = await dlqTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.NotNull(deadMsg);
        Assert.NotNull(deadMsg.Headers);
        Assert.Equal("nack", deadMsg.Headers!["x-death-reason"]);
    }

    // ── 7. Priority queue delivers high-priority message first ────────────────

    [Fact]
    public async Task Priority_Queue_DeliversHighPriorityFirst()
    {
        var mgr = new ExchangeManager();
        mgr.DeclareExchange("prio.x", ExchangeType.Direct);
        mgr.DeclareQueue("prio.q", new RabbitQueueArgs { MaxPriority = 10 });
        mgr.Bind("prio.x", "prio.q", "prio");

        // Enqueue without a consumer so messages accumulate
        mgr.Publish("prio.x", "prio", "low"u8.ToArray(),    priority: 1);
        mgr.Publish("prio.x", "prio", "highest"u8.ToArray(), priority: 9);
        mgr.Publish("prio.x", "prio", "medium"u8.ToArray(),  priority: 5);

        var order = new List<string>();
        mgr.Consume("prio.q", "prio.ct", msg =>
            order.Add(Encoding.UTF8.GetString(msg.Payload)));

        await Task.Delay(100);

        Assert.Equal(3, order.Count);
        Assert.Equal("highest", order[0]);
        Assert.Equal("medium",  order[1]);
        Assert.Equal("low",     order[2]);
    }

    // ── 8. QoS prefetch count is respected ───────────────────────────────────

    [Fact]
    public async Task QoS_PrefetchCount_Respected()
    {
        var mgr = new ExchangeManager();
        mgr.DeclareExchange("qos.x", ExchangeType.Direct);
        mgr.DeclareQueue("qos.q");
        mgr.Bind("qos.x", "qos.q", "qos");

        int delivered = 0;
        mgr.Consume("qos.q", "qos.ct", _ => Interlocked.Increment(ref delivered), prefetchCount: 2);

        // Publish 5 messages — only 2 should be pushed (prefetch limit)
        for (int i = 0; i < 5; i++)
            mgr.Publish("qos.x", "qos", Encoding.UTF8.GetBytes($"m{i}"));

        await Task.Delay(100);
        Assert.Equal(2, delivered);
    }

    [Fact]
    public async Task Consume_AutoAck_DoesNotAccumulateUnacked()
    {
        var mgr = new ExchangeManager();
        mgr.DeclareExchange("autoack.x", ExchangeType.Direct);
        var queue = mgr.DeclareQueue("autoack.q");
        mgr.Bind("autoack.x", "autoack.q", "auto");

        int delivered = 0;
        mgr.Consume("autoack.q", "autoack.ct", _ =>
        {
            Interlocked.Increment(ref delivered);
            return true;
        }, autoAck: true);

        mgr.Publish("autoack.x", "auto", "payload"u8.ToArray());
        await Task.Delay(50);

        Assert.Equal(1, delivered);
        Assert.Empty(queue.Unacked);
        Assert.Equal(0, queue.Count);
    }

    [Fact]
    public async Task Consume_CallbackFailure_RequeuesMessage()
    {
        var mgr = new ExchangeManager();
        mgr.DeclareExchange("retry.x", ExchangeType.Direct);
        var queue = mgr.DeclareQueue("retry.q");
        mgr.Bind("retry.x", "retry.q", "retry");

        int attempts = 0;
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        mgr.Consume("retry.q", "retry.ct", _ =>
        {
            attempts++;
            if (attempts == 1)
                return false;

            delivered.TrySetResult();
            return true;
        }, autoAck: true);

        mgr.Publish("retry.x", "retry", "payload"u8.ToArray());
        await delivered.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(2, attempts);
        Assert.Equal(0, queue.Count);
        Assert.Empty(queue.Unacked);
    }

    [Fact]
    public async Task ExclusiveQueue_ShouldRejectDifferentOwner()
    {
        var mgr = new ExchangeManager();
        mgr.DeclareQueue("exclusive.q", new RabbitQueueArgs { Exclusive = true });

        mgr.Consume("exclusive.q", "ct-1", _ => true, autoAck: true, ownerId: "conn-1");

        var ex = Assert.Throws<InvalidOperationException>(() =>
            mgr.Consume("exclusive.q", "ct-2", _ => true, autoAck: true, ownerId: "conn-2"));

        Assert.Contains("exclusive", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AutoDeleteQueue_ShouldDeleteAfterLastConsumerCancelled()
    {
        var mgr = new ExchangeManager();
        mgr.DeclareQueue("autodel.q", new RabbitQueueArgs { AutoDelete = true });

        mgr.Consume("autodel.q", "ct-1", _ => true, autoAck: true, ownerId: "conn-1");

        mgr.CancelConsumer("autodel.q", "ct-1");
        await Task.Delay(50);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            mgr.Consume("autodel.q", "ct-2", _ => true, autoAck: true, ownerId: "conn-2"));
        Assert.Contains("does not exist", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ── 9. Publisher confirms — confirm mode is per connection and sequenced ──

    [Fact]
    public async Task Publisher_Confirm_Select_ReturnsSequencedConfirms()
    {
        await using var client = await ConnectedClientAsync();
        await RmqRequestAsync(client, "$RMQ.EXCHANGE.DECLARE",
            new { name = "confirm.x", type = "Direct", durable = true });
        await RmqRequestAsync(client, "$RMQ.QUEUE.DECLARE",
            new { name = "confirm.q", durable = true });
        await RmqRequestAsync(client, "$RMQ.QUEUE.BIND",
            new { exchange = "confirm.x", queue = "confirm.q", routingKey = "confirm" });

        var resp = await RmqRequestAsync(client, "$RMQ.CONFIRM.SELECT", new { });
        Assert.True(resp.RootElement.GetProperty("ok").GetBoolean());
        var result = resp.RootElement.GetProperty("result");
        Assert.True(result.GetProperty("confirm_mode").GetBoolean());
        Assert.Equal(0L, result.GetProperty("publish_seq").GetInt64());

        var pub1 = await RmqRequestAsync(client, "$RMQ.PUBLISH.confirm.x.confirm",
            new { body = Convert.ToBase64String("one"u8.ToArray()) });
        var result1 = pub1.RootElement.GetProperty("result");
        Assert.True(result1.GetProperty("confirm_mode").GetBoolean());
        Assert.True(result1.GetProperty("confirmed").GetBoolean());
        Assert.True(result1.GetProperty("durable_accepted").GetBoolean());
        Assert.Equal(1L, result1.GetProperty("publish_seq").GetInt64());

        var pub2 = await RmqRequestAsync(client, "$RMQ.PUBLISH.confirm.x.confirm",
            new { body = Convert.ToBase64String("two"u8.ToArray()) });
        var result2 = pub2.RootElement.GetProperty("result");
        Assert.Equal(2L, result2.GetProperty("publish_seq").GetInt64());
    }

    [Fact]
    public async Task Publisher_Confirm_Mode_ShouldBeScopedPerConnection()
    {
        await using var client1 = await ConnectedClientAsync();
        await using var client2 = await ConnectedClientAsync();

        await RmqRequestAsync(client1, "$RMQ.EXCHANGE.DECLARE",
            new { name = "confirm.scope.x", type = "Direct", durable = true });
        await RmqRequestAsync(client1, "$RMQ.QUEUE.DECLARE",
            new { name = "confirm.scope.q", durable = true });
        await RmqRequestAsync(client1, "$RMQ.QUEUE.BIND",
            new { exchange = "confirm.scope.x", queue = "confirm.scope.q", routingKey = "confirm" });

        await RmqRequestAsync(client1, "$RMQ.CONFIRM.SELECT", new { });

        var pub1 = await RmqRequestAsync(client1, "$RMQ.PUBLISH.confirm.scope.x.confirm",
            new { body = Convert.ToBase64String("one"u8.ToArray()) });
        Assert.True(pub1.RootElement.GetProperty("result").GetProperty("confirm_mode").GetBoolean());

        var pub2 = await RmqRequestAsync(client2, "$RMQ.PUBLISH.confirm.scope.x.confirm",
            new { body = Convert.ToBase64String("two"u8.ToArray()) });
        Assert.False(pub2.RootElement.GetProperty("result").TryGetProperty("confirm_mode", out _));
    }

    // ── 10. $RMQ.STATS returns exchange and queue summary ─────────────────────

    [Fact]
    public async Task Stats_ReturnsExchangesAndQueues()
    {
        await using var client = await ConnectedClientAsync();
        var resp = await RmqRequestAsync(client, "$RMQ.STATS", new { });
        Assert.True(resp.RootElement.GetProperty("ok").GetBoolean());
        var result = resp.RootElement.GetProperty("result");
        Assert.True(result.TryGetProperty("exchanges", out _));
        Assert.True(result.TryGetProperty("queues", out _));
    }

    [Fact]
    public async Task Cancel_Consumer_ShouldStopFurtherDeliveries()
    {
        var controlClient = await ConnectedClientAsync();
        var consumerClient = await ConnectedClientAsync();

        await RmqRequestAsync(controlClient, "$RMQ.EXCHANGE.DECLARE",
            new { name = "cancel.x", type = "Direct", durable = true });
        await RmqRequestAsync(controlClient, "$RMQ.QUEUE.DECLARE",
            new { name = "cancel.q", durable = true });
        await RmqRequestAsync(controlClient, "$RMQ.QUEUE.BIND",
            new { exchange = "cancel.x", queue = "cancel.q", routingKey = "cancel" });

        string replySubj = $"_rmq.cancel.{Guid.NewGuid():N}";
        string consumerTag = $"ctag-{Guid.NewGuid():N}";
        int deliveries = 0;
        using var subCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var firstDelivery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellationNotice = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = Task.Run(async () =>
        {
            await foreach (var msg in consumerClient.SubscribeAsync(replySubj, ct: subCts.Token))
            {
                using var doc = JsonDocument.Parse(msg.GetStringData());
                if (doc.RootElement.TryGetProperty("event_type", out var eventType) &&
                    eventType.GetString() == "consumer_cancelled")
                {
                    cancellationNotice.TrySetResult(msg.GetStringData());
                    continue;
                }

                if (Interlocked.Increment(ref deliveries) == 1)
                    firstDelivery.TrySetResult();
            }
        });

        await Task.Delay(150);
        await RmqRequestAsync(controlClient, "$RMQ.CONSUME.cancel.q",
            new { replySubject = replySubj, consumerTag, autoAck = true });

        await RmqRequestAsync(controlClient, "$RMQ.PUBLISH.cancel.x.cancel",
            new { body = Convert.ToBase64String("one"u8.ToArray()) });
        await firstDelivery.Task.WaitAsync(TimeSpan.FromSeconds(5));

        int previousDeliveries = deliveries;
        await RmqRequestAsync(controlClient, "$RMQ.CANCEL.cancel.q",
            new { consumerTag });
        var notice = JsonDocument.Parse(await cancellationNotice.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("consumer_cancelled", notice.RootElement.GetProperty("event_type").GetString());
        Assert.Equal(consumerTag, notice.RootElement.GetProperty("consumer_tag").GetString());

        await RmqRequestAsync(controlClient, "$RMQ.PUBLISH.cancel.x.cancel",
            new { body = Convert.ToBase64String("two"u8.ToArray()) });
        await Task.Delay(300);

        Assert.Equal(previousDeliveries, deliveries);
    }

    [Fact]
    public async Task Cancel_ManualAckConsumer_ShouldRequeueAndMarkRedelivered()
    {
        var mgr = new ExchangeManager();
        mgr.DeclareExchange("manual.x", ExchangeType.Direct);
        var queue = mgr.DeclareQueue("manual.q");
        mgr.Bind("manual.x", "manual.q", "manual");

        RabbitMessage? first = null;
        RabbitMessage? second = null;
        var redelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        mgr.Consume("manual.q", "ct-1", msg =>
        {
            first ??= msg;
            return true;
        }, prefetchCount: 1, autoAck: false, ownerId: "conn-1");

        mgr.Publish("manual.x", "manual", "payload"u8.ToArray());
        await Task.Delay(50);
        Assert.NotNull(first);
        Assert.False(first!.Redelivered);

        mgr.CancelConsumer("manual.q", "ct-1");
        Assert.Empty(queue.Unacked);

        mgr.Consume("manual.q", "ct-2", msg =>
        {
            second = msg;
            redelivered.TrySetResult();
            return true;
        }, autoAck: false, ownerId: "conn-2");

        await redelivered.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.NotNull(second);
        Assert.True(second!.Redelivered);
    }

    [Fact]
    public void DeleteQueue_ShouldRefuseWhileConsumersOrUnackedExist()
    {
        var mgr = new ExchangeManager();
        mgr.DeclareExchange("delete.x", ExchangeType.Direct);
        mgr.DeclareQueue("delete.q");
        mgr.Bind("delete.x", "delete.q", "delete");

        mgr.Consume("delete.q", "ct-1", _ => true, autoAck: false, ownerId: "conn-1");
        mgr.Publish("delete.x", "delete", "payload"u8.ToArray());

        Assert.False(mgr.DeleteQueue("delete.q"));
        var secondConsumer = Record.Exception(() =>
            mgr.Consume("delete.q", "ct-2", _ => true, autoAck: true, ownerId: "conn-2"));
        Assert.Null(secondConsumer);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        foreach (var client in _ownedClients)
            await client.DisposeAsync();
        await _server.DisposeAsync();
    }
}
