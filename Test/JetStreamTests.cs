using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using CosmoBroker.JetStream.Models;
using System.Collections.Generic;

namespace CosmoBroker.Tests;

public class JetStreamTests : TestBase
{
    public JetStreamTests(ITestOutputHelper output) : base(output) { }

    [Fact]
    public async Task TestJetStreamAddStream()
    {
        await Server.StartAsync(Cts.Token);
        using var client1 = await CreateClientAsync();
        using var client2 = await CreateClientAsync();

        var streamConfig = new {
            name = "ORDERS",
            subjects = new[] { "orders.*" }
        };
        string createJson = System.Text.Json.JsonSerializer.Serialize(streamConfig);
        
        // Client 1 waits for the response
        await client1.SendAsync("SUB JS_REPLY 1\r\n");
        
        // Polling wait for the sub to reach TopicTree
        for (int i = 0; i < 20; i++) {
            if (Server.HasSubscribers("JS_REPLY")) break;
            await Task.Delay(50);
        }

        // Client 2 sends the request
        await client2.SendAsync($"PUB $JS.API.STREAM.CREATE.ORDERS JS_REPLY {createJson.Length}\r\n{createJson}\r\n");
        
        string resp = "";
        for (int i = 0; i < 10; i++) {
            resp += await client1.ReadResponseAsync(1000);
            if (resp.Contains("io.nats.jetstream.api.v1.stream_create_response")) break;
            await Task.Delay(200);
        }

        Assert.Contains("io.nats.jetstream.api.v1.stream_create_response", resp);
        Assert.Contains("\"name\":\"ORDERS\"", resp);
    }

    [Fact]
    public async Task TestJetStreamPushConsumer()
    {
        await Server.StartAsync(Cts.Token);
        using var client1 = await CreateClientAsync();
        using var client2 = await CreateClientAsync();

        // Create Stream
        var streamConfig = new { name = "S1", subjects = new[] { "foo" } };
        await client1.SendAsync($"PUB $JS.API.STREAM.CREATE.S1 _ {System.Text.Json.JsonSerializer.Serialize(streamConfig).Length}\r\n{System.Text.Json.JsonSerializer.Serialize(streamConfig)}\r\n");
        await Task.Delay(200);

        // Create Push Consumer
        var consumerConfig = new { durable_name = "C1", deliver_subject = "deliver.c1" };
        await client1.SendAsync($"PUB $JS.API.CONSUMER.CREATE.S1.C1 _ {System.Text.Json.JsonSerializer.Serialize(consumerConfig).Length}\r\n{System.Text.Json.JsonSerializer.Serialize(consumerConfig)}\r\n");
        await Task.Delay(200);

        // Sub to deliver subject
        await client1.SendAsync("SUB deliver.c1 sub1\r\n");
        await Task.Delay(200);

        // Publish to stream
        await client2.SendAsync("PUB foo 2\r\nok\r\n");
        
        var resp = await client1.ReadResponseAsync();
        Assert.Contains("MSG deliver.c1 sub1", resp);
        Assert.Contains("ok", resp);
    }

    [Fact]
    public async Task TestMessageTTL()
    {
        await Server.StartAsync(Cts.Token);
        using var client1 = await CreateClientAsync();
        using var client2 = await CreateClientAsync();

        // Create Stream for TTL
        var streamConfig = new { name = "EXP", subjects = new[] { "exp" } };
        await client1.SendAsync($"PUB $JS.API.STREAM.CREATE.EXP _ {System.Text.Json.JsonSerializer.Serialize(streamConfig).Length}\r\n{System.Text.Json.JsonSerializer.Serialize(streamConfig)}\r\n");
        await Task.Delay(100);

        await client1.SendAsync("SUB exp sub1\r\n");
        await Task.Delay(100);

        string headers = "NATS/1.0\r\nNats-Msg-TTL: 1\r\n\r\n";
        string payload = "HI";
        await client2.SendAsync($"HPUB exp {headers.Length} {headers.Length + payload.Length}\r\n{headers}{payload}\r\n");

        var resp = await client1.ReadResponseAsync();
        Assert.Contains("HMSG exp sub1", resp);
        Assert.Contains("Nats-Msg-TTL: 1", resp);
    }

    [Fact]
    public async Task TestJetStreamDedupMsgId()
    {
        await Server.StartAsync(Cts.Token);
        using var client1 = await CreateClientAsync();
        using var client2 = await CreateClientAsync();

        var streamConfig = new { name = "D1", subjects = new[] { "dedup" } };
        string createJson = System.Text.Json.JsonSerializer.Serialize(streamConfig);
        await client1.SendAsync($"PUB $JS.API.STREAM.CREATE.D1 _ {createJson.Length}\r\n{createJson}\r\n");
        await Task.Delay(100);

        string headers = "NATS/1.0\r\nNats-Msg-Id: ABC123\r\n\r\n";
        string payload = "HI";
        await client2.SendAsync($"HPUB dedup {headers.Length} {headers.Length + payload.Length}\r\n{headers}{payload}\r\n");
        await client2.SendAsync($"HPUB dedup {headers.Length} {headers.Length + payload.Length}\r\n{headers}{payload}\r\n");

        await client1.SendAsync("SUB JS_INFO 1\r\n");
        for (int i = 0; i < 20; i++) {
            if (Server.HasSubscribers("JS_INFO")) break;
            await Task.Delay(50);
        }

        await client2.SendAsync("PUB $JS.API.STREAM.INFO.D1 JS_INFO 2\r\n{}\r\n");
        string resp = "";
        for (int i = 0; i < 10; i++) {
            resp += await client1.ReadResponseAsync(1000);
            if (resp.Contains("stream_info_response")) break;
            await Task.Delay(100);
        }

        Assert.Contains("\"messages\":1", resp);
    }

    [Fact]
    public async Task TestJetStreamAckAllUpdatesOffset()
    {
        await Server.StartAsync(Cts.Token);
        using var client1 = await CreateClientAsync();
        using var client2 = await CreateClientAsync();

        var streamConfig = new { name = "A1", subjects = new[] { "ack" }, retention = 2 };
        string createJson = System.Text.Json.JsonSerializer.Serialize(streamConfig);
        await client1.SendAsync($"PUB $JS.API.STREAM.CREATE.A1 _ {createJson.Length}\r\n{createJson}\r\n");
        await Task.Delay(100);

        var consumerConfig = new { durable_name = "C1", deliver_subject = "deliver.a1", ack_policy = 1 };
        string consumerJson = System.Text.Json.JsonSerializer.Serialize(consumerConfig);
        await client1.SendAsync($"PUB $JS.API.CONSUMER.CREATE.A1.C1 _ {consumerJson.Length}\r\n{consumerJson}\r\n");
        await Task.Delay(100);

        await client1.SendAsync("SUB deliver.a1 s1\r\n");
        await Task.Delay(100);

        await client2.SendAsync("PUB ack 2\r\nok\r\n");
        await client2.SendAsync("PUB ack 2\r\nok\r\n");
        await Task.Delay(200);

        var resp = await client1.ReadResponseAsync();
        Assert.Contains("MSG deliver.a1 s1", resp);

        // Ack the second message (sequence 2) with AckPolicy.All
        await client1.SendAsync("PUB $JS.ACK.A1.C1.2 2\r\n+ACK\r\n");
        await Task.Delay(100);

        // Re-create consumer to load offset from repo and verify no replay
        await client1.SendAsync("PUB $JS.API.CONSUMER.CREATE.A1.C1 _ 2\r\n{}\r\n");
        await Task.Delay(100);

        var resp2 = await client1.ReadResponseAsync();
        // Should not replay 2 messages for WorkQueue retention.
        Assert.DoesNotContain("MSG deliver.a1 s1", resp2);
    }
}
