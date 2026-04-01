using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker;
using CosmoBroker.Client;
using Xunit;

namespace CosmoBroker.Client.Tests;

public class ClientTests : IAsyncDisposable
{
    private readonly BrokerServer _server;
    private readonly CancellationTokenSource _cts;

    public ClientTests()
    {
        _server = new BrokerServer(port: 4233, monitorPort: 8233);
        _cts = new CancellationTokenSource();
        _ = _server.StartAsync(_cts.Token);
    }

    [Fact]
    public async Task CanConnectAndPublishSubscribe()
    {
        // Wait for server to start
        await Task.Delay(100);

        var options = new CosmoClientOptions
        {
            Url = "nats://localhost:4233"
        };
        
        await using var client = new CosmoClient(options);
        await client.ConnectAsync();

        Assert.True(client.IsConnected);

        var receiveTask = Task.Run(async () =>
        {
            await foreach (var msg in client.SubscribeAsync("test.foo"))
            {
                return msg.GetStringData();
            }
            return null;
        });

        // Give subscription a moment to reach server
        await Task.Delay(100);

        await client.PublishAsync("test.foo", "hello world");

        var received = await receiveTask;
        Assert.Equal("hello world", received);
    }

    [Fact]
    public async Task RequestReply_ShouldWork()
    {
        // Wait for server to start
        await Task.Delay(100);

        var options = new CosmoClientOptions
        {
            Url = "nats://localhost:4233"
        };
        
        await using var client = new CosmoClient(options);
        await client.ConnectAsync();

        var responderTask = Task.Run(async () =>
        {
            await foreach (var msg in client.SubscribeAsync("service.echo"))
            {
                if (msg.ReplyTo != null)
                {
                    await client.PublishAsync(msg.ReplyTo, msg.GetStringData() + " echo");
                }
            }
        });

        await Task.Delay(100); // Wait for sub

        var reply = await client.RequestAsync("service.echo", Encoding.UTF8.GetBytes("hello"));
        Assert.Equal("hello echo", reply.GetStringData());
    }

    [Fact]
    public async Task JetStreamPublish_ShouldReturnAck()
    {
        await Task.Delay(100);

        var options = new CosmoClientOptions
        {
            Url = "nats://localhost:4233"
        };

        await using var client = new CosmoClient(options);
        await client.ConnectAsync();

        var js = new CosmoJetStream(client);
        await js.EnsureStreamAsync(new StreamConfig
        {
            Name = "ACKS",
            Subjects = ["acks.>"],
            Storage = "memory"
        });

        var ack = await js.PublishAsync("acks.test", Encoding.UTF8.GetBytes("hello"));
        Assert.Equal("ACKS", ack.Stream);
        Assert.True(ack.Seq > 0);
        Assert.False(ack.Duplicate);
    }

    [Fact]
    public async Task IsConnected_ShouldBecomeFalse_AfterServerDisconnect()
    {
        await Task.Delay(100);

        var options = new CosmoClientOptions
        {
            Url = "nats://localhost:4233"
        };

        await using var client = new CosmoClient(options);
        await client.ConnectAsync();

        Assert.True(client.IsConnected);

        _cts.Cancel();
        await _server.DisposeAsync();

        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (client.IsConnected && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        Assert.False(client.IsConnected);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await _server.DisposeAsync();
    }
}
