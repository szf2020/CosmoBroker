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

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await _server.DisposeAsync();
    }
}
