using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker;
using CosmoBroker.Client;
using Xunit;

namespace CosmoBroker.Client.Tests;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// <summary>Minimal fake TCP server that speaks just enough NATS to satisfy CosmoClient.</summary>
internal sealed class FakeNatsServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    public int Port { get; }

    public FakeNatsServer(int port)
    {
        Port = port;
        _listener = new TcpListener(IPAddress.Loopback, port);
        _listener.Start();
    }

    /// <summary>Accept one connection, run <paramref name="handler"/>, then close.</summary>
    public Task AcceptOneAsync(Func<NetworkStream, Task> handler) =>
        Task.Run(async () =>
        {
            using var tcp = await _listener.AcceptTcpClientAsync();
            await handler(tcp.GetStream());
        });

    /// <summary>Perform the NATS handshake (INFO → drain CONNECT+PING → PONG).</summary>
    public static async Task HandshakeAsync(NetworkStream stream)
    {
        var info = "INFO {\"server_id\":\"fake\",\"version\":\"2.0.0\",\"proto\":1,\"headers\":true,\"max_payload\":1048576}\r\n";
        await stream.WriteAsync(Encoding.UTF8.GetBytes(info));
        // drain CONNECT + PING from client
        var buf = new byte[4096];
        await Task.Delay(50);
        if (stream.DataAvailable) _ = await stream.ReadAsync(buf);
        // acknowledge client's PING
        await stream.WriteAsync("PONG\r\n"u8.ToArray());
    }

    public async ValueTask DisposeAsync()
    {
        _listener.Stop();
        await Task.CompletedTask;
    }
}

// ── 1. Connect timeout ───────────────────────────────────────────────────────

public class TimeoutTests : IAsyncDisposable
{
    private readonly FakeNatsServer _fake = new(4245);

    [Fact]
    public async Task ConnectTimeout_WhenServerNeverSendsInfo_ShouldThrow()
    {
        // Accept the connection but never send INFO
        _ = _fake.AcceptOneAsync(async stream => await Task.Delay(10_000));

        var client = new CosmoClient(new CosmoClientOptions
        {
            Url = $"nats://localhost:{_fake.Port}",
            Timeout = TimeSpan.FromMilliseconds(300)
        });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ConnectAsync());
    }

    public ValueTask DisposeAsync() => _fake.DisposeAsync();
}

// ── 2–4. Integration tests against a real BrokerServer ──────────────────────

public class BrokerFixTests : IAsyncDisposable
{
    private readonly BrokerServer _server;
    private readonly CancellationTokenSource _cts = new();
    private const int Port = 4244;

    public BrokerFixTests()
    {
        _server = new BrokerServer(port: Port, monitorPort: 8244);
        _ = _server.StartAsync(_cts.Token);
    }

    private async Task<CosmoClient> ConnectedClientAsync()
    {
        await Task.Delay(100); // let server start
        var client = new CosmoClient(new CosmoClientOptions { Url = $"nats://localhost:{Port}" });
        await client.ConnectAsync();
        return client;
    }

    // ── 2. Shared inbox: concurrent requests all get correct replies ──────────

    [Fact]
    public async Task ConcurrentRequests_SharedInbox_AllGetCorrectReplies()
    {
        await using var client = await ConnectedClientAsync();

        // Echo responder: replies with "<payload>-reply"
        var responderCts = new CancellationTokenSource();
        _ = Task.Run(async () =>
        {
            await foreach (var msg in client.SubscribeAsync("inbox.echo", ct: responderCts.Token))
                if (msg.ReplyTo != null)
                    await client.PublishAsync(msg.ReplyTo, msg.GetStringData() + "-reply");
        });

        await Task.Delay(100); // let subscription reach server

        const int count = 10;
        var tasks = Enumerable.Range(0, count).Select(i =>
            client.RequestAsync("inbox.echo", Encoding.UTF8.GetBytes($"msg{i}"))
        ).ToArray();

        var replies = await Task.WhenAll(tasks);

        for (int i = 0; i < count; i++)
            Assert.Contains($"msg{i}-reply", replies.Select(r => r.GetStringData()));

        responderCts.Cancel();
    }

    // ── 3. Wildcard subscribe receives messages on matching subjects ──────────

    [Fact]
    public async Task WildcardSubscribe_ShouldReceiveMatchingMessages()
    {
        await using var client = await ConnectedClientAsync();

        var received = new List<string>();
        var cts = new CancellationTokenSource();

        var subTask = Task.Run(async () =>
        {
            await foreach (var msg in client.SubscribeAsync("wild.*", ct: cts.Token))
            {
                received.Add(msg.GetStringData());
                if (received.Count >= 2) cts.Cancel();
            }
        });

        await Task.Delay(100);

        await client.PublishAsync("wild.a", "alpha");
        await client.PublishAsync("wild.b", "beta");
        await client.PublishAsync("other.subject", "should-not-arrive");

        await subTask.WaitAsync(TimeSpan.FromSeconds(3)).ContinueWith(_ => { });

        Assert.Contains("alpha", received);
        Assert.Contains("beta", received);
        Assert.DoesNotContain("should-not-arrive", received);
    }

    // ── 4. Wildcard lock: concurrent subscribe/unsubscribe must not deadlock ──

    [Fact]
    public async Task WildcardLock_ConcurrentSubscribeAndUnsubscribe_NoDeadlock()
    {
        await using var client = await ConnectedClientAsync();

        const int workers = 5;
        var allDone = new TaskCompletionSource();

        var tasks = Enumerable.Range(0, workers).Select(i => Task.Run(async () =>
        {
            var cts = new CancellationTokenSource();
            var subTask = Task.Run(async () =>
            {
                await foreach (var _ in client.SubscribeAsync($"stress{i}.*", ct: cts.Token))
                    break; // exit after first message
            });

            await Task.Delay(50);
            await client.PublishAsync($"stress{i}.ping", "x");
            await subTask.WaitAsync(TimeSpan.FromSeconds(3));

            // Unsubscribe by cancelling and starting another subscription
            cts.Cancel();
        })).ToArray();

        // Should complete without deadlock within 5 seconds
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(5));
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await _server.DisposeAsync();
    }
}

// ── 5. HMSG: headers and body parsed correctly ───────────────────────────────

public class HmsgTests : IAsyncDisposable
{
    private readonly FakeNatsServer _fake = new(4246);

    [Fact]
    public async Task HmsgMessage_ShouldExposeHeadersAndBodySeparately()
    {
        _ = _fake.AcceptOneAsync(async stream =>
        {
            await FakeNatsServer.HandshakeAsync(stream);

            // Wait for client to subscribe
            await Task.Delay(200);

            // Build HMSG: NATS/1.0\r\nX-App: test\r\n\r\n  +  hello
            var headerBlock = "NATS/1.0\r\nX-App: test\r\n\r\n";
            var bodyStr = "hello";
            var headerBytes = Encoding.UTF8.GetByteCount(headerBlock);
            var totalBytes = headerBytes + Encoding.UTF8.GetByteCount(bodyStr);

            var line = $"HMSG test.hmsg 1 {headerBytes} {totalBytes}\r\n";
            await stream.WriteAsync(Encoding.UTF8.GetBytes(line));
            await stream.WriteAsync(Encoding.UTF8.GetBytes(headerBlock + bodyStr));
            await stream.WriteAsync("\r\n"u8.ToArray());

            await Task.Delay(500); // hold connection open until client reads
        });

        await using var client = new CosmoClient(new CosmoClientOptions
        {
            Url = $"nats://localhost:{_fake.Port}"
        });
        await client.ConnectAsync();

        var msgTcs = new TaskCompletionSource<CosmoMessage>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        _ = Task.Run(async () =>
        {
            await foreach (var msg in client.SubscribeAsync("test.hmsg", ct: cts.Token))
            {
                msgTcs.TrySetResult(msg);
                break;
            }
        });

        var received = await msgTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.Equal("hello", received.GetStringData());
        Assert.NotNull(received.Headers);
        Assert.True(received.Headers!.ContainsKey("X-App"));
        Assert.Equal("test", received.Headers["X-App"]);
    }

    public ValueTask DisposeAsync() => _fake.DisposeAsync();
}
