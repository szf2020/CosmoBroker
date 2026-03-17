using System;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using CosmoBroker;
using CosmoBroker.Persistence;
using CosmoBroker.Auth;

namespace CosmoBroker.Tests;

public class BrokerIntegrationTests : IDisposable
{
    private static int _nextPort = 5000;
    private readonly BrokerServer _server;
    private readonly int _port;

    public BrokerIntegrationTests()
    {
        _port = Interlocked.Increment(ref _nextPort);
        _server = new BrokerServer(_port);
        _server.Start();
    }

    [Fact]
    public async Task BasicPubSub_ShouldDeliverMessage()
    {
        using var sub = await CreateClient();
        await sub.WriteAsync("SUB foo 1\r\n");

        using var pub = await CreateClient();
        await pub.WriteAsync("PUB foo 5\r\nHello\r\n");

        await sub.ReadAsync(); // INFO
        string? resp = await sub.ReadAsync(); // MSG foo 1 5
        Assert.NotNull(resp);
        Assert.StartsWith("MSG foo 1 5", resp);
        
        string? payload = await sub.ReadAsync();
        Assert.Equal("Hello", payload);
    }

    [Fact]
    public async Task WildcardSub_ShouldReceiveMatchingMessages()
    {
        using var sub = await CreateClient();
        await sub.WriteAsync("SUB news.* 1\r\n");

        using var pub = await CreateClient();
        await pub.WriteAsync("PUB news.tech 2\r\nhi\r\n");

        await sub.ReadAsync(); // INFO
        string? msg = await sub.ReadAsync();
        Assert.NotNull(msg);
        Assert.StartsWith("MSG news.tech 1 2", msg);
    }

    [Fact]
    public async Task QueueGroup_ShouldLoadBalance()
    {
        using var worker1 = await CreateClient();
        await worker1.WriteAsync("SUB task group1 1\r\n");

        using var worker2 = await CreateClient();
        await worker2.WriteAsync("SUB task group1 2\r\n");

        using var pub = await CreateClient();
        await pub.WriteAsync("PUB task 2\r\nJ1\r\n");
        await pub.WriteAsync("PUB task 2\r\nJ2\r\n");

        await worker1.ReadAsync(); // Handshake INFO
        await worker2.ReadAsync(); // Handshake INFO

        // Start reading tasks from both workers
        var t1 = worker1.ReadAsync();
        var t2 = worker2.ReadAsync();

        // Wait for at least one worker to get a message
        var completed = await Task.WhenAny(t1, t2);
        string? msg = await completed;
        Assert.NotNull(msg);
        Assert.StartsWith("MSG task", msg);
        
        // Read payload for first message
        if (completed == t1) await worker1.ReadAsync(); else await worker2.ReadAsync();

        // Now wait for the second message
        var remaining = (completed == t1) ? t2 : t1;
        string? msg2 = await remaining;
        Assert.NotNull(msg2);
        Assert.StartsWith("MSG task", msg2);
        
        // Read payload for second message
        if (remaining == t1) await worker1.ReadAsync(); else await worker2.ReadAsync();
    }

    [Fact]
    public async Task PersistenceAndReplay_ShouldWork()
    {
        string dbFile = $"test_replay_{_port}.db";
        if (System.IO.File.Exists(dbFile)) System.IO.File.Delete(dbFile);
        
        var repo = new MessageRepository($"Data Source={dbFile};");
        await repo.InitializeAsync();

        // Use a dedicated server for this test to use the repo
        var pServer = new BrokerServer(_port + 100, repo);
        pServer.Start();

        try {
            // 1. Publish persistent messages
            using (var pub = new TestClient(new TcpClient("localhost", _port + 100)))
            {
                await pub.WriteAsync("PUB persist.test 2\r\nP1\r\n");
                await pub.WriteAsync("PUB persist.test 2\r\nP2\r\n");
                await Task.Delay(500); // Ensure DB write
            }

            // 2. Connect durable sub and check replay
            using (var sub = new TestClient(new TcpClient("localhost", _port + 100)))
            {
                await sub.ReadAsync(); // Handshake
                await sub.WriteAsync("SUB persist.test 1 my-durable\r\n");
                
                string? m1 = await sub.ReadAsync();
                Console.WriteLine($"[Test] Read Line 1: {m1}");
                string? p1 = await sub.ReadAsync();
                Console.WriteLine($"[Test] Read Payload 1: {p1}");
                Assert.Equal("P1", p1);

                string? m2 = await sub.ReadAsync();
                Console.WriteLine($"[Test] Read Line 2: {m2}");
                string? p2 = await sub.ReadAsync();
                Console.WriteLine($"[Test] Read Payload 2: {p2}");
                Assert.Equal("P2", p2);
            }
        }
        finally {
            await pServer.DisposeAsync();
            if (System.IO.File.Exists(dbFile)) System.IO.File.Delete(dbFile);
        }
    }

    [Fact]
    public async Task Authentication_ShouldRejectInvalidCredentials()
    {
        var auth = new SimpleAuthenticator(user: "admin", pass: "secret");
        var aServer = new BrokerServer(_port + 200, authenticator: auth);
        aServer.Start();

        try {
            using var client = await CreateClient(_port + 200);
            await client.ReadAsync(); // Handshake INFO
            
            await client.WriteAsync("CONNECT {\"user\":\"admin\",\"pass\":\"wrong\"}\r\n");
            
            try {
                string? resp = await client.ReadAsync();
                Assert.Equal("-ERR 'Authentication Failed'", resp);
            } catch (System.IO.IOException) {
                // Connection was reset by peer after sending error, which is also valid behavior
            }
        }
        finally {
            await aServer.DisposeAsync();
        }
    }

    private async Task<TestClient> CreateClient(int? altPort = null)
    {
        var client = new TcpClient();
        await client.ConnectAsync("localhost", altPort ?? _port);
        return new TestClient(client);
    }

    public void Dispose()
    {
        _server.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private class TestClient : IDisposable
    {
        private readonly TcpClient _client;
        private readonly NetworkStream _stream;
        private readonly StreamReader _reader;
        private readonly StreamWriter _writer;

        public TestClient(TcpClient client)
        {
            _client = client;
            _stream = client.GetStream();
            var utf8 = new UTF8Encoding(false);
            _reader = new StreamReader(_stream, utf8);
            _writer = new StreamWriter(_stream, utf8) { AutoFlush = true };
        }

        public async Task WriteAsync(string cmd) => await _writer.WriteAsync(cmd);
        public async Task<string?> ReadAsync() => await _reader.ReadLineAsync();

        public void Dispose()
        {
            _writer.Dispose();
            _reader.Dispose();
            _stream.Dispose();
            _client.Dispose();
        }
    }
}
