using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.IO.Pipelines;

namespace CosmoBroker;

public class BrokerServer : IAsyncDisposable
{
    private readonly int _port;
    private Socket? _listenSocket;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;
    private readonly TopicTree _topicTree = new();
    private readonly Persistence.MessageRepository? _repo;
    private readonly Auth.IAuthenticator? _authenticator;

    public BrokerServer(int port = 4222, Persistence.MessageRepository? repo = null, Auth.IAuthenticator? authenticator = null)
    {
        _port = port;
        _repo = repo;
        _authenticator = authenticator;
    }

    public void Start()
    {
        _cts = new CancellationTokenSource();
        _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listenSocket.Bind(new IPEndPoint(IPAddress.Any, _port));
        _listenSocket.Listen(1000);

        Console.WriteLine($"[CosmoBroker] Listening on port {_port}...");
        _acceptTask = AcceptLoopAsync(_cts.Token);
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var socket = await _listenSocket!.AcceptAsync(ct);
                socket.NoDelay = true;
                
                var connection = new BrokerConnection(socket, _topicTree, _repo, _authenticator);
                _ = connection.RunAsync(); // Fire and forget connection loop
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Console.WriteLine($"[CosmoBroker] Accept error: {ex.Message}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();
        _listenSocket?.Dispose();
        if (_acceptTask != null)
        {
            try { await _acceptTask; } catch { }
        }
    }
}
