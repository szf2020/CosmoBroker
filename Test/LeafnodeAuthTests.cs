using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Xunit;
using CosmoBroker.Auth;
using CosmoBroker.Services;

namespace CosmoBroker.Tests;

public class LeafnodeAuthTests
{
    [Fact]
    public async Task Leafnode_ShouldSendConnectOptions_ToHub()
    {
        var tcs = new TaskCompletionSource<ConnectOptions>(TaskCreationOptions.RunContinuationsAsynchronously);
        var auth = new CaptureAuthenticator(tcs);

        int hubPort = GetFreePort();
        int hubMon = GetFreePort();
        var hub = new BrokerServer(hubPort, authenticator: auth, monitorPort: hubMon);
        await hub.StartAsync();

        int leafPort = GetFreePort();
        int leafMon = GetFreePort();
        var leaf = new BrokerServer(leafPort, monitorPort: leafMon);
        leaf.AddLeafnodeHub($"nats://127.0.0.1:{hubPort}", new LeafnodeHubOptions {
            ConnectOptions = new ConnectOptions { User = "leaf", Pass = "secret" }
        });
        await leaf.StartAsync();

        var options = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal("leaf", options.User);
        Assert.Equal("secret", options.Pass);

        await leaf.DisposeAsync();
        await hub.DisposeAsync();
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class CaptureAuthenticator : IAuthenticator
    {
        private readonly TaskCompletionSource<ConnectOptions> _tcs;
        public CaptureAuthenticator(TaskCompletionSource<ConnectOptions> tcs) => _tcs = tcs;

        public Task<AuthResult> AuthenticateAsync(ConnectOptions options)
        {
            _tcs.TrySetResult(options);
            return Task.FromResult(new AuthResult {
                Success = true,
                Account = new Account { Name = "leaf", SubjectPrefix = null },
                User = new User { Name = options.User ?? "leaf", AccountName = "leaf" }
            });
        }
    }
}

