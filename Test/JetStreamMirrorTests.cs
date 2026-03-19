using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace CosmoBroker.Tests;

public class JetStreamMirrorTests : TestBase
{
    public JetStreamMirrorTests(ITestOutputHelper output) : base(output) { }

    [Fact]
    public async Task MirrorStream_ShouldReceiveFromSource()
    {
        await Server.StartAsync(Cts.Token);
        using var client1 = await CreateClientAsync();
        using var client2 = await CreateClientAsync();

        var sourceConfig = new { name = "SRC", subjects = new[] { "src.*" } };
        await client1.SendAsync($"PUB $JS.API.STREAM.CREATE.SRC _ {System.Text.Json.JsonSerializer.Serialize(sourceConfig).Length}\r\n{System.Text.Json.JsonSerializer.Serialize(sourceConfig)}\r\n");
        await Task.Delay(100);

        var mirrorConfig = new { name = "MIR", mirror = new { name = "SRC" } };
        await client1.SendAsync($"PUB $JS.API.STREAM.CREATE.MIR _ {System.Text.Json.JsonSerializer.Serialize(mirrorConfig).Length}\r\n{System.Text.Json.JsonSerializer.Serialize(mirrorConfig)}\r\n");
        await Task.Delay(100);

        await client2.SendAsync("PUB src.foo 2\r\nok\r\n");
        await Task.Delay(200);

        await client1.SendAsync("SUB JS_INFO 1\r\n");
        for (int i = 0; i < 20; i++) {
            if (Server.HasSubscribers("JS_INFO")) break;
            await Task.Delay(50);
        }

        await client2.SendAsync("PUB $JS.API.STREAM.INFO.MIR JS_INFO 2\r\n{}\r\n");
        string resp = "";
        for (int i = 0; i < 10; i++) {
            resp += await client1.ReadResponseAsync(1000);
            if (resp.Contains("stream_info_response")) break;
            await Task.Delay(100);
        }

        Assert.Contains("\"messages\":1", resp);
    }
}

