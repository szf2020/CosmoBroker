using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker.Client.Models;

namespace CosmoBroker.Client;

public class CosmoJetStream
{
    private readonly CosmoClient _client;
    private readonly string _prefix;

    public CosmoJetStream(CosmoClient client, string prefix = "$JS.API")
    {
        _client = client;
        _prefix = prefix;
    }

    public async Task CreateStreamAsync(StreamConfig config, CancellationToken ct = default)
    {
        var subject = $"{_prefix}.STREAM.CREATE.{config.Name}";
        var json = JsonSerializer.Serialize(config);
        var bytes = System.Text.Encoding.UTF8.GetBytes(json);

        var reply = await _client.RequestAsync(subject, bytes, TimeSpan.FromSeconds(5), ct);
        var replyJson = reply.GetStringData();

        if (replyJson.Contains("\"error\""))
        {
            throw new Exception($"Failed to create stream: {replyJson}");
        }
    }
    
    public async Task PublishAsync(string subject, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        // JetStream publish expects an ack
        var reply = await _client.RequestAsync(subject, payload, TimeSpan.FromSeconds(5), ct);
        var replyJson = reply.GetStringData();
        
        if (replyJson.Contains("\"error\""))
        {
            throw new Exception($"Failed to publish to stream: {replyJson}");
        }
    }
}

public class StreamConfig
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("subjects")]
    public List<string>? Subjects { get; set; }
}
