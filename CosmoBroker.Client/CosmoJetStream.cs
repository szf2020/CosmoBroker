using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

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
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(config));

        var reply = await _client.RequestAsync(subject, bytes, TimeSpan.FromSeconds(5), ct);
        var response = JsonSerializer.Deserialize<JsApiResponse>(reply.GetStringData());
        if (response?.Error != null)
            throw new Exception($"JetStream error {response.Error.Code}: {response.Error.Description}");
    }

    public async Task<JsPubAck> PublishAsync(string subject, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        var reply = await _client.RequestAsync(subject, payload, TimeSpan.FromSeconds(5), ct);
        var ack = JsonSerializer.Deserialize<JsPubAck>(reply.GetStringData())
                  ?? throw new Exception("JetStream publish: empty ack response.");
        if (ack.Error != null)
            throw new Exception($"JetStream error {ack.Error.Code}: {ack.Error.Description}");
        return ack;
    }
}

public class StreamConfig
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("subjects")]
    public List<string>? Subjects { get; set; }
}

public class JsPubAck
{
    [JsonPropertyName("stream")]
    public string Stream { get; set; } = "";

    [JsonPropertyName("seq")]
    public ulong Seq { get; set; }

    [JsonPropertyName("duplicate")]
    public bool Duplicate { get; set; }

    [JsonPropertyName("error")]
    public JsApiError? Error { get; set; }
}

internal class JsApiResponse
{
    [JsonPropertyName("error")]
    public JsApiError? Error { get; set; }
}

public class JsApiError
{
    [JsonPropertyName("code")]
    public int Code { get; set; }

    [JsonPropertyName("description")]
    public string Description { get; set; } = "";
}
