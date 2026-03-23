using System.Text.Json.Serialization;

namespace CosmoBroker.Client.Models;

public class ServerInfo
{
    [JsonPropertyName("server_id")]
    public string ServerId { get; set; } = "";

    [JsonPropertyName("server_name")]
    public string ServerName { get; set; } = "";

    [JsonPropertyName("version")]
    public string Version { get; set; } = "";

    [JsonPropertyName("proto")]
    public int ProtocolVersion { get; set; }

    [JsonPropertyName("host")]
    public string Host { get; set; } = "";

    [JsonPropertyName("port")]
    public int Port { get; set; }

    [JsonPropertyName("headers")]
    public bool HeadersSupported { get; set; }

    [JsonPropertyName("auth_required")]
    public bool AuthRequired { get; set; }

    [JsonPropertyName("tls_required")]
    public bool TlsRequired { get; set; }

    [JsonPropertyName("tls_verify")]
    public bool TlsVerify { get; set; }

    [JsonPropertyName("max_payload")]
    public int MaxPayload { get; set; }
}

public class ConnectRequest
{
    [JsonPropertyName("verbose")]
    public bool Verbose { get; set; } = true;

    [JsonPropertyName("pedantic")]
    public bool Pedantic { get; set; }

    [JsonPropertyName("tls_required")]
    public bool TlsRequired { get; set; }

    [JsonPropertyName("name")]
    public string Name { get; set; } = "CosmoBroker.Client";

    [JsonPropertyName("lang")]
    public string Lang { get; set; } = "C#";

    [JsonPropertyName("version")]
    public string Version { get; set; } = "1.0.0";

    [JsonPropertyName("protocol")]
    public int Protocol { get; set; } = 1;

    [JsonPropertyName("user")]
    public string? User { get; set; }

    [JsonPropertyName("pass")]
    public string? Pass { get; set; }

    [JsonPropertyName("auth_token")]
    public string? AuthToken { get; set; }

    [JsonPropertyName("jwt")]
    public string? Jwt { get; set; }
    
    [JsonPropertyName("headers")]
    public bool Headers { get; set; } = true;
}
