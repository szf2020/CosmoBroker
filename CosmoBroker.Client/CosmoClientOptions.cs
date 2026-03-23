using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;

namespace CosmoBroker.Client;

public record CosmoClientOptions
{
    /// <summary>
    /// Connection URL (e.g., nats://localhost:4222 or tls://localhost:4222).
    /// </summary>
    public string Url { get; init; } = "nats://localhost:4222";

    /// <summary>
    /// The username for basic authentication.
    /// </summary>
    public string? Username { get; init; }

    /// <summary>
    /// The password for basic authentication.
    /// </summary>
    public string? Password { get; init; }

    /// <summary>
    /// The token for token-based authentication.
    /// </summary>
    public string? Token { get; init; }

    /// <summary>
    /// Client certificate for TLS mutual authentication.
    /// </summary>
    public X509Certificate2? ClientCertificate { get; init; }

    /// <summary>
    /// Optional JWT.
    /// </summary>
    public string? Jwt { get; init; }

    /// <summary>
    /// Timeout for operations like Connect and Request.
    /// </summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(10);
}
