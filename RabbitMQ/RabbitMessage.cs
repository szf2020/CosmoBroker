using System;
using System.Collections.Generic;
using System.Threading;

namespace CosmoBroker.RabbitMQ;

/// <summary>
/// A message enqueued inside a RabbitQueue. Carries RabbitMQ-style metadata.
/// </summary>
public sealed class RabbitMessage
{
    private static long _globalDeliveryTag = 0;

    public long PersistedId { get; set; }

    public ulong DeliveryTag { get; } = (ulong)Interlocked.Increment(ref _globalDeliveryTag);

    /// <summary>Routing key the message was published with.</summary>
    public string RoutingKey { get; init; } = string.Empty;

    /// <summary>Raw payload bytes.</summary>
    public byte[] Payload { get; init; } = Array.Empty<byte>();

    /// <summary>AMQP-style application headers (e.g. for Headers exchange routing).</summary>
    public Dictionary<string, string>? Headers { get; init; }

    public RabbitMessageProperties Properties { get; init; } = new();

    /// <summary>Priority (0–255). Only meaningful when queue has x-max-priority.</summary>
    public byte Priority { get; init; } = 0;

    /// <summary>When non-null, the message expires at this UTC time.</summary>
    public DateTime? ExpiresAt { get; init; }

    /// <summary>Enqueue time (used for ordering within same priority).</summary>
    public long EnqueueTicks { get; } = DateTime.UtcNow.Ticks;

    // Dead-letter tracking
    public int DeathCount { get; set; } = 0;
    public bool Redelivered { get; set; }
    public string? OriginalExchange { get; init; }
    public string? OriginalRoutingKey { get; init; }

    public bool IsExpired => ExpiresAt.HasValue && DateTime.UtcNow >= ExpiresAt.Value;
}

public sealed class RabbitMessageProperties
{
    public string? ContentType { get; init; }
    public string? ContentEncoding { get; init; }
    public Dictionary<string, object?>? Headers { get; init; }
    public byte? DeliveryMode { get; init; }
    public byte? Priority { get; init; }
    public string? CorrelationId { get; init; }
    public string? ReplyTo { get; init; }
    public string? Expiration { get; init; }
    public string? MessageId { get; init; }
    public DateTimeOffset? Timestamp { get; init; }
    public string? Type { get; init; }
    public string? UserId { get; init; }
    public string? AppId { get; init; }
}
