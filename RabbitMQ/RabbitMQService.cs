using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CosmoBroker.RabbitMQ;

/// <summary>
/// Handles $RMQ.* NATS API subjects — the same pattern as JetStream's $JS.API.*.
/// Called from BrokerConnection.HandlePub when the subject starts with "$RMQ.".
/// Returns a response payload that is sent to the replyTo address.
/// </summary>
public sealed class RabbitMQService
{
    private readonly ExchangeManager _mgr;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _confirmSequences = new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _confirmEnabled = new(StringComparer.Ordinal);
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public RabbitMQService(ExchangeManager manager) => _mgr = manager;

    public byte[]? HandleRequest(
        string subject,
        byte[] payload,
        string? replyTo,
        Func<string, byte[], bool>? publish,
        Auth.Account? account = null,
        Auth.User? user = null,
        string? sessionId = null)
    {
        try
        {
            var path = subject.Length > 5 ? subject.Substring(5) : "";
            var parts = path.Split('.');
            if (parts.Length == 0 || string.IsNullOrEmpty(parts[0]))
                return Error("Missing $RMQ verb");

            return parts[0] switch
            {
                "EXCHANGE" => HandleExchange(parts, payload, account),
                "QUEUE" => HandleQueue(parts, payload, account),
                "PUBLISH" => HandlePublish(path, payload, account, user, sessionId),
                "CONSUME" => HandleConsume(path, payload, publish, account),
                "CANCEL" => HandleCancel(path, payload, account),
                "ACK" => HandleAck(path, payload, account),
                "NACK" => HandleNack(path, payload, account),
                "REJECT" => HandleReject(path, payload, account),
                "QOS" => HandleQos(path, payload, account),
                "CONFIRM" => HandleConfirm(payload, account, user, sessionId),
                "STATS" => HandleStats(payload, account),
                _ => Error($"Unknown $RMQ verb: {parts[0]}")
            };
        }
        catch (Exception ex)
        {
            return Error(ex.Message);
        }
    }

    private byte[] HandleExchange(string[] parts, byte[] payload, Auth.Account? account)
    {
        if (parts.Length < 2) return Error("Missing EXCHANGE sub-command");

        switch (parts[1])
        {
            case "DECLARE":
            {
                var req = Deserialize<ExchangeDeclareRequest>(payload);
                if (req?.Name == null) return Error("Missing exchange name");
                var request = req;
                var exchangeName = request.Name!;
                var vhost = ResolveVhost(request.Vhost, account);
                if (!CanConfigure(account, vhost, $"exchange:{exchangeName}"))
                    return Error($"Access denied to configure exchange '{exchangeName}' in vhost '{vhost}'");

                var type = Enum.TryParse<ExchangeType>(request.Type ?? "Direct", true, out var et) ? et : ExchangeType.Direct;
                var exchange = _mgr.DeclareExchange(vhost, exchangeName, type, request.Durable, request.AutoDelete);
                return Ok(new { vhost, exchange = exchange.Name, type = exchange.Type.ToString(), durable = exchange.Durable });
            }
            case "DELETE":
            {
                var req = Deserialize<NamedRequest>(payload);
                if (req?.Name == null) return Error("Missing exchange name");
                var exchangeName = req.Name!;
                var vhost = ResolveVhost(req?.Vhost, account);
                if (!CanConfigure(account, vhost, $"exchange:{exchangeName}"))
                    return Error($"Access denied to configure exchange '{exchangeName}' in vhost '{vhost}'");

                bool deleted = _mgr.DeleteExchange(vhost, exchangeName);
                return Ok(new { vhost, deleted });
            }
            default:
                return Error($"Unknown EXCHANGE sub-command: {parts[1]}");
        }
    }

    private byte[] HandleQueue(string[] parts, byte[] payload, Auth.Account? account)
    {
        if (parts.Length < 2) return Error("Missing QUEUE sub-command");

        switch (parts[1])
        {
            case "DECLARE":
            {
                var req = Deserialize<QueueDeclareRequest>(payload);
                if (req?.Name == null) return Error("Missing queue name");
                var request = req;
                var queueName = request.Name!;
                var vhost = ResolveVhost(request.Vhost, account);
                if (!CanConfigure(account, vhost, $"queue:{queueName}"))
                    return Error($"Access denied to configure queue '{queueName}' in vhost '{vhost}'");

                var args = new RabbitQueueArgs
                {
                    Type = string.Equals(request.QueueType, "stream", StringComparison.OrdinalIgnoreCase)
                        ? RabbitQueueType.Stream
                        : RabbitQueueType.Classic,
                    Durable = request.Durable,
                    Exclusive = request.Exclusive,
                    AutoDelete = request.AutoDelete,
                    MaxPriority = request.MaxPriority,
                    DeadLetterExchange = request.DeadLetterExchange,
                    DeadLetterRoutingKey = request.DeadLetterRoutingKey,
                    MessageTtlMs = request.MessageTtlMs,
                    QueueTtlMs = request.QueueTtlMs,
                    StreamMaxLengthBytes = request.StreamMaxLengthBytes,
                    StreamMaxLengthMessages = request.StreamMaxLengthMessages,
                    StreamMaxAgeMs = ParseDurationMs(request.StreamMaxAge)
                };
                var queue = _mgr.DeclareQueue(vhost, queueName, args);
                return Ok(new { vhost, queue = queue.Name, durable = queue.Durable, message_count = queue.Count });
            }
            case "BIND":
            {
                var req = Deserialize<BindRequest>(payload);
                if (req?.Queue == null || req.Exchange == null) return Error("Missing queue or exchange");
                var request = req;
                var queueName = request.Queue!;
                var exchangeName = request.Exchange!;
                var vhost = ResolveVhost(request.Vhost, account);
                if (!CanConfigure(account, vhost, $"exchange:{exchangeName}") ||
                    !CanConfigure(account, vhost, $"queue:{queueName}"))
                {
                    return Error($"Access denied to bind '{queueName}' to '{exchangeName}' in vhost '{vhost}'");
                }

                _mgr.Bind(vhost, exchangeName, queueName, request.RoutingKey ?? "", request.HeaderArgs);
                return Ok(new { vhost, bound = true });
            }
            case "UNBIND":
            {
                var req = Deserialize<BindRequest>(payload);
                if (req?.Queue == null || req.Exchange == null) return Error("Missing queue or exchange");
                var request = req;
                var queueName = request.Queue!;
                var exchangeName = request.Exchange!;
                var vhost = ResolveVhost(request.Vhost, account);
                if (!CanConfigure(account, vhost, $"exchange:{exchangeName}") ||
                    !CanConfigure(account, vhost, $"queue:{queueName}"))
                {
                    return Error($"Access denied to unbind '{queueName}' from '{exchangeName}' in vhost '{vhost}'");
                }

                _mgr.Unbind(vhost, exchangeName, queueName, request.RoutingKey ?? "");
                return Ok(new { vhost, unbound = true });
            }
            case "DELETE":
            {
                var req = Deserialize<NamedRequest>(payload);
                if (req?.Name == null) return Error("Missing queue name");
                var queueName = req.Name!;
                var vhost = ResolveVhost(req?.Vhost, account);
                if (!CanConfigure(account, vhost, $"queue:{queueName}"))
                    return Error($"Access denied to configure queue '{queueName}' in vhost '{vhost}'");

                bool deleted = _mgr.DeleteQueue(vhost, queueName);
                return Ok(new { vhost, deleted });
            }
            default:
                return Error($"Unknown QUEUE sub-command: {parts[1]}");
        }
    }

    private byte[] HandlePublish(string path, byte[] payload, Auth.Account? account, Auth.User? user, string? sessionId)
    {
        PublishEnvelope? envelope = null;
        byte[] body = payload;
        Dictionary<string, string>? headers = null;
        byte priority = 0;
        int? ttlMs = null;
        string? explicitVhost = null;
        string? explicitProducerId = null;

        if (payload.Length > 0 && payload[0] == '{')
        {
            try
            {
                envelope = Deserialize<PublishEnvelope>(payload);
                explicitVhost = envelope?.Vhost;
                explicitProducerId = envelope?.ProducerId;
                body = envelope?.Body != null ? Convert.FromBase64String(envelope.Body) : payload;
                headers = envelope?.Headers;
                priority = envelope?.Priority ?? 0;
                ttlMs = envelope?.TtlMs;
            }
            catch
            {
                envelope = null;
            }
        }

        var vhost = ResolveVhost(explicitVhost, account);
        if (!TryParsePublishPath(path, vhost, out var exchange, out var routingKey))
            return Error("Usage: $RMQ.PUBLISH.<exchange>.<routingKey>");
        if (!CanWrite(account, vhost, $"exchange:{exchange}"))
            return Error($"Access denied to publish to exchange '{exchange}' in vhost '{vhost}'");

        var producerKey = GetConfirmProducerKey(vhost, explicitProducerId, user, sessionId);
        bool confirmEnabled = producerKey != null && _confirmEnabled.ContainsKey(producerKey);

        int queued = _mgr.Publish(vhost, exchange, routingKey, body, headers, priority, ttlMs);
        if (!confirmEnabled)
            return Ok(new { vhost, routed_to = queued });

        long publishSeq = _confirmSequences.AddOrUpdate(producerKey!, 1, static (_, current) => current + 1);
        return Ok(new
        {
            vhost,
            routed_to = queued,
            confirm_mode = true,
            confirmed = true,
            publish_seq = publishSeq,
            producer_id = explicitProducerId,
            durable_accepted = true
        });
    }

    private byte[] HandleConsume(string path, byte[] payload, Func<string, byte[], bool>? publish, Auth.Account? account)
    {
        if (!TryGetEntityName(path, "CONSUME", out var queueName))
            return Error("Usage: $RMQ.CONSUME.<queue>");

        var req = payload.Length > 0 ? Deserialize<ConsumeRequest>(payload) : null;
        var vhost = ResolveVhost(req?.Vhost, account);
        if (!CanRead(account, vhost, $"queue:{queueName}"))
            return Error($"Access denied to consume queue '{queueName}' in vhost '{vhost}'");

        string consumerTag = req?.ConsumerTag ?? $"ctag-{Guid.NewGuid():N}";
        int prefetch = req?.PrefetchCount ?? 0;
        string? replySubject = ScopeSubject(req?.ReplySubject, account);
        bool autoAck = req?.AutoAck ?? true;
        string? ownerId = req?.OwnerId;
        RabbitStreamOffsetSpec? streamOffset = ParseStreamOffset(req?.StreamOffset);

        if (replySubject == null)
            return Error("ConsumeRequest.ReplySubject is required");
        if (publish == null)
            return Error("Internal publish callback is required for push consumers");

        _mgr.Consume(vhost, queueName, consumerTag, msg =>
        {
            var delivered = new
            {
                consumer_tag = consumerTag,
                delivery_tag = msg.DeliveryTag,
                routing_key = msg.RoutingKey,
                body = Convert.ToBase64String(msg.Payload),
                headers = msg.Headers,
                priority = msg.Priority,
                death_count = msg.DeathCount,
                redelivered = msg.Redelivered,
                vhost
            };
            return publish(replySubject, JsonSerializer.SerializeToUtf8Bytes(delivered));
        }, prefetch, autoAck, ownerId, (queue, tag) =>
        {
            var notice = new
            {
                event_type = "consumer_cancelled",
                vhost,
                queue,
                consumer_tag = tag
            };
            publish(replySubject, JsonSerializer.SerializeToUtf8Bytes(notice));
        }, streamOffset: streamOffset);

        return Ok(new { vhost, consumer_tag = consumerTag, queue = queueName });
    }

    private byte[] HandleCancel(string path, byte[] payload, Auth.Account? account)
    {
        if (!TryGetEntityName(path, "CANCEL", out var queueName))
            return Error("Usage: $RMQ.CANCEL.<queue>");

        var req = Deserialize<CancelConsumeRequest>(payload);
        if (string.IsNullOrWhiteSpace(req?.ConsumerTag))
            return Error("CancelConsumeRequest.ConsumerTag is required");
        var consumerTag = req.ConsumerTag;
        var vhost = ResolveVhost(req?.Vhost, account);
        if (!CanRead(account, vhost, $"queue:{queueName}"))
            return Error($"Access denied to consume queue '{queueName}' in vhost '{vhost}'");

        _mgr.CancelConsumer(vhost, queueName, consumerTag);
        return Ok(new { vhost, cancelled = true, consumer_tag = consumerTag });
    }

    private byte[] HandleAck(string path, byte[] payload, Auth.Account? account)
    {
        if (!TryGetEntityName(path, "ACK", out var queueName))
            return Error("Usage: $RMQ.ACK.<queue>");
        var req = Deserialize<AckRequest>(payload);
        var vhost = ResolveVhost(req?.Vhost, account);
        if (!CanRead(account, vhost, $"queue:{queueName}"))
            return Error($"Access denied to ack queue '{queueName}' in vhost '{vhost}'");

        _mgr.Ack(vhost, queueName, req?.DeliveryTag ?? 0, req?.Multiple ?? false);
        return Ok(new { vhost, acked = true });
    }

    private byte[] HandleNack(string path, byte[] payload, Auth.Account? account)
    {
        if (!TryGetEntityName(path, "NACK", out var queueName))
            return Error("Usage: $RMQ.NACK.<queue>");
        var req = Deserialize<NackRequest>(payload);
        var vhost = ResolveVhost(req?.Vhost, account);
        if (!CanRead(account, vhost, $"queue:{queueName}"))
            return Error($"Access denied to nack queue '{queueName}' in vhost '{vhost}'");

        _mgr.Nack(vhost, queueName, req?.DeliveryTag ?? 0, req?.Multiple ?? false, req?.Requeue ?? true);
        return Ok(new { vhost, nacked = true });
    }

    private byte[] HandleReject(string path, byte[] payload, Auth.Account? account)
    {
        if (!TryGetEntityName(path, "REJECT", out var queueName))
            return Error("Usage: $RMQ.REJECT.<queue>");
        var req = Deserialize<NackRequest>(payload);
        var vhost = ResolveVhost(req?.Vhost, account);
        if (!CanRead(account, vhost, $"queue:{queueName}"))
            return Error($"Access denied to reject queue '{queueName}' in vhost '{vhost}'");

        _mgr.Reject(vhost, queueName, req?.DeliveryTag ?? 0, req?.Requeue ?? false);
        return Ok(new { vhost, rejected = true });
    }

    private byte[] HandleQos(string path, byte[] payload, Auth.Account? account)
    {
        if (!TryGetEntityName(path, "QOS", out var queueName))
            return Error("Usage: $RMQ.QOS.<queue>");
        var req = Deserialize<QosRequest>(payload);
        if (req == null) return Error("Invalid QoS request");
        var vhost = ResolveVhost(req.Vhost, account);
        if (!CanRead(account, vhost, $"queue:{queueName}"))
            return Error($"Access denied to manage queue '{queueName}' in vhost '{vhost}'");

        _mgr.SetQos(vhost, queueName, req.ConsumerTag ?? "", req.PrefetchCount);
        return Ok(new { vhost, prefetch_count = req.PrefetchCount });
    }

    private byte[] HandleConfirm(byte[] payload, Auth.Account? account, Auth.User? user, string? sessionId)
    {
        var req = Deserialize<ConfirmSelectRequest>(payload);
        var vhost = ResolveVhost(req?.Vhost, account);
        if (!CanAccessVhost(account, vhost))
            return Error($"Access denied to vhost '{vhost}'");

        var producerKey = GetConfirmProducerKey(vhost, req?.ProducerId, user, sessionId);
        if (producerKey == null)
            return Error("Unable to resolve confirm session");

        _confirmEnabled[producerKey] = 1;
        _confirmSequences.TryAdd(producerKey, 0);

        return Ok(new { vhost, confirm_mode = true, producer_id = req?.ProducerId, publish_seq = 0L });
    }

    private byte[] HandleStats(byte[] payload, Auth.Account? account)
    {
        var req = Deserialize<VhostRequest>(payload);
        var vhost = ResolveOptionalVhost(req?.Vhost, account);
        if (vhost != null && !CanAccessVhost(account, vhost))
            return Error($"Access denied to vhost '{vhost}'");
        if (vhost == null && account?.RabbitPermissionsConfigured == true)
            return Ok(new { vhosts = account.RabbitAllowedVhosts, queues = Array.Empty<object>(), exchanges = Array.Empty<object>() });

        return Ok(_mgr.GetStats(vhost));
    }

    private static byte[] Ok(object result) => JsonSerializer.SerializeToUtf8Bytes(new { ok = true, result });
    private static byte[] Error(string msg) => JsonSerializer.SerializeToUtf8Bytes(new { ok = false, error = msg });

    private bool TryParsePublishPath(string path, string vhost, out string exchange, out string routingKey)
    {
        exchange = string.Empty;
        routingKey = string.Empty;

        const string prefix = "PUBLISH.";
        if (!path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return false;

        var remainder = path[prefix.Length..];
        if (string.IsNullOrEmpty(remainder))
            return false;

        foreach (var candidate in _mgr.GetExchangeNames(vhost).OrderByDescending(static k => k.Length))
        {
            if (candidate.Length == 0)
                continue;
            if (remainder.Equals(candidate, StringComparison.OrdinalIgnoreCase))
            {
                exchange = candidate;
                routingKey = string.Empty;
                return true;
            }
            if (remainder.StartsWith(candidate + ".", StringComparison.OrdinalIgnoreCase))
            {
                exchange = candidate;
                routingKey = remainder[(candidate.Length + 1)..];
                return true;
            }
        }

        int splitAt = remainder.IndexOf('.');
        if (splitAt < 0)
            return false;

        exchange = remainder[..splitAt];
        routingKey = remainder[(splitAt + 1)..];
        return true;
    }

    private static bool TryGetEntityName(string path, string verb, out string name)
    {
        var prefix = verb + ".";
        if (path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) && path.Length > prefix.Length)
        {
            name = path[prefix.Length..];
            return true;
        }

        name = string.Empty;
        return false;
    }

    private static T? Deserialize<T>(byte[] payload)
    {
        if (payload == null || payload.Length == 0) return default;
        return JsonSerializer.Deserialize<T>(payload, JsonOpts);
    }

    private static string ResolveVhost(string? requestedVhost, Auth.Account? account)
        => ResolveOptionalVhost(requestedVhost, account) ?? "/";

    private static string? ResolveOptionalVhost(string? requestedVhost, Auth.Account? account)
    {
        if (!string.IsNullOrWhiteSpace(requestedVhost))
            return requestedVhost;
        if (account != null && !string.IsNullOrWhiteSpace(account.DefaultRabbitVhost))
            return account.DefaultRabbitVhost;
        return "/";
    }

    private static bool CanAccessVhost(Auth.Account? account, string vhost)
        => account?.CanAccessRabbitVhost(vhost) ?? true;

    private static bool CanConfigure(Auth.Account? account, string vhost, string resource)
        => account?.CanRabbitConfigure(vhost, resource) ?? true;

    private static bool CanWrite(Auth.Account? account, string vhost, string resource)
        => account?.CanRabbitWrite(vhost, resource) ?? true;

    private static bool CanRead(Auth.Account? account, string vhost, string resource)
        => account?.CanRabbitRead(vhost, resource) ?? true;

    private static string? ScopeSubject(string? subject, Auth.Account? account)
    {
        if (string.IsNullOrWhiteSpace(subject) || string.IsNullOrWhiteSpace(account?.SubjectPrefix))
            return subject;
        if (subject.StartsWith(account.SubjectPrefix + ".", StringComparison.Ordinal))
            return subject;
        return account.SubjectPrefix + "." + subject;
    }

    private static string? GetConfirmProducerKey(string vhost, string? explicitProducerId, Auth.User? user, string? sessionId)
    {
        if (!string.IsNullOrWhiteSpace(explicitProducerId))
            return $"{vhost}\u001Fproducer\u001F{explicitProducerId}";
        if (!string.IsNullOrWhiteSpace(sessionId))
            return $"{vhost}\u001Fsession\u001F{sessionId}";
        if (!string.IsNullOrWhiteSpace(user?.Name))
            return $"{vhost}\u001Fuser\u001F{user.Name}";
        return null;
    }

    private class VhostRequest
    {
        public string? Vhost { get; set; }
    }

    private sealed class NamedRequest : VhostRequest
    {
        public string? Name { get; set; }
    }

    private sealed class ExchangeDeclareRequest : VhostRequest
    {
        public string? Name { get; set; }
        public string? Type { get; set; }
        public bool Durable { get; set; } = true;
        public bool AutoDelete { get; set; }
    }

    private sealed class QueueDeclareRequest : VhostRequest
    {
        public string? Name { get; set; }
        public string? QueueType { get; set; }
        public bool Durable { get; set; } = true;
        public bool Exclusive { get; set; }
        public bool AutoDelete { get; set; }
        public byte MaxPriority { get; set; }
        public string? DeadLetterExchange { get; set; }
        public string? DeadLetterRoutingKey { get; set; }
        public int? MessageTtlMs { get; set; }
        public int? QueueTtlMs { get; set; }
        public long? StreamMaxLengthBytes { get; set; }
        public long? StreamMaxLengthMessages { get; set; }
        public string? StreamMaxAge { get; set; }
    }

    private sealed class BindRequest : VhostRequest
    {
        public string? Exchange { get; set; }
        public string? Queue { get; set; }
        public string? RoutingKey { get; set; }
        public Dictionary<string, string>? HeaderArgs { get; set; }
    }

    private sealed class PublishEnvelope : VhostRequest
    {
        public string? Body { get; set; }
        public Dictionary<string, string>? Headers { get; set; }
        public byte Priority { get; set; }
        public int? TtlMs { get; set; }
        public string? ProducerId { get; set; }
    }

    private sealed class ConfirmSelectRequest : VhostRequest
    {
        public string? ProducerId { get; set; }
    }

    private sealed class ConsumeRequest : VhostRequest
    {
        public string? ConsumerTag { get; set; }
        public string? ReplySubject { get; set; }
        public string? StreamOffset { get; set; }
        public int PrefetchCount { get; set; }
        public bool? AutoAck { get; set; }
        public string? OwnerId { get; set; }
    }

    private sealed class CancelConsumeRequest : VhostRequest
    {
        public string? ConsumerTag { get; set; }
    }

    private sealed class AckRequest : VhostRequest
    {
        public ulong DeliveryTag { get; set; }
        public bool Multiple { get; set; }
    }

    private sealed class NackRequest : VhostRequest
    {
        public ulong DeliveryTag { get; set; }
        public bool Multiple { get; set; }
        public bool Requeue { get; set; } = true;
    }

    private sealed class QosRequest : VhostRequest
    {
        public string? ConsumerTag { get; set; }
        public int PrefetchCount { get; set; }
    }

    private static RabbitStreamOffsetSpec? ParseStreamOffset(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        if (long.TryParse(value, out var offset))
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = offset };

        return value.Trim().ToLowerInvariant() switch
        {
            "first" => new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.First },
            "last" => new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Last },
            "next" => new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Next },
            _ => null
        };
    }

    private static long? ParseDurationMs(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var text = value.Trim();
        if (long.TryParse(text, out var rawMs))
            return rawMs;

        if (TimeSpan.TryParse(text, out var timeSpan))
            return (long)timeSpan.TotalMilliseconds;

        if (text.EndsWith("ms", StringComparison.OrdinalIgnoreCase) &&
            long.TryParse(text[..^2], out var explicitMs))
            return explicitMs;

        if (text.Length < 2)
            return null;

        var suffix = char.ToLowerInvariant(text[^1]);
        if (!long.TryParse(text[..^1], out var magnitude))
            return null;

        return suffix switch
        {
            's' => magnitude * 1000L,
            'm' => magnitude * 60_000L,
            'h' => magnitude * 3_600_000L,
            'd' => magnitude * 86_400_000L,
            _ => null
        };
    }
}
