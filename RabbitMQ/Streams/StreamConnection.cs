using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using CosmoBroker.Auth;
using System.Threading;
using System.Threading.Tasks;

namespace CosmoBroker.RabbitMQ.Streams;

internal sealed class StreamConnection : IAsyncDisposable
{
    private const ushort PeerPropertiesKey = 17;
    private const ushort SaslHandshakeKey = 18;
    private const ushort SaslAuthenticateKey = 19;
    private const ushort TuneKey = 20;
    private const ushort OpenKey = 21;
    private const ushort CreateKey = 13;
    private const ushort DeleteKey = 14;
    private const ushort DeclarePublisherKey = 1;
    private const ushort PublishKey = 2;
    private const ushort PublishConfirmKey = 3;
    private const ushort PublishErrorKey = 4;
    private const ushort DeletePublisherKey = 6;
    private const ushort SubscribeKey = 7;
    private const ushort DeliverKey = 8;
    private const ushort CreditKey = 9;
    private const ushort StoreOffsetKey = 10;
    private const ushort QueryOffsetKey = 11;
    private const ushort UnsubscribeKey = 12;
    private const ushort PartitionsQueryKey = 0x0019;
    private const ushort RouteQueryKey = 0x0018;
    private const ushort CommandVersionsKey = 0x001b;
    private const ushort HeartbeatKey = 23;

    private readonly Stream _stream;
    private readonly ExchangeManager _manager;
    private readonly IAuthenticator? _authenticator;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ConcurrentDictionary<byte, string> _publishers = new();
    private readonly ConcurrentDictionary<byte, SubscriptionState> _subscriptions = new();
    private readonly string _sessionId = Guid.NewGuid().ToString("N");
    private static readonly uint[] Crc32Table = BuildCrc32Table();
    private bool _authenticated;
    private string _vhost = "/";
    private string? _username;

    private sealed class SubscriptionState
    {
        public required string Stream { get; init; }
        public required string ConsumerTag { get; init; }
        public required byte SubscriptionId { get; init; }
        public ushort Credit { get; set; }
    }

    public StreamConnection(Stream stream, ExchangeManager manager, IAuthenticator? authenticator = null)
    {
        _stream = stream;
        _manager = manager;
        _authenticator = authenticator;
        _manager.StreamMessageAppended += OnStreamAppended;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var frame = await StreamWire.ReadFrameAsync(_stream, ct);
            if (frame == null)
                break;

            await HandleFrameAsync(frame, ct);
        }
    }

    private async Task HandleFrameAsync(byte[] frame, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var offset = 0;
        var rawKey = StreamWire.ReadUInt16(span, ref offset);
        var key = (ushort)(rawKey & ~StreamWire.ResponseMask);
        _ = StreamWire.ReadUInt16(span, ref offset);

        switch (key)
        {
            case PeerPropertiesKey:
                await HandlePeerPropertiesAsync(frame, offset, ct);
                break;
            case SaslHandshakeKey:
                await HandleSaslHandshakeAsync(frame, offset, ct);
                break;
            case SaslAuthenticateKey:
                await HandleSaslAuthenticateAsync(frame, offset, ct);
                break;
            case TuneKey:
                break;
            case OpenKey:
                await HandleOpenAsync(frame, offset, ct);
                break;
            case CommandVersionsKey:
                await HandleCommandVersionsAsync(frame, offset, ct);
                break;
            case CreateKey:
                await HandleCreateAsync(frame, offset, ct);
                break;
            case DeleteKey:
                await HandleDeleteAsync(frame, offset, ct);
                break;
            case DeclarePublisherKey:
                await HandleDeclarePublisherAsync(frame, offset, ct);
                break;
            case PublishKey:
                await HandlePublishAsync(frame, offset, ct);
                break;
            case DeletePublisherKey:
                await HandleDeletePublisherAsync(frame, offset, ct);
                break;
            case SubscribeKey:
                await HandleSubscribeAsync(frame, offset, ct);
                break;
            case CreditKey:
                await HandleCreditAsync(frame, offset);
                break;
            case StoreOffsetKey:
                await HandleStoreOffsetAsync(frame, offset, ct);
                break;
            case QueryOffsetKey:
                await HandleQueryOffsetAsync(frame, offset, ct);
                break;
            case UnsubscribeKey:
                await HandleUnsubscribeAsync(frame, offset, ct);
                break;
            case PartitionsQueryKey:
                await HandlePartitionsQueryAsync(frame, offset, ct);
                break;
            case RouteQueryKey:
                await HandleRouteQueryAsync(frame, offset, ct);
                break;
            case HeartbeatKey:
                break;
            default:
                break;
        }
    }

    private async Task HandlePeerPropertiesAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(frame, ref offset);
        _ = StreamWire.ReadStringMap(span, ref offset);

        var properties = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["product"] = "CosmoBroker Stream",
            ["version"] = "1.2.0",
            ["platform"] = ".NET",
            ["copyright"] = "CosmoBroker",
            ["information"] = "RabbitMQ Stream-compatible slice"
        };

        await SendResponseAsync(PeerPropertiesKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, 1);
            StreamWire.WriteStringMap(writer.Buffer, ref writer.Offset, properties);
        }, ct);
    }

    private async Task HandleSaslHandshakeAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var correlationId = StreamWire.ReadUInt32(frame.AsSpan(), ref offset);
        await SendResponseAsync(SaslHandshakeKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, 1);
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, 1);
            StreamWire.WriteString(writer.Buffer, ref writer.Offset, "PLAIN");
        }, ct);
    }

    private async Task HandleSaslAuthenticateAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var mechanism = StreamWire.ReadString(span, ref offset);
        var bytes = StreamWire.ReadBytes(span, ref offset);

        var code = await AuthenticateAsync(mechanism, bytes, ct);
        _authenticated = code == 1;
        await SendResponseAsync(SaslAuthenticateKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, (ushort)code);
        }, ct);

        if (_authenticated)
            await SendTuneAsync(ct);
    }

    private async Task<ushort> AuthenticateAsync(string mechanism, byte[] data, CancellationToken ct)
    {
        if (!string.Equals(mechanism, "PLAIN", StringComparison.OrdinalIgnoreCase))
            return 7;

        var parts = Encoding.UTF8.GetString(data).Split('\0');
        var username = parts.Length >= 2 ? parts[^2] : string.Empty;
        var password = parts.Length >= 1 ? parts[^1] : string.Empty;

        if (_authenticator != null)
        {
            var result = await _authenticator.AuthenticateAsync(new ConnectOptions
            {
                User = username,
                Pass = password
            });
            if (!result.Success)
                return 8;
            _username = result.User?.Name ?? username;
            return 1;
        }

        if (string.Equals(username, "guest", StringComparison.Ordinal) &&
            string.Equals(password, "guest", StringComparison.Ordinal))
        {
            _username = username;
            return 1;
        }

        return 8;
    }

    private async Task SendTuneAsync(CancellationToken ct)
        => await SendFrameAsync(TuneKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, 1024 * 1024);
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, 60);
        }, ct);

    private async Task HandleOpenAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var vhost = StreamWire.ReadString(span, ref offset);
        _vhost = string.IsNullOrWhiteSpace(vhost) ? "/" : vhost;

        ushort code = 1;
        if (!_authenticated)
            code = 8;
        var properties = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["advertised_host"] = "localhost",
            ["stream_port"] = "5552"
        };

        await SendResponseAsync(OpenKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, code);
            if (code == 1)
                StreamWire.WriteStringMap(writer.Buffer, ref writer.Offset, properties);
        }, ct);
    }

    private async Task HandleCommandVersionsAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var correlationId = StreamWire.ReadUInt32(frame.AsSpan(), ref offset);
        var supported = new (ushort Key, ushort Min, ushort Max)[]
        {
            (PeerPropertiesKey, 1, 1),
            (SaslHandshakeKey, 1, 1),
            (SaslAuthenticateKey, 1, 1),
            (TuneKey, 1, 1),
            (OpenKey, 1, 1),
            (CreateKey, 1, 1),
            (DeleteKey, 1, 1),
            (DeclarePublisherKey, 1, 1),
            (PublishKey, 1, 1),
            (DeletePublisherKey, 1, 1),
            (SubscribeKey, 1, 1),
            (CreditKey, 1, 1),
            (StoreOffsetKey, 1, 1),
            (QueryOffsetKey, 1, 1),
            (UnsubscribeKey, 1, 1),
            (PartitionsQueryKey, 1, 1),
            (RouteQueryKey, 1, 1),
            (CommandVersionsKey, 1, 1),
            (HeartbeatKey, 1, 1)
        };

        await SendResponseAsync(CommandVersionsKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, 1);
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, (uint)supported.Length);
            foreach (var command in supported)
            {
                StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, command.Key);
                StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, command.Min);
                StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, command.Max);
            }
        }, ct);
    }

    private async Task HandleCreateAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var streamName = StreamWire.ReadString(span, ref offset);
        var args = StreamWire.ReadStringMap(span, ref offset);

        ushort code;
        if (_manager.HasQueue(_vhost, streamName))
        {
            code = 5;
        }
        else
        {
            _manager.DeclareQueue(_vhost, streamName, new RabbitQueueArgs
            {
                Type = RabbitQueueType.Stream,
                Durable = true,
                StreamMaxLengthBytes = TryGetLong(args, "max-length-bytes"),
                StreamMaxLengthMessages = TryGetLong(args, "max-length"),
                StreamMaxAgeMs = TryGetDurationMs(args, "max-age")
            });
            code = 1;
        }

        await SendResponseAsync(CreateKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, code);
        }, ct);
    }

    private async Task HandleDeleteAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var streamName = StreamWire.ReadString(span, ref offset);
        var deleted = _manager.DeleteQueue(_vhost, streamName);
        await SendResponseAsync(DeleteKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, deleted ? (ushort)1 : (ushort)2);
        }, ct);
    }

    private async Task HandleDeclarePublisherAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var publisherId = StreamWire.ReadByte(span, ref offset);
        _ = StreamWire.ReadString(span, ref offset);
        var streamName = StreamWire.ReadString(span, ref offset);

        ushort code;
        if (_manager.GetQueue(_vhost, streamName)?.Type == RabbitQueueType.Stream)
        {
            _publishers[publisherId] = streamName;
            code = 1;
        }
        else
        {
            code = 2;
        }

        await SendResponseAsync(DeclarePublisherKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, code);
        }, ct);
    }

    private async Task HandlePublishAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var publisherId = StreamWire.ReadByte(span, ref offset);
        var count = StreamWire.ReadInt32(span, ref offset);

        if (!_publishers.TryGetValue(publisherId, out var streamName))
        {
            await SendPublishErrorAsync(publisherId, [], ct);
            return;
        }

        var confirmed = new List<ulong>(count);
        var errors = new List<(ulong Id, ushort Code)>();
        for (var i = 0; i < count; i++)
        {
            var publishingId = StreamWire.ReadUInt64(span, ref offset);
            var payloadLength = (int)StreamWire.ReadUInt32(span, ref offset);
            var payload = span.Slice(offset, payloadLength).ToArray();
            offset += payloadLength;
            if (_manager.TryAppendToStream(_vhost, streamName, payload, out _))
                confirmed.Add(publishingId);
            else
                errors.Add((publishingId, 2));
        }

        if (confirmed.Count > 0)
            await SendPublishConfirmAsync(publisherId, confirmed, ct);
        if (errors.Count > 0)
            await SendPublishErrorAsync(publisherId, errors, ct);
    }

    private async Task HandleDeletePublisherAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var publisherId = StreamWire.ReadByte(span, ref offset);
        var removed = _publishers.TryRemove(publisherId, out _);

        await SendResponseAsync(DeletePublisherKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, removed ? (ushort)1 : (ushort)2);
        }, ct);
    }

    private async Task HandleSubscribeAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var subscriptionId = StreamWire.ReadByte(span, ref offset);
        var streamName = StreamWire.ReadString(span, ref offset);
        var offsetSpec = ReadOffsetSpec(span, ref offset);
        var credit = StreamWire.ReadUInt16(span, ref offset);
        if (offset < frame.Length)
        {
            _ = StreamWire.ReadStringMap(span, ref offset);
        }

        ushort code;
        if (_manager.GetQueue(_vhost, streamName)?.Type != RabbitQueueType.Stream)
        {
            code = 2;
        }
        else
        {
            var consumerTag = $"stream-sub-{subscriptionId}";
            _manager.TryStoreStreamConsumerOffset(_vhost, streamName, consumerTag, ResolveInitialOffset(streamName, offsetSpec), out _);
            _subscriptions[subscriptionId] = new SubscriptionState
            {
                Stream = streamName,
                ConsumerTag = consumerTag,
                SubscriptionId = subscriptionId,
                Credit = credit
            };
            code = 1;
        }

        await SendResponseAsync(SubscribeKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, code);
        }, ct);

        if (code == 1)
            await DrainSubscriptionAsync(subscriptionId, ct);
    }

    private async Task HandleCreditAsync(byte[] frame, int offset)
    {
        var span = frame.AsSpan();
        var subscriptionId = StreamWire.ReadByte(span, ref offset);
        var credit = StreamWire.ReadUInt16(span, ref offset);
        if (_subscriptions.TryGetValue(subscriptionId, out var subscription))
        {
            subscription.Credit += credit;
            await DrainSubscriptionAsync(subscriptionId, CancellationToken.None);
        }
    }

    private Task HandleStoreOffsetAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var first = StreamWire.ReadString(span, ref offset);
        var second = StreamWire.ReadString(span, ref offset);
        var nextOffset = StreamWire.ReadUInt64(span, ref offset);
        var consumer = first;
        var streamName = second;

        var firstQueue = _manager.GetQueue(_vhost, first);
        if (firstQueue?.Type == RabbitQueueType.Stream &&
            _manager.GetQueue(_vhost, second)?.Type != RabbitQueueType.Stream)
        {
            streamName = first;
            consumer = second;
        }

        _manager.TryStoreStreamConsumerOffset(_vhost, streamName, consumer, (long)nextOffset, out _);
        return Task.CompletedTask;
    }

    private async Task HandleQueryOffsetAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var consumer = StreamWire.ReadString(span, ref offset);
        var streamName = StreamWire.ReadString(span, ref offset);
        var ok = _manager.TryQueryStreamConsumerOffset(_vhost, streamName, consumer, out _, out var nextOffset);
        await SendResponseAsync(QueryOffsetKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, ok ? (ushort)1 : (ushort)19);
            StreamWire.WriteUInt64(writer.Buffer, ref writer.Offset, (ulong)Math.Max(0, nextOffset));
        }, ct);
    }

    private async Task HandleUnsubscribeAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var subscriptionId = StreamWire.ReadByte(span, ref offset);
        var removed = _subscriptions.TryRemove(subscriptionId, out _);

        await SendResponseAsync(UnsubscribeKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, removed ? (ushort)1 : (ushort)2);
        }, ct);
    }

    private async Task HandlePartitionsQueryAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var superStream = StreamWire.ReadString(span, ref offset);
        var exchange = _manager.GetExchange(_vhost, superStream);
        var partitions = exchange?.Type == ExchangeType.SuperStream
            ? exchange.GetSuperStreamPartitions().ToArray()
            : Array.Empty<string>();

        await SendResponseAsync(PartitionsQueryKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, partitions.Length > 0 ? (ushort)1 : (ushort)2);
            StreamWire.WriteInt32(writer.Buffer, ref writer.Offset, partitions.Length);
            foreach (var partition in partitions)
                StreamWire.WriteString(writer.Buffer, ref writer.Offset, partition);
        }, ct);
    }

    private async Task HandleRouteQueryAsync(byte[] frame, int offset, CancellationToken ct)
    {
        var span = frame.AsSpan();
        var correlationId = StreamWire.ReadUInt32(span, ref offset);
        var routingKey = StreamWire.ReadString(span, ref offset);
        var superStream = StreamWire.ReadString(span, ref offset);
        var ok = _manager.TryResolveSuperStreamPartition(_vhost, superStream, routingKey, null, out _, out var partition);
        var partitions = ok && !string.IsNullOrWhiteSpace(partition) ? new[] { partition! } : Array.Empty<string>();

        await SendResponseAsync(RouteQueryKey, writer =>
        {
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, correlationId);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, partitions.Length > 0 ? (ushort)1 : (ushort)2);
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, (uint)partitions.Length);
            foreach (var value in partitions)
                StreamWire.WriteString(writer.Buffer, ref writer.Offset, value);
        }, ct);
    }

    private long ResolveInitialOffset(string streamName, (ushort Kind, ulong? Value) offsetSpec)
    {
        var queue = _manager.GetQueue(_vhost, streamName);
        if (queue == null)
            return 0;

        return offsetSpec.Kind switch
        {
            1 => queue.ResolveRequestedStreamOffset(new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.First }),
            2 => queue.ResolveRequestedStreamOffset(new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Last }),
            3 => queue.ResolveRequestedStreamOffset(new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Next }),
            4 when offsetSpec.Value.HasValue => queue.ResolveRequestedStreamOffset(new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = (long)offsetSpec.Value.Value }),
            _ => queue.ResolveRequestedStreamOffset(new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Next })
        };
    }

    private static (ushort Kind, ulong? Value) ReadOffsetSpec(ReadOnlySpan<byte> frame, ref int offset)
    {
        var kind = StreamWire.ReadUInt16(frame, ref offset);
        ulong? value = null;
        if (kind == 4)
            value = StreamWire.ReadUInt64(frame, ref offset);
        else if (kind == 5)
            _ = StreamWire.ReadInt64(frame, ref offset);
        return (kind, value);
    }

    private async void OnStreamAppended(string vhost, string queue)
    {
        if (!string.Equals(vhost, _vhost, StringComparison.Ordinal))
            return;

        var matching = _subscriptions.Values
            .Where(x => string.Equals(x.Stream, queue, StringComparison.Ordinal))
            .Select(x => x.SubscriptionId)
            .ToArray();

        foreach (var subscriptionId in matching)
        {
            try { await DrainSubscriptionAsync(subscriptionId, CancellationToken.None); } catch { }
        }
    }

    private async Task DrainSubscriptionAsync(byte subscriptionId, CancellationToken ct)
    {
        if (!_subscriptions.TryGetValue(subscriptionId, out var subscription))
            return;

        var queue = _manager.GetQueue(_vhost, subscription.Stream);
        if (queue == null)
            return;

        while (subscription.Credit > 0 && queue.TryGetStreamMessage(subscription.ConsumerTag, out var message) && message != null)
        {
            subscription.Credit--;
            await SendDeliverAsync(subscriptionId, message, ct);
        }
    }

    private async Task SendPublishConfirmAsync(byte publisherId, IReadOnlyList<ulong> ids, CancellationToken ct)
        => await SendFrameAsync(PublishConfirmKey, writer =>
        {
            StreamWire.WriteByte(writer.Buffer, ref writer.Offset, publisherId);
            StreamWire.WriteInt32(writer.Buffer, ref writer.Offset, ids.Count);
            foreach (var id in ids)
                StreamWire.WriteUInt64(writer.Buffer, ref writer.Offset, id);
        }, ct);

    private async Task SendPublishErrorAsync(byte publisherId, IReadOnlyList<(ulong Id, ushort Code)> errors, CancellationToken ct)
        => await SendFrameAsync(PublishErrorKey, writer =>
        {
            StreamWire.WriteByte(writer.Buffer, ref writer.Offset, publisherId);
            StreamWire.WriteInt32(writer.Buffer, ref writer.Offset, errors.Count);
            foreach (var (id, code) in errors)
            {
                StreamWire.WriteUInt64(writer.Buffer, ref writer.Offset, id);
                StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, code);
            }
        }, ct);

    private async Task SendDeliverAsync(byte subscriptionId, RabbitMessage message, CancellationToken ct)
    {
        var payload = message.Payload;
        var chunkData = new byte[4 + payload.Length];
        var chunkDataOffset = 0;
        StreamWire.WriteUInt32(chunkData, ref chunkDataOffset, (uint)payload.Length);
        payload.CopyTo(chunkData.AsSpan(chunkDataOffset));
        var crc = ComputeCrc32(chunkData);
        var chunkLength = 1 + 1 + 2 + 4 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + payload.Length;
        await SendFrameAsync(DeliverKey, writer =>
        {
            StreamWire.WriteByte(writer.Buffer, ref writer.Offset, subscriptionId);
            StreamWire.WriteByte(writer.Buffer, ref writer.Offset, 1);
            StreamWire.WriteByte(writer.Buffer, ref writer.Offset, 0);
            StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, 1);
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, 1);
            StreamWire.WriteInt64(writer.Buffer, ref writer.Offset, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            StreamWire.WriteUInt64(writer.Buffer, ref writer.Offset, 0);
            StreamWire.WriteUInt64(writer.Buffer, ref writer.Offset, (ulong)Math.Max(0, message.StreamOffset));
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, crc);
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, (uint)(4 + payload.Length));
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, 0);
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, 0);
            StreamWire.WriteUInt32(writer.Buffer, ref writer.Offset, (uint)payload.Length);
            payload.CopyTo(writer.Buffer.AsSpan(writer.Offset));
            writer.Offset += payload.Length;
        }, ct, payloadSizeHint: chunkLength);
    }

    private async Task SendResponseAsync(ushort key, Action<FrameBuilder> write, CancellationToken ct)
        => await SendFrameAsync((ushort)(key | StreamWire.ResponseMask), write, ct);

    private async Task SendFrameAsync(ushort key, Action<FrameBuilder> write, CancellationToken ct, int payloadSizeHint = 256)
    {
        var buffer = new byte[Math.Max(payloadSizeHint + 32, 64)];
        var writer = new FrameBuilder(buffer);
        StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, key);
        StreamWire.WriteUInt16(writer.Buffer, ref writer.Offset, StreamWire.Version1);
        write(writer);

        await _writeLock.WaitAsync(ct);
        try
        {
            await StreamWire.WriteFrameAsync(_stream, writer.Buffer[..writer.Offset], ct);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private sealed class FrameBuilder
    {
        public FrameBuilder(byte[] buffer)
        {
            Buffer = buffer;
            Offset = 0;
        }

        public byte[] Buffer { get; }
        public int Offset;
    }

    private static long? TryGetLong(IReadOnlyDictionary<string, string> args, string key)
        => args.TryGetValue(key, out var value) && long.TryParse(value, out var parsed) ? parsed : null;

    private static long? TryGetDurationMs(IReadOnlyDictionary<string, string> args, string key)
    {
        if (!args.TryGetValue(key, out var value) || string.IsNullOrWhiteSpace(value))
            return null;
        if (long.TryParse(value, out var raw))
            return raw;
        if (value.EndsWith("ms", StringComparison.OrdinalIgnoreCase) && long.TryParse(value[..^2], out var ms))
            return ms;
        if (value.EndsWith("s", StringComparison.OrdinalIgnoreCase) && long.TryParse(value[..^1], out var seconds))
            return seconds * 1000L;
        return null;
    }

    private static uint[] BuildCrc32Table()
    {
        var table = new uint[256];
        for (uint i = 0; i < table.Length; i++)
        {
            var value = i;
            for (var bit = 0; bit < 8; bit++)
                value = (value & 1) != 0 ? 0xEDB88320u ^ (value >> 1) : value >> 1;
            table[i] = value;
        }

        return table;
    }

    private static uint ComputeCrc32(ReadOnlySpan<byte> data)
    {
        var crc = 0xFFFFFFFFu;
        foreach (var b in data)
            crc = Crc32Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return ~crc;
    }

    public ValueTask DisposeAsync()
    {
        _manager.StreamMessageAppended -= OnStreamAppended;
        return ValueTask.CompletedTask;
    }
}
