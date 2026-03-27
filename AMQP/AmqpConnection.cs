using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker.RabbitMQ;

namespace CosmoBroker.AMQP;

internal sealed class AmqpConnection : IAsyncDisposable
{
    private static string ResourceLockedText(string queueName, string vhost)
        => $"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '{queueName}' in vhost '{vhost}'";

    private readonly Stream _stream;
    private readonly ExchangeManager _manager;
    private readonly Auth.IAuthenticator? _authenticator;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ConcurrentDictionary<ushort, ChannelState> _channels = new();
    private readonly ConcurrentDictionary<ulong, string> _deliveryQueueByTag = new();
    private readonly ConcurrentDictionary<ulong, DeliveryContext> _deliveryContextByTag = new();
    private readonly string _sessionId = Guid.NewGuid().ToString("N");
    private bool _closed;
    private bool _disposed;
    private string _vhost = "/";
    private Auth.Account? _account;
    private Auth.User? _user;

    private sealed class ChannelState
    {
        public bool FlowActive { get; set; } = true;
        public bool ConfirmMode { get; set; }
        public bool TransactionMode { get; set; }
        public ulong PublishSeq { get; set; }
        public int ConsumerPrefetchCount { get; set; }
        public int GlobalPrefetchCount { get; set; }
        public string? CurrentQueueName { get; set; }
        public PendingPublish? PendingPublish { get; set; }
        public List<PendingPublish> PendingTransactions { get; } = new();
        public Dictionary<string, string> Consumers { get; } = new(StringComparer.Ordinal);
    }

    private sealed class PendingPublish
    {
        public required string Exchange { get; init; }
        public required string RoutingKey { get; init; }
        public bool Mandatory { get; init; }
        public ulong BodySize { get; set; }
        public RabbitMessageProperties Properties { get; set; } = new();
        public MemoryStream Body { get; } = new();
    }

    private readonly record struct DeliveryContext(ushort ChannelNumber, string QueueName, string ConsumerTag);

    public AmqpConnection(Stream stream, ExchangeManager manager, Auth.IAuthenticator? authenticator = null)
    {
        _stream = stream;
        _manager = manager;
        _authenticator = authenticator;
        if (_authenticator == null)
        {
            _account = new Auth.Account { Name = "global" };
            _user = new Auth.User { Name = "anonymous", AccountName = "global" };
        }
    }

    public async Task RunAsync(CancellationToken ct)
    {
        if (!await ExpectProtocolHeaderAsync(ct))
            return;
        await SendConnectionStartAsync(ct);

        while (!ct.IsCancellationRequested && !_closed)
        {
            var frame = await ReadFrameAsync(ct);
            if (_closed)
                break;
            switch (frame.Type)
            {
                case AmqpWire.FrameHeartbeat:
                    await WriteFrameAsync(AmqpWire.FrameHeartbeat, frame.Channel, ReadOnlyMemory<byte>.Empty, ct);
                    break;
                case AmqpWire.FrameMethod:
                    await HandleMethodFrameAsync(frame.Channel, frame.Payload, ct);
                    break;
                case AmqpWire.FrameHeader:
                    await HandleHeaderFrameAsync(frame.Channel, frame.Payload, ct);
                    break;
                case AmqpWire.FrameBody:
                    await HandleBodyFrameAsync(frame.Channel, frame.Payload, ct);
                    break;
                default:
                    await SendConnectionCloseAsync(540, $"Unsupported AMQP frame type {frame.Type}", 0, 0, ct);
                    _closed = true;
                    break;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        _closed = true;

        foreach (var channelNumber in _channels.Keys.ToArray())
            CleanupChannelState(channelNumber);

        _manager.DeleteExclusiveQueuesForOwner(_sessionId);

        await _stream.DisposeAsync();
        _writeLock.Dispose();
    }

    private async Task HandleMethodFrameAsync(ushort channelNumber, ReadOnlyMemory<byte> payloadMemory, CancellationToken ct)
    {
        var payload = payloadMemory.Span;
        ushort classId = AmqpWire.ReadUInt16(payload);
        ushort methodId = AmqpWire.ReadUInt16(payload[2..]);
        payload = payload[4..];

        switch (classId, methodId)
        {
            case (10, 11): // connection.start-ok
                AmqpWire.ReadTable(ref payload);
                string mechanism = AmqpWire.ReadShortString(ref payload);
                byte[] response = AmqpWire.ReadLongStringBytes(ref payload);
                _ = AmqpWire.ReadShortString(ref payload); // locale
                await AuthenticateAsync(mechanism, response, ct);
                await SendConnectionTuneAsync(ct);
                break;
            case (10, 31): // connection.tune-ok
                break;
            case (10, 40): // connection.open
                _vhost = AmqpWire.ReadShortString(ref payload);
                _ = AmqpWire.ReadShortString(ref payload);
                _ = payload.Length > 0 && AmqpWire.ReadBoolean(ref payload);
                if (!CanAccessVhost(_vhost))
                {
                    await SendConnectionCloseAsync(403, $"Access denied to vhost '{_vhost}'", 10, 40, ct);
                    return;
                }
                await SendMethodAsync(channelNumber, 10, 41, static writer => AmqpWire.WriteShortString(writer, string.Empty), ct);
                break;
            case (10, 50): // connection.close
                await SendMethodAsync(channelNumber, 10, 51, static _ => { }, ct);
                _closed = true;
                break;
            case (10, 51): // connection.close-ok
                _closed = true;
                break;
            case (20, 10): // channel.open
                _channels.TryAdd(channelNumber, new ChannelState());
                _ = AmqpWire.ReadShortString(ref payload);
                await SendMethodAsync(channelNumber, 20, 11, static writer => AmqpWire.WriteLongString(writer, string.Empty), ct);
                break;
            case (20, 20): // channel.flow
                bool active = ReadBit(ref payload, 0);
                var flowState = _channels.GetOrAdd(channelNumber, _ => new ChannelState());
                flowState.FlowActive = active;
                await SendMethodAsync(channelNumber, 20, 21, writer =>
                {
                    AmqpWire.WriteByte(writer, active ? (byte)1 : (byte)0);
                }, ct);
                if (active)
                {
                    foreach (var consumerQueue in flowState.Consumers.Values.Distinct(StringComparer.Ordinal))
                        _manager.GetQueue(_vhost, consumerQueue)?.DrainToConsumers();
                }
                break;
            case (20, 40): // channel.close
                CleanupChannelState(channelNumber);
                await SendMethodAsync(channelNumber, 20, 41, static _ => { }, ct);
                _channels.TryRemove(channelNumber, out _);
                break;
            case (20, 41): // channel.close-ok
                _channels.TryRemove(channelNumber, out _);
                break;
            case (40, 10): // exchange.declare
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string exchangeName = AmqpWire.ReadShortString(ref payload);
                string exchangeType = AmqpWire.ReadShortString(ref payload);
                bool passive = ReadBit(ref payload, 0);
                bool durable = ReadBit(ref payload, 1);
                bool autoDelete = ReadBit(ref payload, 2);
                _ = ReadBit(ref payload, 3);
                _ = ReadBit(ref payload, 4);
                bool exchangeDeclareNoWait = ReadBit(ref payload, 5);
                var exchangeArgs = AmqpWire.ReadTable(ref payload);
                if (passive)
                {
                    if (!_manager.HasExchange(_vhost, exchangeName))
                    {
                        await SendChannelCloseAsync(channelNumber, 404, $"NOT_FOUND - no exchange '{exchangeName}' in vhost '{_vhost}'", classId, methodId, ct);
                        return;
                    }
                }
                else
                {
                    if (!CanConfigure($"exchange:{exchangeName}"))
                    {
                        await SendChannelCloseAsync(channelNumber, 403, $"Access denied to exchange '{exchangeName}'", classId, methodId, ct);
                        return;
                    }

                    var existingExchange = _manager.GetExchange(_vhost, exchangeName);
                    var declaredType = ParseExchangeType(exchangeType);
                    int? declaredSuperPartitions = declaredType == ExchangeType.SuperStream
                        ? (int?)(GetLongArg(exchangeArgs, "x-partitions") ?? 0)
                        : null;
                    if (existingExchange != null &&
                        (existingExchange.Type != declaredType ||
                         existingExchange.Durable != durable ||
                         existingExchange.AutoDelete != autoDelete ||
                         (declaredType == ExchangeType.SuperStream &&
                          existingExchange.SuperStreamPartitions != declaredSuperPartitions)))
                    {
                        await SendChannelCloseAsync(channelNumber, 406, $"Exchange '{exchangeName}' redeclared with different properties", classId, methodId, ct);
                        return;
                    }
                    var type = declaredType;
                    try
                    {
                        if (type == ExchangeType.SuperStream)
                        {
                            int partitions = declaredSuperPartitions ?? 0;
                            if (partitions <= 0)
                            {
                                await SendChannelCloseAsync(channelNumber, 406, $"Super stream '{exchangeName}' requires x-partitions > 0", classId, methodId, ct);
                                return;
                            }

                            _manager.DeclareSuperStream(_vhost, exchangeName, partitions, durable, autoDelete);
                        }
                        else
                        {
                            _manager.DeclareExchange(_vhost, exchangeName, type, durable, autoDelete);
                        }
                    }
                    catch (InvalidOperationException ex)
                    {
                        await SendChannelCloseAsync(channelNumber, 403, ex.Message, classId, methodId, ct);
                        return;
                    }
                }
                if (!exchangeDeclareNoWait)
                    await SendMethodAsync(channelNumber, 40, 11, static _ => { }, ct);
                break;
            case (40, 30): // exchange.bind
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string destinationExchange = AmqpWire.ReadShortString(ref payload);
                string sourceExchange = AmqpWire.ReadShortString(ref payload);
                string exchangeBindRoutingKey = AmqpWire.ReadShortString(ref payload);
                bool exchangeBindNoWait = ReadBit(ref payload, 0);
                var exchangeBindArgs = AmqpWire.ReadTable(ref payload);
                if (string.IsNullOrEmpty(sourceExchange) || string.IsNullOrEmpty(destinationExchange))
                {
                    await SendChannelCloseAsync(channelNumber, 403, "ACCESS_REFUSED - operation not permitted on the default exchange", classId, methodId, ct);
                    return;
                }
                if (!CanConfigure($"exchange:{sourceExchange}") || !CanConfigure($"exchange:{destinationExchange}"))
                {
                    await SendChannelCloseAsync(channelNumber, 403, "Access denied to bind exchange", classId, methodId, ct);
                    return;
                }
                if (!_manager.HasExchange(_vhost, sourceExchange) || !_manager.HasExchange(_vhost, destinationExchange))
                {
                    await SendChannelCloseAsync(channelNumber, 404, "Exchange not found", classId, methodId, ct);
                    return;
                }
                _manager.BindExchange(_vhost, sourceExchange, destinationExchange, exchangeBindRoutingKey, ToStringDictionary(exchangeBindArgs));
                if (!exchangeBindNoWait)
                    await SendMethodAsync(channelNumber, 40, 31, static _ => { }, ct);
                break;
            case (40, 40): // exchange.unbind
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string destinationExchangeUnbind = AmqpWire.ReadShortString(ref payload);
                string sourceExchangeUnbind = AmqpWire.ReadShortString(ref payload);
                string exchangeUnbindRoutingKey = AmqpWire.ReadShortString(ref payload);
                bool exchangeUnbindNoWait = ReadBit(ref payload, 0);
                _ = AmqpWire.ReadTable(ref payload);
                if (string.IsNullOrEmpty(sourceExchangeUnbind) || string.IsNullOrEmpty(destinationExchangeUnbind))
                {
                    await SendChannelCloseAsync(channelNumber, 403, "Default exchange cannot be unbound", classId, methodId, ct);
                    return;
                }
                if (!CanConfigure($"exchange:{sourceExchangeUnbind}") || !CanConfigure($"exchange:{destinationExchangeUnbind}"))
                {
                    await SendChannelCloseAsync(channelNumber, 403, "Access denied to unbind exchange", classId, methodId, ct);
                    return;
                }
                if (!_manager.HasExchange(_vhost, sourceExchangeUnbind) || !_manager.HasExchange(_vhost, destinationExchangeUnbind))
                {
                    await SendChannelCloseAsync(channelNumber, 404, "Exchange not found", classId, methodId, ct);
                    return;
                }
                _manager.UnbindExchange(_vhost, sourceExchangeUnbind, destinationExchangeUnbind, exchangeUnbindRoutingKey);
                if (!exchangeUnbindNoWait)
                    await SendMethodAsync(channelNumber, 40, 51, static _ => { }, ct);
                break;
            case (40, 20): // exchange.delete
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string deleteExchange = AmqpWire.ReadShortString(ref payload);
                bool deleteIfUnused = ReadBit(ref payload, 0);
                bool deleteExchangeNoWait = ReadBit(ref payload, 1);
                if (_manager.IsBuiltInExchangeName(deleteExchange))
                {
                    await SendChannelCloseAsync(channelNumber, 403, $"Exchange '{deleteExchange}' cannot be deleted", classId, methodId, ct);
                    return;
                }
                if (!CanConfigure($"exchange:{deleteExchange}"))
                {
                    await SendChannelCloseAsync(channelNumber, 403, $"Access denied to exchange '{deleteExchange}'", classId, methodId, ct);
                    return;
                }
                if (!_manager.HasExchange(_vhost, deleteExchange))
                {
                    await SendChannelCloseAsync(channelNumber, 404, $"Exchange '{deleteExchange}' not found", classId, methodId, ct);
                    return;
                }
                if (!_manager.DeleteExchange(_vhost, deleteExchange, deleteIfUnused))
                {
                    await SendChannelCloseAsync(channelNumber, 406, $"Exchange '{deleteExchange}' cannot be deleted", classId, methodId, ct);
                    return;
                }
                if (!deleteExchangeNoWait)
                    await SendMethodAsync(channelNumber, 40, 21, static _ => { }, ct);
                break;
            case (50, 10): // queue.declare
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                var declareState = _channels.GetOrAdd(channelNumber, _ => new ChannelState());
                string queueName = AmqpWire.ReadShortString(ref payload);
                bool qPassive = ReadBit(ref payload, 0);
                bool qDurable = ReadBit(ref payload, 1);
                bool exclusive = ReadBit(ref payload, 2);
                bool qAutoDelete = ReadBit(ref payload, 3);
                _ = ReadBit(ref payload, 4);
                bool queueDeclareNoWait = ReadBit(ref payload, 5);
                var arguments = AmqpWire.ReadTable(ref payload);
                var queueType = ParseQueueType(GetStringArg(arguments, "x-queue-type"));
                bool generatedQueue = false;
                if (string.IsNullOrEmpty(queueName))
                {
                    if (qPassive)
                    {
                        if (string.IsNullOrEmpty(declareState.CurrentQueueName))
                        {
                            await SendChannelCloseAsync(channelNumber, 404, "No current queue declared on this channel", classId, methodId, ct);
                            return;
                        }

                        queueName = declareState.CurrentQueueName;
                    }
                    else
                    {
                        queueName = $"amq.gen-{Guid.NewGuid():N}";
                        generatedQueue = true;
                    }
                }
                RabbitQueue queue;
                if (qPassive)
                {
                    var existingQueue = _manager.GetQueue(_vhost, queueName);
                    if (existingQueue == null)
                    {
                        await SendChannelCloseAsync(channelNumber, 404, $"NOT_FOUND - no queue '{queueName}' in vhost '{_vhost}'", classId, methodId, ct);
                        return;
                    }
                    if (!existingQueue.IsAccessibleBy(_sessionId))
                    {
                        await SendChannelCloseAsync(channelNumber, 405, ResourceLockedText(queueName, _vhost), classId, methodId, ct);
                        return;
                    }
                    queue = existingQueue;
                }
                else
                {
                    if (!CanConfigure($"queue:{queueName}"))
                    {
                        await SendChannelCloseAsync(channelNumber, 403, $"Access denied to queue '{queueName}'", classId, methodId, ct);
                        return;
                    }
                    var existingQueue = _manager.GetQueue(_vhost, queueName);
                    if (existingQueue != null &&
                        (existingQueue.Type != queueType ||
                         existingQueue.Durable != qDurable ||
                         existingQueue.Exclusive != exclusive ||
                         existingQueue.AutoDelete != qAutoDelete ||
                         existingQueue.MaxPriority != GetByteArg(arguments, "x-max-priority") ||
                         !string.Equals(existingQueue.DeadLetterExchange, GetStringArg(arguments, "x-dead-letter-exchange"), StringComparison.Ordinal) ||
                         !string.Equals(existingQueue.DeadLetterRoutingKey, GetStringArg(arguments, "x-dead-letter-routing-key"), StringComparison.Ordinal) ||
                         existingQueue.MessageTtlMs != GetIntArg(arguments, "x-message-ttl") ||
                         existingQueue.QueueTtlMs != GetIntArg(arguments, "x-expires") ||
                         existingQueue.StreamMaxLengthMessages != GetLongArg(arguments, "x-max-length") ||
                         existingQueue.StreamMaxLengthBytes != GetLongArg(arguments, "x-max-length-bytes") ||
                         existingQueue.StreamMaxAgeMs != ParseDurationMs(GetStringArg(arguments, "x-max-age"))))
                    {
                        await SendChannelCloseAsync(channelNumber, 406, $"Queue '{queueName}' redeclared with different properties", classId, methodId, ct);
                        return;
                    }
                    try
                    {
                        queue = _manager.DeclareQueue(_vhost, queueName, new RabbitQueueArgs
                        {
                            Type = queueType,
                            Durable = qDurable,
                            Exclusive = exclusive,
                            AutoDelete = qAutoDelete,
                            MaxPriority = GetByteArg(arguments, "x-max-priority"),
                            DeadLetterExchange = GetStringArg(arguments, "x-dead-letter-exchange"),
                            DeadLetterRoutingKey = GetStringArg(arguments, "x-dead-letter-routing-key"),
                            MessageTtlMs = GetIntArg(arguments, "x-message-ttl"),
                            QueueTtlMs = GetIntArg(arguments, "x-expires"),
                            StreamMaxLengthMessages = GetLongArg(arguments, "x-max-length"),
                            StreamMaxLengthBytes = GetLongArg(arguments, "x-max-length-bytes"),
                            StreamMaxAgeMs = ParseDurationMs(GetStringArg(arguments, "x-max-age"))
                        }, _sessionId);
                    }
                    catch (InvalidOperationException ex)
                    {
                        await SendChannelCloseAsync(channelNumber, 405, ex.Message, classId, methodId, ct);
                        return;
                    }
                }
                if (generatedQueue)
                    declareState.CurrentQueueName = queue.Name;
                if (!queueDeclareNoWait)
                {
                    await SendMethodAsync(channelNumber, 50, 11, writer =>
                    {
                        AmqpWire.WriteShortString(writer, queue.Name);
                        AmqpWire.WriteUInt32(writer, (uint)queue.Count);
                        AmqpWire.WriteUInt32(writer, (uint)queue.Consumers.Count);
                    }, ct);
                }
                break;
            case (50, 20): // queue.bind
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string bindQueue = ResolveQueueName(channelNumber, AmqpWire.ReadShortString(ref payload));
                if (bindQueue.Length == 0)
                {
                    await SendChannelCloseAsync(channelNumber, 404, "No current queue declared on this channel", classId, methodId, ct);
                    return;
                }
                string bindExchange = AmqpWire.ReadShortString(ref payload);
                string bindRouting = AmqpWire.ReadShortString(ref payload);
                bool queueBindNoWait = ReadBit(ref payload, 0);
                var bindArgs = AmqpWire.ReadTable(ref payload);
                if (string.IsNullOrEmpty(bindExchange))
                {
                    await SendChannelCloseAsync(channelNumber, 403, "ACCESS_REFUSED - operation not permitted on the default exchange", classId, methodId, ct);
                    return;
                }
                if (!CanConfigure($"exchange:{bindExchange}") || !CanConfigure($"queue:{bindQueue}"))
                {
                    await SendChannelCloseAsync(channelNumber, 403, "Access denied to bind queue", classId, methodId, ct);
                    return;
                }
                var bindQueueRef = _manager.GetQueue(_vhost, bindQueue);
                if (!_manager.HasExchange(_vhost, bindExchange) || bindQueueRef == null)
                {
                    await SendChannelCloseAsync(channelNumber, 404, "Queue or exchange not found", classId, methodId, ct);
                    return;
                }
                if (!bindQueueRef.IsAccessibleBy(_sessionId))
                {
                    await SendChannelCloseAsync(channelNumber, 405, ResourceLockedText(bindQueue, _vhost), classId, methodId, ct);
                    return;
                }
                _manager.Bind(_vhost, bindExchange, bindQueue, bindRouting, ToStringDictionary(bindArgs));
                if (!queueBindNoWait)
                    await SendMethodAsync(channelNumber, 50, 21, static _ => { }, ct);
                break;
            case (50, 40): // queue.delete
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string deleteQueue = ResolveQueueName(channelNumber, AmqpWire.ReadShortString(ref payload));
                if (deleteQueue.Length == 0)
                {
                    await SendChannelCloseAsync(channelNumber, 404, "No current queue declared on this channel", classId, methodId, ct);
                    return;
                }
                bool ifUnused = ReadBit(ref payload, 0);
                bool ifEmpty = ReadBit(ref payload, 1);
                bool deleteNoWait = ReadBit(ref payload, 2);
                if (!CanConfigure($"queue:{deleteQueue}"))
                {
                    await SendChannelCloseAsync(channelNumber, 403, $"Access denied to queue '{deleteQueue}'", classId, methodId, ct);
                    return;
                }
                var deleteQueueRef = _manager.GetQueue(_vhost, deleteQueue);
                if (deleteQueueRef == null)
                {
                    await SendChannelCloseAsync(channelNumber, 404, $"Queue '{deleteQueue}' not found", classId, methodId, ct);
                    return;
                }
                if (!deleteQueueRef.IsAccessibleBy(_sessionId))
                {
                    await SendChannelCloseAsync(channelNumber, 405, $"Queue '{deleteQueue}' is locked to another connection", classId, methodId, ct);
                    return;
                }
                bool deleted = _manager.TryDeleteQueue(_vhost, deleteQueue, ifUnused, ifEmpty, out var deletedCount);
                if (!deleted)
                {
                    await SendChannelCloseAsync(channelNumber, 406, $"PRECONDITION_FAILED - queue '{deleteQueue}' in vhost '{_vhost}' in use", classId, methodId, ct);
                    return;
                }
                if (_channels.TryGetValue(channelNumber, out var deleteState) &&
                    string.Equals(deleteState.CurrentQueueName, deleteQueue, StringComparison.Ordinal))
                {
                    deleteState.CurrentQueueName = null;
                }
                if (!deleteNoWait)
                    await SendMethodAsync(channelNumber, 50, 41, writer => AmqpWire.WriteUInt32(writer, deletedCount), ct);
                break;
            case (50, 30): // queue.purge
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string purgeQueue = ResolveQueueName(channelNumber, AmqpWire.ReadShortString(ref payload));
                if (purgeQueue.Length == 0)
                {
                    await SendChannelCloseAsync(channelNumber, 404, "No current queue declared on this channel", classId, methodId, ct);
                    return;
                }
                bool purgeNoWait = ReadBit(ref payload, 0);
                if (!CanConfigure($"queue:{purgeQueue}"))
                {
                    await SendChannelCloseAsync(channelNumber, 403, $"Access denied to queue '{purgeQueue}'", classId, methodId, ct);
                    return;
                }
                var purgeQueueRef = _manager.GetQueue(_vhost, purgeQueue);
                if (purgeQueueRef == null)
                {
                    await SendChannelCloseAsync(channelNumber, 404, $"Queue '{purgeQueue}' not found", classId, methodId, ct);
                    return;
                }
                if (!purgeQueueRef.IsAccessibleBy(_sessionId))
                {
                    await SendChannelCloseAsync(channelNumber, 405, $"Queue '{purgeQueue}' is locked to another connection", classId, methodId, ct);
                    return;
                }
                uint purged = _manager.PurgeQueue(_vhost, purgeQueue);
                if (!purgeNoWait)
                    await SendMethodAsync(channelNumber, 50, 31, writer => AmqpWire.WriteUInt32(writer, purged), ct);
                break;
            case (50, 50): // queue.unbind
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string unbindQueue = ResolveQueueName(channelNumber, AmqpWire.ReadShortString(ref payload));
                if (unbindQueue.Length == 0)
                {
                    await SendChannelCloseAsync(channelNumber, 404, "No current queue declared on this channel", classId, methodId, ct);
                    return;
                }
                string unbindExchange = AmqpWire.ReadShortString(ref payload);
                string unbindRouting = AmqpWire.ReadShortString(ref payload);
                _ = AmqpWire.ReadTable(ref payload);
                if (string.IsNullOrEmpty(unbindExchange))
                {
                    await SendChannelCloseAsync(channelNumber, 403, "ACCESS_REFUSED - operation not permitted on the default exchange", classId, methodId, ct);
                    return;
                }
                if (!CanConfigure($"exchange:{unbindExchange}") || !CanConfigure($"queue:{unbindQueue}"))
                {
                    await SendChannelCloseAsync(channelNumber, 403, "Access denied to unbind queue", classId, methodId, ct);
                    return;
                }
                var unbindQueueRef = _manager.GetQueue(_vhost, unbindQueue);
                if (!_manager.HasExchange(_vhost, unbindExchange) || unbindQueueRef == null)
                {
                    await SendChannelCloseAsync(channelNumber, 404, "Queue or exchange not found", classId, methodId, ct);
                    return;
                }
                if (!unbindQueueRef.IsAccessibleBy(_sessionId))
                {
                    await SendChannelCloseAsync(channelNumber, 405, $"Queue '{unbindQueue}' is locked to another connection", classId, methodId, ct);
                    return;
                }
                _manager.Unbind(_vhost, unbindExchange, unbindQueue, unbindRouting);
                await SendMethodAsync(channelNumber, 50, 51, static _ => { }, ct);
                break;
            case (60, 10): // basic.qos
                _ = AmqpWire.ReadUInt32(payload);
                payload = payload[4..];
                ushort prefetchCount = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                bool global = ReadBit(ref payload, 0);
                var qosState = _channels.GetOrAdd(channelNumber, _ => new ChannelState());
                if (global)
                {
                    qosState.GlobalPrefetchCount = prefetchCount;
                }
                else
                {
                    qosState.ConsumerPrefetchCount = prefetchCount;
                    foreach (var entry in qosState.Consumers)
                        _manager.SetQos(_vhost, entry.Value, entry.Key, prefetchCount);
                }
                await SendMethodAsync(channelNumber, 60, 11, static _ => { }, ct);
                break;
            case (60, 20): // basic.consume
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string consumeQueue = AmqpWire.ReadShortString(ref payload);
                consumeQueue = ResolveQueueName(channelNumber, consumeQueue);
                if (consumeQueue.Length == 0)
                {
                    await SendChannelCloseAsync(channelNumber, 404, "No current queue declared on this channel", classId, methodId, ct);
                    return;
                }
                string consumerTag = AmqpWire.ReadShortString(ref payload);
                bool noLocal = ReadBit(ref payload, 0);
                bool noAck = ReadBit(ref payload, 1);
                bool exclusiveConsume = ReadBit(ref payload, 2);
                bool noWait = ReadBit(ref payload, 3);
                var consumeArgs = AmqpWire.ReadTable(ref payload);
                var streamOffset = ParseStreamOffset(GetStreamOffsetArg(consumeArgs, "x-stream-offset"));

                if (noLocal)
                {
                    await SendChannelCloseAsync(channelNumber, 540, "basic.consume no-local is not supported", classId, methodId, ct);
                    return;
                }

                var state = _channels.GetOrAdd(channelNumber, _ => new ChannelState());
                consumerTag = string.IsNullOrWhiteSpace(consumerTag) ? $"ctag-{Guid.NewGuid():N}" : consumerTag;
                if (state.Consumers.ContainsKey(consumerTag))
                {
                    await SendChannelCloseAsync(channelNumber, 530, $"NOT_ALLOWED - attempt to reuse consumer tag '{consumerTag}'", classId, methodId, ct);
                    return;
                }
                if (!CanRead($"queue:{consumeQueue}"))
                {
                    await SendChannelCloseAsync(channelNumber, 403, $"Access denied to queue '{consumeQueue}'", classId, methodId, ct);
                    return;
                }
                var consumeQueueRef = _manager.GetQueue(_vhost, consumeQueue);
                if (consumeQueueRef == null)
                {
                    await SendChannelCloseAsync(channelNumber, 404, $"NOT_FOUND - no queue '{consumeQueue}' in vhost '{_vhost}'", classId, methodId, ct);
                    return;
                }
                if (!consumeQueueRef.IsAccessibleBy(_sessionId))
                {
                    await SendChannelCloseAsync(channelNumber, 405, ResourceLockedText(consumeQueue, _vhost), classId, methodId, ct);
                    return;
                }

                try
                {
                    _manager.Consume(_vhost, consumeQueue, consumerTag, msg =>
                    {
                        if (!_channels.TryGetValue(channelNumber, out var currentState) || !currentState.FlowActive)
                            return false;

                        if (!noAck &&
                            currentState.GlobalPrefetchCount > 0 &&
                            CountTrackedDeliveries(channelNumber) >= currentState.GlobalPrefetchCount)
                        {
                            return false;
                        }

                        if (!noAck)
                            TrackDelivery(msg.DeliveryTag, channelNumber, consumeQueue, consumerTag);
                        SendBasicDeliverAsync(channelNumber, consumerTag, msg, ct).GetAwaiter().GetResult();
                        return true;
                    }, noAck ? 0 : state.ConsumerPrefetchCount, noAck, _sessionId, (_, tag) =>
                    {
                        if (_channels.TryGetValue(channelNumber, out var activeState))
                            activeState.Consumers.Remove(tag);
                        SendServerBasicCancelAsync(channelNumber, tag, ct).GetAwaiter().GetResult();
                    }, exclusiveConsumer: exclusiveConsume, drainImmediately: false, streamOffset: streamOffset);
                    state.Consumers[consumerTag] = consumeQueue;
                    if (!noWait)
                        await SendMethodAsync(channelNumber, 60, 21, writer => AmqpWire.WriteShortString(writer, consumerTag), ct);
                    _manager.DrainQueue(_vhost, consumeQueue);
                }
                catch (InvalidOperationException ex)
                {
                    await SendChannelCloseAsync(channelNumber, 403, ex.Message, classId, methodId, ct);
                    return;
                }
                break;
            case (60, 30): // basic.cancel
                string cancelTag = AmqpWire.ReadShortString(ref payload);
                bool cancelNoWait = ReadBit(ref payload, 0);
                if (_channels.TryGetValue(channelNumber, out var cancelState) &&
                    cancelState.Consumers.Remove(cancelTag, out var cancelQueue))
                {
                    _manager.CancelConsumer(_vhost, cancelQueue, cancelTag);
                }
                if (!cancelNoWait)
                    await SendMethodAsync(channelNumber, 60, 31, writer => AmqpWire.WriteShortString(writer, cancelTag), ct);
                break;
            case (60, 31): // basic.cancel-ok
                _ = AmqpWire.ReadShortString(ref payload);
                break;
            case (60, 70): // basic.get
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string getQueue = AmqpWire.ReadShortString(ref payload);
                getQueue = ResolveQueueName(channelNumber, getQueue);
                if (getQueue.Length == 0)
                {
                    await SendChannelCloseAsync(channelNumber, 404, "No current queue declared on this channel", classId, methodId, ct);
                    return;
                }
                bool getNoAck = ReadBit(ref payload, 0);
                if (!CanRead($"queue:{getQueue}"))
                {
                    await SendChannelCloseAsync(channelNumber, 403, $"Access denied to queue '{getQueue}'", classId, methodId, ct);
                    return;
                }
                await HandleBasicGetAsync(channelNumber, getQueue, getNoAck, ct);
                break;
            case (60, 40): // basic.publish
                _ = AmqpWire.ReadUInt16(payload);
                payload = payload[2..];
                string exchange = AmqpWire.ReadShortString(ref payload);
                string routingKey = AmqpWire.ReadShortString(ref payload);
                bool mandatory = ReadBit(ref payload, 0);
                _ = ReadBit(ref payload, 1);
                if (!CanWrite($"exchange:{exchange}"))
                {
                    await SendChannelCloseAsync(channelNumber, 403, $"Access denied to exchange '{exchange}'", classId, methodId, ct);
                    return;
                }
                if (!string.IsNullOrEmpty(exchange) &&
                    !_manager.GetExchangeNames(_vhost).Contains(exchange, StringComparer.OrdinalIgnoreCase))
                {
                    await SendChannelCloseAsync(channelNumber, 404, $"Exchange '{exchange}' not found", classId, methodId, ct);
                    return;
                }
                _channels.GetOrAdd(channelNumber, _ => new ChannelState()).PendingPublish = new PendingPublish
                {
                    Exchange = exchange,
                    RoutingKey = routingKey,
                    Mandatory = mandatory
                };
                break;
            case (60, 80): // basic.ack
                ulong ackTag = AmqpWire.ReadUInt64(payload);
                payload = payload[8..];
                bool ackMultiple = ReadBit(ref payload, 0);
                if (!TryResolveTrackedDelivery(channelNumber, ackTag, ackMultiple, out var ackQueue))
                {
                    await SendChannelCloseAsync(channelNumber, 406, $"PRECONDITION_FAILED - unknown delivery tag {ackTag}", classId, methodId, ct);
                    return;
                }

                _manager.Ack(_vhost, ackQueue!, ackTag, ackMultiple);
                ClearTrackedDeliveries(ackQueue!, ackTag, ackMultiple);
                break;
            case (60, 90): // basic.reject
                ulong rejectTag = AmqpWire.ReadUInt64(payload);
                payload = payload[8..];
                bool requeueReject = ReadBit(ref payload, 0);
                if (!TryResolveTrackedDelivery(channelNumber, rejectTag, multiple: false, out var rejectQueue))
                {
                    await SendChannelCloseAsync(channelNumber, 406, $"PRECONDITION_FAILED - unknown delivery tag {rejectTag}", classId, methodId, ct);
                    return;
                }

                _manager.Reject(_vhost, rejectQueue!, rejectTag, requeueReject);
                ClearTrackedDeliveries(rejectQueue!, rejectTag, multiple: false);
                break;
            case (60, 120): // basic.nack
                ulong nackTag = AmqpWire.ReadUInt64(payload);
                payload = payload[8..];
                bool nackMultiple = ReadBit(ref payload, 0);
                bool nackRequeue = ReadBit(ref payload, 1);
                if (!TryResolveTrackedDelivery(channelNumber, nackTag, nackMultiple, out var nackQueue))
                {
                    await SendChannelCloseAsync(channelNumber, 406, $"PRECONDITION_FAILED - unknown delivery tag {nackTag}", classId, methodId, ct);
                    return;
                }

                _manager.Nack(_vhost, nackQueue!, nackTag, nackMultiple, nackRequeue);
                ClearTrackedDeliveries(nackQueue!, nackTag, nackMultiple);
                break;
            case (60, 100): // basic.recover-async
                bool recoverAsyncRequeue = ReadBit(ref payload, 0);
                await HandleBasicRecoverAsync(channelNumber, recoverAsyncRequeue, sendOk: false, ct);
                break;
            case (60, 110): // basic.recover
                bool recoverRequeue = ReadBit(ref payload, 0);
                await HandleBasicRecoverAsync(channelNumber, recoverRequeue, sendOk: true, ct);
                break;
            case (85, 10): // confirm.select
                _ = ReadBit(ref payload, 0);
                var confirmState = _channels.GetOrAdd(channelNumber, _ => new ChannelState());
                if (confirmState.TransactionMode)
                {
                    await SendChannelCloseAsync(channelNumber, 406, "Channel is already in transaction mode", classId, methodId, ct);
                    return;
                }
                confirmState.ConfirmMode = true;
                await SendMethodAsync(channelNumber, 85, 11, static _ => { }, ct);
                break;
            case (90, 10): // tx.select
                var txState = _channels.GetOrAdd(channelNumber, _ => new ChannelState());
                if (txState.ConfirmMode)
                {
                    await SendChannelCloseAsync(channelNumber, 406, "Channel is already in confirm mode", classId, methodId, ct);
                    return;
                }
                txState.TransactionMode = true;
                await SendMethodAsync(channelNumber, 90, 11, static _ => { }, ct);
                break;
            case (90, 20): // tx.commit
                if (!_channels.TryGetValue(channelNumber, out var commitState) || !commitState.TransactionMode)
                {
                    await SendChannelCloseAsync(channelNumber, 406, "PRECONDITION_FAILED - channel is not transactional", classId, methodId, ct);
                    return;
                }

                foreach (var txPublish in commitState.PendingTransactions)
                    await ApplyPublishAsync(channelNumber, commitState, txPublish, ct);
                commitState.PendingTransactions.Clear();
                await SendMethodAsync(channelNumber, 90, 21, static _ => { }, ct);
                break;
            case (90, 30): // tx.rollback
                if (!_channels.TryGetValue(channelNumber, out var rollbackState) || !rollbackState.TransactionMode)
                {
                    await SendChannelCloseAsync(channelNumber, 406, "PRECONDITION_FAILED - channel is not transactional", classId, methodId, ct);
                    return;
                }

                rollbackState.PendingTransactions.Clear();
                await SendMethodAsync(channelNumber, 90, 31, static _ => { }, ct);
                break;
            default:
                if (channelNumber == 0)
                    await SendConnectionCloseAsync(540, $"AMQP method {classId}.{methodId} is not supported", classId, methodId, ct);
                else
                    await SendChannelCloseAsync(channelNumber, 540, $"AMQP method {classId}.{methodId} is not supported", classId, methodId, ct);
                break;
        }
    }

    private async Task HandleHeaderFrameAsync(ushort channelNumber, ReadOnlyMemory<byte> payloadMemory, CancellationToken ct)
    {
        var payload = payloadMemory.Span;
        ushort classId = AmqpWire.ReadUInt16(payload);
        payload = payload[2..];
        _ = AmqpWire.ReadUInt16(payload);
        payload = payload[2..];
        ulong bodySize = AmqpWire.ReadUInt64(payload);
        payload = payload[8..];
        ushort propertyFlags = AmqpWire.ReadUInt16(payload);
        payload = payload[2..];

        if (classId != 60)
        {
            await SendChannelCloseAsync(channelNumber, 540, $"AMQP content class {classId} is not supported", classId, 0, ct);
            return;
        }

        if (!_channels.TryGetValue(channelNumber, out var state) || state.PendingPublish == null)
        {
            await SendChannelCloseAsync(channelNumber, 503, "Received content header without pending basic.publish", classId, 0, ct);
            return;
        }

        state.PendingPublish.BodySize = bodySize;
        state.PendingPublish.Properties = ReadMessageProperties(propertyFlags, ref payload);
    }

    private async Task HandleBodyFrameAsync(ushort channelNumber, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        if (!_channels.TryGetValue(channelNumber, out var state) || state.PendingPublish == null)
        {
            await SendChannelCloseAsync(channelNumber, 503, "Received body frame without pending basic.publish", 60, 40, ct);
            return;
        }

        var pending = state.PendingPublish;
        await pending.Body.WriteAsync(payload, ct);
        if ((ulong)pending.Body.Length < pending.BodySize)
            return;

        state.PendingPublish = null;
        if (state.TransactionMode)
        {
            state.PendingTransactions.Add(pending);
            return;
        }

        await ApplyPublishAsync(channelNumber, state, pending, ct);
    }

    private async Task SendBasicDeliverAsync(ushort channelNumber, string consumerTag, RabbitMessage message, CancellationToken ct)
    {
        await SendMethodAsync(channelNumber, 60, 60, writer =>
        {
            AmqpWire.WriteShortString(writer, consumerTag);
            AmqpWire.WriteUInt64(writer, message.DeliveryTag);
            AmqpWire.WriteByte(writer, (byte)(message.Redelivered ? 1 : 0));
            AmqpWire.WriteShortString(writer, message.OriginalExchange ?? string.Empty);
            AmqpWire.WriteShortString(writer, message.RoutingKey);
        }, ct);

        var header = new BufferBuilder();
        AmqpWire.WriteUInt16(header, 60);
        AmqpWire.WriteUInt16(header, 0);
        AmqpWire.WriteUInt64(header, (ulong)message.Payload.Length);
        WriteMessageProperties(header, message.Properties);
        await WriteFrameAsync(AmqpWire.FrameHeader, channelNumber, header.ToArray(), ct);
        await WriteFrameAsync(AmqpWire.FrameBody, channelNumber, message.Payload, ct);
    }

    private async Task HandleBasicGetAsync(ushort channelNumber, string queueName, bool autoAck, CancellationToken ct)
    {
        var queue = _manager.GetQueue(_vhost, queueName);
        if (queue == null)
        {
            await SendChannelCloseAsync(channelNumber, 404, $"NOT_FOUND - no queue '{queueName}' in vhost '{_vhost}'", 60, 70, ct);
            return;
        }
        if (!queue.IsAccessibleBy(_sessionId))
        {
            await SendChannelCloseAsync(channelNumber, 405, ResourceLockedText(queueName, _vhost), 60, 70, ct);
            return;
        }

        if (queue.Type == RabbitQueueType.Stream)
        {
            await SendChannelCloseAsync(channelNumber, 540, "NOT_IMPLEMENTED - basic.get is not supported for stream queues", 60, 70, ct);
            return;
        }

        if (!queue.TryDequeue(out var message) || message == null)
        {
            await SendMethodAsync(channelNumber, 60, 72, writer =>
            {
                AmqpWire.WriteShortString(writer, string.Empty);
            }, ct);
            return;
        }

        if (!autoAck)
        {
            queue.Unacked[message.DeliveryTag] = (message, "basic.get");
            TrackDelivery(message.DeliveryTag, channelNumber, queueName, "basic.get");
        }
        else
        {
            queue.Unacked[message.DeliveryTag] = (message, "basic.get");
            TrackDelivery(message.DeliveryTag, channelNumber, queueName, "basic.get");
            _manager.Ack(_vhost, queueName, message.DeliveryTag, false);
            ClearTrackedDeliveries(queueName, message.DeliveryTag, multiple: false);
        }

        await SendMethodAsync(channelNumber, 60, 71, writer =>
        {
            AmqpWire.WriteUInt64(writer, message.DeliveryTag);
            AmqpWire.WriteByte(writer, (byte)(message.Redelivered ? 1 : 0));
            AmqpWire.WriteShortString(writer, message.OriginalExchange ?? string.Empty);
            AmqpWire.WriteShortString(writer, message.RoutingKey);
            AmqpWire.WriteUInt32(writer, (uint)queue.Count);
        }, ct);

        var header = new BufferBuilder();
        AmqpWire.WriteUInt16(header, 60);
        AmqpWire.WriteUInt16(header, 0);
        AmqpWire.WriteUInt64(header, (ulong)message.Payload.Length);
        WriteMessageProperties(header, message.Properties);
        await WriteFrameAsync(AmqpWire.FrameHeader, channelNumber, header.ToArray(), ct);
        await WriteFrameAsync(AmqpWire.FrameBody, channelNumber, message.Payload, ct);
    }

    private async Task SendBasicReturnAsync(ushort channelNumber, ushort replyCode, string replyText, PendingPublish pending, CancellationToken ct)
    {
        await SendMethodAsync(channelNumber, 60, 50, writer =>
        {
            AmqpWire.WriteUInt16(writer, replyCode);
            AmqpWire.WriteShortString(writer, replyText);
            AmqpWire.WriteShortString(writer, pending.Exchange);
            AmqpWire.WriteShortString(writer, pending.RoutingKey);
        }, ct);

        var header = new BufferBuilder();
        AmqpWire.WriteUInt16(header, 60);
        AmqpWire.WriteUInt16(header, 0);
        AmqpWire.WriteUInt64(header, (ulong)pending.Body.Length);
        WriteMessageProperties(header, pending.Properties);
        await WriteFrameAsync(AmqpWire.FrameHeader, channelNumber, header.ToArray(), ct);
        await WriteFrameAsync(AmqpWire.FrameBody, channelNumber, pending.Body.ToArray(), ct);
    }

    private async Task HandleBasicRecoverAsync(ushort channelNumber, bool requeue, bool sendOk, CancellationToken ct)
    {
        var recoverable = _deliveryContextByTag
            .Where(x => x.Value.ChannelNumber == channelNumber)
            .OrderBy(x => x.Key)
            .ToList();

        foreach (var entry in recoverable)
        {
            if (!requeue && !string.Equals(entry.Value.ConsumerTag, "basic.get", StringComparison.Ordinal))
            {
                var queue = _manager.GetQueue(_vhost, entry.Value.QueueName);
                if (queue != null &&
                    queue.Unacked.TryGetValue(entry.Key, out var unacked) &&
                    _channels.TryGetValue(channelNumber, out var state) &&
                    state.Consumers.ContainsKey(entry.Value.ConsumerTag))
                {
                    unacked.Msg.Redelivered = true;
                    await SendBasicDeliverAsync(channelNumber, entry.Value.ConsumerTag, unacked.Msg, ct);
                    continue;
                }
            }

            _manager.Nack(_vhost, entry.Value.QueueName, entry.Key, multiple: false, requeue: true);
            ClearTrackedDeliveries(entry.Value.QueueName, entry.Key, multiple: false);
        }

        if (sendOk)
            await SendMethodAsync(channelNumber, 60, 111, static _ => { }, ct);
    }

    private Task SendServerBasicCancelAsync(ushort channelNumber, string consumerTag, CancellationToken ct)
        => SendMethodAsync(channelNumber, 60, 30, writer =>
        {
            AmqpWire.WriteShortString(writer, consumerTag);
            AmqpWire.WriteByte(writer, 0);
        }, ct);

    private async Task ApplyPublishAsync(ushort channelNumber, ChannelState state, PendingPublish pending, CancellationToken ct)
    {
        var stringHeaders = ToStringDictionary(pending.Properties.Headers);
        int routed = _manager.Publish(
            _vhost,
            pending.Exchange,
            pending.RoutingKey,
            pending.Body.ToArray(),
            stringHeaders,
            pending.Properties.Priority ?? 0,
            null,
            pending.Properties.CorrelationId,
            pending.Properties,
            immediatePersist: state.ConfirmMode);

        if (routed == 0 && pending.Mandatory)
            await SendBasicReturnAsync(channelNumber, 312, "NO_ROUTE", pending, ct);

        if (state.ConfirmMode)
        {
            state.PublishSeq++;
            await SendMethodAsync(channelNumber, 60, 80, writer =>
            {
                AmqpWire.WriteUInt64(writer, state.PublishSeq);
                AmqpWire.WriteByte(writer, 0);
            }, ct);
        }
    }

    private async Task SendConnectionStartAsync(CancellationToken ct)
    {
        await SendMethodAsync(0, 10, 10, writer =>
        {
            AmqpWire.WriteByte(writer, 0);
            AmqpWire.WriteByte(writer, 9);
            AmqpWire.WriteTable(writer, new Dictionary<string, object?>
            {
                ["product"] = "CosmoBroker",
                ["version"] = "1.0",
                ["capabilities"] = new Dictionary<string, object?>()
            });
            AmqpWire.WriteLongString(writer, "PLAIN");
            AmqpWire.WriteLongString(writer, "en_US");
        }, ct);
    }

    private async Task AuthenticateAsync(string mechanism, byte[] response, CancellationToken ct)
    {
        if (!string.Equals(mechanism, "PLAIN", StringComparison.Ordinal))
        {
            await SendConnectionCloseAsync(504, $"Unsupported auth mechanism '{mechanism}'", 10, 11, ct);
            return;
        }

        string[] parts = System.Text.Encoding.UTF8.GetString(response).Split('\0');
        string? username = parts.Length >= 2 ? parts[^2] : null;
        string? password = parts.Length >= 1 ? parts[^1] : null;

        if (_authenticator == null)
        {
            if (!string.Equals(username, "guest", StringComparison.Ordinal) ||
                !string.Equals(password, "guest", StringComparison.Ordinal))
            {
                await SendConnectionCloseAsync(403, "Authentication failed", 10, 11, ct);
                return;
            }

            _account ??= new Auth.Account { Name = "global" };
            _user = new Auth.User { Name = username ?? "guest", AccountName = _account.Name };
            return;
        }

        var result = await _authenticator.AuthenticateAsync(new Auth.ConnectOptions
        {
            User = username,
            Pass = password
        });

        if (!result.Success || result.Account == null || result.User == null)
        {
            await SendConnectionCloseAsync(403, result.ErrorMessage ?? "Authentication failed", 10, 11, ct);
            return;
        }

        _account = result.Account;
        _user = result.User;
    }

    private async Task SendConnectionTuneAsync(CancellationToken ct)
    {
        await SendMethodAsync(0, 10, 30, writer =>
        {
            AmqpWire.WriteUInt16(writer, 0);
            AmqpWire.WriteUInt32(writer, 131072);
            AmqpWire.WriteUInt16(writer, 60);
        }, ct);
    }

    private async Task SendMethodAsync(ushort channel, ushort classId, ushort methodId, Action<BufferBuilder> writePayload, CancellationToken ct)
    {
        var payload = new BufferBuilder();
        AmqpWire.WriteUInt16(payload, classId);
        AmqpWire.WriteUInt16(payload, methodId);
        writePayload(payload);
        await WriteFrameAsync(AmqpWire.FrameMethod, channel, payload.ToArray(), ct);
    }

    private async Task SendConnectionCloseAsync(ushort replyCode, string text, ushort classId, ushort methodId, CancellationToken ct)
    {
        await SendMethodAsync(0, 10, 50, writer =>
        {
            AmqpWire.WriteUInt16(writer, replyCode);
            AmqpWire.WriteShortString(writer, text);
            AmqpWire.WriteUInt16(writer, classId);
            AmqpWire.WriteUInt16(writer, methodId);
        }, ct);
        _closed = true;
    }

    private async Task SendChannelCloseAsync(ushort channel, ushort replyCode, string text, ushort classId, ushort methodId, CancellationToken ct)
    {
        await SendMethodAsync(channel, 20, 40, writer =>
        {
            AmqpWire.WriteUInt16(writer, replyCode);
            AmqpWire.WriteShortString(writer, text);
            AmqpWire.WriteUInt16(writer, classId);
            AmqpWire.WriteUInt16(writer, methodId);
        }, ct);
    }

    private async Task WriteFrameAsync(byte type, ushort channel, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        byte[] header = new byte[7];
        header[0] = type;
        BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(1, 2), channel);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(3, 4), (uint)payload.Length);

        await _writeLock.WaitAsync(ct);
        try
        {
            await _stream.WriteAsync(header, ct);
            if (!payload.IsEmpty)
                await _stream.WriteAsync(payload, ct);
            await _stream.WriteAsync(new byte[] { AmqpWire.FrameEnd }, ct);
            await _stream.FlushAsync(ct);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task<bool> ExpectProtocolHeaderAsync(CancellationToken ct)
    {
        byte[] header = new byte[8];
        await ReadExactAsync(header, ct);
        if (header[0] != (byte)'A' || header[1] != (byte)'M' || header[2] != (byte)'Q' || header[3] != (byte)'P' ||
            header[4] != 0 || header[5] != 0 || header[6] != 9 || header[7] != 1)
        {
            _closed = true;
            await _stream.DisposeAsync();
            return false;
        }

        return true;
    }

    private async Task<Frame> ReadFrameAsync(CancellationToken ct)
    {
        byte[] header = new byte[7];
        await ReadExactAsync(header, ct);

        byte type = header[0];
        ushort channel = BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(1, 2));
        int size = checked((int)BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(3, 4)));
        byte[] payload = size == 0 ? Array.Empty<byte>() : new byte[size];
        if (size > 0)
            await ReadExactAsync(payload, ct);

        byte[] end = new byte[1];
        await ReadExactAsync(end, ct);
        if (end[0] != AmqpWire.FrameEnd)
        {
            await SendConnectionCloseAsync(501, "Invalid AMQP frame end marker", 0, 0, ct);
            _closed = true;
            return new Frame(AmqpWire.FrameHeartbeat, 0, Array.Empty<byte>());
        }

        return new Frame(type, channel, payload);
    }

    private async Task ReadExactAsync(byte[] buffer, CancellationToken ct)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await _stream.ReadAsync(buffer.AsMemory(offset), ct);
            if (read == 0)
                throw new IOException("AMQP connection closed.");
            offset += read;
        }
    }

    private static bool ReadBit(ref ReadOnlySpan<byte> buffer, int bit)
    {
        if (bit == 0)
        {
            byte value = buffer[0];
            buffer = buffer[1..];
            _currentBits = value;
            _currentBitIndex = 0;
        }

        bool result = ((_currentBits >> bit) & 0x01) != 0;
        _currentBitIndex = bit;
        return result;
    }

    [ThreadStatic] private static byte _currentBits;
    [ThreadStatic] private static int _currentBitIndex;

    private static byte GetByteArg(Dictionary<string, object?> args, string key)
        => args.TryGetValue(key, out var value) && value is byte b ? b : (byte)0;

    private static string? GetStringArg(Dictionary<string, object?> args, string key)
        => args.TryGetValue(key, out var value) ? value?.ToString() : null;

    private static int? GetIntArg(Dictionary<string, object?> args, string key)
        => args.TryGetValue(key, out var value) && value is int i ? i : null;

    private static long? GetLongArg(Dictionary<string, object?> args, string key)
    {
        if (!args.TryGetValue(key, out var value) || value == null)
            return null;

        return value switch
        {
            byte b => b,
            sbyte sb => sb,
            short s => s,
            ushort us => us,
            int i => i,
            uint ui => ui,
            long l => l,
            ulong ul when ul <= long.MaxValue => (long)ul,
            _ when long.TryParse(value.ToString(), out var parsed) => parsed,
            _ => null
        };
    }

    private static object? GetStreamOffsetArg(Dictionary<string, object?> args, string key)
        => args.TryGetValue(key, out var value) ? value : null;

    private static RabbitQueueType ParseQueueType(string? value)
        => string.Equals(value, "stream", StringComparison.OrdinalIgnoreCase)
            ? RabbitQueueType.Stream
            : RabbitQueueType.Classic;

    private static ExchangeType ParseExchangeType(string? value)
        => string.Equals(value, "x-super-stream", StringComparison.OrdinalIgnoreCase)
            ? ExchangeType.SuperStream
            : Enum.TryParse<ExchangeType>(value, true, out var parsed) ? parsed : ExchangeType.Direct;

    private static RabbitStreamOffsetSpec? ParseStreamOffset(object? value)
    {
        if (value == null)
            return null;

        if (value is byte byteOffset)
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = byteOffset };
        if (value is sbyte sbyteOffset)
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = sbyteOffset };
        if (value is short shortOffset)
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = shortOffset };
        if (value is ushort ushortOffset)
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = ushortOffset };
        if (value is int intOffset)
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = intOffset };
        if (value is uint uintOffset)
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = uintOffset };
        if (value is long longOffset)
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = longOffset };
        if (value is ulong ulongOffset && ulongOffset <= long.MaxValue)
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = (long)ulongOffset };

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
            return null;

        if (long.TryParse(text, out var parsedOffset))
            return new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.Offset, Offset = parsedOffset };

        return text.Trim().ToLowerInvariant() switch
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

    private static Dictionary<string, string>? ToStringDictionary(Dictionary<string, object?>? args)
    {
        if (args == null || args.Count == 0) return null;
        return args.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? string.Empty, StringComparer.OrdinalIgnoreCase);
    }

    private void TrackDelivery(ulong deliveryTag, ushort channelNumber, string queueName, string consumerTag)
    {
        _deliveryQueueByTag[deliveryTag] = queueName;
        _deliveryContextByTag[deliveryTag] = new DeliveryContext(channelNumber, queueName, consumerTag);
    }

    private int CountTrackedDeliveries(ushort channelNumber)
        => _deliveryContextByTag.Values.Count(x => x.ChannelNumber == channelNumber);

    private string ResolveQueueName(ushort channelNumber, string queueName)
    {
        if (!string.IsNullOrEmpty(queueName))
            return queueName;

        return _channels.TryGetValue(channelNumber, out var state)
            ? state.CurrentQueueName ?? string.Empty
            : string.Empty;
    }

    private void ClearTrackedDeliveries(string queueName, ulong deliveryTag, bool multiple)
    {
        IEnumerable<ulong> tags = multiple
            ? _deliveryContextByTag
                .Where(x => string.Equals(x.Value.QueueName, queueName, StringComparison.Ordinal) && x.Key <= deliveryTag)
                .Select(x => x.Key)
                .ToArray()
            : new[] { deliveryTag };

        foreach (var tag in tags)
        {
            _deliveryQueueByTag.TryRemove(tag, out _);
            _deliveryContextByTag.TryRemove(tag, out _);
        }
    }

    private void CleanupChannelState(ushort channelNumber)
    {
        if (!_channels.TryGetValue(channelNumber, out var state))
            return;

        foreach (var (consumerTag, queueName) in state.Consumers.ToArray())
            _manager.CancelConsumer(_vhost, queueName, consumerTag);

        var remaining = _deliveryContextByTag
            .Where(x => x.Value.ChannelNumber == channelNumber)
            .OrderBy(x => x.Key)
            .ToList();

        foreach (var entry in remaining)
        {
            _manager.Nack(_vhost, entry.Value.QueueName, entry.Key, multiple: false, requeue: true);
            ClearTrackedDeliveries(entry.Value.QueueName, entry.Key, multiple: false);
        }
    }

    private bool TryResolveTrackedDelivery(ushort channelNumber, ulong deliveryTag, bool multiple, out string? queueName)
    {
        queueName = null;

        if (!multiple)
        {
            if (_deliveryContextByTag.TryGetValue(deliveryTag, out var exact) &&
                exact.ChannelNumber == channelNumber &&
                _deliveryQueueByTag.TryGetValue(deliveryTag, out var exactQueue))
            {
                queueName = exactQueue;
                return true;
            }

            return false;
        }

        var match = _deliveryContextByTag
            .Where(x => x.Value.ChannelNumber == channelNumber && x.Key <= deliveryTag)
            .OrderBy(x => x.Key)
            .FirstOrDefault();

        if (match.Equals(default(KeyValuePair<ulong, DeliveryContext>)))
            return false;

        return _deliveryQueueByTag.TryGetValue(match.Key, out queueName);
    }

    private static RabbitMessageProperties ReadMessageProperties(ushort flags, ref ReadOnlySpan<byte> payload)
    {
        Dictionary<string, object?>? headers = null;
        string? contentType = null;
        string? contentEncoding = null;
        byte? deliveryMode = null;
        byte? priority = null;
        string? correlationId = null;
        string? replyTo = null;
        string? expiration = null;
        string? messageId = null;
        DateTimeOffset? timestamp = null;
        string? type = null;
        string? userId = null;
        string? appId = null;

        if ((flags & (1 << 15)) != 0) contentType = AmqpWire.ReadShortString(ref payload);
        if ((flags & (1 << 14)) != 0) contentEncoding = AmqpWire.ReadShortString(ref payload);
        if ((flags & (1 << 13)) != 0) headers = AmqpWire.ReadTable(ref payload);
        if ((flags & (1 << 12)) != 0) deliveryMode = AmqpWire.ReadByte(ref payload);
        if ((flags & (1 << 11)) != 0) priority = AmqpWire.ReadByte(ref payload);
        if ((flags & (1 << 10)) != 0) correlationId = AmqpWire.ReadShortString(ref payload);
        if ((flags & (1 << 9)) != 0) replyTo = AmqpWire.ReadShortString(ref payload);
        if ((flags & (1 << 8)) != 0) expiration = AmqpWire.ReadShortString(ref payload);
        if ((flags & (1 << 7)) != 0) messageId = AmqpWire.ReadShortString(ref payload);
        if ((flags & (1 << 6)) != 0) timestamp = DateTimeOffset.FromUnixTimeSeconds((long)AmqpWire.ReadUInt64(payload));
        if ((flags & (1 << 6)) != 0) payload = payload[8..];
        if ((flags & (1 << 5)) != 0) type = AmqpWire.ReadShortString(ref payload);
        if ((flags & (1 << 4)) != 0) userId = AmqpWire.ReadShortString(ref payload);
        if ((flags & (1 << 3)) != 0) appId = AmqpWire.ReadShortString(ref payload);

        return new RabbitMessageProperties
        {
            ContentType = contentType,
            ContentEncoding = contentEncoding,
            Headers = headers,
            DeliveryMode = deliveryMode,
            Priority = priority,
            CorrelationId = correlationId,
            ReplyTo = replyTo,
            Expiration = expiration,
            MessageId = messageId,
            Timestamp = timestamp,
            Type = type,
            UserId = userId,
            AppId = appId
        };
    }

    private static void WriteMessageProperties(BufferBuilder writer, RabbitMessageProperties properties)
    {
        ushort flags = 0;
        if (!string.IsNullOrEmpty(properties.ContentType)) flags |= (1 << 15);
        if (!string.IsNullOrEmpty(properties.ContentEncoding)) flags |= (1 << 14);
        if (properties.Headers is { Count: > 0 }) flags |= (1 << 13);
        if (properties.DeliveryMode.HasValue) flags |= (1 << 12);
        if (properties.Priority.HasValue) flags |= (1 << 11);
        if (!string.IsNullOrEmpty(properties.CorrelationId)) flags |= (1 << 10);
        if (!string.IsNullOrEmpty(properties.ReplyTo)) flags |= (1 << 9);
        if (!string.IsNullOrEmpty(properties.Expiration)) flags |= (1 << 8);
        if (!string.IsNullOrEmpty(properties.MessageId)) flags |= (1 << 7);
        if (properties.Timestamp.HasValue) flags |= (1 << 6);
        if (!string.IsNullOrEmpty(properties.Type)) flags |= (1 << 5);
        if (!string.IsNullOrEmpty(properties.UserId)) flags |= (1 << 4);
        if (!string.IsNullOrEmpty(properties.AppId)) flags |= (1 << 3);

        AmqpWire.WriteUInt16(writer, flags);
        if (!string.IsNullOrEmpty(properties.ContentType)) AmqpWire.WriteShortString(writer, properties.ContentType);
        if (!string.IsNullOrEmpty(properties.ContentEncoding)) AmqpWire.WriteShortString(writer, properties.ContentEncoding);
        if (properties.Headers is { Count: > 0 }) AmqpWire.WriteTable(writer, properties.Headers);
        if (properties.DeliveryMode.HasValue) AmqpWire.WriteByte(writer, properties.DeliveryMode.Value);
        if (properties.Priority.HasValue) AmqpWire.WriteByte(writer, properties.Priority.Value);
        if (!string.IsNullOrEmpty(properties.CorrelationId)) AmqpWire.WriteShortString(writer, properties.CorrelationId);
        if (!string.IsNullOrEmpty(properties.ReplyTo)) AmqpWire.WriteShortString(writer, properties.ReplyTo);
        if (!string.IsNullOrEmpty(properties.Expiration)) AmqpWire.WriteShortString(writer, properties.Expiration);
        if (!string.IsNullOrEmpty(properties.MessageId)) AmqpWire.WriteShortString(writer, properties.MessageId);
        if (properties.Timestamp.HasValue) AmqpWire.WriteUInt64(writer, (ulong)properties.Timestamp.Value.ToUnixTimeSeconds());
        if (!string.IsNullOrEmpty(properties.Type)) AmqpWire.WriteShortString(writer, properties.Type);
        if (!string.IsNullOrEmpty(properties.UserId)) AmqpWire.WriteShortString(writer, properties.UserId);
        if (!string.IsNullOrEmpty(properties.AppId)) AmqpWire.WriteShortString(writer, properties.AppId);
    }

    private bool CanAccessVhost(string vhost) => _account?.CanAccessRabbitVhost(vhost) ?? true;
    private bool CanConfigure(string resource) => _account?.CanRabbitConfigure(_vhost, resource) ?? true;
    private bool CanWrite(string resource) => _account?.CanRabbitWrite(_vhost, resource) ?? true;
    private bool CanRead(string resource) => _account?.CanRabbitRead(_vhost, resource) ?? true;

    private readonly record struct Frame(byte Type, ushort Channel, byte[] Payload);
}
