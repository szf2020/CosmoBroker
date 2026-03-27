using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Xunit;
using StreamClient = global::RabbitMQ.Stream.Client.Client;
using StreamClientParameters = global::RabbitMQ.Stream.Client.ClientParameters;
using StreamDeliver = global::RabbitMQ.Stream.Client.Deliver;
using StreamPublish = global::RabbitMQ.Stream.Client.Publish;
using StreamMessage = global::RabbitMQ.Stream.Client.Message;
using StreamOffsetFirst = global::RabbitMQ.Stream.Client.OffsetTypeFirst;
using StreamSpec = global::RabbitMQ.Stream.Client.StreamSpec;
using StreamSystem = global::RabbitMQ.Stream.Client.StreamSystem;
using StreamSystemConfig = global::RabbitMQ.Stream.Client.StreamSystemConfig;
using StreamConsumer = global::RabbitMQ.Stream.Client.Reliable.Consumer;
using StreamConsumerConfig = global::RabbitMQ.Stream.Client.Reliable.ConsumerConfig;
using StreamProducer = global::RabbitMQ.Stream.Client.Reliable.Producer;
using StreamProducerConfig = global::RabbitMQ.Stream.Client.Reliable.ProducerConfig;

namespace CosmoBroker.Client.Tests;

public sealed class OfficialStreamClientInteropTests : IAsyncDisposable
{
    private const int StreamPort = 5560;
    private const int AmqpPort = 5690;
    private const int MonitorPort = 8260;
    private readonly BrokerServer _server;
    private readonly CancellationTokenSource _cts = new();

    public OfficialStreamClientInteropTests()
    {
        _server = new BrokerServer(port: 0, amqpPort: AmqpPort, streamPort: StreamPort, monitorPort: MonitorPort);
        _ = _server.StartAsync(_cts.Token);
        WaitForPort("127.0.0.1", StreamPort, TimeSpan.FromSeconds(5));
        WaitForPort("127.0.0.1", AmqpPort, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task OfficialStreamClient_ShouldConnectAndPerformBasicStreamOperations()
    {
        var streamName = $"official.client.stream.{Guid.NewGuid():N}";
        var superStreamName = $"official.client.super.{Guid.NewGuid():N}";

        using (var connection = new ConnectionFactory
               {
                   HostName = "127.0.0.1",
                   Port = AmqpPort,
                   UserName = "guest",
                   Password = "guest",
                   VirtualHost = "/"
               }.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.ExchangeDeclare(
                superStreamName,
                "x-super-stream",
                durable: true,
                autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-partitions"] = 2 });
        }

        var client = await StreamClient.Create(new StreamClientParameters
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoint = new IPEndPoint(IPAddress.Loopback, StreamPort)
        });

        try
        {
            var create = await client.CreateStream(streamName, new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(create, "ResponseCode").ToString());

            var stored = await client.StoreOffset("official-consumer", streamName, 7);
            Assert.True(stored);

            var queryOffset = await client.QueryOffset("official-consumer", streamName);
            Assert.Equal("Ok", ReadProperty<object>(queryOffset, "ResponseCode").ToString());
            Assert.Equal<ulong>(7, ReadProperty<ulong>(queryOffset, "Offset"));

            var partitions = await client.QueryPartition(superStreamName);
            Assert.Equal("Ok", ReadProperty<object>(partitions, "ResponseCode").ToString());
            var streams = ReadProperty<IReadOnlyList<string>>(partitions, "Streams");
            Assert.Equal(2, streams.Count);

            var route = await client.QueryRoute(superStreamName, "customer-77");
            Assert.Equal("Ok", ReadProperty<object>(route, "ResponseCode").ToString());
            var routeStreams = ReadProperty<IReadOnlyList<string>>(route, "Streams");
            Assert.Single(routeStreams);
        }
        finally
        {
            await client.Close("test-complete");
        }
    }

    [Fact]
    public async Task OfficialStreamClient_ShouldPublishAndReceiveDeliverFrame()
    {
        var streamName = $"official.client.deliver.{Guid.NewGuid():N}";
        var delivered = new TaskCompletionSource<(byte SubscriptionId, string Body)>(TaskCreationOptions.RunContinuationsAsynchronously);

        var client = await StreamClient.Create(new StreamClientParameters
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoint = new IPEndPoint(IPAddress.Loopback, StreamPort)
        });

        try
        {
            var create = await client.CreateStream(streamName, new Dictionary<string, string>());
            Assert.Equal("Ok", ReadProperty<object>(create, "ResponseCode").ToString());

            var (subscriptionId, subscribe) = await client.Subscribe(
                streamName,
                new StreamOffsetFirst(),
                initialCredit: 10,
                properties: new Dictionary<string, string>(),
                deliverHandler: deliver =>
                {
                    try
                    {
                        delivered.TrySetResult((deliver.SubscriptionId, DecodeFirstChunkBody(deliver)));
                    }
                    catch (Exception ex)
                    {
                        delivered.TrySetException(ex);
                    }
                    return Task.CompletedTask;
                });

            Assert.Equal("Ok", ReadProperty<object>(subscribe, "ResponseCode").ToString());

            var (publisherId, declare) = await client.DeclarePublisher(
                publisherRef: string.Empty,
                stream: streamName,
                confirmCallback: _ => { },
                errorCallback: _ => { });

            Assert.Equal("Ok", ReadProperty<object>(declare, "ResponseCode").ToString());

            var sent = await client.Publish(new StreamPublish(
                publisherId,
                new List<(ulong, StreamMessage)> { (1UL, new StreamMessage(Encoding.UTF8.GetBytes("hello-deliver"))) }));
            Assert.True(sent);

            var frame = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(subscriptionId, frame.SubscriptionId);
            Assert.Equal("hello-deliver", frame.Body);

            await client.Unsubscribe(subscriptionId);
            await client.DeletePublisher(publisherId);
        }
        finally
        {
            await client.Close("test-complete");
        }
    }

    [Fact]
    public async Task OfficialStreamSystem_ShouldProduceAndConsumeSingleMessage()
    {
        var streamName = $"official.system.stream.{Guid.NewGuid():N}";
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var confirmed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var system = await StreamSystem.Create(new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, StreamPort) }
        });

        await system.CreateStream(new StreamSpec(streamName));

        var consumer = await StreamConsumer.Create(new StreamConsumerConfig(system, streamName)
        {
            Reference = "official-system-consumer",
            OffsetSpec = new StreamOffsetFirst(),
            MessageHandler = async (_, _, _, message) =>
            {
                received.TrySetResult("received");
                await Task.CompletedTask;
            }
        });

        var producer = await StreamProducer.Create(new StreamProducerConfig(system, streamName)
        {
            ConfirmationHandler = async confirmation =>
            {
                if (string.Equals(confirmation.Status.ToString(), "Confirmed", StringComparison.Ordinal))
                    confirmed.TrySetResult(true);
                await Task.CompletedTask;
            }
        });

        try
        {
            await producer.Send(new StreamMessage(Encoding.UTF8.GetBytes("hello-stream-system")));

            Assert.True(await confirmed.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal("received", await received.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        }
        finally
        {
            await producer.Close();
            await consumer.Close();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await _server.DisposeAsync();
        _cts.Dispose();
    }

    private static T ReadProperty<T>(object value, string name)
    {
        var property = value.GetType().GetProperty(name, BindingFlags.Instance | BindingFlags.Public);
        Assert.True(
            property != null,
            $"Property '{name}' not found on {value.GetType().FullName}. Available: {string.Join(", ", Array.ConvertAll(value.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public), p => p.Name))}");
        var raw = property!.GetValue(value);
        Assert.NotNull(raw);
        return (T)raw!;
    }

    private static string DecodeFirstChunkBody(StreamDeliver deliver)
    {
        var chunkData = deliver.Chunk.Data.ToArray();
        var messageLength = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(chunkData.AsSpan(0, 4));
        var payloadBytes = chunkData.AsMemory(4, (int)messageLength).ToArray();
        var payload = new ReadOnlySequence<byte>(payloadBytes);
        try
        {
            var message = StreamMessage.From(ref payload, messageLength);
            return Encoding.UTF8.GetString(message.Data.Contents.ToArray());
        }
        catch (Exception ex)
        {
            var preview = payloadBytes.AsSpan(0, Math.Min(payloadBytes.Length, 16)).ToArray();
            throw new Xunit.Sdk.XunitException(
                $"Failed to decode stream payload. length={messageLength}, preview={Convert.ToHexString(preview)}, error={ex.Message}");
        }
    }

    private static void WaitForPort(string host, int port, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var client = new System.Net.Sockets.TcpClient();
                var connect = client.ConnectAsync(host, port);
                if (connect.Wait(TimeSpan.FromMilliseconds(100)) && client.Connected)
                    return;
            }
            catch
            {
            }

            Thread.Sleep(50);
        }

        throw new TimeoutException($"Timed out waiting for {host}:{port}");
    }
}
