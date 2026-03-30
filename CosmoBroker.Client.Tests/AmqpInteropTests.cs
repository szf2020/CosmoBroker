using System.Buffers.Binary;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker.Auth;
using CosmoBroker.Persistence;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace CosmoBroker.Client.Tests;

public class AmqpInteropTests : IAsyncDisposable
{
    private readonly BrokerServer _server;
    private readonly CancellationTokenSource _cts = new();
    private const int BrokerPort = 4265;
    private const int MonitorPort = 8265;
    private const int AmqpPort = 5675;

    public AmqpInteropTests()
    {
        _server = new BrokerServer(port: BrokerPort, amqpPort: AmqpPort, monitorPort: MonitorPort);
        _ = _server.StartAsync(_cts.Token);
        WaitForPort("127.0.0.1", BrokerPort, TimeSpan.FromSeconds(5));
        WaitForPort("127.0.0.1", AmqpPort, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task AmqpClient_ShouldDeclarePublishConsumeAndConfirm()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true,
            RequestedHeartbeat = TimeSpan.FromSeconds(5)
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.test.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare("amqp.test.q", durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind("amqp.test.q", "amqp.test.x", "work");
        channel.ConfirmSelect();

        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            var body = Encoding.UTF8.GetString(ea.Body.ToArray());
            delivered.TrySetResult(body);
            channel.BasicAck(ea.DeliveryTag, multiple: false);
            await Task.CompletedTask;
        };

        channel.BasicConsume("amqp.test.q", autoAck: false, consumer: consumer);

        var bodyBytes = Encoding.UTF8.GetBytes("hello-amqp");
        channel.BasicPublish("amqp.test.x", "work", basicProperties: null, body: bodyBytes);
        Assert.True(channel.WaitForConfirms(TimeSpan.FromSeconds(5)));

        var received = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("hello-amqp", received);
    }

    [Fact]
    public async Task AmqpClient_ShouldRoundTripBasicProperties()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.props.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare("amqp.props.q", durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind("amqp.props.q", "amqp.props.x", "props");

        var delivered = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            delivered.TrySetResult(ea);
            channel.BasicAck(ea.DeliveryTag, false);
            await Task.CompletedTask;
        };

        channel.BasicConsume("amqp.props.q", autoAck: false, consumer: consumer);

        var props = channel.CreateBasicProperties();
        props.ContentType = "application/json";
        props.CorrelationId = "corr-123";
        props.ReplyTo = "reply.queue";
        props.MessageId = "msg-1";
        props.Type = "demo";
        props.AppId = "tests";
        props.Priority = 5;
        props.DeliveryMode = 2;
        props.Headers = new System.Collections.Generic.Dictionary<string, object>
        {
            ["region"] = "sa".ToCharArray().Select(c => (byte)c).ToArray(),
            ["attempts"] = 3,
            ["enabled"] = true,
            ["size"] = 123456789L,
            ["ratio"] = 1.5d,
            ["meta"] = new Dictionary<string, object>
            {
                ["env"] = "test",
                ["build"] = 7
            },
            ["tags"] = new object[] { "alpha", 2, true }
        };

        channel.BasicPublish("amqp.props.x", "props", props, Encoding.UTF8.GetBytes("{\"ok\":true}"));

        var message = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("application/json", message.BasicProperties.ContentType);
        Assert.Equal("corr-123", message.BasicProperties.CorrelationId);
        Assert.Equal("reply.queue", message.BasicProperties.ReplyTo);
        Assert.Equal("msg-1", message.BasicProperties.MessageId);
        Assert.Equal("demo", message.BasicProperties.Type);
        Assert.Equal("tests", message.BasicProperties.AppId);
        Assert.Equal((byte)5, message.BasicProperties.Priority);
        Assert.Equal((byte)2, message.BasicProperties.DeliveryMode);
        Assert.True(message.BasicProperties.Headers.ContainsKey("region"));
        Assert.Equal("sa", Encoding.UTF8.GetString(Assert.IsType<byte[]>(message.BasicProperties.Headers["region"])));
        Assert.Equal(3, Convert.ToInt32(message.BasicProperties.Headers["attempts"]));
        Assert.True(Convert.ToBoolean(message.BasicProperties.Headers["enabled"]));
        Assert.Equal(123456789L, Convert.ToInt64(message.BasicProperties.Headers["size"]));
        Assert.Equal(1.5d, Convert.ToDouble(message.BasicProperties.Headers["ratio"]), 3);

        var meta = Assert.IsAssignableFrom<IDictionary<string, object>>(message.BasicProperties.Headers["meta"]);
        Assert.Equal("test", Encoding.UTF8.GetString(Assert.IsType<byte[]>(meta["env"])));
        Assert.Equal(7, Convert.ToInt32(meta["build"]));

        var tags = Assert.IsAssignableFrom<IEnumerable<object>>(message.BasicProperties.Headers["tags"]).ToArray();
        Assert.Equal("alpha", Encoding.UTF8.GetString(Assert.IsType<byte[]>(tags[0])));
        Assert.Equal(2, Convert.ToInt32(tags[1]));
        Assert.True(Convert.ToBoolean(tags[2]));
    }

    [Fact]
    public async Task AmqpClient_ShouldShareGlobalPrefetchAcrossConsumersOnSameChannel()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.qos.global.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare("amqp.qos.global.q", durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind("amqp.qos.global.q", "amqp.qos.global.x", "qos");
        channel.BasicQos(0, 1, global: true);

        var firstDelivery = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondDelivery = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        int deliveries = 0;

        AsyncEventingBasicConsumer CreateConsumer()
        {
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (_, ea) =>
            {
                int seen = Interlocked.Increment(ref deliveries);
                if (seen == 1)
                    firstDelivery.TrySetResult(ea.DeliveryTag);
                else
                    secondDelivery.TrySetResult(ea.DeliveryTag);
                await Task.CompletedTask;
            };
            return consumer;
        }

        channel.BasicConsume("amqp.qos.global.q", autoAck: false, consumer: CreateConsumer());
        channel.BasicConsume("amqp.qos.global.q", autoAck: false, consumer: CreateConsumer());

        channel.BasicPublish("amqp.qos.global.x", "qos", basicProperties: null, body: Encoding.UTF8.GetBytes("one"));
        channel.BasicPublish("amqp.qos.global.x", "qos", basicProperties: null, body: Encoding.UTF8.GetBytes("two"));

        ulong firstTag = await firstDelivery.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(300);
        Assert.Equal(1, Volatile.Read(ref deliveries));

        channel.BasicAck(firstTag, false);

        ulong secondTag = await secondDelivery.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotEqual(firstTag, secondTag);
        Assert.Equal(2, Volatile.Read(ref deliveries));

        channel.BasicAck(secondTag, false);
    }

    [Fact]
    public async Task AmqpClient_ShouldSupportServerNamedQueueReferencesOnSameChannel()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.servernamed.x", ExchangeType.Direct, durable: true, autoDelete: false);

        var declared = channel.QueueDeclare(string.Empty, durable: false, exclusive: true, autoDelete: true);
        Assert.StartsWith("amq.gen-", declared.QueueName, StringComparison.Ordinal);

        channel.QueueBind(string.Empty, "amqp.servernamed.x", "servernamed");
        channel.BasicPublish("amqp.servernamed.x", "servernamed", basicProperties: null, body: Encoding.UTF8.GetBytes("generated"));

        var fetched = channel.BasicGet(string.Empty, autoAck: false);
        Assert.NotNull(fetched);
        Assert.Equal("generated", Encoding.UTF8.GetString(fetched!.Body.ToArray()));

        channel.BasicAck(fetched.DeliveryTag, false);
        uint purged = channel.QueuePurge(string.Empty);
        Assert.Equal<uint>(0, purged);

        uint deleted = channel.QueueDelete(string.Empty, ifUnused: false, ifEmpty: false);
        Assert.Equal<uint>(0, deleted);
    }

    [Fact]
    public async Task AmqpClient_ShouldAutoDeleteExchangeAfterLastQueueUnbind()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.autodel.exchange.x", ExchangeType.Direct, durable: false, autoDelete: true);
        channel.QueueDeclare("amqp.autodel.exchange.q", durable: false, exclusive: false, autoDelete: true);
        channel.QueueBind("amqp.autodel.exchange.q", "amqp.autodel.exchange.x", "autodel");

        channel.QueueUnbind("amqp.autodel.exchange.q", "amqp.autodel.exchange.x", "autodel", arguments: null);

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.ExchangeDeclarePassive("amqp.autodel.exchange.x"));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("no exchange", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldAllowExclusiveQueueReuseOnSameConnection()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var declareChannel = connection.CreateModel();
        using var secondChannel = connection.CreateModel();

        var declared = declareChannel.QueueDeclare(string.Empty, durable: false, exclusive: true, autoDelete: true);
        secondChannel.QueueDeclarePassive(declared.QueueName);

        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(secondChannel);
        consumer.Received += async (_, ea) =>
        {
            delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
            secondChannel.BasicAck(ea.DeliveryTag, false);
            await Task.CompletedTask;
        };

        secondChannel.BasicConsume(declared.QueueName, autoAck: false, consumer: consumer);
        declareChannel.BasicPublish(string.Empty, declared.QueueName, basicProperties: null, body: Encoding.UTF8.GetBytes("same-connection"));

        var body = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("same-connection", body);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectExclusiveQueueAccessFromDifferentConnection()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var ownerConnection = factory.CreateConnection();
        using var ownerChannel = ownerConnection.CreateModel();
        var declared = ownerChannel.QueueDeclare(string.Empty, durable: false, exclusive: true, autoDelete: true);

        using var otherConnection = factory.CreateConnection();
        using var otherChannel = otherConnection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => otherChannel.QueueDeclarePassive(declared.QueueName));
        Assert.Equal((ushort)405, ex.ShutdownReason.ReplyCode);
        Assert.Contains("locked", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(otherChannel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectPurgingExclusiveQueueFromDifferentConnection()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var ownerConnection = factory.CreateConnection();
        using var ownerChannel = ownerConnection.CreateModel();
        var declared = ownerChannel.QueueDeclare(string.Empty, durable: false, exclusive: true, autoDelete: true);

        using var otherConnection = factory.CreateConnection();
        using var otherChannel = otherConnection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => otherChannel.QueuePurge(declared.QueueName));
        Assert.Equal((ushort)405, ex.ShutdownReason.ReplyCode);
        Assert.Contains("locked", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(otherChannel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldDeleteExclusiveQueueWhenOwningConnectionCloses()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        string queueName = $"amqp.exclusive.lifecycle.{Guid.NewGuid():N}.q";
        using (var ownerConnection = factory.CreateConnection())
        using (var ownerChannel = ownerConnection.CreateModel())
        {
            queueName = ownerChannel.QueueDeclare(queueName, durable: false, exclusive: true, autoDelete: false).QueueName;
        }

        using var otherConnection = factory.CreateConnection();
        using var otherChannel = otherConnection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => otherChannel.QueueDeclarePassive(queueName));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.True(
            ex.ShutdownReason.ReplyText.Contains("no queue", StringComparison.OrdinalIgnoreCase) ||
            ex.ShutdownReason.ReplyText.Contains("not found", StringComparison.OrdinalIgnoreCase));
        Assert.False(otherChannel.IsOpen);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (true)
        {
            using var recreateChannel = otherConnection.CreateModel();
            try
            {
                var recreated = recreateChannel.QueueDeclare(queueName, durable: false, exclusive: false, autoDelete: false);
                Assert.Equal(queueName, recreated.QueueName);
                break;
            }
            catch (OperationInterruptedException redeclareEx) when (
                redeclareEx.ShutdownReason.ReplyCode == 406 &&
                DateTime.UtcNow < deadline)
            {
                await Task.Delay(100);
            }
        }
    }

    [Fact]
    public async Task AmqpClient_ShouldExpireUnusedQueueWithXExpires()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare(
            "amqp.expires.q",
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object> { ["x-expires"] = 200 });

        await Task.Delay(TimeSpan.FromSeconds(7));

        var ex = Assert.Throws<OperationInterruptedException>(() => channel.QueueDeclarePassive("amqp.expires.q"));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("no queue", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldAllowDefaultExchangeDeclareWithMatchingProperties()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare(string.Empty, ExchangeType.Direct, durable: true, autoDelete: false);
        channel.ExchangeDeclarePassive(string.Empty);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectDefaultExchangeRedeclareWithDifferentProperties()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.ExchangeDeclare(string.Empty, ExchangeType.Fanout, durable: true, autoDelete: false));
        Assert.Equal((ushort)406, ex.ShutdownReason.ReplyCode);
        Assert.Contains("different properties", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectDeletingDefaultExchange()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => channel.ExchangeDelete(string.Empty));
        Assert.Equal((ushort)403, ex.ShutdownReason.ReplyCode);
        Assert.Contains("cannot be deleted", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectDeclaringReservedAmqExchangeName()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.ExchangeDeclare("amq.custom", ExchangeType.Direct, durable: false, autoDelete: false));
        Assert.Equal((ushort)403, ex.ShutdownReason.ReplyCode);
        Assert.Contains("reserved", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectBindingQueueToDefaultExchange()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare("amqp.default.bind.q", durable: false, exclusive: false, autoDelete: false);

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.QueueBind("amqp.default.bind.q", string.Empty, "bind"));
        Assert.Equal((ushort)403, ex.ShutdownReason.ReplyCode);
        Assert.Contains("operation not permitted on the default exchange", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectBindingExchangeToDefaultExchange()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.default.bind.src", ExchangeType.Direct, durable: false, autoDelete: false);

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.ExchangeBind("amqp.default.bind.src", string.Empty, "bind"));
        Assert.Equal((ushort)403, ex.ShutdownReason.ReplyCode);
        Assert.Contains("operation not permitted on the default exchange", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectUnbindingQueueFromDefaultExchange()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare("amqp.default.unbind.q", durable: false, exclusive: false, autoDelete: false);

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.QueueUnbind("amqp.default.unbind.q", string.Empty, "unbind", null));
        Assert.Equal((ushort)403, ex.ShutdownReason.ReplyCode);
        Assert.Contains("default exchange", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldEmitBasicReturnForMandatoryUnroutablePublish()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.return.x", ExchangeType.Direct, durable: true, autoDelete: false);

        var returned = new TaskCompletionSource<BasicReturnEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        channel.BasicReturn += (_, ea) => returned.TrySetResult(ea);

        var props = channel.CreateBasicProperties();
        props.CorrelationId = "return-1";
        channel.BasicPublish("amqp.return.x", "missing", mandatory: true, basicProperties: props, body: Encoding.UTF8.GetBytes("unroutable"));

        var result = await returned.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal((ushort)312, result.ReplyCode);
        Assert.Equal("NO_ROUTE", result.ReplyText);
        Assert.Equal("amqp.return.x", result.Exchange);
        Assert.Equal("missing", result.RoutingKey);
        Assert.Equal("return-1", result.BasicProperties.CorrelationId);
        Assert.Equal("unroutable", Encoding.UTF8.GetString(result.Body.ToArray()));
    }

    [Fact]
    public async Task AmqpClient_ShouldUnbindQueueAndStopRouting()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.unbind.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare("amqp.unbind.q", durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind("amqp.unbind.q", "amqp.unbind.x", "unbind");
        channel.QueueUnbind("amqp.unbind.q", "amqp.unbind.x", "unbind", null);

        var returned = new TaskCompletionSource<BasicReturnEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        channel.BasicReturn += (_, ea) => returned.TrySetResult(ea);
        channel.BasicPublish("amqp.unbind.x", "unbind", mandatory: true, basicProperties: null, body: Encoding.UTF8.GetBytes("gone"));

        var result = await returned.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("NO_ROUTE", result.ReplyText);
    }

    [Fact]
    public async Task AmqpClient_ShouldDeleteExchangeAndCloseChannelOnMissingPublish()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.delete.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.ExchangeDelete("amqp.delete.x");

        channel.BasicPublish("amqp.delete.x", "missing", basicProperties: null, body: Encoding.UTF8.GetBytes("boom"));
        await Task.Delay(300);

        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldFailPassiveExchangeDeclareForMissingExchange()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => channel.ExchangeDeclarePassive("amqp.missing.x"));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("no exchange", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldFailPassiveQueueDeclareForMissingQueue()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => channel.QueueDeclarePassive("amqp.missing.q"));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("no queue", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldFailExchangeRedeclareWithDifferentProperties()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.redeclare.x", ExchangeType.Direct, durable: true, autoDelete: false);
        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.ExchangeDeclare("amqp.redeclare.x", ExchangeType.Fanout, durable: true, autoDelete: false));
        Assert.Equal((ushort)406, ex.ShutdownReason.ReplyCode);
        Assert.Contains("different properties", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldFailQueueRedeclareWithDifferentProperties()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare("amqp.redeclare.q", durable: true, exclusive: false, autoDelete: false);
        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.QueueDeclare("amqp.redeclare.q", durable: false, exclusive: false, autoDelete: false));
        Assert.Equal((ushort)406, ex.ShutdownReason.ReplyCode);
        Assert.Contains("different properties", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldFailQueueRedeclareWithDifferentArguments()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var originalArgs = new Dictionary<string, object> { ["x-message-ttl"] = 5000 };
        channel.QueueDeclare("amqp.redeclare.args.q", durable: true, exclusive: false, autoDelete: false, arguments: originalArgs);

        var changedArgs = new Dictionary<string, object> { ["x-message-ttl"] = 1000 };
        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.QueueDeclare("amqp.redeclare.args.q", durable: true, exclusive: false, autoDelete: false, arguments: changedArgs));
        Assert.Equal((ushort)406, ex.ShutdownReason.ReplyCode);
        Assert.Contains("different properties", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldReceiveConsumerCancelledWhenQueueDeleted()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var consumerChannel = connection.CreateModel();
        using var adminChannel = connection.CreateModel();

        consumerChannel.ExchangeDeclare("amqp.cancel.x", ExchangeType.Direct, durable: true, autoDelete: false);
        consumerChannel.QueueDeclare("amqp.cancel.q", durable: true, exclusive: false, autoDelete: false);
        consumerChannel.QueueBind("amqp.cancel.q", "amqp.cancel.x", "cancel");

        var cancelled = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(consumerChannel);
        consumer.ConsumerCancelled += async (_, ea) =>
        {
            cancelled.TrySetResult(ea.ConsumerTags);
            await Task.CompletedTask;
        };

        string consumerTag = consumerChannel.BasicConsume("amqp.cancel.q", autoAck: true, consumer: consumer);
        adminChannel.QueueDelete("amqp.cancel.q", ifUnused: false, ifEmpty: false);

        var tags = await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Contains(consumerTag, tags);
    }

    [Fact]
    public async Task AmqpClient_ShouldRouteAcrossExchangeBindings()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.source.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.ExchangeDeclare("amqp.dest.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare("amqp.exchangebind.q", durable: true, exclusive: false, autoDelete: false);
        channel.ExchangeBind("amqp.dest.x", "amqp.source.x", "chain");
        channel.QueueBind("amqp.exchangebind.q", "amqp.dest.x", "chain");

        channel.BasicPublish("amqp.source.x", "chain", basicProperties: null, body: Encoding.UTF8.GetBytes("through-exchange"));
        var delivered = channel.BasicGet("amqp.exchangebind.q", autoAck: true);
        Assert.NotNull(delivered);
        Assert.Equal("through-exchange", Encoding.UTF8.GetString(delivered!.Body.ToArray()));

        channel.ExchangeUnbind("amqp.dest.x", "amqp.source.x", "chain", null);
        var returned = new TaskCompletionSource<BasicReturnEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        channel.BasicReturn += (_, ea) => returned.TrySetResult(ea);
        channel.BasicPublish("amqp.source.x", "chain", mandatory: true, basicProperties: null, body: Encoding.UTF8.GetBytes("no-route"));

        var result = await returned.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("NO_ROUTE", result.ReplyText);
    }

    [Fact]
    public async Task AmqpClient_ShouldFailQueueDeleteWhenIfUnusedIsSet()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare("amqp.delete.ifunused.q", durable: true, exclusive: false, autoDelete: false);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, _) => await Task.CompletedTask;
        channel.BasicConsume("amqp.delete.ifunused.q", autoAck: true, consumer: consumer);

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.QueueDelete("amqp.delete.ifunused.q", ifUnused: true, ifEmpty: false));
        Assert.Equal((ushort)406, ex.ShutdownReason.ReplyCode);
        Assert.True(
            ex.ShutdownReason.ReplyText.Contains("in use", StringComparison.OrdinalIgnoreCase) ||
            ex.ShutdownReason.ReplyText.Contains("could not", StringComparison.OrdinalIgnoreCase),
            $"Unexpected reply text: {ex.ShutdownReason.ReplyText}");
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldFailExchangeDeleteWhenIfUnusedIsSet()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.delete.ifunused.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare("amqp.delete.ifunused.bound.q", durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind("amqp.delete.ifunused.bound.q", "amqp.delete.ifunused.x", "bound");

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.ExchangeDelete("amqp.delete.ifunused.x", ifUnused: true));
        Assert.Equal((ushort)406, ex.ShutdownReason.ReplyCode);
        Assert.Contains("cannot be deleted", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRecoverUnackedDeliveryAsRedelivered()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var suffix = Guid.NewGuid().ToString("N");
        var exchange = $"amqp.recover.{suffix}.x";
        var queue = $"amqp.recover.{suffix}.q";

        channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind(queue, exchange, "recover");

        var firstDelivery = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        var redelivery = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveries = 0;

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            if (Interlocked.Increment(ref deliveries) == 1)
            {
                firstDelivery.TrySetResult(ea);
            }
            else
            {
                redelivery.TrySetResult(ea);
                channel.BasicAck(ea.DeliveryTag, false);
            }

            await Task.CompletedTask;
        };

        channel.BasicConsume(queue, autoAck: false, consumer: consumer);
        channel.BasicPublish(exchange, "recover", basicProperties: null, body: Encoding.UTF8.GetBytes("recover-me"));

        var first = await firstDelivery.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(first.Redelivered);

        channel.BasicRecover(requeue: true);

        var second = await redelivery.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(second.Redelivered);
        Assert.Equal("recover-me", Encoding.UTF8.GetString(second.Body.ToArray()));
    }

    [Fact]
    public async Task AmqpClient_ShouldRecoverUnackedDeliveryWithoutRequeue()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.recover.direct.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare("amqp.recover.direct.q", durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind("amqp.recover.direct.q", "amqp.recover.direct.x", "recover");

        var firstDelivery = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        var redelivery = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveries = 0;

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            if (Interlocked.Increment(ref deliveries) == 1)
            {
                firstDelivery.TrySetResult(ea);
            }
            else
            {
                redelivery.TrySetResult(ea);
                channel.BasicAck(ea.DeliveryTag, false);
            }

            await Task.CompletedTask;
        };

        channel.BasicConsume("amqp.recover.direct.q", autoAck: false, consumer: consumer);
        channel.BasicPublish("amqp.recover.direct.x", "recover", basicProperties: null, body: Encoding.UTF8.GetBytes("recover-direct"));

        var first = await firstDelivery.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(first.Redelivered);

        channel.BasicRecover(requeue: false);

        var second = await redelivery.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(second.Redelivered);
        Assert.Equal(first.DeliveryTag, second.DeliveryTag);
        Assert.Equal("recover-direct", Encoding.UTF8.GetString(second.Body.ToArray()));
    }

    [Fact]
    public async Task AmqpClient_ShouldSupportChannelFlow()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await stream.WriteAsync(new byte[] { (byte)'A', (byte)'M', (byte)'Q', (byte)'P', 0, 0, 9, 1 });

        var start = await ReadAmqpFrameAsync(stream);
        AssertMethod(start.Payload, 10, 10);

        await WriteMethodFrameAsync(stream, 0, 10, 11, writer =>
        {
            WriteTable(writer, null);
            WriteShortString(writer, "PLAIN");
            var auth = Encoding.UTF8.GetBytes("\0guest\0guest");
            WriteLongString(writer, auth);
            WriteShortString(writer, "en_US");
        });

        var tune = await ReadAmqpFrameAsync(stream);
        AssertMethod(tune.Payload, 10, 30);

        await WriteMethodFrameAsync(stream, 0, 10, 31, writer =>
        {
            WriteUInt16(writer, 0);
            WriteUInt32(writer, 131072);
            WriteUInt16(writer, 0);
        });

        await WriteMethodFrameAsync(stream, 0, 10, 40, writer =>
        {
            WriteShortString(writer, "/");
            WriteShortString(writer, string.Empty);
            writer.WriteByte(0);
        });

        var openOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(openOk.Payload, 10, 41);

        await WriteMethodFrameAsync(stream, 1, 20, 10, writer => WriteShortString(writer, string.Empty));
        var channelOpenOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(channelOpenOk.Payload, 20, 11);

        await WriteMethodFrameAsync(stream, 1, 20, 20, writer => writer.WriteByte(0));
        var flowOffOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(flowOffOk.Payload, 20, 21);
        Assert.False(ReadBoolean(flowOffOk.Payload.AsSpan(4)));

        await WriteMethodFrameAsync(stream, 1, 20, 20, writer => writer.WriteByte(1));
        var flowOnOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(flowOnOk.Payload, 20, 21);
        Assert.True(ReadBoolean(flowOnOk.Payload.AsSpan(4)));
    }

    [Fact]
    public async Task AmqpClient_ShouldAllowClientInitiatedChannelCloseAndReopen()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);
        await OpenRawAmqpChannelAsync(stream, 1);

        await WriteMethodFrameAsync(stream, 1, 20, 40, writer =>
        {
            WriteUInt16(writer, 200);
            WriteShortString(writer, "bye");
            WriteUInt16(writer, 0);
            WriteUInt16(writer, 0);
        });

        var closeOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(closeOk.Payload, 20, 41);

        await OpenRawAmqpChannelAsync(stream, 1);
    }

    [Fact]
    public async Task AmqpClient_ShouldAcknowledgeClientInitiatedConnectionClose()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);

        await WriteMethodFrameAsync(stream, 0, 10, 50, writer =>
        {
            WriteUInt16(writer, 200);
            WriteShortString(writer, "shutdown");
            WriteUInt16(writer, 0);
            WriteUInt16(writer, 0);
        });

        var closeOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(closeOk.Payload, 10, 51);

        await Assert.ThrowsAnyAsync<EndOfStreamException>(async () =>
            await ReadAmqpFrameAsync(stream));
    }

    [Fact]
    public async Task AmqpClient_ShouldPauseAndResumeDeliveriesWithChannelFlow()
    {
        await Task.Delay(250);
        var exchangeName = $"amqp.flow.delivery.{Guid.NewGuid():N}.x";
        var queueName = $"amqp.flow.delivery.{Guid.NewGuid():N}.q";

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using (var setupConnection = factory.CreateConnection())
        using (var setupChannel = setupConnection.CreateModel())
        {
            setupChannel.ExchangeDeclare(exchangeName, ExchangeType.Direct, durable: true, autoDelete: false);
            setupChannel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false);
            setupChannel.QueueBind(queueName, exchangeName, "flow");
        }

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);
        await OpenRawAmqpChannelAsync(stream, 1);

        await WriteMethodFrameAsync(stream, 1, 60, 20, writer =>
        {
            WriteUInt16(writer, 0);
            WriteShortString(writer, queueName);
            WriteShortString(writer, "ctag-flow");
            writer.WriteByte(0b10); // noAck
            WriteTable(writer, null);
        });

        var consumeOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(consumeOk.Payload, 60, 21);

        await WriteMethodFrameAsync(stream, 1, 20, 20, writer => writer.WriteByte(0));
        var flowOffOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(flowOffOk.Payload, 20, 21);
        Assert.False(ReadBoolean(flowOffOk.Payload.AsSpan(4)));

        using (var publishConnection = factory.CreateConnection())
        using (var publishChannel = publishConnection.CreateModel())
        {
            publishChannel.BasicPublish(exchangeName, "flow", basicProperties: null, body: Encoding.UTF8.GetBytes("held-back"));
        }

        var pendingFrame = await ReadAmqpFrameWithTimeoutAsync(stream, TimeSpan.FromMilliseconds(300));
        Assert.Null(pendingFrame);

        await WriteMethodFrameAsync(stream, 1, 20, 20, writer => writer.WriteByte(1));
        var flowOnOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(flowOnOk.Payload, 20, 21);
        Assert.True(ReadBoolean(flowOnOk.Payload.AsSpan(4)));

        var deliver = await ReadAmqpFrameUntilMethodAsync(stream, 60, 60, TimeSpan.FromSeconds(2));
        var header = await ReadAmqpFrameAsync(stream);
        Assert.Equal((byte)2, header.Type);
        var body = await ReadAmqpFrameAsync(stream);
        Assert.Equal((byte)3, body.Type);
        Assert.Equal("held-back", Encoding.UTF8.GetString(body.Payload));
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelOnUnknownAckDeliveryTag()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.BasicAck(999999, false);
        await Task.Delay(300);
        Assert.False(channel.IsOpen);
        Assert.NotNull(channel.CloseReason);
        Assert.Equal((ushort)406, channel.CloseReason!.ReplyCode);
        Assert.Contains("Unknown delivery tag", channel.CloseReason.ReplyText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelOnUnknownRejectDeliveryTag()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.BasicReject(999999, true);
        await Task.Delay(300);
        Assert.False(channel.IsOpen);
        Assert.NotNull(channel.CloseReason);
        Assert.Equal((ushort)406, channel.CloseReason!.ReplyCode);
        Assert.Contains("Unknown delivery tag", channel.CloseReason.ReplyText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelOnUnknownNackDeliveryTag()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.BasicNack(999999, false, true);
        await Task.Delay(300);
        Assert.False(channel.IsOpen);
        Assert.NotNull(channel.CloseReason);
        Assert.Equal((ushort)406, channel.CloseReason!.ReplyCode);
        Assert.Contains("Unknown delivery tag", channel.CloseReason.ReplyText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelOnConsumeNoLocal()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);
        await OpenRawAmqpChannelAsync(stream, 1);

        await WriteMethodFrameAsync(stream, 1, 60, 20, writer =>
        {
            WriteUInt16(writer, 0);
            WriteShortString(writer, "amqp.no-local.q");
            WriteShortString(writer, "ctag-nolocal");
            writer.WriteByte(0b0000_0001); // no-local
            WriteTable(writer, null);
        });

        var close = await ReadAmqpFrameAsync(stream);
        AssertMethod(close.Payload, 20, 40);
        Assert.Equal((ushort)540, BinaryPrimitives.ReadUInt16BigEndian(close.Payload.AsSpan(4, 2)));
        Assert.Contains("no-local", ReadCloseReplyText(close.Payload), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelOnUnsupportedMethod()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);
        await OpenRawAmqpChannelAsync(stream, 1);

        await WriteMethodFrameAsync(stream, 1, 20, 99, static _ => { });

        var close = await ReadAmqpFrameAsync(stream);
        AssertMethod(close.Payload, 20, 40);
        Assert.Equal((ushort)540, BinaryPrimitives.ReadUInt16BigEndian(close.Payload.AsSpan(4, 2)));
        Assert.Contains("not supported", ReadCloseReplyText(close.Payload), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseConnectionOnUnsupportedConnectionMethod()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);

        await WriteMethodFrameAsync(stream, 0, 10, 99, static _ => { });

        var close = await ReadAmqpFrameAsync(stream);
        AssertMethod(close.Payload, 10, 50);
        Assert.Equal((ushort)540, BinaryPrimitives.ReadUInt16BigEndian(close.Payload.AsSpan(4, 2)));
        Assert.Contains("not supported", ReadCloseReplyText(close.Payload), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelOnHeaderWithoutPublish()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);
        await OpenRawAmqpChannelAsync(stream, 1);

        using var header = new MemoryStream();
        WriteUInt16(header, 60);
        WriteUInt16(header, 0);
        WriteUInt64(header, 0);
        WriteUInt16(header, 0);
        await WriteFrameAsync(stream, 2, 1, header.ToArray());

        var close = await ReadAmqpFrameAsync(stream);
        AssertMethod(close.Payload, 20, 40);
        Assert.Equal((ushort)503, BinaryPrimitives.ReadUInt16BigEndian(close.Payload.AsSpan(4, 2)));
        Assert.Contains("content header without pending basic.publish", ReadCloseReplyText(close.Payload), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelOnBodyWithoutPublish()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);
        await OpenRawAmqpChannelAsync(stream, 1);

        await WriteFrameAsync(stream, 3, 1, Encoding.UTF8.GetBytes("orphan-body"));

        var close = await ReadAmqpFrameAsync(stream);
        AssertMethod(close.Payload, 20, 40);
        Assert.Equal((ushort)503, BinaryPrimitives.ReadUInt16BigEndian(close.Payload.AsSpan(4, 2)));
        Assert.Contains("body frame without pending basic.publish", ReadCloseReplyText(close.Payload), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldPurgeQueue()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.purge.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare("amqp.purge.q", durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind("amqp.purge.q", "amqp.purge.x", "purge");

        channel.BasicPublish("amqp.purge.x", "purge", null, Encoding.UTF8.GetBytes("one"));
        channel.BasicPublish("amqp.purge.x", "purge", null, Encoding.UTF8.GetBytes("two"));

        var purged = channel.QueuePurge("amqp.purge.q");
        Assert.Equal((uint)2, purged);
        Assert.Null(channel.BasicGet("amqp.purge.q", autoAck: true));
    }

    [Fact]
    public async Task AmqpClient_ShouldReturnDeletedMessageCount()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.delete.count.x", ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare("amqp.delete.count.q", durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind("amqp.delete.count.q", "amqp.delete.count.x", "delete");

        channel.BasicPublish("amqp.delete.count.x", "delete", null, Encoding.UTF8.GetBytes("one"));
        channel.BasicPublish("amqp.delete.count.x", "delete", null, Encoding.UTF8.GetBytes("two"));

        var deleted = channel.QueueDelete("amqp.delete.count.q", ifUnused: false, ifEmpty: false);
        Assert.Equal((uint)2, deleted);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelOnPurgeMissingQueue()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => channel.QueuePurge("amqp.missing.purge.q"));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("not found", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldDisconnectOnInvalidProtocolHeader()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await stream.WriteAsync(new byte[] { (byte)'B', (byte)'A', (byte)'D', (byte)'!', 0, 0, 9, 1 });
        await stream.FlushAsync();

        var data = await ReadAtMostAsync(stream, 1, TimeSpan.FromMilliseconds(300));
        Assert.Empty(data);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseConnectionOnUnsupportedFrameType()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);

        await WriteFrameAsync(stream, 7, 0, Array.Empty<byte>());

        var close = await ReadAmqpFrameAsync(stream);
        AssertMethod(close.Payload, 10, 50);
        Assert.Equal((ushort)540, BinaryPrimitives.ReadUInt16BigEndian(close.Payload.AsSpan(4, 2)));
        Assert.Contains("Unsupported AMQP frame type", ReadCloseReplyText(close.Payload), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldNotReplyToQueueBindWhenNoWaitIsSet()
    {
        await Task.Delay(250);

        using var setupClient = new TcpClient();
        await setupClient.ConnectAsync("127.0.0.1", AmqpPort);
        await using (var setupStream = setupClient.GetStream())
        {
            await OpenRawAmqpConnectionAsync(setupStream);
            await OpenRawAmqpChannelAsync(setupStream, 1);

            await WriteMethodFrameAsync(setupStream, 1, 40, 10, writer =>
            {
                WriteUInt16(writer, 0);
                WriteShortString(writer, "amqp.nowait.bind.x");
                WriteShortString(writer, "direct");
                writer.WriteByte(0b0000_0010); // durable
                WriteTable(writer, null);
            });
            AssertMethod((await ReadAmqpFrameAsync(setupStream)).Payload, 40, 11);

            await WriteMethodFrameAsync(setupStream, 1, 50, 10, writer =>
            {
                WriteUInt16(writer, 0);
                WriteShortString(writer, "amqp.nowait.bind.q");
                writer.WriteByte(0b0000_0010); // durable
                WriteTable(writer, null);
            });
            AssertMethod((await ReadAmqpFrameAsync(setupStream)).Payload, 50, 11);

            await WriteMethodFrameAsync(setupStream, 1, 50, 20, writer =>
            {
                WriteUInt16(writer, 0);
                WriteShortString(writer, "amqp.nowait.bind.q");
                WriteShortString(writer, "amqp.nowait.bind.x");
                WriteShortString(writer, "bind");
                writer.WriteByte(0b0000_0001); // no-wait
                WriteTable(writer, null);
            });

            var response = await ReadAmqpFrameWithTimeoutAsync(setupStream, TimeSpan.FromMilliseconds(300));
            Assert.Null(response);
        }
    }

    [Fact]
    public async Task AmqpClient_ShouldNotReplyToExchangeDeleteWhenNoWaitIsSet()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);
        await OpenRawAmqpChannelAsync(stream, 1);

        await WriteMethodFrameAsync(stream, 1, 40, 10, writer =>
        {
            WriteUInt16(writer, 0);
            WriteShortString(writer, "amqp.nowait.delete.x");
            WriteShortString(writer, "direct");
            writer.WriteByte(0b0000_0010); // durable
            WriteTable(writer, null);
        });
        AssertMethod((await ReadAmqpFrameAsync(stream)).Payload, 40, 11);

        await WriteMethodFrameAsync(stream, 1, 40, 20, writer =>
        {
            WriteUInt16(writer, 0);
            WriteShortString(writer, "amqp.nowait.delete.x");
            writer.WriteByte(0b0000_0010); // no-wait only
        });

        var response = await ReadAmqpFrameWithTimeoutAsync(stream, TimeSpan.FromMilliseconds(300));
        Assert.Null(response);
    }

    [Fact]
    public async Task AmqpClient_ShouldNotReplyToExchangeDeclareWhenNoWaitIsSet()
    {
        await Task.Delay(250);
        var exchangeName = $"amqp.nowait.declare.{Guid.NewGuid():N}.x";

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);
        await OpenRawAmqpChannelAsync(stream, 1);

        await WriteMethodFrameAsync(stream, 1, 40, 10, writer =>
        {
            WriteUInt16(writer, 0);
            WriteShortString(writer, exchangeName);
            WriteShortString(writer, "direct");
            writer.WriteByte(0b0010_0010); // durable + no-wait
            WriteTable(writer, null);
        });

        var response = await ReadAmqpFrameWithTimeoutAsync(stream, TimeSpan.FromMilliseconds(300));
        Assert.Null(response);

        await AssertExchangeExistsEventuallyAsync(exchangeName);
    }

    [Fact]
    public async Task AmqpClient_ShouldNotReplyToQueueDeclareWhenNoWaitIsSet()
    {
        await Task.Delay(250);
        var queueName = $"amqp.nowait.declare.{Guid.NewGuid():N}.q";

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await OpenRawAmqpConnectionAsync(stream);
        await OpenRawAmqpChannelAsync(stream, 1);

        await WriteMethodFrameAsync(stream, 1, 50, 10, writer =>
        {
            WriteUInt16(writer, 0);
            WriteShortString(writer, queueName);
            writer.WriteByte(0b0010_0010); // durable + no-wait
            WriteTable(writer, null);
        });

        var response = await ReadAmqpFrameWithTimeoutAsync(stream, TimeSpan.FromMilliseconds(300));
        Assert.Null(response);

        await AssertQueueExistsEventuallyAsync(queueName);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelWhenQueueBindTargetsMissingResource()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.bind.exists.x", ExchangeType.Direct, durable: true, autoDelete: false);
        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.QueueBind("amqp.bind.missing.q", "amqp.bind.exists.x", "bind"));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("not found", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelWhenExchangeBindTargetsMissingResource()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.exchange.bind.exists.x", ExchangeType.Direct, durable: true, autoDelete: false);
        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.ExchangeBind("amqp.exchange.bind.missing.x", "amqp.exchange.bind.exists.x", "bind"));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("not found", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelWhenQueueUnbindTargetsMissingResource()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.unbind.exists.x", ExchangeType.Direct, durable: true, autoDelete: false);
        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.QueueUnbind("amqp.unbind.missing.q", "amqp.unbind.exists.x", "unbind", null));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("not found", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelWhenExchangeDeleteTargetIsMissing()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => channel.ExchangeDelete("amqp.delete.missing.x"));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("not found", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelWhenQueueDeleteTargetIsMissing()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => channel.QueueDelete("amqp.delete.missing.q", ifUnused: false, ifEmpty: false));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("not found", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectSecondConsumerWhenExclusiveConsumerExists()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var firstChannel = connection.CreateModel();
        using var secondChannel = connection.CreateModel();

        firstChannel.ExchangeDeclare("amqp.consume.exclusive.x", ExchangeType.Direct, durable: true, autoDelete: false);
        firstChannel.QueueDeclare("amqp.consume.exclusive.q", durable: true, exclusive: false, autoDelete: false);
        firstChannel.QueueBind("amqp.consume.exclusive.q", "amqp.consume.exclusive.x", "exclusive");

        var firstConsumer = new AsyncEventingBasicConsumer(firstChannel);
        firstConsumer.Received += async (_, _) => await Task.CompletedTask;
        firstChannel.BasicConsume("amqp.consume.exclusive.q", autoAck: true, consumerTag: "exclusive-1", noLocal: false, exclusive: true, arguments: null, consumer: firstConsumer);

        var secondConsumer = new AsyncEventingBasicConsumer(secondChannel);
        secondConsumer.Received += async (_, _) => await Task.CompletedTask;

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            secondChannel.BasicConsume("amqp.consume.exclusive.q", autoAck: true, consumerTag: "exclusive-2", noLocal: false, exclusive: false, arguments: null, consumer: secondConsumer));
        Assert.Equal((ushort)403, ex.ShutdownReason.ReplyCode);
        Assert.Contains("exclusive consumer", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(secondChannel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldAllowNewConsumerAfterExclusiveConsumerCancels()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var firstChannel = connection.CreateModel();
        using var secondChannel = connection.CreateModel();

        firstChannel.ExchangeDeclare("amqp.consume.exclusive.release.x", ExchangeType.Direct, durable: true, autoDelete: false);
        firstChannel.QueueDeclare("amqp.consume.exclusive.release.q", durable: true, exclusive: false, autoDelete: false);
        firstChannel.QueueBind("amqp.consume.exclusive.release.q", "amqp.consume.exclusive.release.x", "exclusive");

        var firstConsumer = new AsyncEventingBasicConsumer(firstChannel);
        firstConsumer.Received += async (_, _) => await Task.CompletedTask;
        firstChannel.BasicConsume("amqp.consume.exclusive.release.q", autoAck: true, consumerTag: "exclusive-release-1", noLocal: false, exclusive: true, arguments: null, consumer: firstConsumer);
        firstChannel.Close();
        await Task.Delay(200);

        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondConsumer = new AsyncEventingBasicConsumer(secondChannel);
        secondConsumer.Received += async (_, ea) =>
        {
            delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
            await Task.CompletedTask;
        };

        secondChannel.BasicConsume("amqp.consume.exclusive.release.q", autoAck: true, consumerTag: "exclusive-release-2", noLocal: false, exclusive: false, arguments: null, consumer: secondConsumer);
        secondChannel.BasicPublish("amqp.consume.exclusive.release.x", "exclusive", basicProperties: null, body: Encoding.UTF8.GetBytes("after-cancel"));

        Assert.Equal("after-cancel", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.True(secondChannel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRequeueBasicGetDeliveryWhenChannelCloses()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var firstChannel = connection.CreateModel();
        using var secondChannel = connection.CreateModel();

        firstChannel.ExchangeDeclare("amqp.get.close.x", ExchangeType.Direct, durable: true, autoDelete: false);
        firstChannel.QueueDeclare("amqp.get.close.q", durable: true, exclusive: false, autoDelete: false);
        firstChannel.QueueBind("amqp.get.close.q", "amqp.get.close.x", "getclose");
        firstChannel.BasicPublish("amqp.get.close.x", "getclose", basicProperties: null, body: Encoding.UTF8.GetBytes("close-requeue"));

        var firstGet = firstChannel.BasicGet("amqp.get.close.q", autoAck: false);
        Assert.NotNull(firstGet);
        Assert.Equal("close-requeue", Encoding.UTF8.GetString(firstGet!.Body.ToArray()));

        firstChannel.Close();

        var secondGet = secondChannel.BasicGet("amqp.get.close.q", autoAck: true);
        Assert.NotNull(secondGet);
        Assert.True(secondGet!.Redelivered);
        Assert.Equal("close-requeue", Encoding.UTF8.GetString(secondGet.Body.ToArray()));
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseChannelWhenConsumeQueueIsMissing()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, _) => await Task.CompletedTask;

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.BasicConsume("amqp.consume.missing.q", autoAck: true, consumer: consumer));
        Assert.Equal((ushort)404, ex.ShutdownReason.ReplyCode);
        Assert.Contains("no queue", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldCommitTransactionalPublish()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        var exchangeName = $"amqp.tx.{Guid.NewGuid():N}.x";
        var queueName = $"amqp.tx.{Guid.NewGuid():N}.q";

        channel.ExchangeDeclare(exchangeName, ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind(queueName, exchangeName, "tx");
        channel.TxSelect();

        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
            channel.BasicAck(ea.DeliveryTag, false);
            await Task.CompletedTask;
        };
        channel.BasicConsume(queueName, autoAck: false, consumer: consumer);

        channel.BasicPublish(exchangeName, "tx", basicProperties: null, body: Encoding.UTF8.GetBytes("commit-me"));
        await Task.Delay(200);
        Assert.False(delivered.Task.IsCompleted);

        channel.TxCommit();
        Assert.Equal("commit-me", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AmqpClient_ShouldRollbackTransactionalPublish()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        var exchangeName = $"amqp.tx.rollback.{Guid.NewGuid():N}.x";
        var queueName = $"amqp.tx.rollback.{Guid.NewGuid():N}.q";

        channel.ExchangeDeclare(exchangeName, ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind(queueName, exchangeName, "tx");
        channel.TxSelect();

        channel.BasicPublish(exchangeName, "tx", basicProperties: null, body: Encoding.UTF8.GetBytes("rollback-me"));
        channel.TxRollback();

        var result = channel.BasicGet(queueName, autoAck: true);
        Assert.Null(result);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectTxCommitOutsideTransactionMode()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => channel.TxCommit());
        Assert.Equal((ushort)406, ex.ShutdownReason.ReplyCode);
        Assert.Contains("not transactional", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectTxRollbackOutsideTransactionMode()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var ex = Assert.Throws<OperationInterruptedException>(() => channel.TxRollback());
        Assert.Equal((ushort)406, ex.ShutdownReason.ReplyCode);
        Assert.Contains("not transactional", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectTxSelectAfterConfirmSelect()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ConfirmSelect();
        var ex = Assert.Throws<OperationInterruptedException>(() => channel.TxSelect());
        Assert.Equal((ushort)406, ex.ShutdownReason.ReplyCode);
        Assert.Contains("confirm mode", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectConfirmSelectAfterTxSelect()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.TxSelect();
        var ex = Assert.Throws<OperationInterruptedException>(() => channel.ConfirmSelect());
        Assert.Equal((ushort)406, ex.ShutdownReason.ReplyCode);
        Assert.Contains("transaction mode", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task AmqpClient_ShouldRejectDuplicateConsumerTagOnSameChannel()
    {
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare("amqp.consume.duptag.x", ExchangeType.Direct, durable: false, autoDelete: false);
        channel.QueueDeclare("amqp.consume.duptag.q1", durable: false, exclusive: false, autoDelete: false);
        channel.QueueDeclare("amqp.consume.duptag.q2", durable: false, exclusive: false, autoDelete: false);
        channel.QueueBind("amqp.consume.duptag.q1", "amqp.consume.duptag.x", "q1");
        channel.QueueBind("amqp.consume.duptag.q2", "amqp.consume.duptag.x", "q2");

        var firstConsumer = new AsyncEventingBasicConsumer(channel);
        firstConsumer.Received += async (_, _) => await Task.CompletedTask;
        channel.BasicConsume("amqp.consume.duptag.q1", autoAck: true, consumerTag: "dup-tag", noLocal: false, exclusive: false, arguments: null, consumer: firstConsumer);

        var secondConsumer = new AsyncEventingBasicConsumer(channel);
        secondConsumer.Received += async (_, _) => await Task.CompletedTask;

        var ex = Assert.Throws<OperationInterruptedException>(() =>
            channel.BasicConsume("amqp.consume.duptag.q2", autoAck: true, consumerTag: "dup-tag", noLocal: false, exclusive: false, arguments: null, consumer: secondConsumer));
        Assert.Equal((ushort)530, ex.ShutdownReason.ReplyCode);
        Assert.Contains("attempt to reuse consumer tag", ex.ShutdownReason.ReplyText, StringComparison.OrdinalIgnoreCase);
        Assert.False(channel.IsOpen);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await _server.DisposeAsync();
    }

    private static void WaitForPort(string host, int port, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        Exception? lastError = null;

        while (DateTime.UtcNow < deadline)
        {
            using var client = new TcpClient();
            try
            {
                client.ConnectAsync(host, port).GetAwaiter().GetResult();
                return;
            }
            catch (Exception ex)
            {
                lastError = ex;
                Thread.Sleep(50);
            }
        }

        throw new InvalidOperationException($"Timed out waiting for {host}:{port} to accept connections.", lastError);
    }

    private static async Task WriteMethodFrameAsync(Stream stream, ushort channel, ushort classId, ushort methodId, Action<MemoryStream> bodyWriter)
    {
        using var body = new MemoryStream();
        WriteUInt16(body, classId);
        WriteUInt16(body, methodId);
        bodyWriter(body);
        await WriteFrameAsync(stream, 1, channel, body.ToArray());
    }

    private static async Task WriteFrameAsync(Stream stream, byte type, ushort channel, byte[] payload)
    {
        var header = new byte[7];
        header[0] = type;
        BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(1, 2), channel);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(3, 4), (uint)payload.Length);
        await stream.WriteAsync(header);
        await stream.WriteAsync(payload);
        await stream.WriteAsync(new byte[] { 0xCE });
        await stream.FlushAsync();
    }

    private async Task AssertExchangeExistsEventuallyAsync(string exchangeName, TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(2);
        var deadline = DateTime.UtcNow + timeout.Value;
        Exception? lastError = null;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var verifyConnection = CreateConnectionFactory().CreateConnection();
                using var verifyChannel = verifyConnection.CreateModel();
                verifyChannel.ExchangeDeclarePassive(exchangeName);
                return;
            }
            catch (Exception ex)
            {
                lastError = ex;
                await Task.Delay(50);
            }
        }

        throw new Xunit.Sdk.XunitException($"Exchange '{exchangeName}' did not become visible within {timeout.Value.TotalMilliseconds}ms. Last error: {lastError?.Message}");
    }

    private async Task AssertQueueExistsEventuallyAsync(string queueName, TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(2);
        var deadline = DateTime.UtcNow + timeout.Value;
        Exception? lastError = null;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var verifyConnection = CreateConnectionFactory().CreateConnection();
                using var verifyChannel = verifyConnection.CreateModel();
                verifyChannel.QueueDeclarePassive(queueName);
                return;
            }
            catch (Exception ex)
            {
                lastError = ex;
                await Task.Delay(50);
            }
        }

        throw new Xunit.Sdk.XunitException($"Queue '{queueName}' did not become visible within {timeout.Value.TotalMilliseconds}ms. Last error: {lastError?.Message}");
    }

    private ConnectionFactory CreateConnectionFactory()
        => new()
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

    private static async Task<(byte Type, ushort Channel, byte[] Payload)> ReadAmqpFrameAsync(Stream stream)
    {
        var header = await ReadExactAsync(stream, 7);
        var type = header[0];
        var channel = BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(1, 2));
        var size = BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(3, 4));
        var payload = await ReadExactAsync(stream, (int)size);
        var end = await ReadExactAsync(stream, 1);
        Assert.Equal(0xCE, end[0]);
        return (type, channel, payload);
    }

    private static async Task<(byte Type, ushort Channel, byte[] Payload)?> ReadAmqpFrameWithTimeoutAsync(Stream stream, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            var header = await ReadExactAsync(stream, 7, cts.Token);
            var type = header[0];
            var channel = BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(1, 2));
            var size = BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(3, 4));
            var payload = await ReadExactAsync(stream, (int)size, cts.Token);
            var end = await ReadExactAsync(stream, 1, cts.Token);
            Assert.Equal(0xCE, end[0]);
            return (type, channel, payload);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    private static async Task<(byte Type, ushort Channel, byte[] Payload)> ReadAmqpFrameUntilMethodAsync(Stream stream, ushort expectedClassId, ushort expectedMethodId, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
                throw new TimeoutException($"Timed out waiting for AMQP method {expectedClassId}.{expectedMethodId}.");

            var frame = await ReadAmqpFrameWithTimeoutAsync(stream, remaining);
            if (frame == null || frame.Value.Type != 1 || frame.Value.Payload.Length < 4)
                continue;

            var payload = frame.Value.Payload.AsSpan();
            if (BinaryPrimitives.ReadUInt16BigEndian(payload) == expectedClassId &&
                BinaryPrimitives.ReadUInt16BigEndian(payload[2..]) == expectedMethodId)
            {
                return frame.Value;
            }
        }
    }

    private static async Task<byte[]> ReadExactAsync(Stream stream, int count, CancellationToken cancellationToken = default)
    {
        var buffer = new byte[count];
        var offset = 0;
        while (offset < count)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(offset, count - offset), cancellationToken);
            if (read == 0)
                throw new EndOfStreamException();
            offset += read;
        }
        return buffer;
    }

    private static async Task<byte[]> ReadAtMostAsync(Stream stream, int count, TimeSpan timeout)
    {
        var buffer = new byte[count];
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            var read = await stream.ReadAsync(buffer.AsMemory(0, count), cts.Token);
            return buffer[..read];
        }
        catch (OperationCanceledException)
        {
            return Array.Empty<byte>();
        }
    }

    private static void AssertMethod(byte[] payload, ushort expectedClassId, ushort expectedMethodId)
    {
        Assert.True(payload.Length >= 4);
        Assert.Equal(expectedClassId, BinaryPrimitives.ReadUInt16BigEndian(payload.AsSpan(0, 2)));
        Assert.Equal(expectedMethodId, BinaryPrimitives.ReadUInt16BigEndian(payload.AsSpan(2, 2)));
    }

    private static string ReadCloseReplyText(byte[] payload)
    {
        Assert.True(payload.Length >= 7);
        var length = payload[6];
        Assert.True(payload.Length >= 7 + length);
        return Encoding.UTF8.GetString(payload, 7, length);
    }

    private static void WriteUInt16(Stream stream, ushort value)
    {
        Span<byte> buffer = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteUInt32(Stream stream, uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteUInt64(Stream stream, ulong value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteShortString(Stream stream, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        stream.WriteByte((byte)bytes.Length);
        stream.Write(bytes);
    }

    private static void WriteLongString(Stream stream, byte[] value)
    {
        WriteUInt32(stream, (uint)value.Length);
        stream.Write(value);
    }

    private static void WriteTable(Stream stream, Dictionary<string, object?>? table)
    {
        if (table == null || table.Count == 0)
        {
            WriteUInt32(stream, 0);
            return;
        }

        throw new NotSupportedException("Non-empty AMQP tables are not needed in this test.");
    }

    private static bool ReadBoolean(ReadOnlySpan<byte> payload)
        => payload.Length > 0 && payload[0] != 0;

    private static async Task OpenRawAmqpConnectionAsync(Stream stream)
    {
        await stream.WriteAsync(new byte[] { (byte)'A', (byte)'M', (byte)'Q', (byte)'P', 0, 0, 9, 1 });

        var start = await ReadAmqpFrameAsync(stream);
        AssertMethod(start.Payload, 10, 10);

        await WriteMethodFrameAsync(stream, 0, 10, 11, writer =>
        {
            WriteTable(writer, null);
            WriteShortString(writer, "PLAIN");
            var auth = Encoding.UTF8.GetBytes("\0guest\0guest");
            WriteLongString(writer, auth);
            WriteShortString(writer, "en_US");
        });

        var tune = await ReadAmqpFrameAsync(stream);
        AssertMethod(tune.Payload, 10, 30);

        await WriteMethodFrameAsync(stream, 0, 10, 31, writer =>
        {
            WriteUInt16(writer, 0);
            WriteUInt32(writer, 131072);
            WriteUInt16(writer, 0);
        });

        await WriteMethodFrameAsync(stream, 0, 10, 40, writer =>
        {
            WriteShortString(writer, "/");
            WriteShortString(writer, string.Empty);
            writer.WriteByte(0);
        });

        var openOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(openOk.Payload, 10, 41);
    }

    private static async Task OpenRawAmqpChannelAsync(Stream stream, ushort channel)
    {
        await WriteMethodFrameAsync(stream, channel, 20, 10, writer => WriteShortString(writer, string.Empty));
        var channelOpenOk = await ReadAmqpFrameAsync(stream);
        AssertMethod(channelOpenOk.Payload, 20, 11);
    }

    [Fact]
    public async Task AmqpClient_ShouldEnforceStreamMaxLengthBytes()
    {
        const int port = 5683;
        const int brokerPort = 4273;
        const int monitorPort = 8273;

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var setupChannel = connection.CreateModel();

        setupChannel.ExchangeDeclare("amqp.stream.length.x", ExchangeType.Direct, durable: true, autoDelete: false);
        setupChannel.QueueDeclare(
            "amqp.stream.length.q",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "stream", ["x-max-length-bytes"] = 6L });
        setupChannel.QueueBind("amqp.stream.length.q", "amqp.stream.length.x", "stream");
        setupChannel.BasicPublish("amqp.stream.length.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("1111"));
        setupChannel.BasicPublish("amqp.stream.length.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("2222"));

        using (var monitor = new HttpClient())
        {
            var rmqz = await monitor.GetStringAsync($"http://127.0.0.1:{monitorPort}/rmqz");
            using var doc = JsonDocument.Parse(rmqz);
            var queue = doc.RootElement.GetProperty("queues")
                .EnumerateArray()
                .FirstOrDefault(x => x.TryGetProperty("name", out var name) && name.GetString() == "amqp.stream.length.q");
            Assert.True(queue.ValueKind != JsonValueKind.Undefined, rmqz);
            Assert.Equal("stream", queue.GetProperty("queue_type").GetString());
            Assert.Equal(6L, queue.GetProperty("stream_max_length_bytes").GetInt64());
        }

        using var channel = connection.CreateModel();
        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
            await Task.CompletedTask;
        };

        channel.BasicConsume(
            "amqp.stream.length.q",
            autoAck: true,
            consumerTag: "stream-length",
            noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
            consumer: consumer);

        Assert.Equal("2222", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AmqpClient_ShouldEnforceStreamMaxLengthMessages()
    {
        const int port = 5688;
        const int brokerPort = 4278;
        const int monitorPort = 8278;

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var setupChannel = connection.CreateModel();

        setupChannel.ExchangeDeclare("amqp.stream.lengthcount.x", ExchangeType.Direct, durable: true, autoDelete: false);
        setupChannel.QueueDeclare(
            "amqp.stream.lengthcount.q",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-queue-type"] = "stream",
                ["x-max-length"] = 1L
            });
        setupChannel.QueueBind("amqp.stream.lengthcount.q", "amqp.stream.lengthcount.x", "stream");
        setupChannel.BasicPublish("amqp.stream.lengthcount.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("1111"));
        setupChannel.BasicPublish("amqp.stream.lengthcount.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("2222"));

        using (var monitor = new HttpClient())
        {
            var rmqz = await monitor.GetStringAsync($"http://127.0.0.1:{monitorPort}/rmqz");
            using var doc = JsonDocument.Parse(rmqz);
            var queue = doc.RootElement.GetProperty("queues")
                .EnumerateArray()
                .FirstOrDefault(x => x.TryGetProperty("name", out var name) && name.GetString() == "amqp.stream.lengthcount.q");
            Assert.True(queue.ValueKind != JsonValueKind.Undefined, rmqz);
            Assert.Equal("stream", queue.GetProperty("queue_type").GetString());
            Assert.Equal(1L, queue.GetProperty("stream_max_length_messages").GetInt64());
        }

        using var channel = connection.CreateModel();
        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
            await Task.CompletedTask;
        };

        channel.BasicConsume(
            "amqp.stream.lengthcount.q",
            autoAck: true,
            consumerTag: "stream-lengthcount",
            noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
            consumer: consumer);

        Assert.Equal("2222", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AmqpClient_ShouldEnforceStreamMaxAge()
    {
        const int port = 5684;
        const int brokerPort = 4274;
        const int monitorPort = 8274;

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var setupChannel = connection.CreateModel();

        setupChannel.ExchangeDeclare("amqp.stream.age.x", ExchangeType.Direct, durable: true, autoDelete: false);
        setupChannel.QueueDeclare(
            "amqp.stream.age.q",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "stream", ["x-max-age"] = "200ms" });
        setupChannel.QueueBind("amqp.stream.age.q", "amqp.stream.age.x", "stream");
        setupChannel.BasicPublish("amqp.stream.age.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("old"));
        await Task.Delay(300);
        setupChannel.BasicPublish("amqp.stream.age.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("new"));

        using (var monitor = new HttpClient())
        {
            var rmqz = await monitor.GetStringAsync($"http://127.0.0.1:{monitorPort}/rmqz");
            using var doc = JsonDocument.Parse(rmqz);
            var queue = doc.RootElement.GetProperty("queues")
                .EnumerateArray()
                .FirstOrDefault(x => x.TryGetProperty("name", out var name) && name.GetString() == "amqp.stream.age.q");
            Assert.True(queue.ValueKind != JsonValueKind.Undefined, rmqz);
            Assert.Equal("stream", queue.GetProperty("queue_type").GetString());
            Assert.Equal(200L, queue.GetProperty("stream_max_age_ms").GetInt64());
        }

        using var channel = connection.CreateModel();
        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
            await Task.CompletedTask;
        };

        channel.BasicConsume(
            "amqp.stream.age.q",
            autoAck: true,
            consumerTag: "stream-age",
            noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
            consumer: consumer);

        Assert.Equal("new", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AmqpClient_ShouldDeclareSuperStreamAndRouteByPartition()
    {
        int port = GetFreePort();
        int brokerPort = GetFreePort();
        int monitorPort = GetFreePort();

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare(
            "amqp.super.x",
            "x-super-stream",
            durable: true,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-partitions"] = 2L });

        using (var monitor = new HttpClient())
        {
            var rmqz = await monitor.GetStringAsync($"http://127.0.0.1:{monitorPort}/rmqz");
            using var doc = JsonDocument.Parse(rmqz);
            var exchanges = doc.RootElement.GetProperty("exchanges").EnumerateArray().ToArray();
            var queues = doc.RootElement.GetProperty("queues").EnumerateArray().ToArray();
            var superExchange = exchanges.Single(e => e.GetProperty("name").GetString() == "amqp.super.x");
            Assert.Equal("SuperStream", superExchange.GetProperty("type").GetString());
            Assert.Equal(2, superExchange.GetProperty("super_stream_partition_count").GetInt32());
            var partitions = superExchange.GetProperty("super_stream_partitions").EnumerateArray().Select(x => x.GetString()).ToArray();
            Assert.Equal(new[] { "amqp.super.x-0", "amqp.super.x-1" }, partitions);
            Assert.Contains(queues, q => q.GetProperty("name").GetString() == "amqp.super.x-0");
            Assert.Contains(queues, q => q.GetProperty("name").GetString() == "amqp.super.x-1");
            Assert.All(
                queues.Where(q => q.GetProperty("name").GetString() is "amqp.super.x-0" or "amqp.super.x-1"),
                q => Assert.Equal("stream", q.GetProperty("queue_type").GetString()));
        }

        channel.BasicPublish("amqp.super.x", "customer-42", basicProperties: null, body: Encoding.UTF8.GetBytes("p1"));
        channel.BasicPublish("amqp.super.x", "customer-42", basicProperties: null, body: Encoding.UTF8.GetBytes("p2"));

        var q0Messages = new List<string>();
        var q1Messages = new List<string>();
        var q0Done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var q1Done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var q0Channel = connection.CreateModel();
        using var q1Channel = connection.CreateModel();

        var q0Consumer = new AsyncEventingBasicConsumer(q0Channel);
        q0Consumer.Received += async (_, ea) =>
        {
            q0Messages.Add(Encoding.UTF8.GetString(ea.Body.ToArray()));
            if (q0Messages.Count >= 2)
                q0Done.TrySetResult();
            await Task.CompletedTask;
        };

        var q1Consumer = new AsyncEventingBasicConsumer(q1Channel);
        q1Consumer.Received += async (_, ea) =>
        {
            q1Messages.Add(Encoding.UTF8.GetString(ea.Body.ToArray()));
            if (q1Messages.Count >= 2)
                q1Done.TrySetResult();
            await Task.CompletedTask;
        };

        q0Channel.BasicConsume(
            "amqp.super.x-0",
            autoAck: true,
            consumerTag: "super-0",
            noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
            consumer: q0Consumer);

        q1Channel.BasicConsume(
            "amqp.super.x-1",
            autoAck: true,
            consumerTag: "super-1",
            noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
            consumer: q1Consumer);

        await Task.WhenAny(Task.WhenAll(q0Done.Task, q1Done.Task), Task.Delay(500));

        bool queue0ReceivedBoth = q0Messages.Count == 2 && q1Messages.Count == 0;
        bool queue1ReceivedBoth = q1Messages.Count == 2 && q0Messages.Count == 0;

        Assert.True(queue0ReceivedBoth || queue1ReceivedBoth, "Expected both messages to hash to the same super-stream partition.");
    }

    [Fact]
    public async Task AmqpMonitor_ShouldExposeStreamQueueMetadataAndOffsets()
    {
        const int port = 5685;
        const int brokerPort = 4275;
        const int monitorPort = 8275;

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var setupChannel = connection.CreateModel();

        setupChannel.ExchangeDeclare("amqp.stream.monitor.x", ExchangeType.Direct, durable: true, autoDelete: false);
        setupChannel.QueueDeclare(
            "amqp.stream.monitor.q",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-queue-type"] = "stream",
                ["x-max-length-bytes"] = 1024L,
                ["x-max-age"] = "2s"
            });
        setupChannel.QueueBind("amqp.stream.monitor.q", "amqp.stream.monitor.x", "stream");
        setupChannel.BasicPublish("amqp.stream.monitor.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("stream-monitor-1"));

        using var channel = connection.CreateModel();
        var delivered = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            channel.BasicAck(ea.DeliveryTag, false);
            delivered.TrySetResult(ea.DeliveryTag);
            await Task.CompletedTask;
        };

        channel.BasicConsume(
            "amqp.stream.monitor.q",
            autoAck: false,
            consumerTag: "stream-monitor-consumer",
            noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
            consumer: consumer);

        await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(150);

        using var monitor = new HttpClient();
        var rmqz = await monitor.GetStringAsync($"http://127.0.0.1:{monitorPort}/rmqz");
        using var doc = JsonDocument.Parse(rmqz);
        var queue = doc.RootElement.GetProperty("queues")
            .EnumerateArray()
            .FirstOrDefault(x => x.TryGetProperty("name", out var name) && name.GetString() == "amqp.stream.monitor.q");

        Assert.True(queue.ValueKind != JsonValueKind.Undefined, rmqz);
        Assert.Equal("stream", queue.GetProperty("queue_type").GetString());
        Assert.Equal(1024L, queue.GetProperty("stream_max_length_bytes").GetInt64());
        Assert.Equal(2000L, queue.GetProperty("stream_max_age_ms").GetInt64());
        Assert.True(queue.GetProperty("bytes").GetInt64() > 0);
        Assert.Equal(1L, queue.GetProperty("stream_head_offset").GetInt64());
        Assert.Equal(1L, queue.GetProperty("stream_tail_offset").GetInt64());

        var offsets = queue.GetProperty("stream_offsets");
        Assert.True(offsets.TryGetProperty("stream-monitor-consumer", out var nextOffset), rmqz);
        Assert.True(nextOffset.GetInt64() >= 2L, rmqz);

        var lag = queue.GetProperty("stream_consumer_lag");
        Assert.True(lag.TryGetProperty("stream-monitor-consumer", out var consumerLag), rmqz);
        Assert.True(consumerLag.GetInt64() >= 0L, rmqz);
    }

    private static int GetFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        try { return ((System.Net.IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }
}

public class AmqpAuthInteropTests : IAsyncDisposable
{
    private readonly BrokerServer _server;
    private readonly MessageRepository _repo;
    private readonly CancellationTokenSource _cts = new();
    private readonly string _dbPath;
    private const int BrokerPort = 4266;
    private const int MonitorPort = 8266;
    private const int AmqpPort = 5676;

    public AmqpAuthInteropTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"cosmobroker-amqp-auth-{Guid.NewGuid():N}.db");
        _repo = new MessageRepository($"Data Source={_dbPath};");
        _repo.InitializeAsync().GetAwaiter().GetResult();
        _repo.AddUserAsync("alice", "secret").GetAwaiter().GetResult();
        _repo.SaveRabbitPermissionsAsync(
            "alice",
            "tenant-a",
            new[] { "tenant-a" },
            new[] { "exchange:*", "queue:*" },
            new[] { "exchange:*" },
            new[] { "queue:*" }).GetAwaiter().GetResult();

        _server = new BrokerServer(port: BrokerPort, amqpPort: AmqpPort, repo: _repo, authenticator: new SqlAuthenticator(_repo), monitorPort: MonitorPort);
        _ = _server.StartAsync(_cts.Token);
        WaitForPort("127.0.0.1", BrokerPort, TimeSpan.FromSeconds(5));
        WaitForPort("127.0.0.1", AmqpPort, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task AmqpClient_ShouldRespectSqlAuthAndVhostPermissions()
    {
        await Task.Delay(250);

        var allowedFactory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "alice",
            Password = "secret",
            VirtualHost = "tenant-a",
            DispatchConsumersAsync = true
        };

        using (var connection = allowedFactory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.ExchangeDeclare("amqp.auth.x", ExchangeType.Direct, durable: true, autoDelete: false);
            channel.QueueDeclare("amqp.auth.q", durable: true, exclusive: false, autoDelete: false);
            channel.QueueBind("amqp.auth.q", "amqp.auth.x", "auth");

            var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (_, ea) =>
            {
                delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
                channel.BasicAck(ea.DeliveryTag, false);
                await Task.CompletedTask;
            };

            channel.BasicConsume("amqp.auth.q", false, consumer);
            channel.BasicPublish("amqp.auth.x", "auth", null, Encoding.UTF8.GetBytes("secure"));
            Assert.Equal("secure", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        }

        var deniedFactory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = AmqpPort,
            UserName = "alice",
            Password = "secret",
            VirtualHost = "tenant-b"
        };

        Assert.ThrowsAny<Exception>(() => deniedFactory.CreateConnection());
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseConnectionOnUnsupportedAuthMechanism()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await stream.WriteAsync(new byte[] { (byte)'A', (byte)'M', (byte)'Q', (byte)'P', 0, 0, 9, 1 });

        var start = await ReadAmqpFrameAsync(stream);
        AssertMethod(start.Payload, 10, 10);

        await WriteMethodFrameAsync(stream, 0, 10, 11, writer =>
        {
            WriteTable(writer, null);
            WriteShortString(writer, "CRAM-MD5");
            WriteLongString(writer, Encoding.UTF8.GetBytes("ignored"));
            WriteShortString(writer, "en_US");
        });

        var close = await ReadAmqpFrameAsync(stream);
        AssertMethod(close.Payload, 10, 50);
        Assert.Equal((ushort)504, BinaryPrimitives.ReadUInt16BigEndian(close.Payload.AsSpan(4, 2)));
        Assert.Contains("Unsupported auth mechanism", ReadCloseReplyText(close.Payload), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldCloseConnectionWhenVhostIsDenied()
    {
        await Task.Delay(250);

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", AmqpPort);
        await using var stream = client.GetStream();

        await stream.WriteAsync(new byte[] { (byte)'A', (byte)'M', (byte)'Q', (byte)'P', 0, 0, 9, 1 });

        var start = await ReadAmqpFrameAsync(stream);
        AssertMethod(start.Payload, 10, 10);

        await WriteMethodFrameAsync(stream, 0, 10, 11, writer =>
        {
            WriteTable(writer, null);
            WriteShortString(writer, "PLAIN");
            WriteLongString(writer, Encoding.UTF8.GetBytes("\0alice\0secret"));
            WriteShortString(writer, "en_US");
        });

        var tune = await ReadAmqpFrameAsync(stream);
        AssertMethod(tune.Payload, 10, 30);

        await WriteMethodFrameAsync(stream, 0, 10, 31, writer =>
        {
            WriteUInt16(writer, 0);
            WriteUInt32(writer, 131072);
            WriteUInt16(writer, 0);
        });

        await WriteMethodFrameAsync(stream, 0, 10, 40, writer =>
        {
            WriteShortString(writer, "tenant-b");
            WriteShortString(writer, string.Empty);
            writer.WriteByte(0);
        });

        var close = await ReadAmqpFrameAsync(stream);
        AssertMethod(close.Payload, 10, 50);
        Assert.Equal((ushort)403, BinaryPrimitives.ReadUInt16BigEndian(close.Payload.AsSpan(4, 2)));
        Assert.Contains("Access denied to vhost", ReadCloseReplyText(close.Payload), StringComparison.OrdinalIgnoreCase);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await _server.DisposeAsync();
        try { File.Delete(_dbPath); } catch { }
    }

    private static void WaitForPort(string host, int port, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        Exception? lastError = null;

        while (DateTime.UtcNow < deadline)
        {
            using var client = new TcpClient();
            try
            {
                client.ConnectAsync(host, port).GetAwaiter().GetResult();
                return;
            }
            catch (Exception ex)
            {
                lastError = ex;
                Thread.Sleep(50);
            }
        }

        throw new InvalidOperationException($"Timed out waiting for {host}:{port} to accept connections.", lastError);
    }

    private static async Task WriteMethodFrameAsync(Stream stream, ushort channel, ushort classId, ushort methodId, Action<MemoryStream> bodyWriter)
    {
        using var body = new MemoryStream();
        WriteUInt16(body, classId);
        WriteUInt16(body, methodId);
        bodyWriter(body);
        await WriteFrameAsync(stream, 1, channel, body.ToArray());
    }

    private static async Task WriteFrameAsync(Stream stream, byte type, ushort channel, byte[] payload)
    {
        var header = new byte[7];
        header[0] = type;
        BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(1, 2), channel);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(3, 4), (uint)payload.Length);
        await stream.WriteAsync(header);
        await stream.WriteAsync(payload);
        await stream.WriteAsync(new byte[] { 0xCE });
        await stream.FlushAsync();
    }

    private static async Task<(byte Type, ushort Channel, byte[] Payload)> ReadAmqpFrameAsync(Stream stream)
    {
        var header = await ReadExactAsync(stream, 7);
        var type = header[0];
        var channel = BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(1, 2));
        var size = BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(3, 4));
        var payload = await ReadExactAsync(stream, (int)size);
        var end = await ReadExactAsync(stream, 1);
        Assert.Equal(0xCE, end[0]);
        return (type, channel, payload);
    }

    private static async Task<byte[]> ReadExactAsync(Stream stream, int count, CancellationToken cancellationToken = default)
    {
        var buffer = new byte[count];
        var offset = 0;
        while (offset < count)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(offset, count - offset), cancellationToken);
            if (read == 0)
                throw new EndOfStreamException();
            offset += read;
        }

        return buffer;
    }

    private static void AssertMethod(byte[] payload, ushort expectedClassId, ushort expectedMethodId)
    {
        Assert.True(payload.Length >= 4);
        Assert.Equal(expectedClassId, BinaryPrimitives.ReadUInt16BigEndian(payload.AsSpan(0, 2)));
        Assert.Equal(expectedMethodId, BinaryPrimitives.ReadUInt16BigEndian(payload.AsSpan(2, 2)));
    }

    private static string ReadCloseReplyText(byte[] payload)
    {
        Assert.True(payload.Length >= 7);
        var length = payload[6];
        Assert.True(payload.Length >= 7 + length);
        return Encoding.UTF8.GetString(payload, 7, length);
    }

    private static void WriteUInt16(Stream stream, ushort value)
    {
        Span<byte> buffer = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteUInt32(Stream stream, uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteLongString(Stream stream, byte[] value)
    {
        WriteUInt32(stream, (uint)value.Length);
        stream.Write(value);
    }

    private static void WriteShortString(Stream stream, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        stream.WriteByte((byte)bytes.Length);
        stream.Write(bytes);
    }

    private static void WriteTable(Stream stream, Dictionary<string, object?>? table)
    {
        if (table == null || table.Count == 0)
        {
            WriteUInt32(stream, 0);
            return;
        }

        throw new NotSupportedException("Non-empty AMQP tables are not needed in this test.");
    }
}

public class AmqpPersistenceInteropTests
{
    [Fact]
    public async Task AmqpDurableMessageProperties_ShouldSurviveRestart()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"cosmobroker-amqp-persist-{Guid.NewGuid():N}.db");
        string connectionString = $"Data Source={dbPath};";
        const int port1 = 4267;
        const int mon1 = 8267;
        const int amqp1 = 5677;

        var repo1 = new MessageRepository(connectionString);
        await using (var server1 = new BrokerServer(port: port1, amqpPort: amqp1, repo: repo1, monitorPort: mon1))
        {
            using var cts = new CancellationTokenSource();
            await server1.StartAsync(cts.Token);
            await Task.Delay(250);

            var factory = new ConnectionFactory
            {
                HostName = "127.0.0.1",
                Port = amqp1,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/"
            };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.ExchangeDeclare("amqp.persist.x", ExchangeType.Direct, durable: true, autoDelete: false);
            channel.QueueDeclare("amqp.persist.q", durable: true, exclusive: false, autoDelete: false);
            channel.QueueBind("amqp.persist.q", "amqp.persist.x", "persist");

            var props = channel.CreateBasicProperties();
            props.ContentType = "application/json";
            props.CorrelationId = "persist-corr";
            props.MessageId = "persist-msg";
            props.AppId = "persist-tests";
            props.DeliveryMode = 2;
            props.Headers = new System.Collections.Generic.Dictionary<string, object>
            {
                ["tenant"] = "alpha".ToCharArray().Select(c => (byte)c).ToArray()
            };

            channel.BasicPublish("amqp.persist.x", "persist", props, Encoding.UTF8.GetBytes("{\"persist\":true}"));
            await Task.Delay(150);
        }

        const int port2 = 4268;
        const int mon2 = 8268;
        const int amqp2 = 5678;

        var repo2 = new MessageRepository(connectionString);
        await using (var server2 = new BrokerServer(port: port2, amqpPort: amqp2, repo: repo2, monitorPort: mon2))
        {
            using var cts = new CancellationTokenSource();
            await server2.StartAsync(cts.Token);
            await Task.Delay(250);

            var factory = new ConnectionFactory
            {
                HostName = "127.0.0.1",
                Port = amqp2,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/",
                DispatchConsumersAsync = true
            };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            var delivered = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (_, ea) =>
            {
                delivered.TrySetResult(ea);
                channel.BasicAck(ea.DeliveryTag, false);
                await Task.CompletedTask;
            };

            channel.BasicConsume("amqp.persist.q", autoAck: false, consumer: consumer);
            var message = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal("application/json", message.BasicProperties.ContentType);
            Assert.Equal("persist-corr", message.BasicProperties.CorrelationId);
            Assert.Equal("persist-msg", message.BasicProperties.MessageId);
            Assert.Equal("persist-tests", message.BasicProperties.AppId);
            Assert.Equal((byte)2, message.BasicProperties.DeliveryMode);
            Assert.True(message.BasicProperties.Headers.ContainsKey("tenant"));
            Assert.Equal("{\"persist\":true}", Encoding.UTF8.GetString(message.Body.ToArray()));
        }

        try { File.Delete(dbPath); } catch { }
    }

    [Fact]
    public async Task AmqpClient_ShouldReplayStreamQueueFromFirstOffset()
    {
        const int port = 5679;
        const int brokerPort = 4269;
        const int monitorPort = 8269;

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var setupChannel = connection.CreateModel();

        setupChannel.ExchangeDeclare("amqp.stream.replay.x", ExchangeType.Direct, durable: true, autoDelete: false);
        setupChannel.QueueDeclare(
            "amqp.stream.replay.q",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "stream" });
        setupChannel.QueueBind("amqp.stream.replay.q", "amqp.stream.replay.x", "stream");
        setupChannel.BasicPublish("amqp.stream.replay.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("stream-1"));

        using var firstChannel = connection.CreateModel();
        using var secondChannel = connection.CreateModel();

        var firstDelivery = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondDelivery = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        var firstConsumer = new AsyncEventingBasicConsumer(firstChannel);
        firstConsumer.Received += async (_, ea) =>
        {
            firstDelivery.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
            await Task.CompletedTask;
        };

        var secondConsumer = new AsyncEventingBasicConsumer(secondChannel);
        secondConsumer.Received += async (_, ea) =>
        {
            secondDelivery.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
            await Task.CompletedTask;
        };

        var consumeArgs = new Dictionary<string, object?> { ["x-stream-offset"] = "first" };
        firstChannel.BasicConsume("amqp.stream.replay.q", autoAck: true, consumerTag: "stream-first-1", noLocal: false, exclusive: false, arguments: consumeArgs, consumer: firstConsumer);
        secondChannel.BasicConsume("amqp.stream.replay.q", autoAck: true, consumerTag: "stream-first-2", noLocal: false, exclusive: false, arguments: consumeArgs, consumer: secondConsumer);

        Assert.Equal("stream-1", await firstDelivery.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("stream-1", await secondDelivery.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AmqpClient_ShouldTailStreamQueueFromNextOffset()
    {
        const int port = 5680;
        const int brokerPort = 4270;
        const int monitorPort = 8270;

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var setupChannel = connection.CreateModel();

        setupChannel.ExchangeDeclare("amqp.stream.tail.x", ExchangeType.Direct, durable: true, autoDelete: false);
        setupChannel.QueueDeclare(
            "amqp.stream.tail.q",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "stream" });
        setupChannel.QueueBind("amqp.stream.tail.q", "amqp.stream.tail.x", "stream");
        setupChannel.BasicPublish("amqp.stream.tail.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("before-consume"));

        using var channel = connection.CreateModel();
        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, ea) =>
        {
            delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
            await Task.CompletedTask;
        };

        channel.BasicConsume(
            "amqp.stream.tail.q",
            autoAck: true,
            consumerTag: "stream-next",
            noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "next" },
            consumer: consumer);

        await Task.Delay(200);
        setupChannel.BasicPublish("amqp.stream.tail.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("after-consume"));

        Assert.Equal("after-consume", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AmqpStreamConsumerOffset_ShouldSurviveRestart()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"cosmobroker-amqp-stream-offset-{Guid.NewGuid():N}.db");
        string connectionString = $"Data Source={dbPath};";
        const int port1 = 4271;
        const int mon1 = 8271;
        const int amqp1 = 5681;

        var repo1 = new MessageRepository(connectionString);
        await using (var server1 = new BrokerServer(port: port1, amqpPort: amqp1, repo: repo1, monitorPort: mon1))
        {
            using var cts = new CancellationTokenSource();
            await server1.StartAsync(cts.Token);
            await Task.Delay(250);

            var factory = new ConnectionFactory
            {
                HostName = "127.0.0.1",
                Port = amqp1,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/",
                DispatchConsumersAsync = true
            };

            using var connection = factory.CreateConnection();
            using var setupChannel = connection.CreateModel();
            setupChannel.ExchangeDeclare("amqp.stream.persist.x", ExchangeType.Direct, durable: true, autoDelete: false);
            setupChannel.QueueDeclare(
                "amqp.stream.persist.q",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-queue-type"] = "stream" });
            setupChannel.QueueBind("amqp.stream.persist.q", "amqp.stream.persist.x", "stream");
            setupChannel.BasicPublish("amqp.stream.persist.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("stream-a"));

            using var channel = connection.CreateModel();
            var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (_, ea) =>
            {
                delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
                channel.BasicAck(ea.DeliveryTag, false);
                await Task.CompletedTask;
            };

            channel.BasicConsume(
                "amqp.stream.persist.q",
                autoAck: false,
                consumerTag: "stream-persist-consumer",
                noLocal: false,
                exclusive: false,
                arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
                consumer: consumer);

            Assert.Equal("stream-a", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        }

        const int port2 = 4272;
        const int mon2 = 8272;
        const int amqp2 = 5682;

        var repo2 = new MessageRepository(connectionString);
        await using (var server2 = new BrokerServer(port: port2, amqpPort: amqp2, repo: repo2, monitorPort: mon2))
        {
            using var cts = new CancellationTokenSource();
            await server2.StartAsync(cts.Token);
            await Task.Delay(250);

            var factory = new ConnectionFactory
            {
                HostName = "127.0.0.1",
                Port = amqp2,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/",
                DispatchConsumersAsync = true
            };

            using var connection = factory.CreateConnection();
            using var setupChannel = connection.CreateModel();
            using var channel = connection.CreateModel();
            var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (_, ea) =>
            {
                delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
                channel.BasicAck(ea.DeliveryTag, false);
                await Task.CompletedTask;
            };

            channel.BasicConsume(
                "amqp.stream.persist.q",
                autoAck: false,
                consumerTag: "stream-persist-consumer",
                noLocal: false,
                exclusive: false,
                arguments: null,
                consumer: consumer);

            await Task.Delay(200);
            setupChannel.BasicPublish("amqp.stream.persist.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("stream-b"));

            Assert.Equal("stream-b", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        }

        try { File.Delete(dbPath); } catch { }
    }

    [Fact]
    public async Task AmqpMonitor_ShouldResetPersistedStreamConsumerOffset()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"cosmobroker-amqp-stream-reset-{Guid.NewGuid():N}.db");
        string connectionString = $"Data Source={dbPath};";
        const int brokerPort1 = 4276;
        const int monitorPort1 = 8276;
        const int amqpPort1 = 5686;

        var repo1 = new MessageRepository(connectionString);
        await using (var server1 = new BrokerServer(port: brokerPort1, amqpPort: amqpPort1, repo: repo1, monitorPort: monitorPort1))
        {
            using var cts = new CancellationTokenSource();
            await server1.StartAsync(cts.Token);
            await Task.Delay(250);

            var factory = new ConnectionFactory
            {
                HostName = "127.0.0.1",
                Port = amqpPort1,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/",
                DispatchConsumersAsync = true
            };

            using (var connection = factory.CreateConnection())
            {
                using var setupChannel = connection.CreateModel();
                setupChannel.ExchangeDeclare("amqp.stream.reset.x", ExchangeType.Direct, durable: true, autoDelete: false);
                setupChannel.QueueDeclare(
                    "amqp.stream.reset.q",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: new Dictionary<string, object?> { ["x-queue-type"] = "stream" });
                setupChannel.QueueBind("amqp.stream.reset.q", "amqp.stream.reset.x", "stream");
                setupChannel.BasicPublish("amqp.stream.reset.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("reset-a"));
                setupChannel.BasicPublish("amqp.stream.reset.x", "stream", basicProperties: null, body: Encoding.UTF8.GetBytes("reset-b"));

                using var channel = connection.CreateModel();
                channel.BasicQos(0, 1, global: false);
                var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.Received += async (_, ea) =>
                {
                    delivered.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
                    channel.BasicAck(ea.DeliveryTag, false);
                    channel.BasicCancel("stream-reset-consumer");
                    await Task.CompletedTask;
                };

                channel.BasicConsume(
                    "amqp.stream.reset.q",
                    autoAck: false,
                    consumerTag: "stream-reset-consumer",
                    noLocal: false,
                    exclusive: false,
                    arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
                    consumer: consumer);

                Assert.Equal("reset-a", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
            }

            using var client = new HttpClient();
            using var response = await client.PostAsync(
                $"http://127.0.0.1:{monitorPort1}/rmq/stream/reset?vhost=%2F&queue=amqp.stream.reset.q&consumer=stream-reset-consumer&offset=first",
                content: null);
            response.EnsureSuccessStatusCode();
            var payload = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(payload);
            Assert.True(doc.RootElement.GetProperty("ok").GetBoolean(), payload);
            Assert.Equal(1L, doc.RootElement.GetProperty("next_offset").GetInt64());
        }

        Assert.Equal(0L, await repo1.GetRabbitStreamConsumerOffsetAsync("/", "amqp.stream.reset.q", "stream-reset-consumer"));

        const int brokerPort2 = 4277;
        const int monitorPort2 = 8277;
        const int amqpPort2 = 5687;

        var repo2 = new MessageRepository(connectionString);
        await using (var server2 = new BrokerServer(port: brokerPort2, amqpPort: amqpPort2, repo: repo2, monitorPort: monitorPort2))
        {
            using var cts = new CancellationTokenSource();
            await server2.StartAsync(cts.Token);
            await Task.Delay(250);

            using (var monitor = new HttpClient())
            {
                var rmqz = await monitor.GetStringAsync($"http://127.0.0.1:{monitorPort2}/rmqz");
                using var doc = JsonDocument.Parse(rmqz);
                var queue = doc.RootElement.GetProperty("queues")
                    .EnumerateArray()
                    .FirstOrDefault(x => x.TryGetProperty("name", out var name) && name.GetString() == "amqp.stream.reset.q");
                Assert.True(queue.ValueKind != JsonValueKind.Undefined, rmqz);
                Assert.True(queue.GetProperty("messages").GetInt32() >= 2, rmqz);
            }

            Assert.Equal(0L, await repo2.GetRabbitStreamConsumerOffsetAsync("/", "amqp.stream.reset.q", "stream-reset-consumer"));

            var factory = new ConnectionFactory
            {
                HostName = "127.0.0.1",
                Port = amqpPort2,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/",
                DispatchConsumersAsync = true
            };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            var replayed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (_, ea) =>
            {
                replayed.TrySetResult(Encoding.UTF8.GetString(ea.Body.ToArray()));
                channel.BasicAck(ea.DeliveryTag, false);
                await Task.CompletedTask;
            };

            channel.BasicConsume(
                "amqp.stream.reset.q",
                autoAck: false,
                consumerTag: "stream-reset-consumer",
                noLocal: false,
                exclusive: false,
                arguments: null,
                consumer: consumer);

            Assert.Equal("reset-a", await replayed.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        }

        try { File.Delete(dbPath); } catch { }
    }

    [Fact]
    public async Task AmqpSuperStream_ShouldSurviveRestart()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"cosmobroker-amqp-super-stream-{Guid.NewGuid():N}.db");
        string connectionString = $"Data Source={dbPath};";
        int brokerPort1 = GetFreePort();
        int monitorPort1 = GetFreePort();
        int amqpPort1 = GetFreePort();

        var repo1 = new MessageRepository(connectionString);
        await using (var server1 = new BrokerServer(port: brokerPort1, amqpPort: amqpPort1, repo: repo1, monitorPort: monitorPort1))
        {
            using var cts = new CancellationTokenSource();
            await server1.StartAsync(cts.Token);
            await Task.Delay(250);

            var factory = new ConnectionFactory
            {
                HostName = "127.0.0.1",
                Port = amqpPort1,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/"
            };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.ExchangeDeclare(
                "amqp.super.persist.x",
                "x-super-stream",
                durable: true,
                autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-partitions"] = 2L });

            var props = channel.CreateBasicProperties();
            props.Persistent = true;

            channel.BasicPublish("amqp.super.persist.x", "tenant-7", props, Encoding.UTF8.GetBytes("persist-1"));
            channel.BasicPublish("amqp.super.persist.x", "tenant-7", props, Encoding.UTF8.GetBytes("persist-2"));
            await Task.Delay(150);
        }

        const int brokerPort2 = 4281;
        const int monitorPort2 = 8281;
        const int amqpPort2 = 5691;

        var repo2 = new MessageRepository(connectionString);
        await using (var server2 = new BrokerServer(port: brokerPort2, amqpPort: amqpPort2, repo: repo2, monitorPort: monitorPort2))
        {
            using var cts = new CancellationTokenSource();
            await server2.StartAsync(cts.Token);
            await Task.Delay(250);

            using (var monitor = new HttpClient())
            {
                var rmqz = await monitor.GetStringAsync($"http://127.0.0.1:{monitorPort2}/rmqz");
                using var doc = JsonDocument.Parse(rmqz);
                var exchange = doc.RootElement.GetProperty("exchanges").EnumerateArray()
                    .Single(x => x.GetProperty("name").GetString() == "amqp.super.persist.x");
                Assert.Equal("SuperStream", exchange.GetProperty("type").GetString());
                Assert.Equal(2, exchange.GetProperty("super_stream_partition_count").GetInt32());
                var partitions = exchange.GetProperty("super_stream_partitions").EnumerateArray().Select(x => x.GetString()).ToArray();
                Assert.Equal(new[] { "amqp.super.persist.x-0", "amqp.super.persist.x-1" }, partitions);
            }

            var factory = new ConnectionFactory
            {
                HostName = "127.0.0.1",
                Port = amqpPort2,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/",
                DispatchConsumersAsync = true
            };

            using var connection = factory.CreateConnection();
            using var q0Channel = connection.CreateModel();
            using var q1Channel = connection.CreateModel();
            var q0Messages = new List<string>();
            var q1Messages = new List<string>();
            var q0Done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var q1Done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var q0Consumer = new AsyncEventingBasicConsumer(q0Channel);
            q0Consumer.Received += async (_, ea) =>
            {
                q0Messages.Add(Encoding.UTF8.GetString(ea.Body.ToArray()));
                if (q0Messages.Count >= 2)
                    q0Done.TrySetResult();
                await Task.CompletedTask;
            };

            var q1Consumer = new AsyncEventingBasicConsumer(q1Channel);
            q1Consumer.Received += async (_, ea) =>
            {
                q1Messages.Add(Encoding.UTF8.GetString(ea.Body.ToArray()));
                if (q1Messages.Count >= 2)
                    q1Done.TrySetResult();
                await Task.CompletedTask;
            };

            q0Channel.BasicConsume(
                "amqp.super.persist.x-0",
                autoAck: true,
                consumerTag: "super-persist-0",
                noLocal: false,
                exclusive: false,
                arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
                consumer: q0Consumer);

            q1Channel.BasicConsume(
                "amqp.super.persist.x-1",
                autoAck: true,
                consumerTag: "super-persist-1",
                noLocal: false,
                exclusive: false,
                arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
                consumer: q1Consumer);

            await Task.WhenAny(Task.WhenAll(q0Done.Task, q1Done.Task), Task.Delay(1000));

            bool queue0ReceivedBoth = q0Messages.Count == 2 && q1Messages.Count == 0;
            bool queue1ReceivedBoth = q1Messages.Count == 2 && q0Messages.Count == 0;
            Assert.True(queue0ReceivedBoth || queue1ReceivedBoth, "Expected persisted messages to remain on the same super-stream partition after restart.");
        }

        try { File.Delete(dbPath); } catch { }
    }

    [Fact]
    public async Task AmqpMonitor_ShouldResetPersistedSuperStreamOffsets()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"cosmobroker-amqp-super-reset-{Guid.NewGuid():N}.db");
        string connectionString = $"Data Source={dbPath};";
        const int brokerPort = 4282;
        const int monitorPort = 8282;
        const int amqpPort = 5692;

        var repo = new MessageRepository(connectionString);
        await using var server = new BrokerServer(port: brokerPort, amqpPort: amqpPort, repo: repo, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = amqpPort,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        string[] partitions;
        using (var connection = factory.CreateConnection())
        {
            using var channel = connection.CreateModel();
            channel.ExchangeDeclare(
                "amqp.super.reset.x",
                "x-super-stream",
                durable: true,
                autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-partitions"] = 2L });

            using var monitor = new HttpClient();
            var rmqz = await monitor.GetStringAsync($"http://127.0.0.1:{monitorPort}/rmqz");
            using var monitorDoc = JsonDocument.Parse(rmqz);
            partitions = monitorDoc.RootElement.GetProperty("exchanges").EnumerateArray()
                .Single(x => x.GetProperty("name").GetString() == "amqp.super.reset.x")
                .GetProperty("super_stream_partitions")
                .EnumerateArray()
                .Select(x => x.GetString()!)
                .ToArray();

            Assert.Equal(2, partitions.Length);

            channel.BasicPublish("", partitions[0], null, Encoding.UTF8.GetBytes("seed-0"));
            channel.BasicPublish("", partitions[1], null, Encoding.UTF8.GetBytes("seed-1"));
        }

        using (var client = new HttpClient())
        {
            for (int i = 0; i < partitions.Length; i++)
            {
                long seedOffset = i == 0 ? 7L : 11L;
                using var seedResponse = await client.PostAsync(
                    $"http://127.0.0.1:{monitorPort}/rmq/stream/reset?vhost=%2F&queue={Uri.EscapeDataString(partitions[i])}&consumer=super-reset-consumer&offset={seedOffset}",
                    content: null);
                seedResponse.EnsureSuccessStatusCode();
            }
        }

        var initialOffsets = partitions.ToDictionary(
            partition => partition,
            partition => repo.GetRabbitStreamConsumerOffsetAsync("/", partition, "super-reset-consumer").GetAwaiter().GetResult() ?? -1L,
            StringComparer.OrdinalIgnoreCase);
        Assert.Equal(6L, initialOffsets[partitions[0]]);
        Assert.Equal(10L, initialOffsets[partitions[1]]);

        using (var client = new HttpClient())
        {
            using var response = await client.PostAsync(
                $"http://127.0.0.1:{monitorPort}/rmq/super-stream/reset?vhost=%2F&exchange=amqp.super.reset.x&consumer=super-reset-consumer&offset=first",
                content: null);
            response.EnsureSuccessStatusCode();
            var payload = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(payload);
            Assert.True(doc.RootElement.GetProperty("ok").GetBoolean(), payload);
            var resetPartitions = doc.RootElement.GetProperty("partitions").EnumerateObject().ToDictionary(x => x.Name, x => x.Value.GetInt64(), StringComparer.OrdinalIgnoreCase);
            Assert.Equal(2, resetPartitions.Count);
            Assert.All(partitions, partition => Assert.True(resetPartitions[partition] < initialOffsets[partition]));

            foreach (var partition in partitions)
            {
                var persistedOffset = await repo.GetRabbitStreamConsumerOffsetAsync("/", partition, "super-reset-consumer");
                Assert.Equal(resetPartitions[partition] - 1, persistedOffset);
            }
        }

        try { File.Delete(dbPath); } catch { }
    }

    [Fact]
    public async Task AmqpClient_ShouldFailSuperStreamRedeclareWhenPartitionCountDiffers()
    {
        const int port = 5693;
        const int brokerPort = 4283;
        const int monitorPort = 8283;

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.ExchangeDeclare(
            "amqp.super.precondition.x",
            "x-super-stream",
            durable: true,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-partitions"] = 2L });

        var ex = Assert.Throws<OperationInterruptedException>(() =>
        {
            channel.ExchangeDeclare(
                "amqp.super.precondition.x",
                "x-super-stream",
                durable: true,
                autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-partitions"] = 3L });
        });

        Assert.Equal((ushort)406, ex.ShutdownReason?.ReplyCode);
        Assert.Contains("different properties", ex.ShutdownReason?.ReplyText ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldApplyAndValidateSuperStreamRetentionArguments()
    {
        const int port = 5698;
        const int brokerPort = 4288;
        const int monitorPort = 8288;

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.ExchangeDeclare(
            "amqp.super.retention.x",
            "x-super-stream",
            durable: true,
            autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-partitions"] = 2L,
                ["x-max-length"] = 50L,
                ["x-max-length-bytes"] = 1024L,
                ["x-max-age"] = "2s"
            });

        using var monitor = new HttpClient();
        var rmqz = await monitor.GetStringAsync($"http://127.0.0.1:{monitorPort}/rmqz");
        using var doc = JsonDocument.Parse(rmqz);
        var queues = doc.RootElement.GetProperty("queues").EnumerateArray()
            .Where(x => x.GetProperty("name").GetString() is "amqp.super.retention.x-0" or "amqp.super.retention.x-1")
            .ToList();

        Assert.Equal(2, queues.Count);
        foreach (var queue in queues)
        {
            Assert.Equal("stream", queue.GetProperty("queue_type").GetString());
            Assert.Equal(50L, queue.GetProperty("stream_max_length_messages").GetInt64());
            Assert.Equal(1024L, queue.GetProperty("stream_max_length_bytes").GetInt64());
            Assert.Equal(2000L, queue.GetProperty("stream_max_age_ms").GetInt64());
        }

        var ex = Assert.Throws<OperationInterruptedException>(() =>
        {
            channel.ExchangeDeclare(
                "amqp.super.retention.x",
                "x-super-stream",
                durable: true,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    ["x-partitions"] = 2L,
                    ["x-max-length"] = 51L,
                    ["x-max-length-bytes"] = 1024L,
                    ["x-max-age"] = "2s"
                });
        });

        Assert.Equal((ushort)406, ex.ShutdownReason?.ReplyCode);
        Assert.Contains("different properties", ex.ShutdownReason?.ReplyText ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AmqpClient_ShouldUseSuperStreamPartitionHeaderOverride()
    {
        const int port = 5694;
        const int brokerPort = 4284;
        const int monitorPort = 8284;

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.ExchangeDeclare(
            "amqp.super.header.x",
            "x-super-stream",
            durable: true,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-partitions"] = 2L });

        string keyA = "tenant-a";
        string keyB = "tenant-b";
        if (GetStablePartitionIndex(keyA, 2) == GetStablePartitionIndex(keyB, 2))
        {
            for (int i = 0; i < 16; i++)
            {
                keyB = $"tenant-b-{i}";
                if (GetStablePartitionIndex(keyA, 2) != GetStablePartitionIndex(keyB, 2))
                    break;
            }
        }

        var propsA = channel.CreateBasicProperties();
        propsA.Headers = new Dictionary<string, object?> { ["x-super-stream-partition-key"] = keyA };
        var propsB = channel.CreateBasicProperties();
        propsB.Headers = new Dictionary<string, object?> { ["x-super-stream-partition-key"] = keyB };

        channel.BasicPublish("amqp.super.header.x", "shared-routing-key", propsA, Encoding.UTF8.GetBytes("msg-a"));
        channel.BasicPublish("amqp.super.header.x", "shared-routing-key", propsB, Encoding.UTF8.GetBytes("msg-b"));

        using var q0Channel = connection.CreateModel();
        using var q1Channel = connection.CreateModel();
        var q0Messages = new List<string>();
        var q1Messages = new List<string>();
        var q0Done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var q1Done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var q0Consumer = new AsyncEventingBasicConsumer(q0Channel);
        q0Consumer.Received += async (_, ea) =>
        {
            q0Messages.Add(Encoding.UTF8.GetString(ea.Body.ToArray()));
            if (q0Messages.Count >= 1)
                q0Done.TrySetResult();
            await Task.CompletedTask;
        };

        var q1Consumer = new AsyncEventingBasicConsumer(q1Channel);
        q1Consumer.Received += async (_, ea) =>
        {
            q1Messages.Add(Encoding.UTF8.GetString(ea.Body.ToArray()));
            if (q1Messages.Count >= 1)
                q1Done.TrySetResult();
            await Task.CompletedTask;
        };

        q0Channel.BasicConsume(
            "amqp.super.header.x-0",
            autoAck: true,
            consumerTag: "super-header-0",
            noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
            consumer: q0Consumer);

        q1Channel.BasicConsume(
            "amqp.super.header.x-1",
            autoAck: true,
            consumerTag: "super-header-1",
            noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object?> { ["x-stream-offset"] = "first" },
            consumer: q1Consumer);

        await Task.WhenAny(Task.WhenAll(q0Done.Task, q1Done.Task), Task.Delay(1000));

        int indexA = GetStablePartitionIndex(keyA, 2);
        int indexB = GetStablePartitionIndex(keyB, 2);

        Assert.NotEqual(indexA, indexB);
        Assert.Contains("msg-a", indexA == 0 ? q0Messages : q1Messages);
        Assert.Contains("msg-b", indexB == 0 ? q0Messages : q1Messages);
    }

    [Fact]
    public async Task AmqpMonitor_ShouldPreviewSuperStreamRoute()
    {
        const int port = 5695;
        const int brokerPort = 4285;
        const int monitorPort = 8285;

        await using var server = new BrokerServer(port: brokerPort, amqpPort: port, monitorPort: monitorPort);
        using var cts = new CancellationTokenSource();
        await server.StartAsync(cts.Token);
        await Task.Delay(250);

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.ExchangeDeclare(
            "amqp.super.preview.x",
            "x-super-stream",
            durable: true,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-partitions"] = 2L });

        using var client = new HttpClient();

        string routingKey = "customer-42";
        using var baseResponse = await client.GetAsync(
            $"http://127.0.0.1:{monitorPort}/rmq/super-stream/route?vhost=%2F&exchange=amqp.super.preview.x&routing_key={Uri.EscapeDataString(routingKey)}");
        baseResponse.EnsureSuccessStatusCode();
        using var baseDoc = JsonDocument.Parse(await baseResponse.Content.ReadAsStringAsync());
        Assert.True(baseDoc.RootElement.GetProperty("ok").GetBoolean());
        string? basePartition = baseDoc.RootElement.GetProperty("partition").GetString();
        Assert.Equal($"amqp.super.preview.x-{GetStablePartitionIndex(routingKey, 2)}", basePartition);

        string overrideKey = GetStablePartitionIndex(routingKey, 2) == 0 ? "tenant-b" : "tenant-a";
        if (GetStablePartitionIndex(overrideKey, 2) == GetStablePartitionIndex(routingKey, 2))
            overrideKey = GetStablePartitionIndex(routingKey, 2) == 0 ? "tenant-c" : "tenant-d";

        using var overrideResponse = await client.GetAsync(
            $"http://127.0.0.1:{monitorPort}/rmq/super-stream/route?vhost=%2F&exchange=amqp.super.preview.x&routing_key={Uri.EscapeDataString(routingKey)}&partition_key={Uri.EscapeDataString(overrideKey)}");
        overrideResponse.EnsureSuccessStatusCode();
        using var overrideDoc = JsonDocument.Parse(await overrideResponse.Content.ReadAsStringAsync());
        Assert.True(overrideDoc.RootElement.GetProperty("ok").GetBoolean());
        string? overridePartition = overrideDoc.RootElement.GetProperty("partition").GetString();
        Assert.Equal($"amqp.super.preview.x-{GetStablePartitionIndex(overrideKey, 2)}", overridePartition);
        Assert.NotEqual(basePartition, overridePartition);
    }

    private static int GetStablePartitionIndex(string value, int partitions)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (char ch in value)
            {
                hash ^= ch;
                hash *= 16777619;
            }

            return (int)((hash & 0x7fffffff) % partitions);
        }
    }

    private static int GetFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        try { return ((System.Net.IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }
}
