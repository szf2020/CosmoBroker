using System.Text;
using CosmoBroker.RabbitMQ;
using Xunit;

namespace Tests;

public class RabbitMqStreamTests
{
    [Fact]
    public void StreamQueue_ShouldTrimOldestMessagesWhenMaxLengthBytesExceeded()
    {
        var queue = new RabbitQueue("stream.q", new RabbitQueueArgs
        {
            Type = RabbitQueueType.Stream,
            StreamMaxLengthBytes = 6
        });

        queue.Enqueue(new RabbitMessage { Payload = Encoding.UTF8.GetBytes("1111") });
        queue.Enqueue(new RabbitMessage { Payload = Encoding.UTF8.GetBytes("2222") });

        queue.SetStreamOffset("consumer-1", new RabbitStreamOffsetSpec { Kind = RabbitStreamOffsetKind.First });

        Assert.True(queue.TryGetStreamMessage("consumer-1", out var message));
        Assert.NotNull(message);
        Assert.Equal("2222", Encoding.UTF8.GetString(message!.Payload));
    }
}
