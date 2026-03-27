using CosmoBroker.Management.Models;
using CosmoBroker.Management.Services;

namespace Tests;

public class ManagementSuperStreamSummaryTests
{
    [Fact]
    public void BuildSuperStreamSummaries_ShouldAggregateLogicalConsumerStateAcrossPartitions()
    {
        var stats = new RabbitMqStats
        {
            exchanges =
            [
                new RabbitExchangeStats
                {
                    vhost = "/",
                    name = "orders.super",
                    type = "SuperStream",
                    super_stream_partition_count = 2,
                    super_stream_partitions = ["orders.super-0", "orders.super-1"]
                }
            ],
            queues =
            [
                new RabbitQueueStats
                {
                    vhost = "/",
                    name = "orders.super-0",
                    queue_type = "stream",
                    messages = 10,
                    bytes = 100,
                    consumers = 1,
                    stream_head_offset = 5,
                    stream_tail_offset = 15,
                    stream_offsets = new Dictionary<string, long> { ["consumer-a"] = 11, ["consumer-b"] = 12 },
                    stream_consumer_lag = new Dictionary<string, long> { ["consumer-a"] = 4, ["consumer-b"] = 3 }
                },
                new RabbitQueueStats
                {
                    vhost = "/",
                    name = "orders.super-1",
                    queue_type = "stream",
                    messages = 20,
                    bytes = 200,
                    consumers = 2,
                    stream_head_offset = 6,
                    stream_tail_offset = 18,
                    stream_offsets = new Dictionary<string, long> { ["consumer-a"] = 16 },
                    stream_consumer_lag = new Dictionary<string, long> { ["consumer-a"] = 2 }
                }
            ]
        };

        var summary = Assert.Single(BrokerMonitorClient.BuildSuperStreamSummaries(stats));

        Assert.Equal("orders.super", summary.name);
        Assert.Equal(2, summary.partition_count);
        Assert.Equal(3, summary.consumers);
        Assert.Equal(2, summary.logical_consumers);
        Assert.Equal(5, summary.min_head_offset);
        Assert.Equal(18, summary.max_tail_offset);

        var consumerA = Assert.Single(summary.consumer_details, x => x.consumer == "consumer-a");
        Assert.Equal(2, consumerA.partition_count);
        Assert.Equal(6, consumerA.total_lag);
        Assert.Equal(4, consumerA.max_lag);
        Assert.Equal(11, consumerA.min_next_offset);
        Assert.Equal(16, consumerA.max_next_offset);
        Assert.Equal(2, consumerA.partition_details.Count);

        var consumerB = Assert.Single(summary.consumer_details, x => x.consumer == "consumer-b");
        Assert.Equal(1, consumerB.partition_count);
        Assert.Equal(3, consumerB.total_lag);
        Assert.Equal(3, consumerB.max_lag);
        Assert.Equal(12, consumerB.min_next_offset);
        Assert.Equal(12, consumerB.max_next_offset);
    }
}
