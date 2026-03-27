using System.Net.Http.Json;
using System.Net;
using CosmoBroker.Management.Models;

namespace CosmoBroker.Management.Services;

public sealed class BrokerMonitorClient
{
    private readonly HttpClient _httpClient;
    private readonly BrokerManagementOptions _options;

    public BrokerMonitorClient(BrokerManagementOptions options)
    {
        _options = options;
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(AppendTrailingSlash(options.MonitorBaseUrl)),
            Timeout = TimeSpan.FromSeconds(5)
        };
    }

    public async Task<BrokerHealth> GetHealthAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var response = await _httpClient.GetAsync("varz", cancellationToken);
            response.EnsureSuccessStatusCode();
            return new BrokerHealth
            {
                ok = true,
                monitor_url = _options.MonitorBaseUrl,
                fetched_at_utc = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            return new BrokerHealth
            {
                ok = false,
                monitor_url = _options.MonitorBaseUrl,
                error = ex.Message,
                fetched_at_utc = DateTime.UtcNow
            };
        }
    }

    public async Task<BrokerSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
    {
        var health = await GetHealthAsync(cancellationToken);
        if (!health.ok)
            return new BrokerSnapshot { health = health };

        var varzTask = GetAsync<VarzStats>("varz", cancellationToken);
        var connzTask = GetAsync<ConnectionList>("connz", cancellationToken);
        var routezTask = GetAsync<RouteStats>("routez", cancellationToken);
        var gatewayzTask = GetAsync<GatewayStats>("gatewayz", cancellationToken);
        var leafzTask = GetAsync<LeafStats>("leafz", cancellationToken);
        var jszTask = GetAsync<JetStreamStats>("jsz", cancellationToken);
        var rmqzTask = GetAsync<RabbitMqStats>("rmqz", cancellationToken);

        await Task.WhenAll(varzTask, connzTask, routezTask, gatewayzTask, leafzTask, jszTask, rmqzTask);

        return new BrokerSnapshot
        {
            health = health,
            varz = await varzTask,
            connz = await connzTask ?? new ConnectionList(),
            routez = await routezTask ?? new RouteStats(),
            gatewayz = await gatewayzTask ?? new GatewayStats(),
            leafz = await leafzTask ?? new LeafStats(),
            jsz = await jszTask ?? new JetStreamStats(),
            rmqz = await rmqzTask ?? new RabbitMqStats()
        };
    }

    public async Task<ConnectionList> GetConnectionsAsync(CancellationToken cancellationToken = default)
        => await GetAsync<ConnectionList>("connz", cancellationToken) ?? new ConnectionList();

    public async Task<JetStreamStats> GetJetStreamAsync(CancellationToken cancellationToken = default)
        => await GetAsync<JetStreamStats>("jsz", cancellationToken) ?? new JetStreamStats();

    public async Task<RabbitMqStats> GetRabbitMqAsync(CancellationToken cancellationToken = default)
        => await GetAsync<RabbitMqStats>("rmqz", cancellationToken) ?? new RabbitMqStats();

    public async Task<List<SuperStreamSummary>> GetSuperStreamsAsync(CancellationToken cancellationToken = default)
        => BuildSuperStreamSummaries(await GetRabbitMqAsync(cancellationToken));

    public async Task<SuperStreamSummary?> GetSuperStreamAsync(string vhost, string name, CancellationToken cancellationToken = default)
        => FindSuperStream(await GetRabbitMqAsync(cancellationToken), vhost, name);

    public async Task<StreamOffsetResetResult> ResetStreamOffsetAsync(StreamOffsetResetRequest request, CancellationToken cancellationToken = default)
    {
        var relativePath =
            $"rmq/stream/reset?vhost={Encode(request.vhost)}&queue={Encode(request.queue)}&consumer={Encode(request.consumer)}&offset={Encode(request.offset)}";

        using var response = await _httpClient.PostAsync(relativePath, content: null, cancellationToken);
        var result = await response.Content.ReadFromJsonAsync<StreamOffsetResetResult>(cancellationToken: cancellationToken)
            ?? new StreamOffsetResetResult { ok = false, error = "Empty response from broker monitor." };

        if (!response.IsSuccessStatusCode && string.IsNullOrWhiteSpace(result.error))
            result.error = $"Broker monitor returned {(int)response.StatusCode}.";

        return result;
    }

    public async Task<SuperStreamOffsetResetResult> ResetSuperStreamOffsetAsync(SuperStreamOffsetResetRequest request, CancellationToken cancellationToken = default)
    {
        var relativePath =
            $"rmq/super-stream/reset?vhost={Encode(request.vhost)}&exchange={Encode(request.exchange)}&consumer={Encode(request.consumer)}&offset={Encode(request.offset)}";

        using var response = await _httpClient.PostAsync(relativePath, content: null, cancellationToken);
        var result = await response.Content.ReadFromJsonAsync<SuperStreamOffsetResetResult>(cancellationToken: cancellationToken)
            ?? new SuperStreamOffsetResetResult { ok = false, error = "Empty response from broker monitor." };

        if (!response.IsSuccessStatusCode && string.IsNullOrWhiteSpace(result.error))
            result.error = $"Broker monitor returned {(int)response.StatusCode}.";

        return result;
    }

    private async Task<T?> GetAsync<T>(string relativePath, CancellationToken cancellationToken)
    {
        using var response = await _httpClient.GetAsync(relativePath, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<T>(cancellationToken: cancellationToken);
    }

    private static string AppendTrailingSlash(string baseUrl)
        => baseUrl.EndsWith("/", StringComparison.Ordinal) ? baseUrl : baseUrl + "/";

    private static string Encode(string? value)
        => WebUtility.UrlEncode(value ?? string.Empty);

    public static List<SuperStreamSummary> BuildSuperStreamSummaries(RabbitMqStats stats)
    {
        if (stats.exchanges.Count == 0)
            return [];

        return stats.exchanges
            .Where(x => string.Equals(x.type, "SuperStream", StringComparison.OrdinalIgnoreCase))
            .OrderBy(x => x.vhost, StringComparer.Ordinal)
            .ThenBy(x => x.name, StringComparer.Ordinal)
            .Select(exchange =>
            {
                var partitions = new HashSet<string>(exchange.super_stream_partitions, StringComparer.OrdinalIgnoreCase);
                var partitionQueues = stats.queues
                    .Where(q => string.Equals(q.vhost, exchange.vhost, StringComparison.Ordinal) &&
                                q.name != null &&
                                partitions.Contains(q.name))
                    .ToList();
                var partitionDetails = partitionQueues
                    .OrderBy(q => q.name, StringComparer.Ordinal)
                    .Select(q => new SuperStreamPartitionSummary
                    {
                        name = q.name ?? string.Empty,
                        messages = q.messages,
                        bytes = q.bytes,
                        consumers = q.consumers,
                        head_offset = q.stream_head_offset,
                        tail_offset = q.stream_tail_offset,
                        max_lag = q.stream_consumer_lag.Count == 0 ? 0 : q.stream_consumer_lag.Values.Max(),
                        max_length_messages = q.stream_max_length_messages,
                        max_length_bytes = q.stream_max_length_bytes,
                        max_age_ms = q.stream_max_age_ms
                    })
                    .ToList();
                var retention = partitionDetails
                    .SelectMany(static detail => EnumerateRetention(detail))
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(static x => x, StringComparer.Ordinal)
                    .ToList();

                return new SuperStreamSummary
                {
                    vhost = exchange.vhost ?? "/",
                    name = exchange.name ?? string.Empty,
                    partition_count = exchange.super_stream_partition_count ?? exchange.super_stream_partitions.Count,
                    partitions = [..exchange.super_stream_partitions],
                    partition_details = partitionDetails,
                    messages = partitionQueues.Sum(q => (long)q.messages),
                    bytes = partitionQueues.Sum(q => q.bytes),
                    consumers = partitionQueues.Sum(q => q.consumers),
                    min_head_offset = partitionDetails.Count == 0 ? null : partitionDetails.Min(x => x.head_offset),
                    max_tail_offset = partitionDetails.Count == 0 ? null : partitionDetails.Max(x => x.tail_offset),
                    retention = retention,
                    max_lag = partitionQueues
                        .SelectMany(q => q.stream_consumer_lag.Values.DefaultIfEmpty(0))
                        .DefaultIfEmpty(0)
                        .Max()
                };
            })
            .ToList();
    }

    public static SuperStreamSummary? FindSuperStream(RabbitMqStats stats, string? vhost, string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return null;

        var resolvedVhost = string.IsNullOrWhiteSpace(vhost) ? "/" : vhost;
        return BuildSuperStreamSummaries(stats).FirstOrDefault(x =>
            string.Equals(x.vhost, resolvedVhost, StringComparison.Ordinal) &&
            string.Equals(x.name, name, StringComparison.Ordinal));
    }

    private static IEnumerable<string> EnumerateRetention(SuperStreamPartitionSummary detail)
    {
        if (detail.max_length_messages.HasValue)
            yield return $"max-messages {detail.max_length_messages.Value}";
        if (detail.max_length_bytes.HasValue)
            yield return $"max-bytes {detail.max_length_bytes.Value}";
        if (detail.max_age_ms.HasValue)
            yield return $"max-age-ms {detail.max_age_ms.Value}";
    }
}
