using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace CosmoBroker.RabbitMQ;

public enum ExchangeType { Direct, Fanout, Topic, Headers }

/// <summary>
/// Represents a RabbitMQ-style exchange. Routes incoming messages to bound queues
/// based on exchange type and routing key / header rules.
/// </summary>
public sealed class Exchange
{
    public string Vhost { get; }
    public string Name { get; }
    public ExchangeType Type { get; }
    public bool Durable { get; }
    public bool AutoDelete { get; }

    // Binding table: routing key → list of bound queues
    // For fanout, all queues are stored under the "" key.
    // For headers, the routing key is unused; we store under "" and filter by args.
    private readonly ConcurrentDictionary<string, ConcurrentBag<string>> _bindings = new(StringComparer.OrdinalIgnoreCase);

    // Headers exchange: binding key → required header pairs
    private readonly ConcurrentDictionary<string, Dictionary<string, string>> _headerArgs = new();

    public Exchange(string name, ExchangeType type, bool durable = true, bool autoDelete = false)
        : this("/", name, type, durable, autoDelete)
    {
    }

    public Exchange(string vhost, string name, ExchangeType type, bool durable = true, bool autoDelete = false)
    {
        Vhost = string.IsNullOrWhiteSpace(vhost) ? "/" : vhost;
        Name = name;
        Type = type;
        Durable = durable;
        AutoDelete = autoDelete;
    }

    // --- Binding management --------------------------------------------------

    public void Bind(string queueName, string routingKey, Dictionary<string, string>? headerArgs = null)
    {
        string key = NormaliseKey(routingKey);
        var bag = _bindings.GetOrAdd(key, _ => new ConcurrentBag<string>());
        if (!bag.Contains(queueName))
            bag.Add(queueName);

        if (Type == ExchangeType.Headers && headerArgs != null)
            _headerArgs[queueName] = headerArgs;
    }

    public bool Unbind(string queueName, string routingKey)
    {
        string key = NormaliseKey(routingKey);
        bool removed = false;
        if (_bindings.TryGetValue(key, out var bag))
        {
            // ConcurrentBag doesn't support remove; rebuild without the queue.
            var remaining = bag.Where(q => q != queueName).ToArray();
            removed = remaining.Length != bag.Count;
            _bindings[key] = new ConcurrentBag<string>(remaining);
        }
        _headerArgs.TryRemove(queueName, out _);
        return removed;
    }

    // --- Routing -------------------------------------------------------------

    /// <summary>Returns the names of queues that should receive the message.</summary>
    public IEnumerable<string> Route(string routingKey, Dictionary<string, string>? headers = null)
    {
        return Type switch
        {
            ExchangeType.Direct   => RouteDirect(routingKey),
            ExchangeType.Fanout   => RouteFanout(),
            ExchangeType.Topic    => RouteTopic(routingKey),
            ExchangeType.Headers  => RouteHeaders(headers),
            _                     => Enumerable.Empty<string>()
        };
    }

    private IEnumerable<string> RouteDirect(string routingKey)
    {
        if (_bindings.TryGetValue(routingKey, out var bag))
            foreach (var q in bag) yield return q;
    }

    private IEnumerable<string> RouteFanout()
    {
        foreach (var bag in _bindings.Values)
            foreach (var q in bag)
                yield return q;
    }

    private IEnumerable<string> RouteTopic(string routingKey)
    {
        foreach (var (pattern, bag) in _bindings)
            if (TopicMatches(pattern, routingKey))
                foreach (var q in bag)
                    yield return q;
    }

    private IEnumerable<string> RouteHeaders(Dictionary<string, string>? headers)
    {
        if (headers == null) yield break;

        foreach (var (queueName, required) in _headerArgs)
        {
            bool xMatch = required.TryGetValue("x-match", out var matchMode) && matchMode == "any";
            bool matched = xMatch
                ? required.Any(kv => kv.Key != "x-match" && headers.TryGetValue(kv.Key, out var v) && v == kv.Value)
                : required.All(kv => kv.Key == "x-match" || (headers.TryGetValue(kv.Key, out var v) && v == kv.Value));
            if (matched) yield return queueName;
        }
    }

    // --- Topic pattern matching (RabbitMQ semantics: * = one word, # = zero or more) ---

    public static bool TopicMatches(string pattern, string routingKey)
    {
        var pParts = pattern.Split('.');
        var rParts = routingKey.Split('.');
        return MatchParts(pParts, 0, rParts, 0);
    }

    private static bool MatchParts(string[] p, int pi, string[] r, int ri)
    {
        while (pi < p.Length && ri < r.Length)
        {
            if (p[pi] == "#")
            {
                // # matches zero or more words
                for (int skip = 0; skip <= r.Length - ri; skip++)
                    if (MatchParts(p, pi + 1, r, ri + skip)) return true;
                return false;
            }
            if (p[pi] != "*" && !string.Equals(p[pi], r[ri], StringComparison.OrdinalIgnoreCase))
                return false;
            pi++; ri++;
        }
        // Consume trailing # wildcards
        while (pi < p.Length && p[pi] == "#") pi++;
        return pi == p.Length && ri == r.Length;
    }

    private string NormaliseKey(string key) => Type == ExchangeType.Fanout ? "" : key;

    public IReadOnlyDictionary<string, IReadOnlyList<string>> GetBindings()
    {
        var result = new Dictionary<string, IReadOnlyList<string>>();
        foreach (var (k, bag) in _bindings)
            result[k] = bag.ToList().AsReadOnly();
        return result;
    }

    public bool HasBindings()
        => _bindings.Values.Any(bag => bag.Count > 0);
}
