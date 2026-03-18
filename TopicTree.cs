using System.Collections.Concurrent;
using System.Collections.Generic;

namespace CosmoBroker;

public class TopicTree
{
    private class TopicNode
    {
        // Individual subscribers: SID -> Connection
        public ConcurrentDictionary<string, BrokerConnection> Subscribers { get; } = new();
        
        // Queue Groups: GroupName -> { SID -> Connection }
        public ConcurrentDictionary<string, ConcurrentDictionary<string, BrokerConnection>> QueueGroups { get; } = new();

        // Child nodes (e.g., "foo" -> "bar" for "foo.bar")
        public ConcurrentDictionary<string, TopicNode> Children { get; } = new();
    }

    private readonly TopicNode _root = new();
    private readonly Random _random = new();

    public void Subscribe(string subject, BrokerConnection connection, string sid, string? queueGroup = null)
    {
        var parts = subject.Split('.');
        var current = _root;

        foreach (var part in parts)
        {
            current = current.Children.GetOrAdd(part, _ => new TopicNode());
        }

        if (string.IsNullOrEmpty(queueGroup))
        {
            current.Subscribers[sid] = connection;
        }
        else
        {
            var group = current.QueueGroups.GetOrAdd(queueGroup, _ => new ConcurrentDictionary<string, BrokerConnection>());
            group[sid] = connection;
        }
    }

    public void Unsubscribe(string subject, BrokerConnection connection, string sid, string? queueGroup = null)
    {
        var parts = subject.Split('.');
        var current = _root;

        foreach (var part in parts)
        {
            if (!current.Children.TryGetValue(part, out current))
            {
                return; // Node not found
            }
        }

        if (string.IsNullOrEmpty(queueGroup))
        {
            current.Subscribers.TryRemove(sid, out _);
        }
        else
        {
            if (current.QueueGroups.TryGetValue(queueGroup, out var group))
            {
                group.TryRemove(sid, out _);
                if (group.IsEmpty) current.QueueGroups.TryRemove(queueGroup, out _);
            }
        }
    }

    public void Publish(string subject, System.Buffers.ReadOnlySequence<byte> payload)
    {
        var parts = subject.Split('.');
        MatchAndPublish(_root, parts, 0, subject, payload);
    }

    private void MatchAndPublish(TopicNode node, string[] parts, int index, string originalSubject, System.Buffers.ReadOnlySequence<byte> payload)
    {
        if (index == parts.Length)
        {
            // 1. Direct subscribers (fan-out to ALL)
            foreach (var sub in node.Subscribers)
            {
                sub.Value.SendMessage(originalSubject, sub.Key, payload);
            }

            // 2. Queue groups (load-balance: pick ONE per group)
            foreach (var groupEntry in node.QueueGroups)
            {
                var group = groupEntry.Value;
                if (group.IsEmpty) continue;

                var members = group.ToArray();
                if (members.Length > 0)
                {
                    var pick = members[_random.Next(members.Length)];
                    pick.Value.SendMessage(originalSubject, pick.Key, payload);
                }
            }
            return;
        }

        string part = parts[index];

        // 1. Literal match
        if (node.Children.TryGetValue(part, out var literalNode))
        {
            MatchAndPublish(literalNode, parts, index + 1, originalSubject, payload);
        }

        // 2. Single token wildcard match (*)
        if (node.Children.TryGetValue("*", out var starNode))
        {
            MatchAndPublish(starNode, parts, index + 1, originalSubject, payload);
        }

        // 3. Multi token wildcard match (>)
        if (node.Children.TryGetValue(">", out var chevronNode))
        {
            // Fan out to all subscribers at the chevron node
            foreach (var sub in chevronNode.Subscribers)
            {
                sub.Value.SendMessage(originalSubject, sub.Key, payload);
            }
            // Load balance to groups at the chevron node
            foreach (var groupEntry in chevronNode.QueueGroups)
            {
                var group = groupEntry.Value;
                var members = group.ToArray();
                if (members.Length > 0)
                {
                    var pick = members[_random.Next(members.Length)];
                    pick.Value.SendMessage(originalSubject, pick.Key, payload);
                }
            }
        }
    }
}
