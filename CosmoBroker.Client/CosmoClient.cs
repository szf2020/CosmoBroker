using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using CosmoBroker.Client.Models;

namespace CosmoBroker.Client;

public class CosmoClient : IAsyncDisposable
{
    private readonly CosmoConnection _connection;
    private long _nextSid = 0;

    private readonly ConcurrentDictionary<string, Channel<CosmoMessage>> _exactSubscriptions =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly List<(string Pattern, Channel<CosmoMessage> Channel)> _wildcardSubscriptions = new();
    private readonly ReaderWriterLockSlim _wildcardLock = new();

    private readonly ConcurrentDictionary<string, TaskCompletionSource<CosmoMessage>> _requests =
        new(StringComparer.OrdinalIgnoreCase);

    public bool IsConnected => _connection.ServerInfo != null;

    public CosmoClient(CosmoClientOptions? options = null)
    {
        _connection = new CosmoConnection(options ?? new CosmoClientOptions());
        _connection.OnMessage = HandleIncomingMessage;
    }

    public Task ConnectAsync(CancellationToken ct = default) => _connection.ConnectAsync(ct);

    public Task PingAsync(CancellationToken ct = default) => _connection.PingAsync(ct);

    private void HandleIncomingMessage(CosmoMessage msg)
    {
        if (_requests.TryRemove(msg.Subject, out var tcs))
        {
            tcs.TrySetResult(msg);
            return;
        }

        if (_exactSubscriptions.TryGetValue(msg.Subject, out var exactChannel))
            exactChannel.Writer.TryWrite(msg);

        if (_wildcardSubscriptions.Count > 0)
        {
            _wildcardLock.EnterReadLock();
            try
            {
                foreach (var (pattern, channel) in _wildcardSubscriptions)
                    if (Matches(pattern, msg.Subject))
                        channel.Writer.TryWrite(msg);
            }
            finally
            {
                _wildcardLock.ExitReadLock();
            }
        }
    }

    private static bool Matches(string pattern, string subject)
    {
        if (pattern == ">") return true;
        if (pattern.EndsWith(".>"))
            return subject.StartsWith(pattern.AsSpan(0, pattern.Length - 1), StringComparison.OrdinalIgnoreCase);

        var patternParts = pattern.Split('.');
        var subjectParts = subject.Split('.');

        for (int i = 0; i < patternParts.Length; i++)
        {
            if (patternParts[i] == ">") return true;
            if (i >= subjectParts.Length) return false;
            if (patternParts[i] != "*" && !string.Equals(patternParts[i], subjectParts[i], StringComparison.OrdinalIgnoreCase))
                return false;
        }
        return patternParts.Length == subjectParts.Length;
    }

    public ValueTask PublishAsync(string subject, ReadOnlySequence<byte> payload, string? replyTo = null, CancellationToken ct = default)
    {
        if (payload.IsSingleSegment) return _connection.SendMessageAsync(subject, replyTo, payload.First, ct);
        return _connection.SendMessageAsync(subject, replyTo, payload.ToArray(), ct);
    }

    public ValueTask PublishAsync(string subject, ReadOnlyMemory<byte> payload, string? replyTo = null, CancellationToken ct = default)
        => _connection.SendMessageAsync(subject, replyTo, payload, ct);

    public ValueTask PublishAsync(string subject, string payload, string? replyTo = null, CancellationToken ct = default)
        => PublishAsync(subject, Encoding.UTF8.GetBytes(payload), replyTo, ct);

    public async Task<CosmoMessage> RequestAsync(string subject, ReadOnlyMemory<byte> payload, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var inbox = $"_INBOX.{Guid.NewGuid():N}";
        var tcs = new TaskCompletionSource<CosmoMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout ?? TimeSpan.FromSeconds(5));

        var sid = Interlocked.Increment(ref _nextSid);
        _requests[inbox] = tcs;

        await _connection.SendCommandAsync($"SUB {inbox} {sid}", ct);
        await PublishAsync(subject, payload, inbox, ct);

        try
        {
            using var reg = cts.Token.Register(() => tcs.TrySetCanceled());
            return await tcs.Task;
        }
        finally
        {
            _requests.TryRemove(inbox, out _);
            _ = _connection.SendCommandAsync($"UNSUB {sid}");
        }
    }

    public async IAsyncEnumerable<CosmoMessage> SubscribeAsync(string subject, string? queueGroup = null, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var sid = Interlocked.Increment(ref _nextSid).ToString();
        var channel = Channel.CreateUnbounded<CosmoMessage>(new UnboundedChannelOptions { SingleReader = true });

        bool isWildcard = subject.Contains('*') || subject.Contains('>');
        if (isWildcard)
        {
            _wildcardLock.EnterWriteLock();
            _wildcardSubscriptions.Add((subject, channel));
            _wildcardLock.ExitWriteLock();
        }
        else
        {
            _exactSubscriptions[subject] = channel;
        }

        var qGroupStr = queueGroup != null ? $" {queueGroup}" : "";
        await _connection.SendCommandAsync($"SUB {subject}{qGroupStr} {sid}", ct);

        try
        {
            await foreach (var msg in channel.Reader.ReadAllAsync(ct))
                yield return msg;
        }
        finally
        {
            if (isWildcard)
            {
                _wildcardLock.EnterWriteLock();
                _wildcardSubscriptions.RemoveAll(x => x.Pattern == subject && x.Channel == channel);
                _wildcardLock.ExitWriteLock();
            }
            else
            {
                _exactSubscriptions.TryRemove(subject, out _);
            }
            await _connection.SendCommandAsync($"UNSUB {sid}", CancellationToken.None);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var sub in _exactSubscriptions.Values) sub.Writer.TryComplete();
        _wildcardLock.EnterReadLock();
        foreach (var sub in _wildcardSubscriptions) sub.Channel.Writer.TryComplete();
        _wildcardLock.ExitReadLock();

        await _connection.DisposeAsync();
        _wildcardLock.Dispose();
    }
}
