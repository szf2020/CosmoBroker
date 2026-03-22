using System;
using System.Buffers;
using System.Text;

namespace CosmoBroker;

public static class NatsParser
{
    public static void ParseCommand(BrokerConnection connection, ReadOnlySequence<byte> line, ref ReadOnlySequence<byte> fullBuffer, out bool msgParsed)
    {
        msgParsed = false;
        if (line.IsEmpty) return;

        var lineSpan = line.IsSingleSegment ? line.FirstSpan : line.ToArray().AsSpan();
        if (lineSpan.Length < 3) return;

        // Command detection via Span (case insensitive)
        if (StartsWith(lineSpan, "PING")) { connection.HandlePing(); return; }
        if (StartsWith(lineSpan, "PONG")) { return; }
        if (StartsWith(lineSpan, "INFO"))
        {
            if (connection.IsRoute || connection.IsLeaf) return;
            connection.SendInfo();
            return;
        }

        // Optimized parsing using Span to avoid Split and multiple string allocations
        int spaceIdx = lineSpan.IndexOf((byte)' ');
        if (spaceIdx == -1) return;

        var verb = lineSpan.Slice(0, spaceIdx);
        var rest = lineSpan.Slice(spaceIdx + 1);

        if (Equals(verb, "SUB"))
        {
            // SUB <subject> [queue group] <sid>
            var parts = new Span<Range>(new Range[8]);
            int count = Split(rest, (byte)' ', parts);
            if (count >= 2)
            {
                string subject = GetString(rest[parts[0]]);
                string sid;
                string? queueGroup = null;
                string? durable = null;

                if (count == 2)
                {
                    sid = GetString(rest[parts[1]]);
                }
                else if (count == 3)
                {
                    // Could be SUB <subj> <sid> <durable> or SUB <subj> <group> <sid>
                    var p1 = rest[parts[1]];
                    if (IsDigit(p1))
                    {
                        sid = GetString(p1);
                        durable = GetString(rest[parts[2]]);
                    }
                    else
                    {
                        queueGroup = GetString(p1);
                        sid = GetString(rest[parts[2]]);
                    }
                }
                else
                {
                    queueGroup = GetString(rest[parts[1]]);
                    sid = GetString(rest[parts[2]]);
                    durable = GetString(rest[parts[3]]);
                }
                connection.HandleSub(subject, sid, queueGroup, durable, isRemote: connection.IsRoute);
            }
        }
        else if (Equals(verb, "UNSUB"))
        {
            var parts = new Span<Range>(new Range[4]);
            int count = Split(rest, (byte)' ', parts);
            if (count >= 1)
            {
                string sid = GetString(rest[parts[0]]);
                int? maxMsgs = null;
                if (count >= 2)
                {
                    if (System.Buffers.Text.Utf8Parser.TryParse(rest[parts[1]], out int m, out _))
                        maxMsgs = m;
                }
                connection.HandleUnsub(sid, maxMsgs);
            }
        }
        else if (Equals(verb, "CONNECT"))
        {
            int firstBrace = rest.IndexOf((byte)'{');
            if (firstBrace != -1)
            {
                var jsonSpan = rest.Slice(firstBrace);
                string json = Encoding.UTF8.GetString(jsonSpan).TrimEnd('\r');
                try
                {
                    var options = System.Text.Json.JsonSerializer.Deserialize<Auth.ConnectOptions>(json);
                    if (options != null)
                    {
                        _ = connection.HandleConnect(options);
                    }
                }
                catch { }
            }
        }
    }

    private static string GetString(ReadOnlySpan<byte> span) => Encoding.UTF8.GetString(span).TrimEnd('\r');

    private static bool IsDigit(ReadOnlySpan<byte> span)
    {
        if (span.IsEmpty) return false;
        foreach (var b in span) if (b < '0' || b > '9') return false;
        return true;
    }

    private static int Split(ReadOnlySpan<byte> span, byte separator, Span<Range> ranges)
    {
        int count = 0;
        int start = 0;
        for (int i = 0; i < span.Length; i++)
        {
            if (span[i] == separator)
            {
                if (i > start)
                {
                    ranges[count++] = new Range(start, i);
                    if (count == ranges.Length) return count;
                }
                start = i + 1;
            }
        }
        if (start < span.Length)
        {
            int end = span.Length;
            if (span[end - 1] == '\r') end--;
            if (end > start) ranges[count++] = new Range(start, end);
        }
        return count;
    }

    private static bool Equals(ReadOnlySpan<byte> span, string verb)
    {
        if (span.Length != verb.Length) return false;
        for (int i = 0; i < verb.Length; i++)
        {
            byte b = span[i];
            char c = verb[i];
            if (b != c && b != (c + 32) && b != (c - 32)) return false;
        }
        return true;
    }

    private static bool StartsWith(ReadOnlySpan<byte> span, string verb)
    {
        if (span.Length < verb.Length) return false;
        for (int i = 0; i < verb.Length; i++)
        {
            byte b = span[i];
            char c = verb[i];
            if (b != c && b != (c + 32) && b != (c - 32)) return false; 
        }
        return span.Length == verb.Length || span[verb.Length] == ' ' || span[verb.Length] == '\r';
    }
}
