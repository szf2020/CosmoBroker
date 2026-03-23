using System.Buffers;
using System.Buffers.Text;
using System.Text;

namespace CosmoBroker.Client.Protocol;

internal enum CommandType { Unknown, Ping, Pong, Ok, Err, Msg, HMsg, Info }

internal readonly struct ParsedCommand
{
    public readonly CommandType Type;
    public readonly string Subject;
    public readonly string? ReplyTo;
    public readonly ReadOnlySequence<byte> Payload;

    public ParsedCommand(CommandType type)
    {
        Type = type;
        Subject = string.Empty;
        ReplyTo = null;
        Payload = ReadOnlySequence<byte>.Empty;
    }

    public ParsedCommand(CommandType type, string subject, string? replyTo, ReadOnlySequence<byte> payload)
    {
        Type = type;
        Subject = subject;
        ReplyTo = replyTo;
        Payload = payload;
    }
}

internal static class ProtocolParser
{
    public static bool TryParseCommand(ref ReadOnlySequence<byte> buffer, out ParsedCommand cmd)
    {
        cmd = default;
        var reader = new SequenceReader<byte>(buffer);

        if (!reader.TryReadTo(out ReadOnlySequence<byte> line, "\r\n"u8))
            return false;

        var firstSpan = line.IsSingleSegment ? line.FirstSpan : line.ToArray().AsSpan();

        CommandType type;
        if      (firstSpan.StartsWith("MSG "u8))   type = CommandType.Msg;
        else if (firstSpan.StartsWith("HMSG "u8))  type = CommandType.HMsg;
        else if (firstSpan.StartsWith("PING"u8))   type = CommandType.Ping;
        else if (firstSpan.StartsWith("PONG"u8))   type = CommandType.Pong;
        else if (firstSpan.StartsWith("+OK"u8))    type = CommandType.Ok;
        else if (firstSpan.StartsWith("-ERR"u8))   type = CommandType.Err;
        else if (firstSpan.StartsWith("INFO "u8))  type = CommandType.Info;
        else                                        type = CommandType.Unknown;

        if (type != CommandType.Msg && type != CommandType.HMsg)
        {
            buffer = buffer.Slice(reader.Position);
            cmd = new ParsedCommand(type);
            return true;
        }

        // Parse: MSG <subject> <sid> [reply-to] <#bytes>
        //       HMSG <subject> <sid> [reply-to] <#hdr bytes> <#total bytes>
        int prefixLen = type == CommandType.Msg ? 4 : 5;
        var remaining = firstSpan.Slice(prefixLen);

        // subject
        int spaceIdx = remaining.IndexOf((byte)' ');
        if (spaceIdx < 0) { buffer = buffer.Slice(reader.Position); cmd = new ParsedCommand(type); return true; }
        string subject = Encoding.UTF8.GetString(remaining.Slice(0, spaceIdx));
        remaining = remaining.Slice(spaceIdx + 1);

        // sid — skip it
        spaceIdx = remaining.IndexOf((byte)' ');
        if (spaceIdx < 0) { buffer = buffer.Slice(reader.Position); cmd = new ParsedCommand(type); return true; }
        remaining = remaining.Slice(spaceIdx + 1);

        // optional reply-to: present when first byte is not a digit
        string? replyTo = null;
        if (remaining.Length > 0 && (remaining[0] < (byte)'0' || remaining[0] > (byte)'9'))
        {
            spaceIdx = remaining.IndexOf((byte)' ');
            if (spaceIdx < 0) { buffer = buffer.Slice(reader.Position); cmd = new ParsedCommand(type); return true; }
            replyTo = Encoding.UTF8.GetString(remaining.Slice(0, spaceIdx));
            remaining = remaining.Slice(spaceIdx + 1);
        }

        // HMSG has an extra hdr_bytes token before total_bytes — skip it
        if (type == CommandType.HMsg)
        {
            spaceIdx = remaining.IndexOf((byte)' ');
            if (spaceIdx < 0) { buffer = buffer.Slice(reader.Position); cmd = new ParsedCommand(type); return true; }
            remaining = remaining.Slice(spaceIdx + 1);
        }

        if (!Utf8Parser.TryParse(remaining, out int payloadSize, out _))
        {
            buffer = buffer.Slice(reader.Position);
            cmd = new ParsedCommand(type);
            return true;
        }

        if (reader.Remaining < payloadSize + 2)
            return false;

        var payload = buffer.Slice(reader.Position, payloadSize);
        buffer = buffer.Slice(buffer.GetPosition(payloadSize + 2, reader.Position));
        cmd = new ParsedCommand(type, subject, replyTo, payload);
        return true;
    }
}
