using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace CosmoBroker.Client;

public class CosmoMessage
{
    public string Subject { get; init; } = "";
    public string? ReplyTo { get; init; }
    /// <summary>Body only — header bytes are stripped for HMSG messages.</summary>
    public ReadOnlySequence<byte> Data { get; init; }
    /// <summary>Parsed NATS headers, or null for plain MSG messages.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    public string GetStringData()
    {
        if (Data.IsEmpty) return string.Empty;
        if (Data.IsSingleSegment) return Encoding.UTF8.GetString(Data.FirstSpan);
        return Encoding.UTF8.GetString(Data.ToArray());
    }
}
