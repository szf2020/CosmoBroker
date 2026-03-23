using System;
using System.Buffers;

namespace CosmoBroker.Client;

public class CosmoMessage
{
    public string Subject { get; init; } = "";
    public string? ReplyTo { get; init; }
    public ReadOnlySequence<byte> Data { get; init; }
    
    // Headers could be parsed later, for now we will keep it simple.
    
    public string GetStringData()
    {
        if (Data.IsEmpty) return string.Empty;
        if (Data.IsSingleSegment) return System.Text.Encoding.UTF8.GetString(Data.FirstSpan);
        return System.Text.Encoding.UTF8.GetString(Data.ToArray());
    }
}
