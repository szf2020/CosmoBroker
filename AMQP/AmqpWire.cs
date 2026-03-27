using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CosmoBroker.AMQP;

internal static class AmqpWire
{
    public const byte FrameMethod = 1;
    public const byte FrameHeader = 2;
    public const byte FrameBody = 3;
    public const byte FrameHeartbeat = 8;
    public const byte FrameEnd = 0xCE;

    public static ushort ReadUInt16(ReadOnlySpan<byte> buffer)
        => BinaryPrimitives.ReadUInt16BigEndian(buffer);

    public static uint ReadUInt32(ReadOnlySpan<byte> buffer)
        => BinaryPrimitives.ReadUInt32BigEndian(buffer);

    public static ulong ReadUInt64(ReadOnlySpan<byte> buffer)
        => BinaryPrimitives.ReadUInt64BigEndian(buffer);

    public static void WriteUInt16(IBufferWriter writer, ushort value)
    {
        Span<byte> span = writer.GetSpan(2);
        BinaryPrimitives.WriteUInt16BigEndian(span, value);
        writer.Advance(2);
    }

    public static void WriteUInt32(IBufferWriter writer, uint value)
    {
        Span<byte> span = writer.GetSpan(4);
        BinaryPrimitives.WriteUInt32BigEndian(span, value);
        writer.Advance(4);
    }

    public static void WriteUInt64(IBufferWriter writer, ulong value)
    {
        Span<byte> span = writer.GetSpan(8);
        BinaryPrimitives.WriteUInt64BigEndian(span, value);
        writer.Advance(8);
    }

    public static void WriteByte(IBufferWriter writer, byte value)
    {
        Span<byte> span = writer.GetSpan(1);
        span[0] = value;
        writer.Advance(1);
    }

    public static void WriteShortString(IBufferWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        if (bytes.Length > byte.MaxValue)
            throw new InvalidOperationException("AMQP shortstr too long.");
        WriteByte(writer, (byte)bytes.Length);
        WriteBytes(writer, bytes);
    }

    public static string ReadShortString(ref ReadOnlySpan<byte> buffer)
    {
        int length = buffer[0];
        buffer = buffer[1..];
        string value = Encoding.UTF8.GetString(buffer[..length]);
        buffer = buffer[length..];
        return value;
    }

    public static void WriteLongString(IBufferWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteUInt32(writer, (uint)bytes.Length);
        WriteBytes(writer, bytes);
    }

    public static byte[] ReadLongStringBytes(ref ReadOnlySpan<byte> buffer)
    {
        int length = checked((int)ReadUInt32(buffer));
        buffer = buffer[4..];
        var bytes = buffer[..length].ToArray();
        buffer = buffer[length..];
        return bytes;
    }

    public static string ReadLongString(ref ReadOnlySpan<byte> buffer)
        => Encoding.UTF8.GetString(ReadLongStringBytes(ref buffer));

    public static void WriteTable(IBufferWriter writer, IReadOnlyDictionary<string, object?>? table = null)
    {
        if (table == null || table.Count == 0)
        {
            WriteUInt32(writer, 0);
            return;
        }

        using var ms = new MemoryStream();
        foreach (var (key, value) in table)
        {
            WriteShortString(ms, key);
            WriteFieldValue(ms, value);
        }

        var payload = ms.ToArray();
        WriteUInt32(writer, (uint)payload.Length);
        WriteBytes(writer, payload);
    }

    public static Dictionary<string, object?> ReadTable(ref ReadOnlySpan<byte> buffer)
    {
        int tableLength = checked((int)ReadUInt32(buffer));
        buffer = buffer[4..];
        var tableBytes = buffer[..tableLength];
        buffer = buffer[tableLength..];

        var table = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        while (!tableBytes.IsEmpty)
        {
            string key = ReadShortString(ref tableBytes);
            char kind = (char)tableBytes[0];
            tableBytes = tableBytes[1..];
            table[key] = kind switch
            {
                'S' => ReadLongString(ref tableBytes),
                't' => ReadBoolean(ref tableBytes),
                'B' => ReadByte(ref tableBytes),
                'b' => unchecked((sbyte)ReadByte(ref tableBytes)),
                'U' => ReadUInt16Value(ref tableBytes),
                'u' => ReadUInt16Value(ref tableBytes),
                'I' => ReadInt32(ref tableBytes),
                'i' => ReadInt32(ref tableBytes),
                'l' => ReadInt64(ref tableBytes),
                'L' => ReadInt64(ref tableBytes),
                'f' => ReadSingle(ref tableBytes),
                'd' => ReadDouble(ref tableBytes),
                'T' => ReadTimestamp(ref tableBytes),
                'F' => ReadTable(ref tableBytes),
                'A' => ReadArray(ref tableBytes),
                'D' => ReadDecimal(ref tableBytes),
                'x' => ReadLongStringBytes(ref tableBytes),
                'V' => null,
                _ => SkipFieldValue(kind, ref tableBytes)
            };
        }

        return table;
    }

    public static bool ReadBoolean(ref ReadOnlySpan<byte> buffer)
    {
        bool value = buffer[0] != 0;
        buffer = buffer[1..];
        return value;
    }

    public static byte ReadByte(ref ReadOnlySpan<byte> buffer)
    {
        byte value = buffer[0];
        buffer = buffer[1..];
        return value;
    }

    public static int ReadInt32(ref ReadOnlySpan<byte> buffer)
    {
        int value = unchecked((int)ReadUInt32(buffer));
        buffer = buffer[4..];
        return value;
    }

    public static long ReadInt64(ref ReadOnlySpan<byte> buffer)
    {
        long value = unchecked((long)ReadUInt64(buffer));
        buffer = buffer[8..];
        return value;
    }

    public static float ReadSingle(ref ReadOnlySpan<byte> buffer)
    {
        float value = BitConverter.Int32BitsToSingle(ReadInt32(ref buffer));
        return value;
    }

    public static double ReadDouble(ref ReadOnlySpan<byte> buffer)
    {
        double value = BitConverter.Int64BitsToDouble(ReadInt64(ref buffer));
        return value;
    }

    public static void WriteBytes(IBufferWriter writer, ReadOnlySpan<byte> bytes)
    {
        var span = writer.GetSpan(bytes.Length);
        bytes.CopyTo(span);
        writer.Advance(bytes.Length);
    }

    private static void WriteShortString(Stream stream, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        stream.WriteByte((byte)bytes.Length);
        stream.Write(bytes, 0, bytes.Length);
    }

    private static void WriteLongString(Stream stream, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteUInt32(stream, (uint)bytes.Length);
        stream.Write(bytes, 0, bytes.Length);
    }

    private static void WriteUInt32(Stream stream, uint value)
    {
        Span<byte> span = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(span, value);
        stream.Write(span);
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> span = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(span, value);
        stream.Write(span);
    }

    private static void WriteInt64(Stream stream, long value)
    {
        Span<byte> span = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(span, value);
        stream.Write(span);
    }

    private static void WriteUInt16(Stream stream, ushort value)
    {
        Span<byte> span = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(span, value);
        stream.Write(span);
    }

    private static void WriteUInt64(Stream stream, ulong value)
    {
        Span<byte> span = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(span, value);
        stream.Write(span);
    }

    private static ushort ReadUInt16Value(ref ReadOnlySpan<byte> buffer)
    {
        ushort value = ReadUInt16(buffer);
        buffer = buffer[2..];
        return value;
    }

    private static DateTime ReadTimestamp(ref ReadOnlySpan<byte> buffer)
    {
        ulong seconds = ReadUInt64(buffer);
        buffer = buffer[8..];
        return DateTimeOffset.FromUnixTimeSeconds((long)seconds).UtcDateTime;
    }

    private static object[] ReadArray(ref ReadOnlySpan<byte> buffer)
    {
        int length = checked((int)ReadUInt32(buffer));
        buffer = buffer[4..];
        var arrayBytes = buffer[..length];
        buffer = buffer[length..];
        var values = new List<object?>();
        while (!arrayBytes.IsEmpty)
        {
            char kind = (char)arrayBytes[0];
            arrayBytes = arrayBytes[1..];
            values.Add(SkipFieldValue(kind, ref arrayBytes));
        }
        return values.ToArray()!;
    }

    private static decimal ReadDecimal(ref ReadOnlySpan<byte> buffer)
    {
        byte scale = ReadByte(ref buffer);
        int raw = ReadInt32(ref buffer);
        return raw / (decimal)Math.Pow(10, scale);
    }

    private static void WriteFieldValue(Stream stream, object? value)
    {
        switch (value)
        {
            case null:
                stream.WriteByte((byte)'V');
                break;
            case string s:
                stream.WriteByte((byte)'S');
                WriteLongString(stream, s);
                break;
            case bool b:
                stream.WriteByte((byte)'t');
                stream.WriteByte((byte)(b ? 1 : 0));
                break;
            case byte byt:
                stream.WriteByte((byte)'B');
                stream.WriteByte(byt);
                break;
            case sbyte sb:
                stream.WriteByte((byte)'b');
                stream.WriteByte(unchecked((byte)sb));
                break;
            case short shortValue:
                stream.WriteByte((byte)'s');
                WriteUInt16(stream, unchecked((ushort)shortValue));
                break;
            case ushort ushortValue:
                stream.WriteByte((byte)'u');
                WriteUInt16(stream, ushortValue);
                break;
            case int intValue:
                stream.WriteByte((byte)'I');
                WriteInt32(stream, intValue);
                break;
            case uint uintValue when uintValue <= int.MaxValue:
                stream.WriteByte((byte)'I');
                WriteInt32(stream, checked((int)uintValue));
                break;
            case long longValue:
                stream.WriteByte((byte)'l');
                WriteInt64(stream, longValue);
                break;
            case ulong ulongValue when ulongValue <= long.MaxValue:
                stream.WriteByte((byte)'l');
                WriteInt64(stream, checked((long)ulongValue));
                break;
            case float floatValue:
                stream.WriteByte((byte)'f');
                WriteInt32(stream, BitConverter.SingleToInt32Bits(floatValue));
                break;
            case double doubleValue:
                stream.WriteByte((byte)'d');
                WriteInt64(stream, BitConverter.DoubleToInt64Bits(doubleValue));
                break;
            case decimal decimalValue:
                stream.WriteByte((byte)'D');
                WriteDecimal(stream, decimalValue);
                break;
            case byte[] bytes:
                stream.WriteByte((byte)'x');
                WriteUInt32(stream, (uint)bytes.Length);
                stream.Write(bytes, 0, bytes.Length);
                break;
            case DateTimeOffset dto:
                stream.WriteByte((byte)'T');
                WriteUInt64(stream, checked((ulong)dto.ToUnixTimeSeconds()));
                break;
            case DateTime dt:
                stream.WriteByte((byte)'T');
                WriteUInt64(stream, checked((ulong)new DateTimeOffset(dt.ToUniversalTime()).ToUnixTimeSeconds()));
                break;
            case IReadOnlyDictionary<string, object?> nested:
                stream.WriteByte((byte)'F');
                WriteNestedTable(stream, nested);
                break;
            case IDictionary<string, object?> nested:
                stream.WriteByte((byte)'F');
                WriteNestedTable(stream, new Dictionary<string, object?>(nested, StringComparer.OrdinalIgnoreCase));
                break;
            case IEnumerable<object?> array when value is not string && value is not byte[]:
                stream.WriteByte((byte)'A');
                WriteFieldArray(stream, array);
                break;
            default:
                stream.WriteByte((byte)'S');
                WriteLongString(stream, value.ToString() ?? string.Empty);
                break;
        }
    }

    private static void WriteNestedTable(Stream stream, IReadOnlyDictionary<string, object?> table)
    {
        using var payload = new MemoryStream();
        foreach (var (key, value) in table)
        {
            WriteShortString(payload, key);
            WriteFieldValue(payload, value);
        }

        byte[] bytes = payload.ToArray();
        WriteUInt32(stream, (uint)bytes.Length);
        stream.Write(bytes, 0, bytes.Length);
    }

    private static void WriteFieldArray(Stream stream, IEnumerable<object?> values)
    {
        using var payload = new MemoryStream();
        foreach (var value in values)
            WriteFieldValue(payload, value);

        byte[] bytes = payload.ToArray();
        WriteUInt32(stream, (uint)bytes.Length);
        stream.Write(bytes, 0, bytes.Length);
    }

    private static void WriteDecimal(Stream stream, decimal value)
    {
        int[] bits = decimal.GetBits(value);
        int scale = (bits[3] >> 16) & 0x7F;
        if (bits[1] != 0 || bits[2] != 0 || scale > byte.MaxValue)
            throw new NotSupportedException("AMQP decimal values larger than 32-bit mantissa are not supported.");

        int raw = bits[0];
        if ((bits[3] & unchecked((int)0x80000000)) != 0)
            raw = -raw;

        stream.WriteByte((byte)scale);
        WriteInt32(stream, raw);
    }

    private static object? SkipFieldValue(char kind, ref ReadOnlySpan<byte> buffer)
    {
        return kind switch
        {
            'S' => ReadLongString(ref buffer),
            's' => unchecked((short)ReadUInt16Value(ref buffer)),
            'U' => ReadUInt16Value(ref buffer),
            'u' => ReadUInt16Value(ref buffer),
            'I' => ReadInt32(ref buffer),
            'i' => ReadInt32(ref buffer),
            'L' => ReadInt64(ref buffer),
            'l' => ReadInt64(ref buffer),
            'f' => ReadSingle(ref buffer),
            'd' => ReadDouble(ref buffer),
            't' => ReadBoolean(ref buffer),
            'b' => unchecked((sbyte)ReadByte(ref buffer)),
            'B' => ReadByte(ref buffer),
            'T' => ReadTimestamp(ref buffer),
            'F' => ReadTable(ref buffer),
            'A' => ReadArray(ref buffer),
            'D' => ReadDecimal(ref buffer),
            'V' => null,
            'x' => ReadLongStringBytes(ref buffer),
            _ => throw new NotSupportedException($"AMQP table field type '{kind}' is not supported.")
        };
    }
}

internal interface IBufferWriter
{
    Span<byte> GetSpan(int sizeHint);
    void Advance(int count);
}

internal sealed class BufferBuilder : IBufferWriter
{
    private byte[] _buffer = new byte[256];
    private int _length;

    public Span<byte> GetSpan(int sizeHint)
    {
        Ensure(sizeHint);
        return _buffer.AsSpan(_length);
    }

    public void Advance(int count) => _length += count;

    public byte[] ToArray() => _buffer.AsSpan(0, _length).ToArray();

    private void Ensure(int sizeHint)
    {
        int required = _length + sizeHint;
        if (required <= _buffer.Length) return;
        Array.Resize(ref _buffer, Math.Max(required, _buffer.Length * 2));
    }
}
