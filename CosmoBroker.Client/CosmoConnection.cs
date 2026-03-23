using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using CosmoBroker.Client.Models;
using CosmoBroker.Client.Protocol;

namespace CosmoBroker.Client;

internal class CosmoConnection : IAsyncDisposable
{
    private readonly CosmoClientOptions _options;
    private Socket? _socket;
    private Stream? _stream;
    private PipeReader? _reader;
    private PipeWriter? _writer;
    private Task? _readLoopTask;
    private Task? _writeLoopTask;
    private readonly CancellationTokenSource _cts = new();

    // Channel carries (rented buffer, used length, return-to-pool flag)
    private readonly Channel<(byte[] Buffer, int Length, bool Pooled)> _writeChannel =
        Channel.CreateUnbounded<(byte[], int, bool)>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

    private readonly ConcurrentQueue<TaskCompletionSource> _pings = new();

    private static readonly StreamPipeReaderOptions ReaderOptions =
        new(bufferSize: 65_536, minimumReadSize: 8_192);

    private static readonly StreamPipeWriterOptions WriterOptions =
        new(minimumBufferSize: 65_536);

    public ServerInfo? ServerInfo { get; private set; }

    public Action<CosmoMessage>? OnMessage;
    public Action? OnDisconnected;

    public CosmoConnection(CosmoClientOptions options)
    {
        _options = options;
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        var uri = new Uri(_options.Url);
        var host = uri.Host;
        var port = uri.Port == -1 ? 4222 : uri.Port;
        var useTls = uri.Scheme == "tls";

        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        {
            NoDelay = true,
            SendBufferSize = 1024 * 1024,
            ReceiveBufferSize = 1024 * 1024
        };
        await _socket.ConnectAsync(host, port, cancellationToken);

        var networkStream = new NetworkStream(_socket, ownsSocket: false);
        _stream = networkStream;
        _reader = PipeReader.Create(_stream, ReaderOptions);
        _writer = PipeWriter.Create(_stream, WriterOptions);

        var infoResult = await _reader.ReadAsync(cancellationToken);
        var infoBuffer = infoResult.Buffer;

        if (TryParseInfo(ref infoBuffer, out var serverInfo))
        {
            ServerInfo = serverInfo;
            _reader.AdvanceTo(infoBuffer.Start, infoBuffer.End);
        }
        else
        {
            throw new Exception("Failed to receive INFO from server.");
        }

        if (useTls || (ServerInfo?.TlsRequired ?? false))
        {
            var sslStream = new SslStream(networkStream, false, (_, _, _, _) => true);
            var sslOptions = new SslClientAuthenticationOptions
            {
                TargetHost = host,
                ClientCertificates = _options.ClientCertificate != null ? new() { _options.ClientCertificate } : null,
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12 | System.Security.Authentication.SslProtocols.Tls13
            };

            await sslStream.AuthenticateAsClientAsync(sslOptions, cancellationToken);
            _stream = sslStream;
            _reader = PipeReader.Create(_stream, ReaderOptions);
            _writer = PipeWriter.Create(_stream, WriterOptions);
        }

        var connectReq = new ConnectRequest
        {
            Verbose = false,
            Pedantic = false,
            TlsRequired = useTls,
            User = _options.Username,
            Pass = _options.Password,
            AuthToken = _options.Token,
            Jwt = _options.Jwt
        };

        var connectJson = JsonSerializer.Serialize(connectReq);
        await SendImmediateAsync(Encoding.UTF8.GetBytes($"CONNECT {connectJson}\r\n"), cancellationToken);
        await SendImmediateAsync("PING\r\n"u8.ToArray(), cancellationToken);

        _writeLoopTask = Task.Run(() => WriteLoopAsync(_cts.Token));
        _readLoopTask = Task.Run(() => ReadLoopAsync(_cts.Token));
    }

    private async Task SendImmediateAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
    {
        await _writer!.WriteAsync(data, ct);
        await _writer.FlushAsync(ct);
    }

    private static bool TryParseInfo(ref ReadOnlySequence<byte> buffer, out ServerInfo? info)
    {
        info = null;
        var reader = new SequenceReader<byte>(buffer);
        if (!reader.TryReadTo(out ReadOnlySequence<byte> line, "\r\n"u8))
            return false;

        var lineSpan = line.IsSingleSegment ? line.FirstSpan : line.ToArray().AsSpan();
        if (!lineSpan.StartsWith("INFO "u8))
            return false;

        var json = Encoding.UTF8.GetString(lineSpan.Slice(5));
        info = JsonSerializer.Deserialize<ServerInfo>(json);
        buffer = buffer.Slice(reader.Position);
        return true;
    }

    private async Task WriteLoopAsync(CancellationToken ct)
    {
        try
        {
            while (await _writeChannel.Reader.WaitToReadAsync(ct))
            {
                while (_writeChannel.Reader.TryRead(out var item))
                {
                    // WriteAsync copies into the pipe's internal buffer; safe to return after.
                    await _writer!.WriteAsync(new ReadOnlyMemory<byte>(item.Buffer, 0, item.Length), ct);
                    if (item.Pooled) ArrayPool<byte>.Shared.Return(item.Buffer);
                }
                await _writer!.FlushAsync(ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { Console.Error.WriteLine($"[CONN] WriteLoop: {ex.Message}"); }
    }

    private async Task ReadLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await _reader!.ReadAsync(ct);
                var buffer = result.Buffer;

                while (ProtocolParser.TryParseCommand(ref buffer, out var cmd))
                {
                    HandleCommand(in cmd);
                }

                _reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted) break;
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { Console.Error.WriteLine($"[CONN] ReadLoop: {ex.Message}"); }
        finally { OnDisconnected?.Invoke(); }
    }

    public async Task PingAsync(CancellationToken ct = default)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _pings.Enqueue(tcs);
        await SendCommandAsync("PING", ct);
        await tcs.Task.WaitAsync(ct);
    }

    private void HandleCommand(in ParsedCommand cmd)
    {
        switch (cmd.Type)
        {
            case CommandType.Pong:
                if (_pings.TryDequeue(out var tcs)) tcs.TrySetResult();
                break;
            case CommandType.Ping:
                _ = SendCommandAsync("PONG");
                break;
            case CommandType.Msg:
            case CommandType.HMsg:
                OnMessage?.Invoke(new CosmoMessage
                {
                    Subject = cmd.Subject,
                    ReplyTo = cmd.ReplyTo,
                    Data = cmd.Payload
                });
                break;
        }
    }

    public ValueTask SendCommandAsync(string command, CancellationToken ct = default)
    {
        var encoded = command + "\r\n";
        var maxLen = Encoding.UTF8.GetMaxByteCount(encoded.Length);
        var buf = ArrayPool<byte>.Shared.Rent(maxLen);
        var written = Encoding.UTF8.GetBytes(encoded, buf);
        return _writeChannel.Writer.WriteAsync((buf, written, true), ct);
    }

    public ValueTask SendMessageAsync(string subject, string? replyTo, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        var subjByteLen = Encoding.UTF8.GetMaxByteCount(subject.Length);
        var replyByteLen = replyTo != null ? Encoding.UTF8.GetMaxByteCount(replyTo.Length) : 0;
        // "PUB " + subject + [" " + replyTo] + " " + <10-digit max> + "\r\n" + payload + "\r\n"
        var maxLen = 4 + subjByteLen + (replyByteLen > 0 ? replyByteLen + 1 : 0) + 12 + payload.Length + 2;

        var buf = ArrayPool<byte>.Shared.Rent(maxLen);
        var span = buf.AsSpan();

        "PUB "u8.CopyTo(span);
        int pos = 4;

        pos += Encoding.UTF8.GetBytes(subject, span.Slice(pos));
        if (replyByteLen > 0)
        {
            span[pos++] = (byte)' ';
            pos += Encoding.UTF8.GetBytes(replyTo!, span.Slice(pos));
        }

        span[pos++] = (byte)' ';
        Utf8Formatter.TryFormat(payload.Length, span.Slice(pos), out int numLen);
        pos += numLen;
        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';

        payload.Span.CopyTo(span.Slice(pos));
        pos += payload.Length;

        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';

        return _writeChannel.Writer.WriteAsync((buf, pos, true), ct);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _writeChannel.Writer.TryComplete();
        if (_readLoopTask != null) await _readLoopTask;
        if (_writeLoopTask != null) await _writeLoopTask;
        if (_writer != null) await _writer.CompleteAsync();
        if (_reader != null) await _reader.CompleteAsync();
        _stream?.Dispose();
        _socket?.Dispose();
    }
}
