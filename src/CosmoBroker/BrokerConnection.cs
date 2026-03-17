using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace CosmoBroker;

public class BrokerConnection
{
    private readonly Socket _socket;
    private readonly TopicTree _topicTree;
    private readonly Persistence.MessageRepository? _repo;
    private readonly Auth.IAuthenticator? _authenticator;
    private readonly Pipe _readerPipe;
    private readonly Pipe _writerPipe;
    
    private bool _isAuthenticated = false;

    // Tracks active subscriptions for this connection: SID -> (Subject, QueueGroup)
    private readonly ConcurrentDictionary<string, (string subject, string? queueGroup)> _subscriptions = new();

    public BrokerConnection(Socket socket, TopicTree topicTree, Persistence.MessageRepository? repo = null, Auth.IAuthenticator? authenticator = null)
    {
        _socket = socket;
        _topicTree = topicTree;
        _repo = repo;
        _authenticator = authenticator;
        _readerPipe = new Pipe();
        _writerPipe = new Pipe();

        // If no authenticator is provided, we are authenticated by default
        _isAuthenticated = _authenticator == null;
    }

    public async Task RunAsync()
    {
        var readTask = FillPipeAsync(_socket, _readerPipe.Writer);
        var processTask = ProcessPipeAsync(_readerPipe.Reader);
        var writeTask = FlushPipeAsync(_socket, _writerPipe.Reader);

        // Send initial INFO block (NATS handshake)
        var info = "INFO {\"server_id\":\"cosmo-broker\",\"version\":\"1.0.0\"}\r\n"u8;
        _writerPipe.Writer.Write(info);
        await _writerPipe.Writer.FlushAsync();

        await Task.WhenAny(readTask, processTask, writeTask);
        
        Cleanup();
    }

    private async Task FillPipeAsync(Socket socket, PipeWriter writer)
    {
        const int minimumBufferSize = 512;

        try
        {
            while (true)
            {
                var memory = writer.GetMemory(minimumBufferSize);
                int bytesRead = await socket.ReceiveAsync(memory, SocketFlags.None);
                if (bytesRead == 0) break; // EOF

                writer.Advance(bytesRead);
                var result = await writer.FlushAsync();
                if (result.IsCompleted || result.IsCanceled) break;
            }
        }
        catch (Exception ex) { Console.WriteLine($"[BrokerConnection] FillPipeAsync error: {ex.Message}"); }
        finally { await writer.CompleteAsync(); }
    }

    private async Task ProcessPipeAsync(PipeReader reader)
    {
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;
                Console.WriteLine($"[BrokerConnection] ProcessPipeAsync: Read {buffer.Length} bytes from pipe");

                while (true)
                {
                    var linePosition = buffer.PositionOf((byte)'\n');
                    if (linePosition == null) break;

                    var line = buffer.Slice(0, linePosition.Value);
                    string lineStr = Encoding.UTF8.GetString(line).TrimEnd('\r');
                    
                    // Simple parse of the verb to see if it's a PUB
                    var parts = lineStr.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    
                    if (parts.Length > 0 && string.Equals(parts[0], "PUB", StringComparison.OrdinalIgnoreCase))
                    {
                        if (parts.Length >= 3 && int.TryParse(parts[parts.Length - 1], out int payloadLength))
                        {
                            var payloadStart = buffer.GetPosition(1, linePosition.Value);
                            var remaining = buffer.Slice(payloadStart);
                            
                            if (remaining.Length >= payloadLength + 2)
                            {
                                var payload = remaining.Slice(0, payloadLength);
                                HandlePub(parts[1], payload);
                                
                                buffer = remaining.Slice(payloadLength + 2);
                                continue; 
                            }
                            else
                            {
                                // Incomplete payload, wait for more data
                                break;
                            }
                        }
                    }
                    
                    NatsParser.ParseCommand(this, line, ref buffer, out bool _);
                    buffer = buffer.Slice(buffer.GetPosition(1, linePosition.Value));
                }

                reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted) break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[BrokerConnection] Process error: {ex.Message}");
        }
        finally { await reader.CompleteAsync(); }
    }

    private async Task FlushPipeAsync(Socket socket, PipeReader reader)
    {
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;

                foreach (var segment in buffer)
                {
                    await socket.SendAsync(segment, SocketFlags.None);
                }

                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted || result.IsCanceled) break;
            }
        }
        catch { }
        finally { await reader.CompleteAsync(); }
    }

    public void HandleSub(string subject, string sid, string? queueGroup = null, string? durableName = null)
    {
        if (!_isAuthenticated) return;
        _subscriptions[sid] = (subject, queueGroup);
        _topicTree.Subscribe(subject, this, sid, queueGroup);

        if (!string.IsNullOrEmpty(durableName) && _repo != null)
        {
            // Replay messages from DB
            _ = Task.Run(async () =>
            {
                long lastId = await _repo.GetConsumerOffsetAsync(durableName);
                var messages = await _repo.GetMessagesAsync(subject, lastId);
                
                foreach (var msg in messages)
                {
                    SendMessage(msg.Subject, sid, new ReadOnlySequence<byte>(msg.Payload));
                    lastId = msg.Id;
                }

                if (lastId > 0)
                {
                    await _repo.UpdateConsumerOffsetAsync(durableName, subject, lastId);
                }
            });
        }
    }

    public void HandleUnsub(string sid)
    {
        if (_subscriptions.TryRemove(sid, out var sub))
        {
            _topicTree.Unsubscribe(sub.subject, this, sid, sub.queueGroup);
        }
    }

    public void HandlePub(string subject, ReadOnlySequence<byte> payload)
    {
        if (!_isAuthenticated) return;
        if (_repo != null && subject.StartsWith("persist.", StringComparison.OrdinalIgnoreCase))
        {
            var bytes = payload.ToArray();
            _ = Task.Run(async () => await _repo.SaveMessageAsync(subject, bytes));
        }
        _topicTree.Publish(subject, payload);
    }

    public void HandlePing()
    {
        _writerPipe.Writer.Write("PONG\r\n"u8);
        _ = _writerPipe.Writer.FlushAsync();
    }

    public async Task HandleConnect(Auth.ConnectOptions options)
    {
        if (_authenticator != null)
        {
            _isAuthenticated = await _authenticator.AuthenticateAsync(options);
            if (!_isAuthenticated)
            {
                _writerPipe.Writer.Write("-ERR 'Authentication Failed'\r\n"u8);
                await _writerPipe.Writer.FlushAsync();
                _socket.Close();
            }
            else
            {
                _writerPipe.Writer.Write("+OK\r\n"u8);
                await _writerPipe.Writer.FlushAsync();
            }
        }
    }

    public void SendMessage(string subject, string sid, ReadOnlySequence<byte> payload)
    {
        // Format: MSG <subject> <sid> <bytes>\r\n<payload>\r\n
        var subjectBytes = Encoding.UTF8.GetBytes(subject);
        var sidBytes = Encoding.UTF8.GetBytes(sid);
        var lengthBytes = Encoding.UTF8.GetBytes(payload.Length.ToString());

        var writer = _writerPipe.Writer;
        writer.Write("MSG "u8);
        writer.Write(subjectBytes);
        writer.Write(" "u8);
        writer.Write(sidBytes);
        writer.Write(" "u8);
        writer.Write(lengthBytes);
        writer.Write("\r\n"u8);
        
        foreach (var segment in payload)
        {
            writer.Write(segment.Span);
        }
        writer.Write("\r\n"u8);
        
        _ = writer.FlushAsync();
    }

    private void Cleanup()
    {
        foreach (var sub in _subscriptions)
        {
            _topicTree.Unsubscribe(sub.Value.subject, this, sub.Key, sub.Value.queueGroup);
        }
        _subscriptions.Clear();
        try { _socket.Close(); } catch { }
    }
}
