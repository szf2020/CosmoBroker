using System;
using System.Buffers;
using System.Text;

namespace CosmoBroker;

public static class NatsParser
{
    public static void ParseCommand(BrokerConnection connection, ReadOnlySequence<byte> line, ref ReadOnlySequence<byte> fullBuffer, out bool msgParsed)
    {
        msgParsed = false;
        string commandStr = Encoding.UTF8.GetString(line).TrimEnd('\r');
        if (string.IsNullOrWhiteSpace(commandStr)) return;

        var parts = commandStr.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return;

        string verb = parts[0].ToUpperInvariant();

        switch (verb)
        {
            case "PING":
                connection.HandlePing();
                break;
                
            case "SUB":
                if (parts.Length >= 3)
                {
                    // Standard NATS: SUB <subject> [queue group] <sid>
                    // Our extension: [durable name] at the end
                    
                    string subject = parts[1];
                    string sid;
                    string? queueGroup = null;
                    string? durable = null;

                    if (parts.Length == 3)
                    {
                        // SUB <subject> <sid>
                        sid = parts[2];
                    }
                    else if (parts.Length == 4)
                    {
                        // SUB <subject> <group> <sid>  OR  SUB <subject> <sid> <durable>
                        // We'll assume if the 3rd part is numeric, it's an SID
                        if (int.TryParse(parts[2], out _)) {
                            sid = parts[2];
                            durable = parts[3];
                        } else {
                            queueGroup = parts[2];
                            sid = parts[3];
                        }
                    }
                    else
                    {
                        // SUB <subject> <group> <sid> <durable>
                        queueGroup = parts[2];
                        sid = parts[3];
                        durable = parts[4];
                    }

                    connection.HandleSub(subject, sid, queueGroup, durable);
                }
                break;

            case "UNSUB":
                if (parts.Length >= 2)
                {
                    // UNSUB <sid> [max_msgs]
                    string sid = parts[1];
                    connection.HandleUnsub(sid);
                }
                break;

            case "PUB":
                // Handled in BrokerConnection.cs for payload framing
                break;
                
            case "CONNECT":
                {
                    // CONNECT { "option": value, ... }
                    int firstBrace = commandStr.IndexOf('{');
                    if (firstBrace != -1)
                    {
                        string json = commandStr.Substring(firstBrace);
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
                break;
        }
    }
}
