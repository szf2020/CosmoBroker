using CosmoBroker.JetStream;
using System.Text;

namespace CosmoBroker.Services
{
    public class JetStreamService
    {
        private readonly Dictionary<string, JetStream.Stream> _streams = new();

        public JetStream.Stream CreateStream(string name, string subjectPattern)
        {
            var stream = new JetStream.Stream(name, subjectPattern);
            _streams[name] = stream;
            return stream;
        }

        public void AddConsumer(Consumer consumer)
        {
            if (_streams.TryGetValue(consumer.StreamName, out var stream))
            {
                stream.Consumers.Add(consumer);
                ReplayMissedMessages(consumer, stream);
            }
        }

        public async Task Publish(string streamName, string subject, byte[] payload)
        {
            if (!_streams.TryGetValue(streamName, out var stream))
                throw new Exception($"Stream {streamName} does not exist");

            var msg = stream.AddMessage(subject, payload);
            await BroadcastToConsumers(stream, msg);
        }

        private async Task BroadcastToConsumers(JetStream.Stream stream, StreamMessage msg)
        {
            foreach (var consumer in stream.Consumers)
            {
                if (SubjectMatches(stream.SubjectPattern, msg.Subject))
                {
                    await consumer.PipeWriter.WriteAsync(FormatMessage(msg));
                    await consumer.PipeWriter.FlushAsync();
                    consumer.LastDeliveredSeq = msg.Sequence;
                }
            }
        }

        private void ReplayMissedMessages(Consumer consumer, JetStream.Stream stream)
        {
            var missed = stream.Messages.Where(m => m.Sequence > consumer.LastDeliveredSeq);
            foreach (var msg in missed)
            {
                _ = consumer.PipeWriter.WriteAsync(FormatMessage(msg));
                consumer.LastDeliveredSeq = msg.Sequence;
            }
        }

        private bool SubjectMatches(string pattern, string subject)
        {
            // Simple wildcard matching: * = one token, > = all remaining
            var pTokens = pattern.Split('.');
            var sTokens = subject.Split('.');

            for (int i = 0; i < pTokens.Length; i++)
            {
                if (i >= sTokens.Length) return false;
                if (pTokens[i] == ">") return true;
                if (pTokens[i] == "*") continue;
                if (pTokens[i] != sTokens[i]) return false;
            }

            return pTokens.Length == sTokens.Length;
        }

        private ReadOnlyMemory<byte> FormatMessage(StreamMessage msg)
        {
            var header = $"MSG {msg.Subject} {msg.Sequence} {msg.Payload.Length}\r\n";
            var headerBytes = Encoding.UTF8.GetBytes(header);
            var full = new byte[headerBytes.Length + msg.Payload.Length + 2];
            Buffer.BlockCopy(headerBytes, 0, full, 0, headerBytes.Length);
            Buffer.BlockCopy(msg.Payload, 0, full, headerBytes.Length, msg.Payload.Length);
            full[^2] = (byte)'\r';
            full[^1] = (byte)'\n';
            return full;
        }
    }
}