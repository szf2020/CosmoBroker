using System;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace CosmoBroker.JetStream
{
    public class Consumer
    {
        public string Name { get; set; }
        public string StreamName { get; set; }
        public long LastDeliveredSeq { get; set; } = 0;
        public PipeWriter PipeWriter { get; set; }

        public Func<StreamMessage, Task> MessageHandler { get; set; }

        public Consumer(string name, string streamName, PipeWriter pipeWriter)
        {
            Name = name;
            StreamName = streamName;
            PipeWriter = pipeWriter;
        }
    }
}