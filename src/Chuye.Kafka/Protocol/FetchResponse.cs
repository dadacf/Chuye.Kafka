using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
    //  TopicName => string
    //  Partition => int32
    //  ErrorCode => int16
    //  HighwaterMarkOffset => int64
    //  MessageSetSize => int32
    public class FetchResponse : Response {
        public FetchResponseTopicPartition[] TopicPartitions { get; set; }

        protected override void DeserializeContent(KafkaStreamReader reader) {
            TopicPartitions = reader.ReadArray<FetchResponseTopicPartition>();
        }

        protected override void SerializeContent(KafkaStreamWriter writer) {
            writer.Write(TopicPartitions);
        }

        public override void ThrowIfFail() {
            var errors = TopicPartitions.SelectMany(x => x.MessageBodys)
                .Select(x => x.ErrorCode)
                .Where(x => x != ErrorCode.NoError);
            if (errors.Any()) {
                throw new ProtocolException(errors.First());
            }
        }
    }

    public class FetchResponseTopicPartition : IKafkaReadable, IKafkaWriteable {
        public String TopicName { get; set; }
        public MessageBody[] MessageBodys { get; set; }

        public void FetchFrom(KafkaStreamReader reader) {
            TopicName    = reader.ReadString();
            MessageBodys = reader.ReadArray<MessageBody>();
        }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(TopicName);
            writer.Write(MessageBodys);
        }
    }

    public class MessageBody : IKafkaReadable, IKafkaWriteable {
        public Int32 Partition { get; set; }
        //Possible Error Codes
        //* OFFSET_OUT_OF_RANGE (1)
        //* UNKNOWN_TOPIC_OR_PARTITION (3)
        //* NOT_LEADER_FOR_PARTITION (6)
        //* REPLICA_NOT_AVAILABLE (9)
        //* UNKNOWN (-1)
        public ErrorCode ErrorCode { get; set; }
        public Int64 HighwaterMarkOffset { get; set; }
        public Int32 MessageSetSize { get; set; }
        public MessageSet MessageSet { get; set; }

        public void FetchFrom(KafkaStreamReader reader) {
            Partition           = reader.ReadInt32();
            ErrorCode           = (ErrorCode)reader.ReadInt16();
            HighwaterMarkOffset = reader.ReadInt64();
            MessageSetSize      = reader.ReadInt32();
            MessageSet          = new MessageSet(MessageSetSize);
            MessageSet.FetchFrom(reader);
        }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(Partition);
            writer.Write((Int16)ErrorCode);
            writer.Write(HighwaterMarkOffset);
            writer.Write(MessageSetSize);
            MessageSet.WriteTo(writer);
        }
    }
}
