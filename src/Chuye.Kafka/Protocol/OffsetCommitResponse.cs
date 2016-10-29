using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //v0, v1 and v2:
    //OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
    //  TopicName => string
    //  Partition => int32
    //  ErrorCode => int16
    public class OffsetCommitResponse : Response {
        public OffsetCommitResponseTopicPartition[] TopicPartitions { get; set; }

        protected override void DeserializeContent(KafkaReader reader) {
            TopicPartitions = reader.ReadArray<OffsetCommitResponseTopicPartition>();
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(TopicPartitions);
        }

        public override void TryThrowFirstErrorOccured() {
            var errors = TopicPartitions.SelectMany(x => x.Details)
                .Where(x => x.ErrorCode != ErrorCode.NoError)
                .Select(x => x.ErrorCode);

            if (errors.Any()) {
                throw new ProtocolException(errors.First());
            }
        }
    }

    public class OffsetCommitResponseTopicPartition : IKafkaReadable, IKafkaWriteable {
        public String TopicName { get; set; }
        public OffsetCommitResponseTopicPartitionDetail[] Details { get; set; }

        public void FetchFrom(KafkaReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<OffsetCommitResponseTopicPartitionDetail>();
        }

        public void SaveTo(KafkaWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }
    }

    public class OffsetCommitResponseTopicPartitionDetail : IKafkaReadable, IKafkaWriteable {
        public Int32 Partition { get; set; }
        public ErrorCode ErrorCode { get; set; }

        public void FetchFrom(KafkaReader reader) {
            Partition = reader.ReadInt32();
            ErrorCode = (ErrorCode)reader.ReadInt16();
        }

        public void SaveTo(KafkaWriter writer) {
            writer.Write(Partition);
            writer.Write((Int16)ErrorCode);
        }
    }
}
