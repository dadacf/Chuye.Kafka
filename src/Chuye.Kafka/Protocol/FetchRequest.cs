using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
    //  ReplicaId => int32
    //  MaxWaitTime => int32
    //  MinBytes => int32
    //  TopicName => string
    //  Partition => int32
    //  FetchOffset => int64
    //  MaxBytes => int32
    public class FetchRequest : Request {
        public Int32 ReplicaId { get; set; }
        /// <summary>
        /// e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k 
        ///   would allow the server to wait up to 100ms  
        ///   to try to accumulate 30k of data before responding
        /// </summary>
        public Int32 MaxWaitTime { get; set; }
        public Int32 MinBytes { get; set; }
        public FetchRequestTopicPartition[] TopicPartitions { get; set; }

        public FetchRequest()
            : base(ApiKey.FetchRequest) {
        }

        public FetchRequest(String topic, Int32 partition, Int32 replicaId, Int64 fetchOffset, Int32 maxBytes = 1024 * 16, Int32 maxWaitTime = 1000)
            : base(ApiKey.FetchRequest) {
            if (fetchOffset < 0) {
                throw new ArgumentOutOfRangeException("fetchOffset");
            }

            ReplicaId   = replicaId;
            MaxWaitTime = maxWaitTime;
            MinBytes    = 2048;
            TopicPartitions = new[] {
                new FetchRequestTopicPartition {
                    TopicName          = topic,
                    FetchOffsetDetails = new [] {
                        new FetchRequestTopicPartitionDetail {
                            Partition   = partition,
                            FetchOffset = fetchOffset,
                            MaxBytes    = maxBytes
                        }
                    }
                }
            };
        }

        protected override void SerializeContent(KafkaStreamWriter writer) {
            writer.Write(ReplicaId);
            writer.Write(MaxWaitTime);
            writer.Write(MinBytes);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(KafkaStreamReader reader) {
            ReplicaId       = reader.ReadInt32();
            MaxWaitTime     = reader.ReadInt32();
            MinBytes        = reader.ReadInt32();
            TopicPartitions = reader.ReadArray<FetchRequestTopicPartition>();
        }
    }

    public class FetchRequestTopicPartition : IKafkaWriteable, IKafkaReadable {
        public String TopicName { get; set; }
        public FetchRequestTopicPartitionDetail[] FetchOffsetDetails { get; set; }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(TopicName);
            writer.Write(FetchOffsetDetails);
        }

        public void FetchFrom(KafkaStreamReader reader) {
            TopicName          = reader.ReadString();
            FetchOffsetDetails = reader.ReadArray<FetchRequestTopicPartitionDetail>();
        }
    }

    public class FetchRequestTopicPartitionDetail : IKafkaWriteable, IKafkaReadable {
        public Int32 Partition { get; set; }
        public Int64 FetchOffset { get; set; }
        public Int32 MaxBytes { get; set; }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(Partition);
            writer.Write(FetchOffset);
            writer.Write(MaxBytes);
        }

        public void FetchFrom(KafkaStreamReader reader) {
            Partition   = reader.ReadInt32();
            FetchOffset = reader.ReadInt64();
            MaxBytes    = reader.ReadInt32();
        }
    }
}
