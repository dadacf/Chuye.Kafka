using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
    //  ReplicaId => int32
    //  TopicName => string
    //  Partition => int32
    //  Time => int64
    //  MaxNumberOfOffsets => int32
    public class OffsetRequest : Request {
        public Int32 ReplicaId { get; set; }
        public OffsetsRequestTopicPartition[] TopicPartitions { get; set; }

        public OffsetRequest()
            : base(ApiKey.OffsetRequest) {
        }

        public OffsetRequest(String topic, Int32[] partitions, Int32 replicaId, OffsetOption offsetOption = OffsetOption.Latest)
            : base(ApiKey.OffsetRequest) {
            if (partitions == null || partitions.Length == 0) {
                throw new ArgumentOutOfRangeException("partitions");
            }
                        
            var details = new List<OffsetsRequestTopicPartitionDetail>();
            foreach (var partition in partitions) {
                details.Add(new OffsetsRequestTopicPartitionDetail() {
                    Partition = partition,
                    Time = (Int64)offsetOption,
                    MaxNumberOfOffsets = 1
                });
            }

            ReplicaId       = replicaId;
            TopicPartitions = new[] {
                new OffsetsRequestTopicPartition {
                    TopicName = topic,
                    Details   = details
                }
            };
        }

        protected override void SerializeContent(KafkaStreamWriter writer) {
            writer.Write(ReplicaId);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(KafkaStreamReader reader) {
            ReplicaId       = reader.ReadInt32();
            TopicPartitions = reader.ReadArray<OffsetsRequestTopicPartition>();
        }
    }

    public class OffsetsRequestTopicPartition : IKafkaWriteable, IKafkaReadable {
        public String TopicName { get; set; }
        public IList<OffsetsRequestTopicPartitionDetail> Details { get; set; }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }

        public void FetchFrom(KafkaStreamReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<OffsetsRequestTopicPartitionDetail>();
        }
    }

    public class OffsetsRequestTopicPartitionDetail : IKafkaWriteable, IKafkaReadable {
        public Int32 Partition { get; set; }
        /// <summary>
        /// Used to ask for all messages before a certain time (ms). There are two special values. 
        ///   Specify -1 to receive the latest offset (i.e. the offset of the next coming message) 
        ///   and -2 to receive the earliest available offset. 
        ///   Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// </summary>
        public Int64 Time { get; set; }
        public Int32 MaxNumberOfOffsets { get; set; }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(Partition);
            writer.Write(Time);
            writer.Write(MaxNumberOfOffsets);
        }

        public void FetchFrom(KafkaStreamReader reader) {
            Partition          = reader.ReadInt32();
            Time               = reader.ReadInt64();
            MaxNumberOfOffsets = reader.ReadInt32();
        }
    }
}
