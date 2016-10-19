using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {

    /// <summary>
    /// ApiVersion 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
    /// </summary>
    public abstract class OffsetCommitRequest : Request {
        protected OffsetCommitRequest()
            : base(ApiKey.OffsetCommitRequest) {
        }

        public static OffsetCommitRequest CreateV0(String topic, Int32 partition, String groupId, Int64 offset) {
            return new OffsetCommitRequestV0(topic, partition, groupId, offset);
        }

        public static OffsetCommitRequest CreateV1() {
            return new OffsetCommitRequestV1();
        }

        public static OffsetCommitRequest CreateV2() {
            return new OffsetCommitRequestV2();
        }
    }

    //v0 (supported in 0.8.1 or later)
    //OffsetCommitRequest => ConsumerGroupId [TopicName [Partition Offset Metadata]]
    //  ConsumerGroupId => string
    //  TopicName => string
    //  Partition => int32
    //  Offset => int64
    //  Metadata => string
    public class OffsetCommitRequestV0 : OffsetCommitRequest {
        public String ConsumerGroup { get; set; }
        public OffsetCommitRequestTopicPartitionV0[] TopicPartitions { get; set; }

        public OffsetCommitRequestV0() {
        }

        public OffsetCommitRequestV0(String topic, Int32 partition, String groupId, Int64 offset) {
            ConsumerGroup   = groupId;
            TopicPartitions = new OffsetCommitRequestTopicPartitionV0[1];
            TopicPartitions = new[] {
                new OffsetCommitRequestTopicPartitionV0 {
                    TopicName = topic,
                    Details   = new [] {
                        new OffsetCommitRequestTopicPartitionDetailV0 {
                            Partition = partition,
                            Offset    = offset
                        }
                    }
                }
            };
        }

        public OffsetCommitRequestV0(String topic, String groupId, Int32[] partitions, Int64[] offsets) {
            if (partitions == null || partitions.Length == 0) {
                throw new ArgumentOutOfRangeException("partitions");
            }
            if (offsets == null || offsets.Length == 0) {
                throw new ArgumentOutOfRangeException("offsets");
            }
            if (partitions.Length != offsets.Length) {
                throw new ArgumentOutOfRangeException();
            }

            var details = new List<OffsetCommitRequestTopicPartitionDetailV0>();
            for (int i = 0; i < partitions.Length; i++) {
                details.Add(new OffsetCommitRequestTopicPartitionDetailV0 {
                    Partition = partitions[i],
                    Offset = offsets[i]
                });
            }


            ConsumerGroup = groupId;
            TopicPartitions = new OffsetCommitRequestTopicPartitionV0[1];
            TopicPartitions = new[] {
                new OffsetCommitRequestTopicPartitionV0 {
                    TopicName = topic,
                    Details   = details
                }
            };
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(ConsumerGroup);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(KafkaReader reader) {
            ConsumerGroup = reader.ReadString();
            TopicPartitions = reader.ReadArray<OffsetCommitRequestTopicPartitionV0>();
        }
    }

    //v1 (supported in 0.8.2 or later)
    //OffsetCommitRequest => ConsumerGroupId ConsumerGroupGenerationId ConsumerId [TopicName [Partition Offset TimeStamp Metadata]]
    //  ConsumerGroupId => string
    //  ConsumerGroupGenerationId => int32
    //  ConsumerId => string
    //  TopicName => string
    //  Partition => int32
    //  Offset => int64
    //  TimeStamp => int64
    //  Metadata => string
    public class OffsetCommitRequestV1 : OffsetCommitRequest {
        public String ConsumerGroup { get; set; }
        public Int32 ConsumerGroupGenerationId { get; set; }
        public String ConsumerId { get; set; }
        public OffsetCommitRequestTopicPartitionV1[] TopicPartitions { get; set; }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(ConsumerGroup);
            writer.Write(ConsumerGroupGenerationId);
            writer.Write(ConsumerId);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(KafkaReader reader) {
            ConsumerGroup             = reader.ReadString();
            ConsumerGroupGenerationId = reader.ReadInt32();
            ConsumerId                = reader.ReadString();
            TopicPartitions           = reader.ReadArray<OffsetCommitRequestTopicPartitionV1>();
        }
    }

    //v2 (supported in 0.8.3 or later)
    //OffsetCommitRequest => ConsumerGroup ConsumerGroupGenerationId ConsumerId RetentionTime [TopicName [Partition Offset Metadata]]
    //  ConsumerGroupId => string
    //  ConsumerGroupGenerationId => int32
    //  ConsumerId => string
    //  RetentionTime => int64
    //  TopicName => string
    //  Partition => int32
    //  Offset => int64
    //  Metadata => string
    public class OffsetCommitRequestV2 : OffsetCommitRequest {
        public String ConsumerGroup { get; set; }
        public Int32 ConsumerGroupGenerationId { get; set; }
        public String ConsumerId { get; set; }
        public Int64 RetentionTime { get; set; }
        public OffsetCommitRequestTopicPartitionV0[] TopicPartitions { get; set; }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(ConsumerGroup);
            writer.Write(ConsumerGroupGenerationId);
            writer.Write(ConsumerId);
            writer.Write(RetentionTime);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(KafkaReader reader) {
            ConsumerGroup             = reader.ReadString();
            ConsumerGroupGenerationId = reader.ReadInt32();
            ConsumerId                = reader.ReadString();
            RetentionTime             = reader.ReadInt64();
            TopicPartitions           = reader.ReadArray<OffsetCommitRequestTopicPartitionV0>();
        }
    }

    public class OffsetCommitRequestTopicPartitionV0 : IKafkaWriteable, IKafkaReadable {
        public String TopicName { get; set; }
        public IList<OffsetCommitRequestTopicPartitionDetailV0> Details { get; set; }

        public void SaveTo(KafkaWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }

        public void FetchFrom(KafkaReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<OffsetCommitRequestTopicPartitionDetailV0>();
        }
    }

    public class OffsetCommitRequestTopicPartitionV1 : IKafkaWriteable, IKafkaReadable {
        public String TopicName { get; set; }
        public OffsetCommitRequestTopicPartitionDetailV1[] Details { get; set; }

        public void SaveTo(KafkaWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }

        public void FetchFrom(KafkaReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<OffsetCommitRequestTopicPartitionDetailV1>();
        }
    }

    public class OffsetCommitRequestTopicPartitionDetailV0 : IKafkaWriteable, IKafkaReadable {
        public Int32 Partition { get; set; }
        public Int64 Offset { get; set; }
        public String Metadata { get; set; }

        public void SaveTo(KafkaWriter writer) {
            writer.Write(Partition);
            writer.Write(Offset);
            writer.Write(Metadata);
        }

        public void FetchFrom(KafkaReader reader) {
            Partition = reader.ReadInt32();
            Offset    = reader.ReadInt64();
            Metadata  = reader.ReadString();
        }
    }

    public class OffsetCommitRequestTopicPartitionDetailV1 : IKafkaWriteable, IKafkaReadable {
        public Int32 Partition { get; set; }
        public Int64 Offset { get; set; }
        public Int64 TimeStamp { get; set; }
        public String Metadata { get; set; }

        public void SaveTo(KafkaWriter writer) {
            writer.Write(Partition);
            writer.Write(Offset);
            writer.Write(TimeStamp);
            writer.Write(Metadata);
        }

        public void FetchFrom(KafkaReader reader) {
            Partition = reader.ReadInt32();
            Offset    = reader.ReadInt64();
            TimeStamp = reader.ReadInt64();
            Metadata  = reader.ReadString();
        }
    }
}
