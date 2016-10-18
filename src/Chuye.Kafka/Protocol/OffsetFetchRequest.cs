using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
    //  ConsumerGroup => string
    //  TopicName => string
    //  Partition => int32
    public class OffsetFetchRequest : Request {
        public String ConsumerGroup { get; set; }
        public IList<OffsetFetchRequestTopicPartition> TopicPartitions { get; set; }

        /// <summary>
        /// ApiVersion 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
        /// </summary>
        public OffsetFetchRequest()
            : base(ApiKey.OffsetFetchRequest) {
        }

        public OffsetFetchRequest(String topic, Int32[] partitions, String groupId)
            : base(ApiKey.OffsetFetchRequest) {
            if (partitions == null || partitions.Length == 0) {
                throw new ArgumentOutOfRangeException("partitions");
            }

            var topicPartitions = new List<OffsetFetchRequestTopicPartition>();
            foreach (var partition in partitions) {
                topicPartitions.Add(new OffsetFetchRequestTopicPartition {
                    TopicName = topic,
                    Partitions = new[] { partition }
                });
            }

            ConsumerGroup   = groupId;
            TopicPartitions = topicPartitions;
        }

        protected override void SerializeContent(KafkaStreamWriter writer) {
            writer.Write(ConsumerGroup);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(KafkaStreamReader reader) {
            ConsumerGroup   = reader.ReadString();
            TopicPartitions = reader.ReadArray<OffsetFetchRequestTopicPartition>();
        }
    }

    public class OffsetFetchRequestTopicPartition : IKafkaWriteable, IKafkaReadable {
        public String TopicName { get; set; }
        public Int32[] Partitions { get; set; }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(TopicName);
            writer.Write(Partitions);
        }

        public void FetchFrom(KafkaStreamReader reader) {
            TopicName  = reader.ReadString();
            Partitions = reader.ReadInt32Array();
        }
    }
}
