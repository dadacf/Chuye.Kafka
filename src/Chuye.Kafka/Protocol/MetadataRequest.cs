using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //TopicMetadataRequest => [TopicName]
    //  TopicName => string
    /// <summary>
    /// If "auto.create.topics.enable" is set in the broker configuration, 
    ///   a topic metadata request will create the topic with the default replication factor and number of partitions. 
    /// </summary>
    public class MetadataRequest : Request {
        public String[] TopicNames { get; set; }

        public MetadataRequest()
            : base(ApiKey.MetadataRequest) {
            TopicNames = new String[0];
        }

        public MetadataRequest(params String[] topics)
            : this() {
            TopicNames = topics ?? new String[0];
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(TopicNames);
        }

        protected override void DeserializeContent(KafkaReader reader) {
            TopicNames = reader.ReadStrings();
        }
    }
}
