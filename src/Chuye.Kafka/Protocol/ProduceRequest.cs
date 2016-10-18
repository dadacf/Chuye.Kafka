using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
    //  RequiredAcks => int16
    //  Timeout => int32
    //  Partition => int32
    //  MessageSetSize => int32
    public class ProduceRequest : Request {
        public AcknowlegeStrategy RequiredAcks { get; set; }
        public Int32 Timeout { get; set; }
        public ProduceRequestTopicPartition[] TopicPartitions { get; set; }

        public ProduceRequest()
            : base(ApiKey.ProduceRequest) {
        }

        public ProduceRequest(  IDictionary<String, List<Kafka.Message>> messages,
                                Int32 partition,
                                AcknowlegeStrategy strategy = AcknowlegeStrategy.Written)
            : base(ApiKey.ProduceRequest) {
            if (messages == null || messages.Count == 0) {
                return;
            }

            RequiredAcks        = strategy; //important
            Timeout             = 10;
            var topicPartitions = new List<ProduceRequestTopicPartition>();
            foreach (var list in messages) {
                if(list.Value.Count == 0) {
                    continue;
                }

                var topicPartition = new ProduceRequestTopicPartition {
                    TopicName = list.Key,
                    Details   = new[] {
                        new ProduceRequestTopicDetail  {
                            Partition  = partition,
                            MessageSet = list.Value.Count > 1
                                ? new GZipMessageSet() : new MessageSet()
                        },
                    }
                };

                var messageEntities = new List<MessageSetDetail>();
                foreach (var item in list.Value) {
                    var messageSet = new MessageSetDetail();
                    messageEntities.Add(messageSet);
                    messageSet.Message = new MessageSetItem();
                    if (item.Key != null) {
                        messageSet.Message.Key = Encoding.UTF8.GetBytes(item.Key);
                    }
                    messageSet.Message.Value = Encoding.UTF8.GetBytes(item.Value);
                    
                }
                topicPartition.Details[0].MessageSet.Items = messageEntities.ToArray();
                topicPartitions.Add(topicPartition);
            }
            TopicPartitions = topicPartitions.ToArray();
        }

        public ProduceRequest(  String topic,
                                Int32 partition,
                                IList<Kafka.Message> messages,
                                AcknowlegeStrategy strategy = AcknowlegeStrategy.Written,
                                MessageCodec codec = MessageCodec.None)
            : base(ApiKey.ProduceRequest) {
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentOutOfRangeException("topicName");
            }
            if (messages == null || messages.Count == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }

            var messageEntities = new MessageSetDetail[messages.Count];
            for (int i = 0; i < messageEntities.Length; i++) {
                var messageSet = messageEntities[i] = new MessageSetDetail();
                messageSet.Message = new MessageSetItem();
                //messageSet.Message.Attributes = codec; //Error when using nocompress enum
                if (messages[i].Key != null) {
                    messageSet.Message.Key = Encoding.UTF8.GetBytes(messages[i].Key);
                }
                if (messages[i].Value != null) {
                    messageSet.Message.Value = Encoding.UTF8.GetBytes(messages[i].Value);
                }
            }

            RequiredAcks = strategy; //important
            Timeout = 10;

            if (codec == MessageCodec.None) {
                TopicPartitions = new[] {
                    new ProduceRequestTopicPartition {
                        TopicName = topic,
                        Details  = new [] {
                            new ProduceRequestTopicDetail {
                                Partition = partition,
                                MessageSet = new MessageSet {
                                    Items = messageEntities
                                }
                            }
                        }
                    }
                };
            }
            else if (codec == MessageCodec.Gzip) {
                TopicPartitions = new[] {
                    new ProduceRequestTopicPartition {
                        TopicName = topic,
                        Details   = new [] {
                            new ProduceRequestTopicDetail {
                                Partition = partition,
                                MessageSet = new GZipMessageSet {
                                    Items = messageEntities
                                }
                            }
                        }
                    }
                };
            }
            else {
                throw new ArgumentOutOfRangeException("codec", String.Format("{0} not support", codec));
            }
        }

        protected override void SerializeContent(KafkaStreamWriter writer) {
            writer.Write((Int16)RequiredAcks);
            writer.Write(Timeout);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(KafkaStreamReader reader) {
            RequiredAcks = (AcknowlegeStrategy)reader.ReadInt16();
            Timeout = reader.ReadInt32();
            TopicPartitions = reader.ReadArray<ProduceRequestTopicPartition>();
        }
    }

    public class ProduceRequestTopicPartition : IKafkaWriteable, IKafkaReadable {
        public String TopicName { get; set; }
        public ProduceRequestTopicDetail[] Details { get; set; }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }

        public void FetchFrom(KafkaStreamReader reader) {
            TopicName = reader.ReadString();
            Details = reader.ReadArray<ProduceRequestTopicDetail>();
        }
    }

    public class ProduceRequestTopicDetail : IKafkaWriteable, IKafkaReadable {
        public Int32 Partition { get; set; }
        public Int32 MessageSetSize { get; private set; }
        public MessageSet MessageSet { get; set; }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(Partition);
            var lengthWriter = new KafkaLengthWriter(writer);
            lengthWriter.BeginWrite();
            MessageSet.WriteTo(writer);
            MessageSetSize = lengthWriter.EndWrite();
        }

        public void FetchFrom(KafkaStreamReader reader) {
            Partition      = reader.ReadInt32();
            MessageSetSize = reader.ReadInt32();
            throw new NotImplementedException();
            //MessageSet     = new MessageSet(this);
            MessageSet.FetchFrom(reader);
        }
    }
}
