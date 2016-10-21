using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka {
    public class Consumer {
        private readonly String _groupId;
        private readonly Client _client;
        private readonly TopicPartitionDispatcher _partitionDispatcher;

        public String GroupId {
            get { return _groupId; }
        }

        public Client Client {
            get { return _client; }
        }

        public Consumer(Option option, String groupId) {
            _client = option.GetSharedClient();
            _groupId = groupId;
            _partitionDispatcher = new TopicPartitionDispatcher(_client);
            _client.ReplaceDispatcher(_partitionDispatcher);
        }

        public IEnumerable<Message> Fetch(String topic) {
            var topicPartition = SelectNextTopicPartition(topic);
            var earliestOffset = _client.Offset(topicPartition.Name, topicPartition.Partition, OffsetOption.Earliest);
            //todo: Offset saving
            //todo: Rebalance
            return _client.Fetch(topic, topicPartition.Partition, earliestOffset);
        }

        protected virtual TopicPartition SelectNextTopicPartition(String topic) {
            return _partitionDispatcher.SelectPartition(topic);
        }
    }
}
