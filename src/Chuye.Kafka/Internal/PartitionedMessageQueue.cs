using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class PartitionedMessageQueue {
        private const Int32 Limit = 10;
        private const Int32 MaxIntervalMilliseconds = 1000;
        private DateTime _createTime;
        private readonly Queue<Message> _queue;
        private readonly Object _sync;
        private readonly TopicPartition _topicPartition;
        private readonly Client _client;

        internal PartitionedMessageQueue(TopicPartition topicPartition, Client client) {
            _topicPartition = topicPartition;
            _client = client;
            _queue = new Queue<Message>();
            _sync = new Object();
            _createTime = DateTime.UtcNow;
        }

        public void Enqueue(IEnumerable<String> messages) {
            Enqueue(messages.Select(x => (Message)x));
        }

        public void Enqueue(IEnumerable<Message> messages) {
            lock (_sync) {
                foreach (var message in messages) {
                    _queue.Enqueue(message);
                }
            }
            if (_queue.Count > Limit || DateTime.UtcNow.Subtract(_createTime).TotalMilliseconds > MaxIntervalMilliseconds) {
                Flush();
            }
        }

        public void Flush() {
            lock (_sync) {
                var list = new List<Message>(Limit);
                while (_queue.Count > 0) {
                    list.Add(_queue.Dequeue());
                    if (list.Count > Limit) {
                        BatchSend(list);
                    }
                }
                if (list.Count > 0) {
                    BatchSend(list);
                }
            }
        }

        private void BatchSend(IList<Message> messages) {
            _client.Produce(_topicPartition.Name, _topicPartition.Partition, messages);
        }
    }
}
