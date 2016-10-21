using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;

namespace Chuye.Kafka {
    public class Producer {
        private readonly Client _client;
        private readonly TopicPartitionDispatcher _partitionDispatcher;
        
        protected Client Client {
            get { return _client; }
        }

        public Producer(Option option) {
            _client              = option.GetSharedClient();
            _partitionDispatcher = new TopicPartitionDispatcher(_client.TopicBrokerDispatcher);
        }

        public virtual Int64 Send(String topic, params String[] messages) {
            return Send(topic, messages.Select(x => (Message)x).ToArray());
        }

        public virtual Int64 Send(String topic, IList<Message> messages) {
            var topicPartition = SelectNextTopicPartition(topic);
            return _client.Produce(topic, topicPartition.Partition, messages);
        }

        protected virtual TopicPartition SelectNextTopicPartition(String topic) {
            return _partitionDispatcher.SelectPartition(topic);
        }
    }

    public class ThrottledProducer : Producer, IDisposable {
        private readonly Dictionary<TopicPartition, DelayedMessageQueue> _queues;
        private readonly ReaderWriterLockSlim _sync;

        public ThrottledProducer(Option option)
            : base(option) {
            _queues = new Dictionary<TopicPartition, DelayedMessageQueue>();
            _sync = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        }

        public override Int64 Send(String topic, params String[] messages) {
            return Send(topic, messages.Select(x => (Message)x).ToArray());
        }

        public override Int64 Send(String topic, IList<Message> messages) {
            var topicPartition = base.SelectNextTopicPartition(topic);
            DelayedMessageQueue queue;
            _sync.EnterUpgradeableReadLock();
            try {
                if (_queues.TryGetValue(topicPartition, out queue)) {
                    queue.Enqueue(messages);
                    return 0L;
                }

                _sync.EnterWriteLock();
                try {
                    queue = new DelayedMessageQueue(topicPartition, Client);
                    _queues.Add(topicPartition, queue);
                    queue.Enqueue(messages);
                    return 0L;
                }
                finally {
                    _sync.ExitWriteLock();
                }
            }
            finally {
                _sync.ExitUpgradeableReadLock();
            }
        }

        public void Dispose() {
            _sync.EnterWriteLock();
            try {
                foreach (var queue in _queues) {
                    queue.Value.Flush();
                }
                _queues.Clear();
            }
            finally {
                _sync.ExitWriteLock();

            }
        }
    }
}
