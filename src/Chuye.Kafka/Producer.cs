using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;

namespace Chuye.Kafka {
    public class Producer {
        private const Int32 SegmentSize = 50;
        private readonly Client _client;
        private readonly TopicPartitionDispatcher _partitionDispatcher;

        protected Client Client {
            get { return _client; }
        }

        public Producer(Option option) {
            _client = option.GetSharedClient();
            _partitionDispatcher = new TopicPartitionDispatcher(_client.TopicBrokerDispatcher);
        }

        public virtual Int64 Send(String topic, params String[] messages) {
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentNullException("topic");
            }
            if (messages == null || messages.Length == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }
            return Send(topic, messages.Select(x => (Message)x).ToArray());
        }

        public virtual Int64 Send(String topic, IList<Message> messages) {
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentNullException("topic");
            }
            if (messages == null || messages.Count == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }
            var topicPartition = SelectNextTopicPartition(topic);
            return Send(topic, topicPartition.Partition, messages);
        }

        internal Int64 Send(String topic, Int32 partition, IList<Message> messages) {
            var offset = 0L;
            foreach (var list in Segment(messages)) {
                offset = _client.Produce(topic, partition, list);
            }
            return offset;
        }

        public virtual Task<Int64> SendAsync(String topic, params String[] messages) {
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentNullException("topic");
            }
            if (messages == null || messages.Length == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }
            return SendAsync(topic, messages.Select(x => (Message)x).ToArray());
        }

        public virtual async Task<Int64> SendAsync(String topic, IList<Message> messages) {
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentNullException("topic");
            }
            if (messages == null || messages.Count == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }
            var topicPartition = SelectNextTopicPartition(topic);
            var offset = 0L;
            foreach (var list in Segment(messages)) {
                offset = await _client.ProduceAsync(topic, topicPartition.Partition, list);
            }
            return offset;
        }

        protected virtual TopicPartition SelectNextTopicPartition(String topic) {
            return _partitionDispatcher.SelectPartition(topic);
        }

        protected IEnumerable<IList<Message>> Segment(IEnumerable<Message> messages) {
            var list = new List<Message>(SegmentSize);
            foreach (var msg in messages) {
                list.Add(msg);
                if (list.Count == SegmentSize) {
                    yield return list;
                    list.Clear();
                }
            }
            if (list.Count > 0) {
                yield return list;
            }
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
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentNullException("topic");
            }
            if (messages == null || messages.Length == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }
            return Send(topic, messages.Select(x => (Message)x).ToArray());
        }

        public override Int64 Send(String topic, IList<Message> messages) {
            foreach (var list in Segment(messages)) {
                var topicPartition = base.SelectNextTopicPartition(topic);
                SegmentSend(topicPartition, list);
            }
            return 0L;
        }

        private void SegmentSend(TopicPartition topicPartition, IList<Message> messages) {
            DelayedMessageQueue queue;
            _sync.EnterUpgradeableReadLock();
            try {
                if (_queues.TryGetValue(topicPartition, out queue)) {
                    queue.Enqueue(messages);
                    return;
                }
                _sync.EnterWriteLock();
                try {
                    queue = new DelayedMessageQueue(topicPartition, (Producer)this);
                    _queues.Add(topicPartition, queue);
                    queue.Enqueue(messages);
                    return;
                }
                finally {
                    _sync.ExitWriteLock();
                }
            }
            finally {
                _sync.ExitUpgradeableReadLock();
            }
        }

        public override Task<Int64> SendAsync(string topic, params string[] messages) {
            return Task.FromResult(Send(topic, messages));
        }

        public override Task<Int64> SendAsync(string topic, IList<Message> messages) {
            return Task.FromResult(Send(topic, messages));
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
