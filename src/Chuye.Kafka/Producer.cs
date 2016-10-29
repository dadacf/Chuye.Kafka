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
        private ProducerConfig _config;

        protected Client Client {
            get { return _client; }
        }

        public Producer(Option option) {
            _client              = option.GetSharedClient();
            _config              = option.ProducerConfig;
            _partitionDispatcher = new TopicPartitionDispatcher(_client.TopicBrokerDispatcher);
        }

        public virtual Int64 Send(String topic, params String[] messages) {
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentNullException("topic");
            }
            if (messages == null || messages.Length == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }
            return ChunkingSend(topic, messages.Select(x => (Message)x));
        }

        public virtual Int64 Send(String topic, IList<Message> messages) {
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentNullException("topic");
            }
            if (messages == null || messages.Count == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }
            return ChunkingSend(topic, messages);
        }

        internal Int64 ChunkingSend(String topic, IEnumerable<Message> messages) {
            var offset = 0L;
            foreach (var chunk in messages.Chunking(_config.ThrottleSize)) {
                var topicPartition = SelectNextTopicPartition(topic);
                offset = Send(topicPartition, chunk);
            }
            return offset;
        }

        public Int64 Send(TopicPartition topicPartition, IEnumerable<Message> messages) {
            var offset = 0L;
            foreach (var chunk in messages.Chunking(_config.ThrottleSize)) {
                offset = _client.Produce(topicPartition.Name, topicPartition.Partition, chunk);
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
            return ChunkingSendAsync(topic, messages.Select(x => (Message)x));
        }

        public virtual async Task<Int64> SendAsync(String topic, IList<Message> messages) {
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentNullException("topic");
            }
            if (messages == null || messages.Count == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }
            return await ChunkingSendAsync(topic, messages);
        }

        internal async Task<Int64> ChunkingSendAsync(String topic, IEnumerable<Message> messages) {
            var offset = 0L;
            foreach (var chunk in messages.Chunking(_config.ThrottleSize)) {
                var topicPartition = SelectNextTopicPartition(topic);
                offset = await SendAsync(topicPartition, chunk);
            }
            return offset;
        }

        public async Task<Int64> SendAsync(TopicPartition topicPartition, IEnumerable<Message> messages) {
            var offset = 0L;
            foreach (var chunk in messages.Chunking(_config.ThrottleSize)) {
                offset = await _client.ProduceAsync(topicPartition.Name, topicPartition.Partition, chunk);
            }
            return offset;
        }

        protected virtual TopicPartition SelectNextTopicPartition(String topic) {
            return _partitionDispatcher.SelectPartition(topic);
        }
    }

    public class ThrottledProducer : Producer, IDisposable {
        private readonly Dictionary<TopicPartition, ThrottleMessageQueue> _queues;
        private readonly ReaderWriterLockSlim _sync;
        private ProducerConfig _config;

        public ThrottledProducer(Option option)
            : base(option) {
            _queues = new Dictionary<TopicPartition, ThrottleMessageQueue>();
            _sync = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            _config = option.ProducerConfig;
        }

        public override Int64 Send(String topic, params String[] messages) {
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentNullException("topic");
            }
            if (messages == null || messages.Length == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }
            foreach (var chunk in messages.Select(x => (Message)x).Chunking(_config.ThrottleSize)) {
                var topicPartition = base.SelectNextTopicPartition(topic);
                ChunkingEnqueue(topicPartition, chunk);
            }
            return 0L;
        }

        public override Int64 Send(String topic, IList<Message> messages) {
            foreach (var chunk in messages.Chunking(_config.ThrottleSize)) {
                var topicPartition = base.SelectNextTopicPartition(topic);
                ChunkingEnqueue(topicPartition, chunk);
            }
            return 0L;
        }

        private void ChunkingEnqueue(TopicPartition topicPartition, IList<Message> messages) {
            ThrottleMessageQueue queue;
            _sync.EnterUpgradeableReadLock();
            try {
                if (_queues.TryGetValue(topicPartition, out queue)) {
                    queue.Enqueue(messages);
                    return;
                }
                _sync.EnterWriteLock();
                try {
                    queue = new ThrottleMessageQueue(topicPartition, (Producer)this);
                    queue.ThrottleMilliseconds = _config.ThrottleMilliseconds;
                    queue.ThrottleSize = _config.ThrottleSize;
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

        public override Task<Int64> SendAsync(String topic, params String[] messages) {
            return Task.FromResult(Send(topic, messages));
        }

        public override Task<Int64> SendAsync(String topic, IList<Message> messages) {
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
