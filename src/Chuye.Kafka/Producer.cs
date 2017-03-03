using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka {
    public class Producer {
        private readonly Client _client;
        private readonly TopicPartitionDispatcher _partitionDispatcher;
        private ProducerConfig _config;

        protected Client Client {
            get { return _client; }
        }
        public ProducerConfig Config {
            get { return _config; }
        }

        public Producer(Option option) {
            _client              = option.GetSharedClient();
            _config              = option.ProducerConfig;
            _partitionDispatcher = new TopicPartitionDispatcher(_client.TopicBrokerDispatcher);
        }

        public void UseCodec(MessageCodec codec) {
            if (codec == MessageCodec.Snappy) {
                throw new ArgumentOutOfRangeException("codec", "Snappy not support yet");
            }
            _config.MessageCodec = codec;
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
                offset = ChunkingSend(topicPartition, chunk);
            }
            return offset;
        }

        internal Int64 ChunkingSend(TopicPartition topicPartition, IEnumerable<Message> messages) {
            var offset = 0L;
            foreach (var chunk in messages.Chunking(_config.ThrottleSize)) {
                var codec = _config.MessageCodec;
                if (chunk.Count < 5 && messages.Sum(x => x.Value != null ? x.Value.Length : 0) < 4096) {
                    codec = MessageCodec.None;
                }
                offset = _client.Produce(topicPartition.Name, topicPartition.Partition, chunk,
                    _config.AcknowlegeStrategy, codec);
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
                offset = await ChunkingSendAsync(topicPartition, chunk);
            }
            return offset;
        }

        internal async Task<Int64> ChunkingSendAsync(TopicPartition topicPartition, IEnumerable<Message> messages) {
            var offset = 0L;
            foreach (var chunk in messages.Chunking(_config.ThrottleSize)) {
                var codec = _config.MessageCodec;
                if (chunk.Count < 5 && messages.Sum(x => x.Value != null ? x.Value.Length : 0) < 4096) {
                    codec = MessageCodec.None;
                }
                offset = await _client.ProduceAsync(topicPartition.Name, topicPartition.Partition, chunk,
                    _config.AcknowlegeStrategy, codec);
            }
            return offset;
        }

        protected virtual TopicPartition SelectNextTopicPartition(String topic) {
            return _partitionDispatcher.SelectRandomPartition(topic);
        }
    }

    public class ThrottledProducer : Producer, IDisposable {
        private readonly Dictionary<TopicPartition, ThrottleMessageQueue> _queues;
        private readonly ReaderWriterLockSlim _sync;
        private ProducerConfig _config;

        public ThrottledProducer(Option option)
            : base(option) {
            _queues = new Dictionary<TopicPartition, ThrottleMessageQueue>();
            _sync   = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
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
            if (String.IsNullOrWhiteSpace(topic)) {
                throw new ArgumentNullException("topic");
            }
            if (messages == null || messages.Count == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }

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
#if NET40
            var taskSource = new TaskCompletionSource<Int64>();
            taskSource.SetResult(Send(topic, messages));
            return taskSource.Task;
#else
            return Task.FromResult(Send(topic, messages));
#endif
        }

        public override Task<Int64> SendAsync(String topic, IList<Message> messages) {
#if NET40
            var taskSource = new TaskCompletionSource<Int64>();
            taskSource.SetResult(Send(topic, messages));
            return taskSource.Task;
#else
            return Task.FromResult(Send(topic, messages));
#endif
        }

        public void Flush() {
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

        public void Dispose() {
            Flush();
            if (_sync != null) {
                _sync.Dispose();
            }
        }
    }
}
