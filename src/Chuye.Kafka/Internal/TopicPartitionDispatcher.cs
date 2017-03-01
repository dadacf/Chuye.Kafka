using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class TopicPartitionDispatcher {
        private readonly Dictionary<String, Int32> _sequences;
        private readonly ReaderWriterLockSlim _sync;
        private readonly TopicBrokerDispatcher _dispatcher;

        public TopicPartitionDispatcher(TopicBrokerDispatcher dispatcher) {
            _dispatcher = dispatcher;
            _sequences = new Dictionary<String, Int32>();
            _sync = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        }

        private Int32 Sequential(String topic) {
            var sequence = -1;
            _sync.EnterWriteLock();
            try {
                _sequences.TryGetValue(topic, out sequence);
                _sequences[topic] = ++sequence;
            }
            finally {
                _sync.ExitWriteLock();
            }
            return sequence;
        }

        public TopicPartition SelectRandomPartition(String topic) {
            var topicPartitions = SelectPartitions(topic);
            return topicPartitions[Math.Abs(Guid.NewGuid().GetHashCode()) % topicPartitions.Count];
        }

        public TopicPartition SelectPartition(String topic) {
            Int32 sequence = Sequential(topic);
            var topicPartitions = SelectPartitions(topic);
            return topicPartitions[sequence % topicPartitions.Count];
        }
#if NET40
        public IList<TopicPartition> SelectPartitions(String topic) {
#else
        public IReadOnlyList<TopicPartition> SelectPartitions(String topic) {
#endif
            _dispatcher.SelectBrokers(topic);
            return _dispatcher.Topics.Where(x => x.Name == topic).OrderBy(x => x.Partition).ToArray();
        }
    }
}
