using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Internal {
    class TopicPartitionDispatcher : TopicBrokerDispatcher {
        private readonly Dictionary<String, Int32> _sequences;
        private readonly ReaderWriterLockSlim _sync;

        public TopicPartitionDispatcher(Client client)
            : base(client) {
            _sequences = new Dictionary<String, Int32>();
            _sync = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        }

        public TopicPartition SequentialSelect(String topic) {
            var sequence = -1;
            _sync.EnterWriteLock();
            try {
                _sequences.TryGetValue(topic, out sequence);
                _sequences[topic] = ++sequence;
            }
            finally {
                _sync.ExitWriteLock();
            }

            base.Select(topic);
            var topicPartition = Topics.Where(x => x.Name == topic).ToArray();
            return topicPartition[sequence % topicPartition.Length];
        }
    }
}
