using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Internal {
    class TopicBrokerDispatcher {
        private readonly Client _client;
        private readonly List<TopicPartition> _topics;
        private readonly List<Broker> _brokers;
        private readonly ReaderWriterLockSlim _sync;

        public TopicBrokerDispatcher(Client client) {
            _client = client;
            _sync = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            _topics = new List<TopicPartition>();
            _brokers = new List<Broker>();
        }

        //public Broker Select(String topic) {
        //    throw new NotImplementedException();
        //}

        public Broker Select(String topic, Int32 partition) {
            _sync.EnterUpgradeableReadLock();
            try {
                var broker = SelectCached(topic, partition);
                if (broker != null) {
                    return broker;
                }
                _sync.EnterWriteLock();
                try {
                    var response = _client.Metadata(topic);
                    foreach (var item in response.TopicMetadatas[0].PartitionMetadatas) {
                        _topics.Add(new TopicPartition(topic, item.PartitionId, item.Leader));
                    }
                    foreach (var item in response.Brokers) {
                        if (!_brokers.Contains(item)) {
                            _brokers.Add(item);
                        }
                    }

                    broker = SelectCached(topic, partition);
                    if (broker == null) {
                        throw new ProtocolException(ErrorCode.NotLeaderForPartition);
                    }
                    return broker;
                }
                finally {
                    _sync.ExitWriteLock();
                }
            }
            finally {
                _sync.ExitUpgradeableReadLock();
            }
        }

        private Broker SelectCached(String topic, Int32 partition) {
            var targetTopic = _topics.Find(x => x.Name == topic && x.Partition == partition);
            if (targetTopic == null) {
                return null;
            }
            var broker = _brokers.Find(x => x.NodeId == targetTopic.Leader);
            if (broker == null) {
                throw new NotImplementedException();
            }
            return broker;
        }
    }
}
