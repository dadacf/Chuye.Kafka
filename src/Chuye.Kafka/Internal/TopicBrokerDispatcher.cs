using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Internal {
    class TopicBrokerDispatcher {
        private readonly List<TopicPartition> _topics;
        private readonly List<Broker> _brokers;
        private readonly ReaderWriterLockSlim _sync;
        private readonly Client _client;

        protected List<TopicPartition> Topics {
            get { return _topics; }
        }
        protected List<Broker> Brokers {
            get { return _brokers; }
        }

        protected Client Client {
            get { return _client; }
        }

        protected Broker SelectCached(String topic, Int32 partition) {
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

        public TopicBrokerDispatcher(Client client) {
            _client = client;
            _sync = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            _topics = new List<TopicPartition>();
            _brokers = new List<Broker>();
        }

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

        protected IList<Broker> SelectCached(String topic) {
            var targetTopics = _topics.FindAll(x => x.Name == topic);
            if (targetTopics.Count == 0) {
                return null;
            }
            var brokers = new Broker[targetTopics.Count];
            for (int i = 0; i < targetTopics.Count; i++) {
                brokers[i] = _brokers.Find(x => x.NodeId == targetTopics[i].Leader);
            }
            return brokers;
        }

        public IList<Broker> Select(String topic) {
            _sync.EnterUpgradeableReadLock();
            try {
                var brokers = SelectCached(topic);
                if (brokers != null) {
                    return brokers;
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

                    brokers = SelectCached(topic);
                    if (brokers == null) {
                        throw new ProtocolException(ErrorCode.NotLeaderForPartition);
                    }
                    return brokers;
                }
                finally {
                    _sync.ExitWriteLock();
                }
            }
            finally {
                _sync.ExitUpgradeableReadLock();
            }
        }
    }
}
