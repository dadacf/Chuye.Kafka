using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Internal {
    interface IRouter {
        Broker Dispatch(String topic);
    }

    class ProduceRouter : IRouter {
        private readonly String _topic;
        private readonly Broker[] _brokers;
        private Int32 _currentIndex;

        public ProduceRouter(String topic, Broker[] brokers) {
            _topic = topic;
            _brokers = brokers;
        }

        public Broker Dispatch(String topic) {
            return _brokers[_currentIndex++ % _brokers.Length];
        }
    }

    class Router : IRouter {
        private readonly ConnectionFactory _connectionFactory;
        private Uri[] _brokers;
        private Int32 _index;
        private Dictionary<String, IRouter> _routers;
        private readonly ReaderWriterLockSlim _sync;

        public Router(Uri[] brokers, ConnectionFactory connectionFactory) {
            _brokers = brokers;
            _connectionFactory = connectionFactory;
            _index = 0;
            _routers = new Dictionary<String, IRouter>();
            _sync = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        }

        public Broker Dispatch(String topic) {
            _sync.EnterUpgradeableReadLock();
            IRouter router;
            try {
                if (_routers.TryGetValue(topic, out router)) {
                    return router.Dispatch(topic);
                }

                _sync.EnterWriteLock();
                try {
                    var nextBroker = _brokers[_index++ % _brokers.Length];
                    var request = new MetadataRequest(topic);
                    var connection = _connectionFactory.Connect(nextBroker);
                    var response = (MetadataResponse)connection.Submit(request);
                    router = new ProduceRouter(topic, response.Brokers);
                    _routers.Add(topic, router);
                    return router.Dispatch(topic);
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
