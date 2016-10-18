using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;

namespace Chuye.Kafka {
    public class ConnectionFactory {
        private readonly Dictionary<String, SocketPool> _sockets;
        private readonly Object _sync;
        private IRouter _router;

        public ConnectionFactory(params Uri[] brokers) {
            _sockets = new Dictionary<String, SocketPool>();
            _sync = new Object();
            _router = new Router(brokers, this);
        }

        public PoolState GetPoolState() {
            return new PoolState {
                Constructed = _sockets.Sum(x => x.Value.Constructed),
                Avaliable = _sockets.Sum(x => x.Value.Avaliable),
                Detached = _sockets.Sum(x => x.Value.Detached),
                Occupied = _sockets.Sum(x => x.Value.Occupied)
            };
        }

        public Connection Connect(Uri uri) {
            lock (_sync) {
                SocketPool pool;
                if (!_sockets.TryGetValue(uri.AbsoluteUri, out pool)) {
                    pool = new SocketPool(uri);
                    _sockets.Add(uri.AbsoluteUri, pool);
                }
                var socket = pool.AcquireItem();
                return new Connection(socket);
            }
        }

        public IProducer GetProducer() {
            return new Producer(this, _router);
        }

        public IConsumer GetConsumer(String groupId) {
            return new Consumer(groupId);
        }

        public void CleanUp() {
            foreach (var item in _sockets) {
                item.Value.DetachAvaliables();
            }
        }

        public struct PoolState {
            public int Constructed;
            public int Avaliable;
            public int Occupied;
            public int Detached;
        }
    }
}
