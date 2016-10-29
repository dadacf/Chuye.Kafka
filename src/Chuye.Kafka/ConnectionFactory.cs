using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;

namespace Chuye.Kafka {
    public class ConnectionFactory : IDisposable {
        private readonly Dictionary<String, SocketPool> _sockets;
        private readonly Object _sync;

        public ConnectionFactory() {
            _sockets = new Dictionary<String, SocketPool>();
            _sync = new Object();
        }

        public ConnectionState GetState() {
            return new ConnectionState {
                Constructed = _sockets.Sum(x => x.Value.Constructed),
                Avaliable   = _sockets.Sum(x => x.Value.Avaliable),
                Detached    = _sockets.Sum(x => x.Value.Detached),
                Occupied    = _sockets.Sum(x => x.Value.Occupied)
            };
        }

        public Connection Connect(Uri uri) {
            if (uri == null) {
                throw new ArgumentNullException("uri");
            }
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

        public void Dispose() {
            foreach (var sockets in _sockets.Values) {
                sockets.ReleaseAll();
            }
        }

        public struct ConnectionState {
            public int Constructed;
            public int Avaliable;
            public int Occupied;
            public int Detached;
        }
    }
}
