using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Chuye.Kafka {
    public class ConnectionFactory {
        private readonly Dictionary<String, SocketPool> _sockets;
        private readonly Object _sync;

        public PoolState GetState() {
            return new PoolState {
                Constructed = _sockets.Sum(x => x.Value.Constructed),
                Avaliable   = _sockets.Sum(x => x.Value.Avaliable),
                Detached    = _sockets.Sum(x => x.Value.Detached),
                Occupied    = _sockets.Sum(x => x.Value.Occupied)
            };
        }

        public ConnectionFactory() {
            _sockets = new Dictionary<String, SocketPool>();
            _sync = new Object();
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

        public void CleanUp() {
            foreach (var item in _sockets) {
                item.Value.DetachLeft();
            }
        }

        internal class SocketPool : ObjectPool<Socket> {
            private readonly Uri _uri;

            public SocketPool(Uri uri) {
                _uri = uri;
            }

            protected override Socket Constructing() {
                var socket = new LimitedSocket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp, this);
                socket.Connect(_uri.Host, _uri.Port);
                return socket;
            }

            public override Socket AcquireItem() {
                var socket = base.AcquireItem();
                while (!IsConnected(socket)) {
                    DetachItem(socket);
                    socket = AcquireItem();
                    socket.Connect(_uri.Host, _uri.Port);
                }
                return socket;
            }

            //http://stackoverflow.com/questions/2661764/how-to-check-if-a-socket-is-connected-disconnected-in-c
            private bool IsConnected(Socket socket) {
                return !((socket.Poll(100, SelectMode.SelectRead) && (socket.Available == 0)) || !socket.Connected);
            }

            protected override void OnItemDetached(Socket item) {
                ((LimitedSocket)item).Destroy();
            }
        }

        internal class LimitedSocket : Socket {
            private readonly SocketPool _pool;

            public LimitedSocket(AddressFamily addressFamily, SocketType socketType, ProtocolType protocolType, SocketPool pool)
                : base(addressFamily, socketType, protocolType) {
                _pool = pool;
            }

            protected override void Dispose(bool disposing) {
                // NOT dispose, wait for ObjectPool's DetachItem()
                //base.Dispose(disposing); 
                _pool.ReturnItem(this);
            }

            public void Destroy() {
                base.Dispose(true);
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
