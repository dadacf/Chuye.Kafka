using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.IO;

namespace Chuye.Kafka {
    public class Connection : IDisposable {
        private readonly Socket _socket;

        public Connection(Socket socket) {
            _socket = socket;
        }

        public Response Submit(Request request) {
            using (var reqStream = new MemoryStream()) {
                request.WriteTo(reqStream);
                using (var networkStream = new NetworkStream(_socket)) {
                    reqStream.CopyTo(networkStream);
                    reqStream.Flush();

                    var buffer = new Byte[4];
                    //get resp type
                    networkStream.Read(buffer, 0, buffer.Length);
                    //read the left

                    Response response = null;
                    response.ReadFrom(networkStream);
                    return response;
                }
            }
        }

        public void Dispose() {
            _socket.Dispose();
        }
    }

    public class ConnectionFactory {
        private readonly Dictionary<String, SocketPool> _sockets;
        private readonly Object _sync;

        public PoolState GetState() {
            return new PoolState {
                Constructed = _sockets.Sum(x => x.Value.State.Constructed),
                Avaliable = _sockets.Sum(x => x.Value.State.Avaliable),
                Detached = _sockets.Sum(x => x.Value.State.Detached),
                Occupied = _sockets.Sum(x => x.Value.State.Occupied)
            };
        }

        public ConnectionFactory() {
            _sockets = new Dictionary<String, SocketPool>();
            _sync = new Object();
        }

        public Connection GetPooledConnection(Uri uri) {
            lock (_sync) {
                SocketPool pool;
                if (!_sockets.TryGetValue(uri.AbsoluteUri, out pool)) {
                    pool = new SocketPool(uri);
                    _sockets.Add(uri.AbsoluteUri, pool);
                }
                var socket = pool.Acquire();
                return new Connection(socket);
            }
        }

        public void CleanUp() {
            foreach (var item in _sockets) {
                item.Value.Cleanup();
            }
        }

        internal class SocketPool {
            private readonly ObjectPool<LimitedSocket> _sockets;
            private readonly Uri _uri;

            public PoolState State {
                get { return _sockets.State; }
            }

            public SocketPool(Uri uri) {
                _uri = uri;
                _sockets = new ObjectPool<LimitedSocket>(() =>
                    new LimitedSocket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp, this),
                    detacher: s => s.Destroy());
                _sockets.ItemCreated += Sockets_ItemCreated;
            }

            private void Sockets_ItemCreated(object sender, ItemCreatedEventArgs e) {
                var socket = (Socket)e.Item;
                socket.Connect(_uri.Host, _uri.Port);
            }

            //http://stackoverflow.com/questions/2661764/how-to-check-if-a-socket-is-connected-disconnected-in-c
            private bool IsConnected(Socket socket) {
                return !((socket.Poll(100, SelectMode.SelectRead) && (socket.Available == 0)) || !socket.Connected);
            }

            public void Cleanup() {
                _sockets.Dispose();
            }

            public Socket Acquire() {
                var socket = _sockets.AcquireItem();
                while (!IsConnected(socket)) {
                    _sockets.DetachItem(socket);
                    socket = _sockets.AcquireItem();
                    socket.Connect(_uri.Host, _uri.Port);
                }
                return socket;
            }

            public void Return(LimitedSocket limitedSocket) {
                _sockets.ReturnItem(limitedSocket);
            }
        }

        internal class LimitedSocket : Socket {
            private readonly SocketPool _pool;

            public LimitedSocket(AddressFamily addressFamily, SocketType socketType, ProtocolType protocolType, SocketPool pool)
                : base(addressFamily, socketType, protocolType) {
                _pool = pool;
            }

            protected override void Dispose(bool disposing) {
                //base.Dispose(disposing); // NOT dispose, wait for ObjectPool's DetachItem()
                _pool.Return(this);
            }

            public void Destroy() {
                base.Dispose(true);
            }
        }
    }
}
