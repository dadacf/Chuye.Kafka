using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.IO;

namespace Chuye.Kafka {
    public class Connection : IDisposable {
        private readonly Socket _socket;

        public Socket Socket {
            get { return _socket; }
        }

        public Connection(Uri uri)
            : this(uri.Host, uri.Port) {
        }

        public Connection(String host, Int32 port) {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _socket.Connect(host, port);
        }

        public Connection(Socket socket) {
            if (socket == null) {
                throw new ArgumentOutOfRangeException("socket");
            }
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
                    throw new NotImplementedException();
                    response.ReadFrom(networkStream);
                    return response;
                }
            }
        }

        public void Dispose() {
            if (_socket != null) {
                _socket.Dispose();
            }
        }
    }
}   