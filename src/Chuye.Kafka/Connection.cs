using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.IO;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka {
    public class Connection : IDisposable {
        private const Int32 ResponseLengthSize = 4;
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
            using (var stream = new MemoryStream(4096)) {
                request.Serialize(stream);
                stream.Seek(0L, SeekOrigin.Begin);

                using (var networkStream = new NetworkStream(_socket)) {
                    stream.CopyTo(networkStream);
                    stream.Flush();

                    //var buffer = new Byte[4];
                    //get resp type
                    //networkStream.Read(buffer, 0, buffer.Length);
                    //read the left

                    var response = GenerateResponse(request.ApiKey);
                    response.Deserialize(networkStream);
                    return response;
                }
            }
        }

        private Response GenerateResponse(ApiKey apiKey) {
            var requestTypeName = apiKey.ToString();
            var responseTypeName = requestTypeName.Substring(0, requestTypeName.Length - "Request".Length) + "Response";
            var responseTypeFullName = (Int32)apiKey < 10 ? "Chuye.Kafka.Protocol." + responseTypeName
                : "Chuye.Kafka.Protocol.Management." + responseTypeName;
            var responseType = Type.GetType(responseTypeFullName);
            return (Response)Activator.CreateInstance(responseType);
        }

        public void Dispose() {
            if (_socket != null) {
                _socket.Dispose();
            }
        }
    }
}