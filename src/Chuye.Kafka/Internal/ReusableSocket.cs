using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class ReusableSocket : Socket {
        private readonly SocketPool _pool;

        public ReusableSocket(AddressFamily addressFamily, SocketType socketType, ProtocolType protocolType, SocketPool pool)
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

}
