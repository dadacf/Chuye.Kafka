using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;

namespace Chuye.Kafka {
    public class Option : IDisposable {
        private Uri[] _brokerUris;
        private ConnectionFactory _connectionFactory;
        private Client _client;
        private NameValueCollection _property;

        public IReadOnlyList<Uri> BrokerUris {
            get { return _brokerUris; }
        }

        public NameValueCollection Property {
            get { return _property; }
        }

        public Option(params Uri[] brokerUris) {
            if (brokerUris == null || brokerUris.Length == 0) {
                throw new ArgumentOutOfRangeException("brokerUris");
            }
            _brokerUris = brokerUris;
            _property = new NameValueCollection();
        }

        public ConnectionFactory GetSharedConnections() {
            if (_connectionFactory != null) {
                return _connectionFactory;
            }

            //由于 new ConnectionFactory() 不投入使用则无副作用和托管资源，故以下句式可省略
            //var factory = new ConnectionFactory();
            //if (Interlocked.CompareExchange(ref _factory, factory, null) == null) {
            //    factory.Dispose();
            //}
            Interlocked.CompareExchange(ref _connectionFactory, new ConnectionFactory(), null);
            return _connectionFactory;
        }

        public Client GetSharedClient() {
            if (_client != null) {
                return _client;
            }
            //同 GetSharedConnections()
            Interlocked.CompareExchange(ref _client, new Client(this), null);
            return _client;
        }

        public void Dispose() {
            if (_connectionFactory != null) {
                _connectionFactory.Dispose();
            }
        }
    }
}
