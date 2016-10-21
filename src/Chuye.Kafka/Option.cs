using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Chuye.Kafka {
    public class Option {
        private Uri[] _brokerUris;
        private ConnectionFactory _factory;
        private NameValueCollection _property;

        public IReadOnlyList<Uri> BrokerUris {
            get { return _brokerUris; }
        }

        public NameValueCollection Property {
            get { return _property; }
        }

        public Option(params Uri[] brokerUris) {
            _brokerUris = brokerUris;
            _property = new NameValueCollection();
        }

        public ConnectionFactory OpenShared() {
            if (_factory != null) {
                return _factory;
            }

            Interlocked.CompareExchange(ref _factory, new ConnectionFactory(), null);
            return _factory;
        }
    }
}
