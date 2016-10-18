using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka {
    public interface IConsumer {
        String GroupId { get; }
        IEnumerable<Message> Fetch(String topic);
    }

    public class Consumer : IConsumer {
        private readonly String _groupId;
        public String GroupId {
            get { return _groupId; }
        }

        public Consumer(String groupId) {
            _groupId = groupId;
        }

        public IEnumerable<Message> Fetch(String topic) {
            var fetchReq = new FetchRequest();
            throw new NotImplementedException();
        }
    }
}
