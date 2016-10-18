using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //ListGroupsRequest =>
    public class ListGroupsRequest : Request {
        public ListGroupsRequest()
            : base(ApiKey.ListGroupsRequest) {
        }

        protected override void SerializeContent(KafkaStreamWriter writer) {
        }

        protected override void DeserializeContent(KafkaStreamReader reader) {
        }
    }
}
