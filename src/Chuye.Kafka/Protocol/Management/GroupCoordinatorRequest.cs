using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //GroupCoordinatorRequest => GroupId
    //  GroupId => string
    public class GroupCoordinatorRequest : Request {
        public String GroupId { get; set; }

        public GroupCoordinatorRequest()
            : base(ApiKey.GroupCoordinatorRequest) {
        }

        public GroupCoordinatorRequest(String groupId)
            : this() {
            GroupId = groupId;
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(GroupId);
        }

        protected override void DeserializeContent(KafkaReader reader) {
            GroupId = reader.ReadString();
        }
    }
}
