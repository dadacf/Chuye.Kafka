using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //LeaveGroupRequest => GroupId MemberId
    //  GroupId => string
    //  MemberId => string
    public class LeaveGroupRequest : Request {
        public String GroupId { get; set; }
        public String MemberId { get; set; }

        public LeaveGroupRequest()
            : base(ApiKey.LeaveGroupRequest) {
        }

        public LeaveGroupRequest(String groupId, String memberId)
            : this() {
            GroupId = groupId;
            MemberId = memberId;
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(GroupId);
            writer.Write(MemberId);
        }

        protected override void DeserializeContent(KafkaReader reader) {
            GroupId = reader.ReadString();
            MemberId = reader.ReadString();
        }
    }
}
