using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //HeartbeatRequest => GroupId GenerationId MemberId
    //  GroupId => string
    //  GenerationId => int32
    //  MemberId => string
    public class HeartbeatRequest : Request {
        public String GroupId { get; set; }
        public Int32 GenerationId { get; set; }
        public String MemberId { get; set; }

        public HeartbeatRequest()
            : base(ApiKey.HeartbeatRequest) {
        }

        public HeartbeatRequest(String groupId, Int32 generationId, String memberId)
            : base(ApiKey.HeartbeatRequest) {
            GroupId      = groupId;
            GenerationId = generationId;
            MemberId     = memberId;
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(GroupId);
            writer.Write(GenerationId);
            writer.Write(MemberId);
        }

        protected override void DeserializeContent(KafkaReader reader) {
            GroupId = reader.ReadString();
            GenerationId = reader.ReadInt32();
            MemberId = reader.ReadString();
        }
    }
}
