using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //SyncGroupResponse => ErrorCode MemberAssignment
    //  ErrorCode => int16
    //  MemberAssignment => bytes
    public class SyncGroupResponse : Response {
        public ErrorCode ErrorCode { get; set; }
        public SyncGroupMemberAssignment MemberAssignment { get; set; }

        protected override void DeserializeContent(KafkaReader reader) {
            //Possible Error Codes:
            //* GROUP_COORDINATOR_NOT_AVAILABLE (15)
            //* NOT_COORDINATOR_FOR_GROUP (16)
            //* ILLEGAL_GENERATION (22)
            //* UNKNOWN_MEMBER_ID (25)
            //* REBALANCE_IN_PROGRESS (27)
            //* GROUP_AUTHORIZATION_FAILED (30)
            ErrorCode        = (ErrorCode)reader.ReadInt16();
            MemberAssignment = new SyncGroupMemberAssignment();
            MemberAssignment.FetchFrom(reader);
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write((Int16)ErrorCode);
            MemberAssignment.SaveTo(writer);
        }

        public override void TryThrowFirstErrorOccured() {
            if (ErrorCode != ErrorCode.NoError) {
                throw new ProtocolException(ErrorCode);
            }
        }
    }
}
