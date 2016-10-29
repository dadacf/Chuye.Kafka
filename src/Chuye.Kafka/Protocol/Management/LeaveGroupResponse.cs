using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //LeaveGroupResponse => ErrorCode
    //  ErrorCode => int16
    public class LeaveGroupResponse : Response {
        //Possible Error Codes:
        //* GROUP_LOAD_IN_PROGRESS (14)
        //* CONSUMER_COORDINATOR_NOT_AVAILABLE (15)
        //* NOT_COORDINATOR_FOR_CONSUMER (16)
        //* UNKNOWN_CONSUMER_ID (25)
        //* GROUP_AUTHORIZATION_FAILED (30)
        public ErrorCode ErrorCode { get; set; }

        protected override void DeserializeContent(KafkaReader reader) {
            ErrorCode = (ErrorCode)reader.ReadInt16();
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write((Int16)ErrorCode);
        }

        public override void TryThrowFirstErrorOccured() {
            if (ErrorCode != ErrorCode.NoError) {
                throw new ProtocolException(ErrorCode);
            }
        }
    }
}
