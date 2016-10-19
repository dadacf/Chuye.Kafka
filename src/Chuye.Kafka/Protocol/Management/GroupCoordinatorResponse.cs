using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
    //  ErrorCode => int16
    //  CoordinatorId => int32
    //  CoordinatorHost => string
    //  CoordinatorPort => int32
    public class GroupCoordinatorResponse : Response {
        //Possible Error Codes
        //* GROUP_COORDINATOR_NOT_AVAILABLE (15)
        //* NOT_COORDINATOR_FOR_GROUP (16)
        //  GROUP_AUTHORIZATION_FAILED (30)
        public ErrorCode ErrorCode { get; set; }
        public Int32 CoordinatorId { get; set; }
        public String CoordinatorHost { get; set; }
        public Int32 CoordinatorPort { get; set; }

        protected override void DeserializeContent(KafkaReader reader) {
            ErrorCode       = (ErrorCode)reader.ReadInt16();
            CoordinatorId   = reader.ReadInt32();
            CoordinatorHost = reader.ReadString();
            CoordinatorPort = reader.ReadInt32();
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write((Int16)ErrorCode);
            writer.Write(CoordinatorId);
            writer.Write(CoordinatorHost);
            writer.Write(CoordinatorPort);
        }

        public override void TryThrowFirstErrorOccured() {
            if (ErrorCode != ErrorCode.NoError) {
                throw new ProtocolException(ErrorCode);
            }
        }
    }
}
