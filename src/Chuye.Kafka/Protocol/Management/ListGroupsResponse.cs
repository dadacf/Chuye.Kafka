using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //ListGroupsResponse => ErrorCode Groups
    //  ErrorCode => int16
    //  Groups => [GroupId ProtocolType]
    //    GroupId => string
    //    ProtocolType => string
    public class ListGroupsResponse : Response {
        /// <summary>
        /// Possible Error Codes:
        //   GROUP_COORDINATOR_NOT_AVAILABLE(15)
        //   AUTHORIZATION_FAILED(29)
        /// </summary>
        public ErrorCode ErrorCode { get; set; }
        public ListGroupsResponseGroup[] Groups { get; set; }

        protected override void DeserializeContent(KafkaStreamReader reader) {
            ErrorCode = (ErrorCode)reader.ReadInt16();
            Groups    = reader.ReadArray<ListGroupsResponseGroup>();
        }

        protected override void SerializeContent(KafkaStreamWriter writer) {
            writer.Write((Int16)ErrorCode);
            writer.Write(Groups);
        }

        public override void ThrowIfFail() {
            if (ErrorCode != ErrorCode.NoError) {
                throw new ProtocolException(ErrorCode);
            }
        }
    }

    public class ListGroupsResponseGroup : IKafkaReadable, IKafkaWriteable {
        public String GroupId { get; set; }
        public String ProtocolType { get; set; }

        public void FetchFrom(KafkaStreamReader reader) {
            GroupId      = reader.ReadString();
            ProtocolType = reader.ReadString();
        }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(GroupId);
            writer.Write(ProtocolType);
        }
    }
}
