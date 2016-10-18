using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //DescribeGroupsResponse => [ErrorCode GroupId State ProtocolType Protocol Members]
    //  ErrorCode => int16
    //  GroupId => string
    //  State => string
    //  ProtocolType => string
    //  Protocol => string
    //  Members => [MemberId ClientId ClientHost MemberMetadata MemberAssignment]
    //    MemberId => string
    //    ClientId => string
    //    ClientHost => string
    //    MemberMetadata => bytes
    //    MemberAssignment => bytes
    public class DescribeGroupsResponse : Response {
        public DescribeGroupsResponseDetail[] Details { get; set; }

        protected override void DeserializeContent(KafkaStreamReader reader) {
            Details = reader.ReadArray<DescribeGroupsResponseDetail>();
        }

        protected override void SerializeContent(KafkaStreamWriter writer) {
            writer.Write(Details);
        }

        public override void ThrowIfFail() {
            var errors = Details.Select(x => x.ErrorCode)
                .Where(x => x != ErrorCode.NoError);
            if (errors.Any()) {
                throw new ProtocolException(errors.First());
            }
        }
    }

    public class DescribeGroupsResponseDetail : IKafkaReadable, IKafkaWriteable {
        /// <summary>
        /// Possible Error Codes:
        ///  GROUP_LOAD_IN_PROGRESS (14)
        ///  GROUP_COORDINATOR_NOT_AVAILABLE (15)
        ///  NOT_COORDINATOR_FOR_GROUP (16)
        ///  AUTHORIZATION_FAILED (29)
        /// </summary>
        public ErrorCode ErrorCode { get; set; }
        public String GroupId { get; set; }
        public String State { get; set; }
        public String ProtocolType { get; set; }
        public String Protocol { get; set; }
        public DescribeGroupsResponseMember[] Members { get; set; }

        public void FetchFrom(KafkaStreamReader reader) {
            ErrorCode    = (ErrorCode)reader.ReadInt16();
            GroupId      = reader.ReadString();
            State        = reader.ReadString();
            ProtocolType = reader.ReadString();
            Protocol     = reader.ReadString();
            Members      = reader.ReadArray<DescribeGroupsResponseMember>();
        }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write((Int16)ErrorCode);
            writer.Write(GroupId);
            writer.Write(State);
            writer.Write(ProtocolType);
            writer.Write(Protocol);
            writer.Write(Members);
        }
    }

    public class DescribeGroupsResponseMember : IKafkaReadable, IKafkaWriteable {
        public String MemberId { get; set; }
        public String ClientId { get; set; }
        public String ClientHost { get; set; }
        public JoinGroupMemberMetadata MemberMetadata { get; set; }
        public SyncGroupMemberAssignment MemberAssignment { get; set; }

        public void FetchFrom(KafkaStreamReader reader) {
            MemberId         = reader.ReadString();
            ClientId         = reader.ReadString();
            ClientHost       = reader.ReadString();
            //MemberMetadata   = reader.ReadBytes();
            //MemberAssignment = reader.ReadBytes();
            MemberMetadata = new JoinGroupMemberMetadata();
            MemberMetadata.FetchFrom(reader);
            MemberAssignment = new SyncGroupMemberAssignment();
            MemberAssignment.FetchFrom(reader);
        }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(MemberId);
            writer.Write(ClientId);
            writer.Write(ClientHost);
            //writer.Write(MemberMetadata);
            //writer.Write(MemberAssignment);
            if (MemberMetadata == null) {
                writer.Write((Byte[])null);
            }
            else {
                MemberMetadata.WriteTo(writer);
            }
            if (MemberAssignment == null) {
                writer.Write((Byte[])null);
            }
            else {
                MemberAssignment.WriteTo(writer);
            }
        }
    }
}
