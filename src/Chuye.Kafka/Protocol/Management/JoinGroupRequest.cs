using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //JoinGroupRequest => GroupId SessionTimeout MemberId ProtocolType GroupProtocols
    //  GroupId => string
    //  SessionTimeout => int32
    //  MemberId => string
    //  ProtocolType => string
    //  GroupProtocols => [ProtocolName ProtocolMetadata]
    //    ProtocolName => string
    //    ProtocolMetadata => bytes
    public class JoinGroupRequest : Request {
        private const String Protocol_Type = "consumer";
        private const String Protocol_Name = "AssignmentStrategy";

        public String GroupId { get; set; }
        public Int32 SessionTimeout { get; set; }
        public String MemberId { get; set; }
        public String ProtocolType { get; set; }
        public IList<JoinGroupRequestGroupProtocol> GroupProtocols { get; set; }

        public JoinGroupRequest()
            : base(ApiKey.JoinGroupRequest) {
        }

        public JoinGroupRequest(String groupId, String memberId, String[] topics, Int32 sessionTimeout = 5000)
            : base(ApiKey.JoinGroupRequest) {
            GroupId        = groupId;
            MemberId       = memberId;
            SessionTimeout = sessionTimeout;
            ProtocolType   = Protocol_Type;
            GroupProtocols = new[] {
                new JoinGroupRequestGroupProtocol {
                    Protocol       = Protocol_Name,
                    MemberMetadata = new JoinGroupMemberMetadata {
                        Topics     = topics
                    }
                }
            };
        }


        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(GroupId);
            writer.Write(SessionTimeout);
            writer.Write(MemberId);
            writer.Write(ProtocolType);
            writer.Write(GroupProtocols);
        }

        protected override void DeserializeContent(KafkaReader reader) {
            GroupId        = reader.ReadString();
            SessionTimeout = reader.ReadInt32();
            MemberId       = reader.ReadString();
            ProtocolType   = reader.ReadString();
            GroupProtocols = reader.ReadArray<JoinGroupRequestGroupProtocol>();
        }
    }

    public class JoinGroupRequestGroupProtocol : IKafkaWriteable, IKafkaReadable {
        public String Protocol { get; set; }
        public JoinGroupMemberMetadata MemberMetadata { get; set; }

        public void SaveTo(KafkaWriter writer) {
            writer.Write(Protocol);
            MemberMetadata.SaveTo(writer);
        }

        public void FetchFrom(KafkaReader reader) {
            Protocol = reader.ReadString();
            MemberMetadata = new JoinGroupMemberMetadata();
            MemberMetadata.FetchFrom(reader);
        }
    }

    public class JoinGroupMemberMetadata : IKafkaWriteable, IKafkaReadable {
        public Int16 Version { get; set; }
        public String[] Topics { get; set; }
        public Byte[] UserData { get; set; }

        public void SaveTo(KafkaWriter writer) {
            //writer.Write(Version);
            //writer.Write(Topics);
            //writer.Write(UserData);

            using (var stream = new MemoryStream(4096)) {
                var writer2 = new KafkaWriter(stream);
                writer2.Write(Version);
                writer2.Write(Topics);
                writer2.Write(UserData);
                writer2.Dispose();

                stream.Seek(0L, SeekOrigin.Begin);
                var protocolMetadata = stream.ToArray();
                writer.Write(protocolMetadata);
            }
        }

        public void FetchFrom(KafkaReader reader) {
            //Version = reader.ReadInt16();
            //Topics = reader.ReadStrings();
            //UserData = reader.ReadBytes();

            var protocolMetadata = reader.ReadBytes();
            if (protocolMetadata == null) {
                return;
            }
            using (var stream = new MemoryStream(protocolMetadata))
            using (var reader2 = new KafkaReader(stream)) {
                Version  = reader2.ReadInt16();
                Topics   = reader2.ReadStrings();
                UserData = reader2.ReadBytes();
            }
        }
    }
}
