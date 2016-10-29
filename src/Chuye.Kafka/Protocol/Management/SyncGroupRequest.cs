using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //SyncGroupRequest => GroupId GenerationId MemberId GroupAssignment
    //  GroupId => string
    //  GenerationId => int32
    //  MemberId => string
    //  GroupAssignment => [MemberId MemberAssignment]
    //    MemberId => string
    //    MemberAssignment => bytes
    public class SyncGroupRequest : Request {
        public String GroupId { get; set; }
        public Int32 GenerationId { get; set; }
        public String MemberId { get; set; }
        public IList<SyncGroupGroupAssignment> GroupAssignments { get; set; }

        public SyncGroupRequest()
            : base(ApiKey.SyncGroupRequest) {
        }

        public SyncGroupRequest(String groupId, Int32 generationId, String memberId)
            : base(ApiKey.SyncGroupRequest) {
            GroupId      = groupId;
            GenerationId = generationId;
            MemberId     = memberId;
        }

        public SyncGroupRequest(String groupId, Int32 generationId, String memberId, SyncGroupMemberAssignment memberAssignment)
            : base(ApiKey.SyncGroupRequest) {
            GroupId          = groupId;
            GenerationId     = generationId;
            MemberId         = memberId;
            GroupAssignments = new[] {
                new SyncGroupGroupAssignment {
                    MemberId         = memberId,
                    MemberAssignment = memberAssignment,
                }
            };
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(GroupId);
            writer.Write(GenerationId);
            writer.Write(MemberId);
            writer.Write(GroupAssignments);
        }

        protected override void DeserializeContent(KafkaReader reader) {
            GroupId          = reader.ReadString();
            GenerationId     = reader.ReadInt32();
            MemberId         = reader.ReadString();
            GroupAssignments = reader.ReadArray<SyncGroupGroupAssignment>();
        }
    }

    public class SyncGroupGroupAssignment : IKafkaWriteable, IKafkaReadable {
        public String MemberId { get; set; }
        public SyncGroupMemberAssignment MemberAssignment { get; set; }

        public void SaveTo(KafkaWriter writer) {
            writer.Write(MemberId);
            if (MemberAssignment != null) {
                MemberAssignment.SaveTo(writer);
            }
            else {
                writer.Write((Byte[])null);
            }
        }

        public void FetchFrom(KafkaReader reader) {
            MemberId         = reader.ReadString();
            MemberAssignment = new SyncGroupMemberAssignment();
            MemberAssignment.FetchFrom(reader);
        }
    }

    public class SyncGroupMemberAssignment : IKafkaWriteable, IKafkaReadable {
        public Int16 Version { get; set; }
        public IList<SyncGroupPartitionAssignment> PartitionAssignments { get; set; }
        public Byte[] UserData { get; set; }

        public void FetchFrom(KafkaReader reader) {
            //Version = reader.ReadInt16();
            //PartitionAssignments = reader.ReadArray<SyncGroupPartitionAssignment>();
            //UserData = reader.ReadBytes();

            var memberAssignment = reader.ReadBytes();
            if (memberAssignment == null || memberAssignment.Length == 0) {
                return;
            }

            using (var stream = new MemoryStream(memberAssignment))
            using (var reader2 = new KafkaReader(stream)) {
                Version              = reader2.ReadInt16();
                PartitionAssignments = reader2.ReadArray<SyncGroupPartitionAssignment>();
                UserData             = reader2.ReadBytes();
            }
        }

        public void SaveTo(KafkaWriter writer) {
            //writer.Write(Version);
            //writer.Write(PartitionAssignments);
            //writer.Write(UserData);

            using (var stream = new MemoryStream(4096)) {
                var writer2 = new KafkaWriter(stream);
                writer2.Write(Version);
                writer2.Write(PartitionAssignments);
                writer2.Write(UserData);
                writer2.Dispose();

                stream.Seek(0L, SeekOrigin.Begin);
                var memberAssignment = stream.ToArray();
                writer.Write(memberAssignment);
            }
        }
    }

    public class SyncGroupPartitionAssignment : IKafkaWriteable, IKafkaReadable {
        public String Topic { get; set; }
        public Int32[] Partitions { get; set; }

        public void FetchFrom(KafkaReader reader) {
            Topic      = reader.ReadString();
            Partitions = reader.ReadInt32Array();
        }

        public void SaveTo(KafkaWriter writer) {
            writer.Write(Topic);
            writer.Write(Partitions);
        }
    }
}
