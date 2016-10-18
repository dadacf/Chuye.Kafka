using System;
using System.IO;
using System.Linq;
using System.Text;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Management;
using KellermanSoftware.CompareNetObjects;
using Xunit;

namespace Chuye.Kafka.Tests.Protocol {
    public class RequestSerializeTest {
        private readonly Random _random = new Random();

        [Fact]
        public void OffsetRequest() {
            var request             = new OffsetRequest();
            request.ReplicaId       = _random.Next();
            request.TopicPartitions = new[] {
                new OffsetsRequestTopicPartition {
                    TopicName = Guid.NewGuid().ToString(),
                    Details   = new [] {
                        new OffsetsRequestTopicPartitionDetail() {
                            Partition          = _random.Next(),
                            Time               = (Int64)_random.Next(),
                            MaxNumberOfOffsets = _random.Next()
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new OffsetRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void OffsetCommitRequestV0() {
            var request             = new OffsetCommitRequestV0();
            request.ConsumerGroup   = Guid.NewGuid().ToString();
            request.TopicPartitions = new OffsetCommitRequestTopicPartitionV0[1];
            request.TopicPartitions = new[] {
                new OffsetCommitRequestTopicPartitionV0 {
                    TopicName = Guid.NewGuid().ToString(),
                    Details   = new [] {
                        new OffsetCommitRequestTopicPartitionDetailV0 {
                            Partition = _random.Next(),
                            Offset    = (Int64)_random.Next(),
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new OffsetCommitRequestV0();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void OffsetFetchRequest() {
            var request             = new OffsetFetchRequest();
            request.ConsumerGroup   = Guid.NewGuid().ToString();
            request.TopicPartitions = new[] {
                new OffsetFetchRequestTopicPartition {
                    TopicName  = Guid.NewGuid().ToString(),
                    Partitions = new[] { _random.Next() }
                }
            };

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new OffsetFetchRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void FetchRequest() {
            var request             = new FetchRequest();
            request.ReplicaId       = _random.Next();
            request.MaxWaitTime     = _random.Next();
            request.MinBytes        = _random.Next();
            request.TopicPartitions = new[] {
                new FetchRequestTopicPartition {
                    TopicName          = Guid.NewGuid().ToString(),
                    FetchOffsetDetails = new [] {
                        new FetchRequestTopicPartitionDetail {
                            Partition   = _random.Next(),
                            FetchOffset = _random.Next(),
                            MaxBytes    = _random.Next()
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new FetchRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void GroupCoordinatorRequest() {
            var request = new GroupCoordinatorRequest();
            request.GroupId = Guid.NewGuid().ToString();

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new GroupCoordinatorRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void JoinGroupRequest() {
            var request            = new JoinGroupRequest();
            request.GroupId        = Guid.NewGuid().ToString();
            request.MemberId       = String.Empty;
            request.SessionTimeout = 30000;
            request.ProtocolType   = Guid.NewGuid().ToString();
            request.GroupProtocols = new[] {
                new JoinGroupRequestGroupProtocol{
                    Protocol       = Guid.NewGuid().ToString(),
                    MemberMetadata = new JoinGroupMemberMetadata {
                        Topics = new [] {
                            Guid.NewGuid().ToString(),
                            Guid.NewGuid().ToString(),
                        },
                        UserData = new Byte[] {1,2,3},
                        Version  = (Int16)Guid.NewGuid().GetHashCode(),
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new JoinGroupRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void ListGroupsRequest() {
            var request = new ListGroupsRequest();

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new ListGroupsRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void DescribeGroupsRequest() {
            var request = new DescribeGroupsRequest();
            request.Groups = new[] { Guid.NewGuid().ToString() };

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new DescribeGroupsRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void SyncGroupRequest() {
            var request              = new SyncGroupRequest();
            request.GroupId          = Guid.NewGuid().ToString();
            request.GenerationId     = _random.Next();
            request.MemberId         = Guid.NewGuid().ToString();
            request.GroupAssignments = new[] {
                new SyncGroupGroupAssignment {
                    MemberId         = Guid.NewGuid().ToString(),
                    MemberAssignment = new SyncGroupMemberAssignment {
                        PartitionAssignments =new [] {
                            new SyncGroupPartitionAssignment {
                                Topic      = Guid.NewGuid().ToString(),
                                Partitions = new [] {
                                    Guid.NewGuid().GetHashCode(),
                                    Guid.NewGuid().GetHashCode()
                                }
                            }
                        },
                        Version  = (Int16)Guid.NewGuid().GetHashCode(),
                        UserData = new Byte[]{1,2,3 }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new SyncGroupRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void HeartbeatRequest() {
            var request          = new HeartbeatRequest();
            request.GroupId      = Guid.NewGuid().ToString();
            request.GenerationId = _random.Next();
            request.MemberId     = Guid.NewGuid().ToString();

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new HeartbeatRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void LeaveGroupRequest() {
            var request      = new LeaveGroupRequest();
            request.GroupId  = Guid.NewGuid().ToString();
            request.MemberId = Guid.NewGuid().ToString();

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new LeaveGroupRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void ProduceRequest() {
            var count     = _random.Next(2, 5);
            var messages  = Enumerable.Range(0, count)
                .Select(x => new Message(Guid.NewGuid().ToString(), Guid.NewGuid().ToString()))
                .ToArray();
            var messageSetArray = new MessageSetDetail[count];
            for (int i = 0; i < messageSetArray.Length; i++) {
                var messageSet = messageSetArray[i] = new MessageSetDetail();
                messageSet.Message = new Kafka.Protocol.MessageSetItem();
                if (messages[i].Key != null) {
                    messageSet.Message.Key = Encoding.UTF8.GetBytes(messages[i].Key);
                }
                if (messages[i].Value != null) {
                    messageSet.Message.Value = Encoding.UTF8.GetBytes(messages[i].Value);
                }
            }

            var request             = new ProduceRequest();
            request.RequiredAcks    = AcknowlegeStrategy.Block; //important
            request.Timeout         = _random.Next();
            request.TopicPartitions = new[] {
                new ProduceRequestTopicPartition {
                    TopicName = Guid.NewGuid().ToString(),
                    Details   = new [] {
                        new ProduceRequestTopicDetail {
                            Partition  = _random.Next(),
                            MessageSet = new MessageSet {
                                Items  = messageSetArray
                            }
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            request.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var request2 = new ProduceRequest();
            request2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            request.Serialize(binary2);
            Assert.Equal(binary1.Length, binary2.Length);

            using (var stream1 = new MemoryStream())
            using (var stream2 = new MemoryStream()) {
                binary1.Seek(0L, SeekOrigin.Begin);
                binary1.CopyTo(stream1);

                binary2.Seek(0L, SeekOrigin.Begin);
                binary2.CopyTo(stream2);

                Assert.Equal(stream1.Length, stream2.Length);
                stream1.Seek(0L, SeekOrigin.Begin);
                var bytes1 = stream1.ToArray();

                stream2.Seek(0L, SeekOrigin.Begin);
                var bytes2 = stream2.ToArray();
                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i = 0; i < bytes1.Length; i++) {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }
    }
}

