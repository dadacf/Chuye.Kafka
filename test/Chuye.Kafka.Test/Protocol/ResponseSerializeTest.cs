using System;
using System.IO;
using System.Linq;
using System.Text;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Management;
using KellermanSoftware.CompareNetObjects;
using Xunit;

namespace Chuye.Kafka.Tests.Protocol {

    public class ResponseSerializeTest {
        private readonly Random _random = new Random();
        [Fact]
        public void OffsetResponse() {
            var response1 = new OffsetResponse();
            response1.TopicPartitions = new[] {
                new OffsetResponseTopicPartition {
                    TopicName = Guid.NewGuid().ToString(),
                    PartitionOffsets = new [] {
                        new OffsetResponsePartitionOffset {
                            Partition = _random.Next(),
                            ErrorCode = ErrorCode.NoError,
                            Offsets   = new Int64 [] { _random.Next() }
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new OffsetResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void OffsetCommitResponse() {
            var response1 = new OffsetCommitResponse {
                TopicPartitions = new[] {
                    new OffsetCommitResponseTopicPartition {
                        TopicName = Guid.NewGuid().ToString(),
                        Details   = new [] {
                            new OffsetCommitResponseTopicPartitionDetail {
                                Partition = _random.Next(),
                            }
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new OffsetCommitResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void OffsetFetchResponse() {
            var response1 = new OffsetFetchResponse {
                TopicPartitions = new[] {
                    new OffsetFetchResponseTopicPartition {
                        TopicName = Guid.NewGuid().ToString(),
                        Details   = new [] {
                            new OffsetFetchResponseTopicPartitionDetail {
                                Partition = _random.Next(),
                                Metadata  = Guid.NewGuid().ToString(),
                                Offset    = (Int64)_random.Next(),
                            }
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new OffsetFetchResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void FetchResponse() {
            var response1 = new FetchResponse {
                TopicPartitions = new[] {
                    new FetchResponseTopicPartition {
                        TopicName    = Guid.NewGuid().ToString(),
                        MessageBodys = new [] {
                            new MessageBody {
                                Partition           = _random.Next(),
                                HighwaterMarkOffset = (Int64)_random.Next(),
                                //MessageSetSize      = _random.Next(),
                                //todo: MessageSetCollection 的断言需要另外进行
                                MessageSet          = new MessageSet {
                                    Items = new MessageSetDetail[0] {
                                        //new MessageSet {
                                        //    Offset = (Int64)_random.Next(),
                                        //    //MessageSize = _random.Next(),
                                        //    Message = new Message {
                                        //        //Crc = _random.Next(),
                                        //        MagicByte  = (Byte)_random.Next(Byte.MaxValue),
                                        //        Attributes = (Byte)_random.Next(Byte.MaxValue),
                                        //        Key        = new Byte[0],
                                        //        Value      = new Byte[0]
                                        //    }
                                        //}
                                    }
                                }
                            }
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new FetchResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void GroupCoordinatorResponse() {
            var response1 = new GroupCoordinatorResponse {
                //CorrelationId = _random.Next(),
                CoordinatorHost = Guid.NewGuid().ToString(),
                CoordinatorPort = _random.Next(),
            };

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new GroupCoordinatorResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void JoinGroupResponse() {
            var response1 = new JoinGroupResponse {
                GenerationId  = _random.Next(),
                GroupProtocol = Guid.NewGuid().ToString(),
                LeaderId      = Guid.NewGuid().ToString(),
                MemberId      = Guid.NewGuid().ToString(),
                Members       = new[] {
                    new JoinGroupResponseMember {
                        MemberId       = Guid.NewGuid().ToString(),
                        MemberMetadata = new JoinGroupMemberMetadata {
                            Topics     = new [] {
                                Guid.NewGuid().ToString(),
                            },
                            UserData = new Byte[] {1,2,3},
                            Version  = (Int16)Guid.NewGuid().GetHashCode()
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new JoinGroupResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void ListGroupsResponse() {
            var response1 = new ListGroupsResponse {
                Groups = new[] {
                    new ListGroupsResponseGroup {
                        GroupId      = Guid.NewGuid().ToString(),
                        ProtocolType = Guid.NewGuid().ToString(),
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new ListGroupsResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void DescribeGroupsResponse() {
            var response1 = new DescribeGroupsResponse {
                Details = new[] {
                    new DescribeGroupsResponseDetail {
                        GroupId      = Guid.NewGuid().ToString(),
                        State        = Guid.NewGuid().ToString(),
                        ProtocolType = Guid.NewGuid().ToString(),
                        Protocol     = Guid.NewGuid().ToString(),
                        Members      = new [] {
                            new DescribeGroupsResponseMember {
                                MemberId         = Guid.NewGuid().ToString(),
                                ClientId         = Guid.NewGuid().ToString(),
                                ClientHost       = Guid.NewGuid().ToString(),
                                MemberMetadata   = new JoinGroupMemberMetadata(),
                                MemberAssignment = new SyncGroupMemberAssignment(),
                            }
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new DescribeGroupsResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void SyncGroupResponse() {
            var response1 = new SyncGroupResponse {
                MemberAssignment = new SyncGroupMemberAssignment {
                    PartitionAssignments = new[] {
                        new SyncGroupPartitionAssignment {
                            Topic = Guid.NewGuid().ToString(),
                                Partitions = new [] {
                                    Guid.NewGuid().GetHashCode(),
                                    Guid.NewGuid().GetHashCode()
                                }
                            }
                        },
                    Version  = (Int16)Guid.NewGuid().GetHashCode(),
                    UserData = new Byte[] { 1, 2, 3 }
                }
            };

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new SyncGroupResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void HeartbeatResponse() {
            var response1 = new HeartbeatResponse();

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new HeartbeatResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void LeaveGroupResponse() {
            var response1 = new LeaveGroupResponse();

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new LeaveGroupResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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
        public void ProduceResponse() {
            var response1 = new ProduceResponse {
                TopicPartitions = new[] {
                    new ProduceResponseTopicPartition {
                        TopicName = Guid.NewGuid().ToString(),
                        Details   = new [] {
                            new ProduceResponseTopicPartitionDetail {
                                Partition = _random.Next(),
                                Offset    = (Int64)_random.Next(),
                            }
                        }
                    }
                }
            };

            Stream binary1 = new MemoryStream();
            response1.Serialize(binary1);

            binary1.Seek(0L, SeekOrigin.Begin);
            var response2 = new ProduceResponse();
            response2.Deserialize(binary1);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response1, response2);
            Assert.True(result.AreEqual);

            Stream binary2 = new MemoryStream();
            response2.Serialize(binary2);
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

