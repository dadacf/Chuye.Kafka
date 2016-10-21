using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Management;

namespace Chuye.Kafka.Internal {
    public class Coordinator {
        private readonly Client _client;
        private readonly TopicPartitionDispatcher _partitionDispatcher;
        private readonly String _groupId;
        private Broker _coordinateBroker;
        private Int32 _generationId;
        private String _memberId;
        private JoinGroupResponseMember[] _members;
        private Timer _heartbeatTimer;
        private Int32? _sessionTimeout;

        public Coordinator(Option option, String groupId)
            : this(new Client(option), groupId) {
        }

        public Coordinator(Client client, String groupId) {
            _client = client;
            _groupId = groupId;
            _partitionDispatcher = new TopicPartitionDispatcher(_client);
            _client.ReplaceDispatcher(_partitionDispatcher);
            _heartbeatTimer = new Timer(HeartbeatCallback);
        }

        private void HeartbeatCallback(Object state) {
            var heartbeatResponse = Heartbeat(_groupId, _memberId, _generationId);
            var sessionTimeoutStr = _client.Option.Property.Get("JoinGroupRequest.SessionTimeout");
            Int32 sessionTimeout;
            if (!Int32.TryParse(sessionTimeoutStr, out sessionTimeout)) {
                sessionTimeout = 5000;
            }
            _sessionTimeout = sessionTimeout;
            _heartbeatTimer.Change(sessionTimeout, Timeout.Infinite);
        }

        public Response Initialize(params String[] topics) {
            //1. Group Coordinator
            EnsureCoordinateBrokerExsiting();

            //2. Join Group
            var joinGroupResponse = JoinGroup(_groupId, String.Empty, topics);
            _generationId         = joinGroupResponse.GenerationId;
            _memberId             = joinGroupResponse.MemberId;
            _members              = joinGroupResponse.Members;

            //3. SyncGroup
            SyncGroupResponse syncGroupResponse;
            if (_memberId != joinGroupResponse.LeaderId) {
                Debug.WriteLine(String.Format("[{0:d2}] {1:HH:mm:ss.fff} Became follower of {2}, waiting for assingment",
                    Thread.CurrentThread.ManagedThreadId, DateTime.Now, _groupId));
                syncGroupResponse = SyncGroup(_groupId, _memberId, _generationId);
            }
            else {
                Debug.WriteLine(String.Format("[{0:d2}] {1:HH:mm:ss.fff}  Became leader of {2}, assigning topic and partitions",
                    Thread.CurrentThread.ManagedThreadId, DateTime.Now, _groupId));
                var assignments = AssigningTopicPartitions(topics);
                syncGroupResponse = SyncGroup(_groupId, _memberId, _generationId, assignments);
            }

            //4. Heartbeat
            _heartbeatTimer.Change(0L, Timeout.Infinite);
            return syncGroupResponse;
        }

        private void EnsureCoordinateBrokerExsiting() {
            if (_coordinateBroker != null) {
                return;
            }
            if (Interlocked.CompareExchange(ref _coordinateBroker, null, null) == null) {
                _coordinateBroker = GroupCoordinator(_groupId);
                Debug.WriteLine(String.Format("[{0:d2}] {1:HH:mm:ss.fff} Got coordinate broker {2}",
                    Thread.CurrentThread.ManagedThreadId, DateTime.Now, _coordinateBroker.ToUri().AbsoluteUri));
            }
        }

        private IList<SyncGroupGroupAssignment> AssigningTopicPartitions(IList<String> topics) {
            var assignments = new List<SyncGroupGroupAssignment>(topics.Count * _members.Length);
            foreach (var topic in topics) {
                var partitions = _partitionDispatcher.SelectPartitions(topic)
                    .Select(x => x.Partition).ToArray();
                if (_members.Length == 1) {
                    var assignment = AssignToOne(_members[0].MemberId, topic, partitions);
                    assignments.Add(assignment);
                }
                else {
                    var members = _members.Select(x => x.MemberId).ToArray();
                    var groupAssignment = AssignByDivide(members, topic, partitions);
                    assignments.AddRange(groupAssignment);
                }
            }
            return assignments;
        }

        private SyncGroupGroupAssignment AssignToOne(String memberId, String topic, Int32[] partitions) {
            return new SyncGroupGroupAssignment {
                MemberId = memberId,
                MemberAssignment = new SyncGroupMemberAssignment {
                    PartitionAssignments = new[] {
                        new SyncGroupPartitionAssignment {
                            Topic      = topic,
                            Partitions = partitions
                        }
                    }
                }
            };
        }

        private IEnumerable<SyncGroupGroupAssignment> AssignByDivide(String[] members, String topic, Int32[] partitions) {
            var assignedMemberAndPartitions = partitions.Select((partition, index) =>
                    new { partition, member = members[index % members.Length] })
                .GroupBy(x => x.member);
            foreach (var memberAndPartitions in assignedMemberAndPartitions) {
                yield return new SyncGroupGroupAssignment {
                    MemberId = memberAndPartitions.Key,
                    MemberAssignment = new SyncGroupMemberAssignment {
                        PartitionAssignments = new[] {
                            new SyncGroupPartitionAssignment {
                                Topic      = topic,
                                Partitions = memberAndPartitions.Select(x => x.partition).ToArray()
                            }
                        }
                    }
                };
            }
        }

        public ListGroupsResponse ListGroups() {
            var request = new ListGroupsRequest();
            //var brokerUri = _client.ExistingBrokerDispatcher.SequentialSelect();
            EnsureCoordinateBrokerExsiting();
            var response = (ListGroupsResponse)_client.SubmitRequest(_coordinateBroker, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public DescribeGroupsResponse DescribeGroups(IList<String> groupIds) {
            //var brokerUri = _client.ExistingBrokerDispatcher.SequentialSelect();
            EnsureCoordinateBrokerExsiting();
            var request = new DescribeGroupsRequest(groupIds);
            var response = (DescribeGroupsResponse)_client.SubmitRequest(_coordinateBroker, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public Broker GroupCoordinator(String groupId) {
            var request = new GroupCoordinatorRequest(groupId);
            var brokerUri = _client.ExistingBrokerDispatcher.SequentialSelect();
            var response = (GroupCoordinatorResponse)_client.SubmitRequest(brokerUri, request);
            response.TryThrowFirstErrorOccured();
            return new Broker(response.CoordinatorId, response.CoordinatorHost, response.CoordinatorPort);
        }

        public JoinGroupResponse JoinGroup(String groupId, String memberId, IList<String> topics) {
            var request = _sessionTimeout.HasValue
                ? new JoinGroupRequest(groupId, memberId, topics.ToArray(), _sessionTimeout.Value)
                : new JoinGroupRequest(groupId, memberId, topics.ToArray());
            var response = (JoinGroupResponse)_client.SubmitRequest(_coordinateBroker.ToUri(), request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public SyncGroupResponse SyncGroup(String groupId, String memberId, Int32 generationId) {
            var request = new SyncGroupRequest(groupId, generationId, memberId);
            request.GroupAssignments = new SyncGroupGroupAssignment[0];
            var response = (SyncGroupResponse)_client.SubmitRequest(_coordinateBroker, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public SyncGroupResponse SyncGroup(String groupId, String memberId, Int32 generationId, IList<SyncGroupGroupAssignment> assignments) {
            var request = new SyncGroupRequest(groupId, generationId, memberId);
            request.GroupAssignments = assignments;
            var response = (SyncGroupResponse)_client.SubmitRequest(_coordinateBroker, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public HeartbeatResponse Heartbeat(String groupId, String memberId, Int32 generationId) {
            var request = new HeartbeatRequest(groupId, generationId, memberId);
            var response = (HeartbeatResponse)_client.SubmitRequest(_coordinateBroker, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public LeaveGroupResponse LeaveGroup(String groupId, String memberId) {
            var request = new LeaveGroupRequest(groupId, memberId);
            var response = (LeaveGroupResponse)_client.SubmitRequest(_coordinateBroker, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }
    }
}
