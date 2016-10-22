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
        private readonly Option _option;
        private readonly Client _client;
        private readonly TopicPartitionDispatcher _partitionDispatcher;
        private readonly String _groupId;
        private Broker _coordinateBroker;
        private Int32 _generationId;
        private String _memberId;
        private JoinGroupResponseMember[] _members;
        private Timer _heartbeatTimer;
        private Int32? _sessionTimeout;
        private CoordinatorState _state;
        private Dictionary<String, Int32[]> _partitionAssignments;

        public event EventHandler<CoordinatorStateChangedEventArgs> StateChanged;

        public String[] Topics { get; set; }

        public CoordinatorState State {
            get { return _state; }
        }

        public Coordinator(Option option, String groupId) {
            _option              = option;
            _client              = option.GetSharedClient();
            _groupId             = groupId;
            _memberId            = String.Empty;
            _heartbeatTimer      = new Timer(HeartbeatCallback);
            _partitionDispatcher = new TopicPartitionDispatcher(_client.TopicBrokerDispatcher);
        }

        public Int32[] GetPartitionAssigned(String topic) {
            if (_partitionAssignments == null) {
                return null;
            }
            Int32[] partitions;
            _partitionAssignments.TryGetValue(topic, out partitions);
            return partitions;
        }

        private void HeartbeatCallback(Object state) {
            var heartbeatResponse = Heartbeat(_groupId, _memberId, _generationId);
            //heartbeatResponse.TryThrowFirstErrorOccured();
            if (heartbeatResponse.ErrorCode == ErrorCode.RebalanceInProgressCode) {
                Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #1 Start rebalace at group '{2}' for '{3}'",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId, heartbeatResponse.ErrorCode);
                RebalanceAsync();
            }
            else {
                var sessionTimeoutStr = _option.Property.Get("JoinGroupRequest.SessionTimeout");
                Int32 sessionTimeout;
                if (Int32.TryParse(sessionTimeoutStr, out sessionTimeout)) {
                    _sessionTimeout = sessionTimeout;
                }
                _heartbeatTimer.Change(_sessionTimeout.HasValue ? _sessionTimeout.Value : 5000, Timeout.Infinite);
            }
        }

        private void OnStateChange(CoordinatorState state) {
            _state = state;
            if (StateChanged != null) {
                StateChanged(this, new CoordinatorStateChangedEventArgs(state));
            }
        }

        public void RebalanceAsync() {
            if (String.IsNullOrWhiteSpace(_memberId)) {
                Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #1 Start rebalace at group '{2}'",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId);
            }

            //1. Group Coordinator
            OnStateChange(CoordinatorState.Unkown);
            EnsureCoordinateBrokerExsiting();

            //2 & 3 Join Group & SyncGroup
            //todo: magic number
            var syncGroupResponse = TryJoinAndSyncGroup(20, 100);
            syncGroupResponse.TryThrowFirstErrorOccured();
            ResolveTopicPartitionAssigned(syncGroupResponse.MemberAssignment);
            OnStateChange(CoordinatorState.Stable);

            //4. Heartbeat
            Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #5 Member '{2}' heartbeat at group '{3}'",
                DateTime.Now, Thread.CurrentThread.ManagedThreadId, _memberId, _groupId);
            _heartbeatTimer.Change(0L, Timeout.Infinite);
        }

        private SyncGroupResponse TryJoinAndSyncGroup(Int32 retryCount, Int32 retryTimtout) {
            SyncGroupResponse resp = null;
            var retryUsed = 0;
            while (resp == null && ++retryUsed < retryCount) {
                OnStateChange(CoordinatorState.Joining);
                var joinGroupResponse = JoinGroup(_groupId, _memberId, Topics);
                joinGroupResponse.TryThrowFirstErrorOccured();

                OnStateChange(CoordinatorState.AwaitingSync);
                _generationId = joinGroupResponse.GenerationId;
                _memberId     = joinGroupResponse.MemberId;
                _members      = joinGroupResponse.Members;

                var isLeader = _memberId != joinGroupResponse.LeaderId;
                if (isLeader) {
                    if (retryUsed == 1) {
                        Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #3 Became follower at group '{2}', waiting for assingment",
                            DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId);
                    }
                    resp = SyncGroup(_groupId, _memberId, _generationId);
                }
                else {
                    if (retryUsed == 1) {
                        Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #3 Became leader at group '{2}', assigning topic and partitions",
                            DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId);
                    }
                    var assignments = AssigningTopicPartitions(Topics);
                    resp = SyncGroup(_groupId, _memberId, _generationId, assignments);
                }
                if (resp.ErrorCode == ErrorCode.RebalanceInProgressCode) {
                    resp = null;
                    Thread.Sleep(retryTimtout);
                }
            }
            if (resp == null) {
                throw new ProtocolException(ErrorCode.RebalanceInProgressCode);
            }
            return resp;
        }

        private void ResolveTopicPartitionAssigned(SyncGroupMemberAssignment assignment) {
            if (_partitionAssignments == null) {
                _partitionAssignments = new Dictionary<String, Int32[]>();
            }
            else {
                _partitionAssignments.Clear();
            }

            if (assignment.PartitionAssignments != null && assignment.PartitionAssignments.Count > 0) {
                foreach (var partitionAssignment in assignment.PartitionAssignments) {
                    Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #4 Assined at group '{2}', got topic '{3}'({4})",
                        DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId, partitionAssignment.Topic, String.Join("|", partitionAssignment.Partitions.OrderBy(x => x)));
                    _partitionAssignments.Add(partitionAssignment.Topic, partitionAssignment.Partitions);
                }
            }
            else {
                Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #4 Assined at group '{2}', got nothing",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId);
                throw new InvalidOperationException("All partition has been assigned");
            }
        }

        private void EnsureCoordinateBrokerExsiting() {
            if (_coordinateBroker != null) {
                return;
            }
            if (Interlocked.CompareExchange(ref _coordinateBroker, null, null) == null) {
                _coordinateBroker = GroupCoordinator(_groupId);
                Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #2 Got coordinate broker {2} at group '{3}'",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _coordinateBroker.ToUri().AbsoluteUri, _groupId);
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
            //var brokerUri = _client.ExistingBrokerDispatcher.SequentialSelect();
            EnsureCoordinateBrokerExsiting();
            var request = new ListGroupsRequest();
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
            //response.TryThrowFirstErrorOccured();
            return response;
        }

        public SyncGroupResponse SyncGroup(String groupId, String memberId, Int32 generationId, IList<SyncGroupGroupAssignment> assignments) {
            var request = new SyncGroupRequest(groupId, generationId, memberId);
            request.GroupAssignments = assignments;
            var response = (SyncGroupResponse)_client.SubmitRequest(_coordinateBroker, request);
            //response.TryThrowFirstErrorOccured();
            return response;
        }

        public HeartbeatResponse Heartbeat(String groupId, String memberId, Int32 generationId) {
            var request = new HeartbeatRequest(groupId, generationId, memberId);
            var response = (HeartbeatResponse)_client.SubmitRequest(_coordinateBroker, request);
            //response.TryThrowFirstErrorOccured();
            return response;
        }

        public LeaveGroupResponse LeaveGroup() {
            if (String.IsNullOrWhiteSpace(_memberId)) {
                throw new InvalidOperationException();
            }
            var request = new LeaveGroupRequest(_groupId, _memberId);
            var response = (LeaveGroupResponse)_client.SubmitRequest(_coordinateBroker, request);
            Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #6 Member '{2}' leave group '{3}'",
               DateTime.Now, Thread.CurrentThread.ManagedThreadId, _memberId, _groupId);
            _heartbeatTimer.Change(Timeout.Infinite, Timeout.Infinite);
            response.TryThrowFirstErrorOccured();
            return response;
        }
    }
}
