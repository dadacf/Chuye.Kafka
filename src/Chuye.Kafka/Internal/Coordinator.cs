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
        private CoordinatorConfig _config;
        private readonly TopicPartitionDispatcher _partitionDispatcher;
        private readonly String _groupId;
        private Broker _coordinateBroker;
        private Int32 _generationId;
        private String _memberId;
        private JoinGroupResponseMember[] _members;
        private Timer _heartbeatTimer;
        private CoordinatorState _state;
        private Dictionary<String, Int32[]> _partitionAssignments;

        //todo: 使用 memeber-partition 记录分配的 partition 数量

        public event EventHandler<CoordinatorStateChangedEventArgs> StateChanged;

        public String[] Topics { get; set; }

        public CoordinatorState State {
            get { return _state; }
        }

        public Coordinator(Option option, String groupId) {
            _option              = option;
            _client              = option.GetSharedClient();
            _config              = option.CoordinatorConfig;
            _groupId             = groupId;
            _memberId            = String.Empty;
//#if NET452
//            _heartbeatTimer      = new Timer(HeartbeatCallback);
//#elif NETSTANDARD1_6
            _heartbeatTimer      = new Timer(HeartbeatCallback, null, Timeout.Infinite, Timeout.Infinite);
//#endif
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
                Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #6 Need rebalace at group '{2}' for '{3}'",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId, heartbeatResponse.ErrorCode);
                RebalanceAsync();
            }
            else {
                _heartbeatTimer.Change(_config.JoinGroupSessionTimeout, Timeout.Infinite);
            }
        }

        private void OnStateChange(CoordinatorState state) {
            _state = state;
            if (StateChanged != null) {
                StateChanged(this, new CoordinatorStateChangedEventArgs(state));
            }
        }

        public void RebalanceAsync() {
            //1. Group Coordinator
            Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #1 Rebalace start at group '{2}'",
                DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId);
            OnStateChange(CoordinatorState.Unkown);
            EnsureCoordinateBrokerExsiting();

            //2 & 3 Join Group & SyncGroup
            //todo: magic number
            var syncGroupResponse = TryJoinAndSyncGroup(20, 100);
            syncGroupResponse.TryThrowFirstErrorOccured();
            ResolveTopicPartitionAssigned(syncGroupResponse.MemberAssignment);
            OnStateChange(CoordinatorState.Stable);

            //4. Heartbeat
            Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #5 Heartbeat of member '{2}' at group '{3}'",
                DateTime.Now, Thread.CurrentThread.ManagedThreadId, _memberId, _groupId);
            _heartbeatTimer.Change(0, Timeout.Infinite);
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
                    if (partitionAssignment.Partitions.Length > 0) {
                        Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #4 Sync group '{2}', Member '{3}' dispathced Topic '{4}'({5})",
                            DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId, _memberId,
                            partitionAssignment.Topic, String.Join("|", partitionAssignment.Partitions.OrderBy(x => x)));
                        _partitionAssignments.Add(partitionAssignment.Topic, partitionAssignment.Partitions);
                    }
                    else {
                        Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #4 Sync group '{2}', Member '{3}' dispathced Topic '{4}' no partition",
                            DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId, _memberId, partitionAssignment.Topic);
                    }
                }
            }
            else {
                Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #4 Assined at group '{2}', assined nothing",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId);
                //throw new InvalidOperationException("All partition has been assigned");
            }
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
                    Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #3 Join group '{2}', waiting for assingments as follower",
                        DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId);
                    resp = SyncGroup(_groupId, _memberId, _generationId);
                }
                else {
                    Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #3 Join group '{2}', assigning topic and partitions as leader",
                        DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId);
                    var assignments = AssigningTopicPartitions(joinGroupResponse.Members).ToArray();
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

        //m1 join with t2
        //m2 join with t1, t2
        //m3 join with t1, t3
        private IEnumerable<SyncGroupGroupAssignment> AssigningTopicPartitions(JoinGroupResponseMember[] members) {
            //1. find all topic, got t1,t2, t3
            var topics = members.SelectMany(x => x.MemberMetadata.Topics);
            //2. for each topic, find partition and consumer
            var assignments = new List<SyncGroupGroupAssignment>();
            foreach (var topic in topics) {
                var partitions = _partitionDispatcher.SelectPartitions(topic)
                    .Select(x => x.Partition).ToArray();
                var consumers = members.Where(x => x.MemberMetadata.Topics.Contains(topic)).ToArray();
                
                for (int i = 0; i < consumers.Length; i++) {
                    //p: [0,1,2,3], c: [0,1]
                    //result: p0 -> c0, p1 -> c1, p2 -> c0, p3 -> c1
                    //equal: c0 -> [p0,p2], c1 -> [p1, p3]
                    //> 分区数量不能被消费者整除时，靠后的消息者分配不到分区，此情况被 topic 循环放大； 可以通过 consumer 数组乱序改进
                    //> 对于 "重新负载时，原有分配最小变动"的需求，初步想法是在 MemberId 上做文章，使特定 MemberId 优先命中特定的分区索引
                    var partitionDispatched = partitions.Where((partition, index) => index % consumers.Length == i);

                    yield return new SyncGroupGroupAssignment {
                        MemberId = consumers[i].MemberId,
                        MemberAssignment = new SyncGroupMemberAssignment {
                            PartitionAssignments = new[] {
                                    new SyncGroupPartitionAssignment {
                                        Topic      = topic,
                                        Partitions = partitionDispatched.ToArray()
                                    }
                                }
                        }
                    };
                }
            }
        }
        private void EnsureCoordinateBrokerExsiting() {
            if (_coordinateBroker != null) {
                return;
            }
            if (Interlocked.CompareExchange(ref _coordinateBroker, null, null) == null) {
                _coordinateBroker = GroupCoordinator(_groupId);
                Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] #2 Group coordinate got broker {2} at group '{3}'",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _coordinateBroker.ToUri().AbsoluteUri, _groupId);
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
            var request = new JoinGroupRequest(groupId, memberId, topics.ToArray(), _config.JoinGroupSessionTimeout);
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
