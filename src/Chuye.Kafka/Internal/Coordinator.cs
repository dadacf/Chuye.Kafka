using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol.Management;

namespace Chuye.Kafka.Internal {
    public class Coordinator {
        private readonly Client _client;

        public Coordinator(Client client) {
            _client = client;
        }

        public ListGroupsResponse ListGroups() {
            var request = new ListGroupsRequest();
            var brokerUri = _client.ExistingBrokerDispatcher.SequentialSelect();
            var response = (ListGroupsResponse)_client.SubmitRequest(brokerUri, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public DescribeGroupsResponse DescribeGroups(IList<String> groupIds) {
            var request = new DescribeGroupsRequest(groupIds);
            var brokerUri = _client.ExistingBrokerDispatcher.SequentialSelect();
            var response = (DescribeGroupsResponse)_client.SubmitRequest(brokerUri, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public GroupCoordinatorResponse GroupCoordinator(String groupId) {
            var request = new GroupCoordinatorRequest(groupId);
            var brokerUri = _client.ExistingBrokerDispatcher.SequentialSelect();
            var response = (GroupCoordinatorResponse)_client.SubmitRequest(brokerUri, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public JoinGroupResponse JoinGroup(String groupId, String memberId, IList<String> topics) {
            var request = new JoinGroupRequest(groupId, memberId, topics.ToArray());
            var response = (JoinGroupResponse)_client.SubmitRequest(null, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public SyncGroupResponse SyncGroup(String groupId, String memberId, Int32 generationId) {
            var request = new SyncGroupRequest(groupId, generationId, memberId);
            request.GroupAssignments = null;
            var response = (SyncGroupResponse)_client.SubmitRequest(null, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public HeartbeatResponse Heartbeat(String groupId, String memberId, Int32 generationId) {
            var request = new HeartbeatRequest(groupId, generationId, memberId);
            var response = (HeartbeatResponse)_client.SubmitRequest(null, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }

        public LeaveGroupResponse LeaveGroup(String groupId, String memberId) {
            var request = new LeaveGroupRequest(groupId, memberId);
            var response = (LeaveGroupResponse)_client.SubmitRequest(null, request);
            response.TryThrowFirstErrorOccured();
            return response;
        }
    }
}
