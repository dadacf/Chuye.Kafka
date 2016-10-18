using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol {
    public enum ApiKey : short {
        //Non-user facing control APIs 4-7 
        ProduceRequest      = 0,
        FetchRequest        = 1,
        OffsetRequest       = 2,
        MetadataRequest     = 3,
        OffsetCommitRequest = 8,
        OffsetFetchRequest  = 9,

        GroupCoordinatorRequest = 10,
        JoinGroupRequest        = 11,
        HeartbeatRequest        = 12,
        LeaveGroupRequest       = 13,
        SyncGroupRequest        = 14,
        DescribeGroupsRequest   = 15,
        ListGroupsRequest       = 16,
    }
}
