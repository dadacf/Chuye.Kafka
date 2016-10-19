using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Management {
    //DescribeGroupsRequest => [GroupId]
    //  GroupId => string
    public class DescribeGroupsRequest : Request {
        public IList<String> Groups { get; set; }

        public DescribeGroupsRequest()
            : base(ApiKey.DescribeGroupsRequest) {
        }

        public DescribeGroupsRequest(IList<String> groups)
            : this() {
            Groups = groups;
        }

        protected override void SerializeContent(KafkaWriter writer) {
            writer.Write(Groups.ToArray());
        }

        protected override void DeserializeContent(KafkaReader reader) {
            Groups = reader.ReadStrings();
        }
    }
}
