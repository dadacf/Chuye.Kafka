using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka {
    public class Topic {
        public String Name { get; set; }
        public Int32 Partition { get; set; }

        public Topic(String name)
            : this(name, 0) {
        }

        public Topic(String name, Int32 partition) {
            Name = name;
            Partition = partition;
        }

        public static implicit operator Topic(String topicName) {
            return new Topic(topicName);
        }
    }
}
