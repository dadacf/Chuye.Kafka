using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    public class TopicPartition : IEquatable<TopicPartition> {
        public String Name { get; private set; }
        public Int32 Partition { get; private set; }
        public Int32 Leader { get; private set; }

        public TopicPartition(String name, Int32 partition, Int32 leader) {
            Name = name;
            Partition = partition;
            Leader = leader;
        }

        public Boolean Equals(TopicPartition other) {
            return other != null
                && String.Equals(Name, other.Name, StringComparison.Ordinal)
                && Partition == other.Partition
                && Leader == other.Leader;
        }

        public override Boolean Equals(object obj) {
            return base.Equals(obj as TopicPartition);
        }

        public override Int32 GetHashCode() {
            // Overflow is fine, just wrap
            int hash = 17;
            unchecked {
                // Suitable nullity checks etc, of course :)
                hash = hash * 23 ^ Name.GetHashCode();
                hash = hash * 23 ^ Partition;
                hash = hash * 23 ^ Leader;
            }
            return hash;
        }

        public override String ToString() {
            return String.Format("{0}:{1}, Leader-{2}", Name, Partition, Leader);
        }

        public static Boolean operator ==(TopicPartition value1, TopicPartition value2) {
            return Object.Equals(value1, value2);
        }

        public static Boolean operator !=(TopicPartition value1, TopicPartition value2) {
            return !Object.Equals(value1, value2);
        }
    }
}
