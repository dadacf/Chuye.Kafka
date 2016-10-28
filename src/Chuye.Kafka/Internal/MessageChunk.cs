using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class MessageChunk : Enumerable<OffsetMessage> {
        public Int32 Partition { get; private set; }
        public Int64 EndingOffset {
            get {
                if (base.List == null || base.List.Count == 0) {
                    throw new ArgumentOutOfRangeException();
                }
                return base.List[0].Offset + base.Position;
            }
        }

        public MessageChunk(OffsetMessage[] array, Int32 partition)
            : base(array) {
            if (partition < 0) {
                throw new ArgumentOutOfRangeException("partition");
            }
            Partition = partition;
        }
    }
}
