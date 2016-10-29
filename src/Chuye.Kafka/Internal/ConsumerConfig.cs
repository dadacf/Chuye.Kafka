using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    public struct ConsumerConfig {
        public static ConsumerConfig Default = new ConsumerConfig(5000, 1024 * 16, 1000);

        public Int32 RebalaceBlockMilliseconds;
        public Int32 FetchMilliseconds;
        public Int32 FetchBytes;

        public ConsumerConfig(Int32 rebalaceBlockMilliseconds, Int32 fetchBytes, Int32 fetchMilliseconds) {
            RebalaceBlockMilliseconds = rebalaceBlockMilliseconds;
            FetchBytes                = fetchBytes;
            FetchMilliseconds         = fetchMilliseconds;
        }
    }
}
