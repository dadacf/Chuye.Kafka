using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    public struct ConsumerConfig {
        public static ConsumerConfig Default = new ConsumerConfig(1000);

        public Int32 FetchMilliseconds;

        public ConsumerConfig(Int32 fetchMilliseconds) {
            FetchMilliseconds = fetchMilliseconds;
        }
    }
}
