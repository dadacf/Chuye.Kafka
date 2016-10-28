using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    public struct ProducerConfig {
        public static ProducerConfig Default = new ProducerConfig(50, 3000);

        public Int32 ThrottleSize;
        public Int32 ThrottleMilliseconds;

        public ProducerConfig(Int32 throttleSize, Int32 throttleMilliseconds) {
            ThrottleSize = throttleSize;
            ThrottleMilliseconds = throttleMilliseconds;
        }
    }
}
