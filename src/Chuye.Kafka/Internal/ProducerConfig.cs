using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Internal {
    public struct ProducerConfig {
        public static ProducerConfig Default = new ProducerConfig(50, 3000);

        public Int32 ThrottleSize;
        public Int32 ThrottleMilliseconds;
        public MessageCodec MessageCodec;
        public AcknowlegeStrategy AcknowlegeStrategy;

        public ProducerConfig(Int32 throttleSize, Int32 throttleMilliseconds)
            : this(throttleSize, throttleMilliseconds, MessageCodec.None, AcknowlegeStrategy.Written) {
        }

        public ProducerConfig(Int32 throttleSize, Int32 throttleMilliseconds, MessageCodec messageCodec, AcknowlegeStrategy acknowlegeStrategy) {
            ThrottleSize         = throttleSize;
            ThrottleMilliseconds = throttleMilliseconds;
            MessageCodec         = messageCodec;
            AcknowlegeStrategy   = acknowlegeStrategy;
        }
    }
}
