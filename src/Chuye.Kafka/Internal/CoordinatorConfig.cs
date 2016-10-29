using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal
{
    public struct CoordinatorConfig {
        public static CoordinatorConfig Default = new CoordinatorConfig(10000);
        public Int32 JoinGroupSessionTimeout;

        public CoordinatorConfig(Int32 joinGroupSessionTimeout) {
            JoinGroupSessionTimeout = joinGroupSessionTimeout;
        }
    }
}
