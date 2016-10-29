using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal
{
    public struct CoordinatorConfig {
        public static CoordinatorConfig Default = new CoordinatorConfig(10000, 5000);
        public Int32 JoinGroupSessionTimeout;
        public Int32 HeartBeatInterval;

        public CoordinatorConfig(Int32 joinGroupSessionTimeout, Int32 heartBeatInterval) {
            if (joinGroupSessionTimeout < 10000) {
                throw new ArgumentOutOfRangeException("joinGroupSessionTimeout");
            }
            if (heartBeatInterval < 0 || heartBeatInterval >= joinGroupSessionTimeout) {
                throw new ArgumentOutOfRangeException("heartBeatInterval");
            }

            JoinGroupSessionTimeout = joinGroupSessionTimeout;
            HeartBeatInterval = heartBeatInterval;
        }
    }
}
