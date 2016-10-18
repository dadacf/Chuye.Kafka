using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka {
    public class Message {
        public String Key { get; private set; }
        public String Value { get; private set; }

        public Message(String value)
            : this(null, value) {
        }

        public Message(String key, String value) {
            if (value == null) {
                throw new ArgumentOutOfRangeException("value");
            }
        }

        public static implicit operator Message(String value) {
            return new Message(null, value);
        }
    }

    

    

    
}
