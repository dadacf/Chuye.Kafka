using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            Key = key;
            Value = value;
        }

        public Message(Byte[] key, Byte[] value) {
            if (key != null) {
                Key = Encoding.UTF8.GetString(key);
            }
            if (value != null) {
                Value = Encoding.UTF8.GetString(value);
            }
        }

        public static implicit operator Message(String value) {
            return new Message(null, value);
        }

        public override string ToString() {
            if (Key == null) {
                return String.Format("{{\"Value\":\"{0}\"}}", Value);
            }
            return String.Format("{{\"Key\":\"{0}\",\"Value\":\"{1}\"}}", Key, Value);
        }
    }


    public class OffsetMessage : Message {
        public Int64 Offset { get; private set; }

        public OffsetMessage(Int64 offset, String key, String value)
            : base(key, value) {
            Offset = offset;
        }

        public OffsetMessage(Int64 offset, Byte[] key, Byte[] value)
            : base(key, value) {
            Offset = offset;
        }

        public override string ToString() {
            if (Key == null) {
                return String.Format("{{\"Offset\":{0},\"Value\":\"{1}\"}}", Offset, Value);
            }
            return String.Format("{{\"Offset\":{0},\"Key\":\"{1}\",\"Value\":\"{2}\"}}", Offset, Key, Value);
        }
    }
}
