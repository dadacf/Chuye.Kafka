using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol {
    public class ProtocolException : Exception {
        public ErrorCode ErrorCode { get; private set; }

        public ProtocolException(ErrorCode errorCode)
            : base(errorCode.ToString()) {
            ErrorCode = errorCode;
        }
    }
}
