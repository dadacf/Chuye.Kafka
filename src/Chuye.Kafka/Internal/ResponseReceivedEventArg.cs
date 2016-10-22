using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Internal {
    public class ResponseReceivedEventArg : EventArgs {
        public Response Response { get; set; }

        public ResponseReceivedEventArg(Response response) {
            Response = response;
        }
    }
}
