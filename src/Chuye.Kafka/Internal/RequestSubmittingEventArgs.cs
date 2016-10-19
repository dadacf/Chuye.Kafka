using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Internal {
    public class RequestSubmittingEventArgs : EventArgs {
        public Uri Uri { get; set; }
        public Request Request { get; set; }

        public RequestSubmittingEventArgs(Uri uri, Request request) {
            Uri = uri;
            Request = request;
        }
    }
}
