using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Chuye.Kafka {
    //Request
    public abstract class Request {
    }

    public class MetadataRequest : Request {
    }

    public class ProduceRequest : Request {
    }

    public class OffsetRequest : Request {
    }

    public class OffsetCommitRequest : Request {
    }

    public class OffsetFetchRequest : Request {
    }

    public class FetchRequest : Request {
    }

    //Response
    public abstract class Response {

    }

    public class MetadataResponse : Response {
    }

    public class ProduceResponse : Response {
    }

    public class OffsetResponse : Response {
    }

    public class OffsetCommitResponse : Response {
    }

    public class OffsetFetchResponse : Response {
    }

    public class FetchResponse : Response {
    }
}
