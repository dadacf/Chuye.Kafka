using System;

namespace Chuye.Kafka.Protocol {
    /// <summary>
    /// This field indicates how many acknowledgements the servers should receive before responding to the request. 
    ///   If it is  0 the server will not send any response (this is the only case where the server will not reply to a request). 
    ///   If it is  1, the server will wait the data is written to the local log before sending a response. 
    ///   If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. 
    ///   For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more acknowledgements than there are in-sync replicas).
    /// </summary>
    public enum AcknowlegeStrategy : short {
        Immediate = 0, Written = 1, Block = -1
    }

    public enum OffsetOption /*: Int64*/ {
        Earliest = -2, Latest = -1
    }

    public enum MessageCodec: byte {
        None = 0x00, Gzip = 0x01, Snappy = 0x02
    }
}

