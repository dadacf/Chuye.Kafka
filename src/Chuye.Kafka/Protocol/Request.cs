using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //RequestOrResponse => Size (RequestMessage | ResponseMessage)
    //Size => int32
    //-------------------------------------------------------------------------
    //RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
    //ApiKey => int16
    //ApiVersion => int16
    //CorrelationId => int32
    //ClientId => string
    //-------------------------------------------------------------------------
    //RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest
    public abstract class Request {
        private static Int32 CurrentCorrelationId;
        private const String DefaultClientId = "Chuye.Kafka";

        public Int32 Size { get; private set; }

        public ApiKey ApiKey { get; private set; }

        public Int16 ApiVersion { get; set; }

        public Int32 CorrelationId { get; private set; }

        public String ClientId { get; private set; }

        public Request(ApiKey apiKey) {
            ApiKey        = apiKey;
            ApiVersion    = 0;
            CorrelationId = Interlocked.Increment(ref CurrentCorrelationId);
            ClientId      = DefaultClientId;
        }

        public virtual void Verify() {
        }

        public void Serialize(Stream stream) {
            using (var writer = new KafkaWriter(stream)) {
                var lengthWriter = new KafkaLengthWriter(writer);
                lengthWriter.MarkAsStart();

                writer.Write((Int16)ApiKey);
                writer.Write(ApiVersion);
                writer.Write(CorrelationId);
                writer.Write(ClientId);
                SerializeContent(writer);
                Size = lengthWriter.Caculate();
            }
        }

        public void Deserialize(Stream stream) {
            using (var reader = new KafkaReader(stream)) {
                Size = reader.ReadInt32();
                var apiKey = (ApiKey)reader.ReadInt16();
                if (ApiKey != apiKey) {
                    throw new InvalidOperationException("Request type definition error");
                }
                ApiVersion    = reader.ReadInt16();
                CorrelationId = reader.ReadInt32();
                ClientId      = reader.ReadString();
                DeserializeContent(reader);
            }
        }

        protected abstract void SerializeContent(KafkaWriter writer);

        protected abstract void DeserializeContent(KafkaReader reader);
    }
}


