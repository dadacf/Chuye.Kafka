using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //RequestOrResponse => Size (RequestMessage | ResponseMessage)
    //Size => int32
    //-------------------------------------------------------------------------
    //Response => CorrelationId ResponseMessage
    //CorrelationId => int32
    //-------------------------------------------------------------------------
    //ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse
    public abstract class Response {
        public Int32 Size { get; private set; }
        public Int32 CorrelationId { get; private set; }

        public void Serialize(Stream stream) {
            using (var writer = new KafkaWriter(stream)) {
                var lengthWriter = new KafkaLengthWriter(writer);
                lengthWriter.MarkAsStart();
                writer.Write(CorrelationId);
                SerializeContent(writer);
                Size = lengthWriter.Caculate();
            }
        }

        public void Deserialize(Stream stream) {
            using (var reader = new KafkaReader(stream)) {
                Size = reader.ReadInt32();
                CorrelationId = reader.ReadInt32();
                DeserializeContent(reader);
            }
        }

        public static Response DeserializeFrom(Stream stream, ApiKey apiKey) {
            using (var reader = new KafkaReader(stream)) {
                var size = reader.ReadInt32();
                var typeName = apiKey.ToString().Replace("Request", "Response");
                var fullTypeNaem = (Int32)apiKey < 10 ? "Chuye.Kafka.Protocol." + typeName
                    : "Chuye.Kafka.Protocol.Management." + typeName;
                var type = Type.GetType(fullTypeNaem);
                var instance = (Response)Activator.CreateInstance(type); 

                instance.Size = size;
                instance.CorrelationId = reader.ReadInt32();
                instance.DeserializeContent(reader);
                return instance;
            }
        }

        protected abstract void DeserializeContent(KafkaReader reader);

        protected abstract void SerializeContent(KafkaWriter writer);

        public virtual void TryThrowFirstErrorOccured() {
        }
    }
}
