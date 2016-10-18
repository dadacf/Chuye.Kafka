using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    public class KafkaStreamReader : IDisposable {
        private readonly BinaryReader _reader;

        internal Stream BaseStream {
            get { return _reader.BaseStream; }
        }

        public KafkaStreamReader(Stream stream) {
            _reader = new BinaryReader(stream, Encoding.UTF8, true);
        }

        public Byte ReadByte() {
            return _reader.ReadByte();
        }

        public Int16 ReadInt16() {
            var value = _reader.ReadInt16();
            return IPAddress.HostToNetworkOrder(value);
        }

        public Int32 ReadInt32() {
            var value = _reader.ReadInt32();
            return IPAddress.HostToNetworkOrder(value);
        }

        public Int64 ReadInt64() {
            var value = _reader.ReadInt64();
            return IPAddress.HostToNetworkOrder(value);
        }

        public String ReadString() {
            var length = ReadInt16();
            if (length == -1) {
                return null;
            }
            if (length == 0) {
                return String.Empty;
            }
            var bytes = _reader.ReadBytes(length);
            return Encoding.UTF8.GetString(bytes);
        }

        public Byte[] ReadBytes() {
            var length = ReadInt32();
            if (length == -1) {
                return null;
            }
            return _reader.ReadBytes(length);
        }

        public void Dispose() {
            _reader.Dispose();
        }
    }
}

