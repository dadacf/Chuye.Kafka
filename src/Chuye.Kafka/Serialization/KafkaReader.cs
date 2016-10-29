using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    public class KafkaReader : IDisposable {
        private readonly BinaryReader _reader;
        private Int64 _positionProceeded;

        internal Stream BaseStream {
            get { return _reader.BaseStream; }
        }

        public Int64 PositionProceeded {
            get { return _positionProceeded; }
        }

        public KafkaReader(Stream stream) {
            _reader = new BinaryReader(stream, Encoding.UTF8, true);
            _positionProceeded = 0;
        }

        public Byte ReadByte() {
            _positionProceeded++;
            return _reader.ReadByte();
        }

        public Int16 ReadInt16() {
            _positionProceeded += 2;
            var value = _reader.ReadInt16();
            return IPAddress.HostToNetworkOrder(value);
        }

        public Int32 ReadInt32() {
            _positionProceeded += 4;
            var value = _reader.ReadInt32();
            return IPAddress.HostToNetworkOrder(value);
        }

        public Int64 ReadInt64() {
            _positionProceeded += 8;
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
            _positionProceeded += length;
            var bytes = _reader.ReadBytes(length);
            return Encoding.UTF8.GetString(bytes);
        }

        public Byte[] ReadBytes() {
            var length = ReadInt32();
            if (length == -1) {
                return null;
            }
            _positionProceeded += length;
            return _reader.ReadBytes(length);
        }

        public void Dispose() {
            _reader.Dispose();
        }
    }
}

