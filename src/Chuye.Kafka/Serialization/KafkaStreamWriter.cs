using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    public class KafkaStreamWriter : IDisposable {
        private readonly BinaryWriter _writer;

        internal Stream BaseStream {
            get { return _writer.BaseStream; }
        }

        public KafkaStreamWriter(Stream stream) {
            _writer = new BinaryWriter(stream, Encoding.UTF8, true);
        }

        public void Write(Byte value) {
            _writer.Write(value);
        }

        public void Write(Int16 value) {
            var reversed = (Int16)IPAddress.NetworkToHostOrder(value);
            _writer.Write(reversed);
        }

        public void Write(Int32 value) {
            var reversed = IPAddress.NetworkToHostOrder(value);
            _writer.Write(reversed);
        }

        public void Write(Int64 value) {
            var reversed = IPAddress.NetworkToHostOrder(value);
            _writer.Write(reversed);
        }

        public void Write(String value) {
            if (value == null) {
                Write((Int16)(-1));
                return;
            }
            var bytes = Encoding.UTF8.GetBytes(value);
            checked {
                Write((Int16)bytes.Length);
            }
            _writer.Write(bytes, 0, bytes.Length);
        }

        public void Write(Byte[] value) {
            if (value == null) {
                Write(-1);
                return;
            }
            Write((Int32)value.Length);
            _writer.Write(value, 0, value.Length);
        }

        public void Dispose() {
            _writer.Flush();
            _writer.Dispose();
        }
    }
}

