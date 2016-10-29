using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    public class KafkaWriter : IDisposable {
        private readonly BinaryWriter _writer;

        internal Stream BaseStream {
            get { return _writer.BaseStream; }
        }

        public KafkaWriter(Stream stream) {
            _writer = new BinaryWriter(stream, Encoding.UTF8, true);
        }

        public void Write(Byte value) {
            //move 1
            _writer.Write(value);
        }

        public void Write(Int16 value) {
            //move 2
            var reversed = (Int16)IPAddress.NetworkToHostOrder(value);
            _writer.Write(reversed);
        }

        public void Write(Int32 value) {
            //move 4
            var reversed = IPAddress.NetworkToHostOrder(value);
            _writer.Write(reversed);
        }

        public void Write(Int64 value) {
            //move 8
            var reversed = IPAddress.NetworkToHostOrder(value);
            _writer.Write(reversed);
        }

        public void Write(String value) {
            if (value == null) {
                //move 2, return
                Write((Int16)(-1));
                return;
            }
            var bytes = Encoding.UTF8.GetBytes(value);
            checked {
                //move 2
                Write((Int16)bytes.Length);
            }
            //move length(utf8(value))
            _writer.Write(bytes, 0, bytes.Length);
        }

        public void Write(Byte[] value) {
            if (value == null) {
                //move 4, return
                Write(-1);
                return;
            }
            //move 4
            Write((Int32)value.Length);
            //move length(bytes)
            _writer.Write(value, 0, value.Length);
        }

        public void Dispose() {
            _writer.Flush();
            _writer.Dispose();
        }
    }
}

