using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    class KafkaLengthWriter {
        private KafkaStreamWriter _writer;
        private Int64 _previousPosition;

        public KafkaLengthWriter(KafkaStreamWriter writer) {
            _writer = writer;;
        }

        public void BeginWrite() {
            _previousPosition = _writer.BaseStream.Position;
            _writer.Write(0);
        }

        public Int32 EndWrite() {
            var currentPosition = _writer.BaseStream.Position;
            var length = (Int32)(currentPosition - _previousPosition - 4);
            _writer.BaseStream.Seek(_previousPosition, SeekOrigin.Begin);
            _writer.Write(length);
            _writer.BaseStream.Seek(currentPosition, SeekOrigin.Begin);
            return length;
        }
    }
}

