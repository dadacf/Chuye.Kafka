using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    class KafkaCrc32Writer {
        private KafkaWriter _writer;
        private Int64 _previousPosition;

        public KafkaCrc32Writer(KafkaWriter writer) {
            _writer = writer;
        }

        public void MarkAsStart() {
            _previousPosition = _writer.BaseStream.Position;
            _writer.Write(0);
        }

        public Int32 Caculate() {
            var currentPosition = _writer.BaseStream.Position;
            var length = (Int32)(currentPosition - _previousPosition - 4);
            var buffer = new Byte[length];
            _writer.BaseStream.Seek(_previousPosition + 4, SeekOrigin.Begin);
            _writer.BaseStream.Read(buffer, 0, length);
            var crc = CRC32.ComputeHash(buffer, 0, length);
            _writer.BaseStream.Seek(_previousPosition, SeekOrigin.Begin);
            _writer.Write(crc);
            _writer.BaseStream.Seek(currentPosition, SeekOrigin.Begin);
            return crc;
        }
    }
}

