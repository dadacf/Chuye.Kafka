using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    static class GZip {
        public static Byte[] Compress(byte[] data) {
            return Compress(data, 0, data.Length);
        }

        public static Byte[] Compress(byte[] data, Int32 offset, Int32 size) {
            using (var output = new MemoryStream())
            using (var gzip = new GZipStream(output, CompressionMode.Compress)) {
                gzip.Write(data, offset, size);
                gzip.Flush();
                return output.ToArray();
            }
        }

        public static Byte[] Decompress(byte[] data) {
            using (var input = new MemoryStream(data))
            using (var gzip = new GZipStream(input, CompressionMode.Decompress))
            using (var output = new MemoryStream()) {
                var block = new Byte[4096];
                var readed = gzip.Read(block, 0, block.Length);
                while (readed > 0) {
                    output.Write(block, 0, readed);
                    readed = gzip.Read(block, 0, block.Length);
                }
                return output.ToArray();
            }
        }
    }
}
