using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    public class Compress {
        public static class GZip {
            public static Byte[] Compress(Byte[] data) {
                return Compress(data, 0, data.Length);
            }

            public static Byte[] Compress(Byte[] data, Int32 offset, Int32 size) {
                using (var output = new MemoryStream()) {
                    using (var gzip = new GZipStream(output, CompressionMode.Compress)) {
                        gzip.Write(data, 0, data.Length);
                    }
                    return output.ToArray();
                }
            }

            public static Byte[] Decompress(Byte[] data) {
                using (var input = new MemoryStream(data))
                using (var output = new MemoryStream()) {
                    using (var gzip = new GZipStream(input, CompressionMode.Decompress)) {
                        gzip.CopyTo(output);
                    }
                    return output.ToArray();
                }
            }
        }

        public class Deflate {
            public static Byte[] Compress(Byte[] data) {
                using (var output = new MemoryStream()) {
                    using (var deflate = new DeflateStream(output, CompressionMode.Compress, true)) {
                        deflate.Write(data, 0, data.Length);
                        deflate.Flush();
                    }
                    return output.ToArray();
                }
            }

            public static Byte[] Decompress(Byte[] data) {
                using (var input = new MemoryStream(data))
                using (var output = new MemoryStream()) {
                    using (var deflate = new DeflateStream(input, CompressionMode.Decompress)) {
                        var block = new Byte[4096];
                        var readed = deflate.Read(block, 0, block.Length);
                        while (readed > 0) {
                            output.Write(block, 0, readed);
                            readed = deflate.Read(block, 0, block.Length);
                        }
                    }
                    return output.ToArray();
                }
            }
        }
    }
}