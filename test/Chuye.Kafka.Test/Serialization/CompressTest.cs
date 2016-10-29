using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;
using Xunit;

namespace Chuye.Kafka.Test.Serialization {
    public class CompressTest {
        [Fact]
        public void Gzip() {
            var originBytes = Enumerable.Range(0, 100).Select(x => (Byte)(x % 10)).ToArray();
            var compressedBytes = Compress.GZip.Compress(originBytes);
            var decompressBytes = Compress.GZip.Decompress(compressedBytes);
            Assert.Equal(originBytes, decompressBytes);
        }

        [Fact]
        public void Deflate() {
            var originBytes = Enumerable.Range(0, 100).Select(x => (Byte)(x % 10)).ToArray();
            var compressedBytes = Compress.Deflate.Compress(originBytes);
            var decompressBytes = Compress.Deflate.Decompress(compressedBytes);
            Assert.Equal(originBytes, decompressBytes);
        }
    }
}
