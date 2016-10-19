using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Test {
    public class Program {
        public static void Main(string[] args) {
            Byte[] bf = "[00 00 00 60 00 00 00 02 00 00 00 01 ][00 0A 64 65 6D 6F 54 6F 70 69 63 32 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 00 00 03 00 00 00 36 00 00 00 00 00 00 00 01 00 00 00 0F 24 2F 44 8E 00 00 00 00 00 00 00 00 00 01 35 00 00 00 00 00 00 00 02 00 00 00 0F BD 26 15 34 00 00 00 00 00 00 00 00 00 01 36 ]"
              .Split(new[] { '[', ' ', ']' }, StringSplitOptions.RemoveEmptyEntries)
              .Select(x => Byte.Parse(x, NumberStyles.HexNumber))
              .ToArray();
            var ms = new MemoryStream(bf);
            var fr = new FetchResponse();
            ms.Position = 0L;
            fr.Deserialize(ms);
        }
    }
}
