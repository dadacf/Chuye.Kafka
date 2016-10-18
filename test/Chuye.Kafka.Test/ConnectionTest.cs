using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Xunit;

namespace Chuye.Kafka.Test {
    public class ConnectionTest {
        [Fact]
        public void FetchRequestSubmit() {
            var u = new Uri("http://127.0.0.1:9092");
            var c = new Connection(u);
            var req = new FetchRequest("demoTopic", 0, 0, 0L);
            var resp = c.Submit(req);
        }

        [Fact]
        public void FetchResponseDeserialize() {
            var bf = "00-00-00-81-00-00-00-1C-00-00-00-01-00-09-64-65-6D-6F-54-6F-70-69-63-00-00-00-01-00-00-00-00-00-00-00-00-00-00-00-00-00-02-00-00-00-58-00-00-00-00-00-00-00-00-00-00-00-20-0E-1D-F2-6E-00-00-00-00-00-00-00-00-00-12-48-65-6C-6C-6F-20-77-6F-72-6C-64-20-40-31-35-3A-31-32-00-00-00-00-00-00-00-01-00-00-00-20-79-1A-C2-F8-00-00-00-00-00-00-00-00-00-12-48-65-6C-6C-6F-20-77-6F-72-6C-64-20-40-31-35-3A-31-33"
                .Split(' ', '-').Select(x => byte.Parse(x, NumberStyles.HexNumber)).ToArray();
            using (var ms = new MemoryStream(bf)) {
                var resp = new FetchResponse();
                resp.Deserialize(ms);

                Assert.Equal(resp.TopicPartitions.Length, 1);
                Assert.Equal(resp.TopicPartitions[0].MessageBodys.Length, 1);
                Assert.Equal(resp.TopicPartitions[0].MessageBodys[0].HighwaterMarkOffset, 2);
                Assert.Equal(resp.TopicPartitions[0].MessageBodys[0].MessageSet.Items.Length, 2);
            }
        }
    }
}
