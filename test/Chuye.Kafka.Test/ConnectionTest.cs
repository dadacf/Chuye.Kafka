using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Xunit;

namespace Chuye.Kafka.Test {
    public class ConnectionTest {
        [Fact]
        public void Run() {
            var u = new Uri("http://127.0.0.1:9092");
            var c = new Connection(u);
            var req = new FetchRequest("demoTopic", 0, 0, 0L);
            var resp = c.Submit(req);
        }
    }
}
