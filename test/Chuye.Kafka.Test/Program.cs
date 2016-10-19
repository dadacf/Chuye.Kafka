using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Test {
    public class Program {
        public static void Main(string[] args) {
            var u = new Uri("http://ubuntu-16:9093");
            var o = new Option(u);
            var p = new Producer(o);

            p.Client.RequestSubmitting += (obj, evt) => {
                Console.WriteLine("RequestSubmitting {0}, {1}", evt.Uri.AbsoluteUri, evt.Request.GetType().Name);
            };

            for (int i = 0; i < 10; i++) {
                p.Send("demoTopic1", "p#" + i);
            }
        }
    }
}
