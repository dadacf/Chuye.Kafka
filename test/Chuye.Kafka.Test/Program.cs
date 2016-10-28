using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Test {
    public class Program {
        public static void Main(string[] args) {
            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out));
            //SendRequest();

            var cts = new CancellationTokenSource();
            Task.Run(() => StartConsume(cts.Token));

            Console.WriteLine("Press <ENTER> to exit");
            Console.ReadLine();

            cts.Cancel();

            Console.ReadLine();
        }

        static void DeserializeFrom() {
            var str = new[]{
                "[00 00 00 2A 00 00 00 07 00 00 00 01 ]",
                "[00 0A 64 65 6D 6F 54 6F 70 69 63 32 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 00 01 FE 00 00 00 00 ]"
            };
            var bf = str.SelectMany(x => x.Split(new[] { '[', ' ', ']' }, StringSplitOptions.RemoveEmptyEntries))
                .Select(x => Byte.Parse(x, NumberStyles.HexNumber)).ToArray();
            var resp = Response.DeserializeFrom(new MemoryStream(bf), ApiKey.FetchRequest);
        }

        static void SendRequest() {
            var option = new Option(new Uri("http://ubuntu-16:9094"), new Uri("http://ubuntu-16:9093"));
            option.GetSharedClient().RequestSending += (_, e) => {
                e.Uri = new Uri(e.Uri.AbsoluteUri.Replace("ubuntu-16", "localhost"));
            };
            var coordinator = new Coordinator(option, "demoGroupId");
            var resp = coordinator.DescribeGroups(new[] { "demoGroupId" });
        }

        static void StartConsume(CancellationToken token) {
            var option = new Option(new Uri("http://ubuntu-16:9094"), new Uri("http://ubuntu-16:9093"));
            option.GetSharedClient().RequestSending += (_, e) => {
                e.Uri = new Uri(e.Uri.AbsoluteUri.Replace("ubuntu-16", "localhost"));
            };

            var consumer = new Consumer(option, "demoGroupId", "demoTopic3");
            consumer.Initialize();

            foreach (var msg in consumer.Fetch(token)) {
                Console.WriteLine(msg);
                //Thread.Sleep(10);
            };
        }
    }
}
