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
            StartConsume();
            //DeserializeFrom();

            //Console.ReadLine();
        }

        static void DeserializeFrom() {
            var bf2 = "[00 00 00 0A 00 00 00 06 00 1B 00 00 00 00 ]"
                .Split(new[] { '[', ' ', ']' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(x => Byte.Parse(x, NumberStyles.HexNumber))
                .ToArray();
            var resp = Response.DeserializeFrom(new MemoryStream(bf2), ApiKey.SyncGroupRequest);
        }

        static void SendRequest() {
            var option = new Option(new Uri("http://ubuntu-16:9094"), new Uri("http://ubuntu-16:9093"));
            option.GetSharedClient().RequestSending += (_, e) => {
                e.Uri = new Uri(e.Uri.AbsoluteUri.Replace("ubuntu-16", "localhost"));
            };
            var coordinator = new Coordinator(option, "demoGroupId");
            var resp = coordinator.DescribeGroups(new[] { "demoGroupId" });
        }

        static void StartConsume() {
            var option = new Option(new Uri("http://ubuntu-16:9094"), new Uri("http://ubuntu-16:9093"));
            var consumer = new Consumer(option, "demoGroupId", "demoTopic2");
            consumer.Initialize();

            OffsetMessage msg;
            while (consumer.TryNext(out msg)) {
                //Console.WriteLine("Got msg: {0}", msg.ToString());
            }             
        }
    }
}
