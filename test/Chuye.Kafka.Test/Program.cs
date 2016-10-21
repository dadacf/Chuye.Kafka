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
            DeserializeFrom();

            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out));
            Task.Run(action: SendRequest);
            //Task.Run(action: SendRequest);
            Console.ReadLine();
        }

        static void DeserializeFrom() {
            var bf = "[00 00 00 19 00 00 00 01 00 00 00 00 00 01 00 09 75 62 75 6E 74 75 2D 31 36 00 00 23 85 ]"
                .Split(new[] { '[', ' ', ']' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(x => Byte.Parse(x, System.Globalization.NumberStyles.HexNumber))
                .ToArray();
            Response.DeserializeFrom(new MemoryStream(bf), ApiKey.GroupCoordinatorRequest);
        }

        static void SendRequest() {
            var option = new Option(new Uri("http://ubuntu-16:9094"), new Uri("http://ubuntu-16:9093"));
            option.GetSharedClient().RequestSending += (_, e) => {
                e.Uri = new Uri(e.Uri.AbsoluteUri.Replace("ubuntu-16", "localhost"));
            };

            var consumer = new Consumer(option, "demoGroupId", "demoTopic1");
            consumer.Initialize();
            for (int i = 0; i < 100; i++) {
                Console.WriteLine("Fetch got {0}", consumer.Fetch().Count());
                Thread.Sleep(5000);
            }

        }
    }
}
