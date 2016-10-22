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

            var consumer = new Consumer(option, "demoGroupId", "demoTopic1");
            consumer.Initialize();
            for (int i = 0; i < 100; i++) {
                Console.WriteLine("Fetch got {0}", consumer.Fetch().Count());
                Thread.Sleep(5000);
            }

        }
    }
}
