using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Test {
    public class Program {
        public static void Main(string[] args) {
            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out));
            Task.Run(action: SendRequest);
            //Task.Run(action: SendRequest);
            Console.ReadLine();
        }

        static void SendRequest() {
            var option = new Option(new Uri("http://ubuntu-16:9094"), new Uri("http://ubuntu-16:9093"));
            option.GetSharedClient().RequestSending += (_, e) => {
                e.Uri = new Uri(e.Uri.AbsoluteUri.Replace("ubuntu-16", "localhost"));
            };
            var coordinator = new Coordinator(option, "demoGroupId");
            coordinator.Topics = new[] { "demoTopic1" };
            //coordinator.ListGroups().Dump();
            //coordinator.DescribeGroups(new[] {"demoTopic1"}).Dump();
            coordinator.RebalanceAsync();
        }
    }
}
