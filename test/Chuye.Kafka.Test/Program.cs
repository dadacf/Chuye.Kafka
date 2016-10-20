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
            var option = new Option(new Uri("http://ubuntu-16:9094"), new Uri("http://ubuntu-16:9093"));
            var client = new Client(option);
            client.RequestSubmitting += (_, e) => {
                e.Uri = new Uri(e.Uri.AbsoluteUri.Replace("ubuntu-16", "localhost"));
                //e.Request.Dump();
            };
            var coordinator = new Coordinator(client, "demoGroupId");
            //coordinator.ListGroups().Dump();
            //coordinator.DescribeGroups(new String[0]).Dump();
            coordinator.Initialize("demoTopic1");
        }
    }
}
