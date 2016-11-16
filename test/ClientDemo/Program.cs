using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka;

namespace ClientDemo {
    class Program {
        static void Main(String[] args) {
            Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));

            //StartClient(); return;

            var cts = new CancellationTokenSource();
            Task.Run(() => StartConsumer(cts.Token));
            Task.Run(() => StartProducer(cts.Token));

            Console.ReadLine();
            cts.Cancel();
        }

        private static void StartClient() {
            var option = new Option("192.168.0.220:9092");
            var client = option.GetSharedClient();
            var resp = client.Metadata("demoTopic");
        }

        private static void StartProducer(CancellationToken token) {
            var option = new Option("ubuntu-16:9092");
            option.GetSharedClient().RequestSending += Program_RequestSending;
            var producer = new Producer(option);
            while (!token.IsCancellationRequested) {
                var msg = Enumerable.Range(0, Math.Abs(Guid.NewGuid().GetHashCode() % 99) + 1)
                    .Select(x => Guid.NewGuid().ToString("n"));
                producer.Send("demoTopic", msg.ToArray());
            }
        }

        private static void StartConsumer(CancellationToken token) {
            var option = new Option("ubuntu-16:9092");
            option.GetSharedClient().RequestSending += Program_RequestSending;
            using (var consumer = new Consumer(option, "passport", "demoTopic")) {
                consumer.Initialize();
                foreach (var msg in consumer.Fetch(token)) {
                    Console.WriteLine(msg);
                }
            }
        }

        private static void Program_RequestSending(object sender, Chuye.Kafka.Internal.RequestSendingEventArgs e) {
            e.Uri = new Uri("http://127.0.0.1:9092");
        }
    }
}
