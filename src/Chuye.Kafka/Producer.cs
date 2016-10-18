using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka {
    public interface IProducer {
        void Send(String topic, IList<Message> messages);
    }

    public interface IQueuedProducer {
        IProducerQueue Queuing();
    }

    public interface IProducerQueue {
        void Dispose();
        void Enqueue(String topic, Message message);
        void Flush();
    }

    class Producer : IQueuedProducer, IProducer {
        private readonly ConnectionFactory _connectionFactory;
        private readonly IRouter _route;

        public Producer(ConnectionFactory connectionFactory, IRouter route) {
            _connectionFactory = connectionFactory;
            _route = route;
        }

        public void Send(String topic, IList<Message> messages) {
            var req = new ProduceRequest(topic, 0, messages);
            var broker = _route.Dispatch(topic);
            var con = _connectionFactory.Connect(broker.ToAddress());
            var resp = (ProduceResponse)con.Submit(req);
            resp.ThrowIfFail();
        }

        public IProducerQueue Queuing() {
            return new ProducerQueue(this);
        }
    }

    public class ProducerQueue : IProducerQueue {
        private const Int32 Limit = 20;
        private const Int32 MaxIntervalMilliseconds = 1000;

        private readonly IProducer _producer;
        private readonly Dictionary<String, Queue<Message>> _queues;
        private readonly Object _sync;
        private DateTime _createTime;

        internal ProducerQueue(IProducer producer) {
            _producer = producer;
            _queues = new Dictionary<String, Queue<Message>>();
            _sync = new Object();
            _createTime = DateTime.UtcNow;
        }

        public void Enqueue(String topic, Message message) {
            Queue<Message> queue;
            lock (_sync) {
                if (!_queues.TryGetValue(topic, out queue)) {
                    queue = new Queue<Message>();
                    _queues.Add(topic, queue);
                }
                queue.Enqueue(message);
            }

            if (queue.Count > Limit || DateTime.UtcNow.Subtract(_createTime).TotalMilliseconds > MaxIntervalMilliseconds) {
                Flush();
            }
        }

        public void Flush() {
            lock (_sync) {
                var list = new List<Message>(Limit);
                foreach (var item in _queues) {
                    while (item.Value.Count > 0) {
                        list.Add(item.Value.Dequeue());
                        if (list.Count > Limit) {
                            BatchSend(item.Key, list);
                        }
                    }

                    if (list.Count > 0) {
                        BatchSend(item.Key, list);
                    }
                }
            }
        }

        private void BatchSend(String topic, List<Message> messages) {
            _producer.Send(topic, messages.ToArray());
        }

        public void Dispose() {
            Flush();
        }
    }
}
