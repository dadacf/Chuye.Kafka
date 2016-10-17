using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Chuye.Kafka {

    public class Message {
        public String Key { get; private set; }
        public String Value { get; private set; }

        public Message(String value)
            : this(null, value) {
        }

        public Message(String key, String value) {
            if (String.IsNullOrWhiteSpace(value)) {
                throw new ArgumentOutOfRangeException("value");
            }
        }

        public static implicit operator Message(String content) {
            return new Message(null, content);
        }
    }

    public class Topic {
        public String Name { get; set; }
        public Int32 Partition { get; set; }

        public Topic(String name)
            : this(name, 0) {
        }

        public Topic(String name, Int32 partition) {
            Name = name;
            Partition = partition;
        }

        public static implicit operator Topic(String topicName) {
            return new Topic(topicName);
        }
    }

    public interface IProducer {
        void Send(String topic, params Message[] message);
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
        public void Send(String topic, params Message[] message) {
            throw new NotImplementedException();
        }

        public IProducerQueue Queuing() {
            return new ProducerQueue(this);
        }
    }

    public class ProducerQueue : IProducerQueue {
        private const Int32 limit = 100;
        private readonly IProducer _producer;
        private readonly Dictionary<String, Queue<Message>> _queues;
        private readonly Object _sync;
        internal ProducerQueue(IProducer producer) {
            _producer = producer;
            _queues = new Dictionary<String, Queue<Message>>();
            _sync = new Object();
        }

        public void Enqueue(String topic, Message message) {
            lock (_sync) {
                Queue<Message> queue;
                if (!_queues.TryGetValue(topic, out queue)) {
                    queue = new Queue<Message>();
                    _queues.Add(topic, queue);
                }
                queue.Enqueue(message);
            }
        }

        public void Flush() {
            lock (_sync) {
                var list = new List<Message>(limit);
                foreach (var item in _queues) {
                    while (item.Value.Count > 0) {
                        list.Add(item.Value.Dequeue());
                        if (list.Count > limit) {
                            SendMessages(item.Key, list);
                        }
                    }

                    if (list.Count > 0) {
                        SendMessages(item.Key, list);
                    }
                }
            }
        }

        private void SendMessages(String topic, List<Message> list) {
            _producer.Send(topic, list.ToArray());
        }

        public void Dispose() {
            Flush();
        }
    }

    public interface IConsumer {
        String GroupId { get; }
        IEnumerable<Message> Fetch(String topic);
    }

    public class Consumer : IConsumer {
        private readonly String _groupId;
        public String GroupId {
            get { return _groupId; }
        }

        public IEnumerable<Message> Fetch(string topic) {
            throw new NotImplementedException();
        }
    }
}
