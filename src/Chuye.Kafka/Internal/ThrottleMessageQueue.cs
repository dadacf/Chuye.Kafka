using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class ThrottleMessageQueue {
        private DateTime _createTime;
        private readonly Queue<Message> _queue;
        private readonly Object _sync;
        private readonly TopicPartition _topicPartition;
        private readonly Producer _producer;

        public Int32 ThrottleSize { get; set; }
        public Int32 ThrottleMilliseconds { get; set; }

        public ThrottleMessageQueue(TopicPartition topicPartition, Producer producer) {
            _topicPartition = topicPartition;
            _producer       = producer;
            _queue          = new Queue<Message>();
            _sync           = new Object();
            _createTime     = DateTime.UtcNow;
        }

        public void Enqueue(IEnumerable<String> messages) {
            Enqueue(messages.Select(x => (Message)x));
        }

        public void Enqueue(IEnumerable<Message> messages) {
            lock (_sync) {
                foreach (var message in messages) {
                    _queue.Enqueue(message);
                }
            }
            if (_queue.Count > ThrottleSize || DateTime.UtcNow.Subtract(_createTime).TotalMilliseconds > ThrottleMilliseconds) {
                Flush();
            }
        }

        public void Flush() {
            lock (_sync) {
                var list = new List<Message>(ThrottleSize);
                while (_queue.Count > 0) {
                    list.Add(_queue.Dequeue());
                    if (list.Count >= ThrottleSize) {
                        SendBulk(list);
                        list.Clear();
                    }
                }
                if (list.Count > 0) {
                    SendBulk(list);
                }
            }
        }

        private void SendBulk(IList<Message> messages) {
            _producer.ChunkingSend(_topicPartition.Name, messages);
        }
    }
}
