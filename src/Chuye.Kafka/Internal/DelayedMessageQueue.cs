﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class DelayedMessageQueue {
        private const Int32 Limit = 20;
        private const Int32 MaxIntervalMilliseconds = 1000;
        private DateTime _createTime;
        private readonly Queue<Message> _queue;
        private readonly Object _sync;
        private readonly TopicPartition _topicPartition;
        private readonly Producer _producer;

        public DelayedMessageQueue(TopicPartition topicPartition, Producer producer) {
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
            if (_queue.Count > Limit || DateTime.UtcNow.Subtract(_createTime).TotalMilliseconds > MaxIntervalMilliseconds) {
                Flush();
            }
        }

        public void Flush() {
            lock (_sync) {
                var list = new List<Message>(Limit);
                while (_queue.Count > 0) {
                    list.Add(_queue.Dequeue());
                    if (list.Count >= Limit) {
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
            _producer.Send(_topicPartition.Name, _topicPartition.Partition, messages);
        }
    }
}
