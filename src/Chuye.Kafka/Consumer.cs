using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka {
    public class Consumer {
        private readonly Client _client;
        private readonly String _groupId;
        private readonly String _topic;
        private readonly Coordinator _coordinator;
        private readonly KnownPartitionDispatcher _partitionDispatcher;
        private MessageChunk _messages;
        private OffsetRecorder _offsets;

        internal Client Client {
            get { return _client; }
        }

        public String GroupId {
            get { return _groupId; }
        }

        public String Topic {
            get { return _topic; }
        }

        public CoordinatorState CoordinatorState {
            get {
                if (_coordinator == null) {
                    throw new InvalidOperationException("Run initialize first");
                }
                return _coordinator.State;
            }
        }

        public Consumer(Option option, String groupId, String topic) {
            _client = option.GetSharedClient();
            _groupId = groupId;
            _topic = topic;
            _coordinator = new Coordinator(option, groupId);
            _coordinator.StateChanged += Coordinator_StateChanged;
            _partitionDispatcher = new KnownPartitionDispatcher();
        }

        public void Initialize() {
            _coordinator.Topics = new[] { _topic };
            _coordinator.RebalanceAsync();
            WaitForRebalace(20, 100);
        }

        private void Coordinator_StateChanged(Object sender, CoordinatorStateChangedEventArgs e) {
            if (e.State == CoordinatorState.Stable) {
                if (_offsets != null) {
                    _offsets.Proceed(_messages.Partition, _messages.Offset);
                }

                var partitionAssigned = _coordinator.GetPartitionAssigned(_topic);
                _partitionDispatcher.ChangeKnown(partitionAssigned);
                _offsets = new OffsetRecorder(this, partitionAssigned);
            }
        }

        private void WaitForRebalace(Int32 retryCount, Int32 retryTimtout) {
            var retryUsed = 0;
            while (++retryUsed < retryCount && _coordinator.State != CoordinatorState.Stable) {
                Thread.Sleep(retryTimtout);
            }
        }

        private void EnsureMessageFetched() {
            var needFetch = false;
            if (_messages == null) {
                needFetch = true;
            }
            else {
                var executeCommit = false;
                if (_messages.Offset == _messages.Count - 1) {
                    needFetch = true;
                    executeCommit = true;
                }
                _offsets.Proceed(_messages.Partition, _messages.Offset, executeCommit);
            }
            
            if (!needFetch) {
                return;
            }

            if (_coordinator.State != CoordinatorState.Stable) {
                Trace.TraceWarning("{0:HH:mm:ss.fff} [{1:d2}] #6 Rebalance at {2}, fetch interrupted",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _coordinator.State);
                return;
            }

            var partition = _partitionDispatcher.SelectParition();
            var offset = _offsets.GetSavedOffset(partition);
            Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] Fetch at groupId '{2}', topic '{3}'({4}), offset {5}",
                DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId, _topic, partition, offset);
            var messages = _client.Fetch(_topic, partition, offset).ToArray();
            _messages = new MessageChunk(messages, partition);
        }

        public OffsetMessage Next() {
            EnsureMessageFetched();
            OffsetMessage message;
            _messages.Next(out message);
            return message;
        }

        public Boolean TryNext(out OffsetMessage message) {
            EnsureMessageFetched();
            return _messages.Next(out message);
        }

        public IEnumerable<Message> Fetch() {
            EnsureMessageFetched();
            return _messages;
        }

        class MessageChunk : Enumerable<OffsetMessage> {
            private Int32 _partition;
            private Int64 _initialOffset;

            public Int32 Partition {
                get { return _partition; }
            }

            public Int64 Offset {
                get {
                    return _initialOffset + base.Position;
                }
            }

            public MessageChunk(OffsetMessage[] array, Int32 partition)
                : base(array) {
                _partition = partition;
                _initialOffset = array[0].Offset;
            }
        }

        class OffsetRecorder {
            private readonly Consumer _consumer;
            private readonly Int32[] _partitions;
            private readonly Int64[] _offsetSaved;
            private readonly Int64[] _offsetSubmited;
            private DateTime _lastSubmit;

            public OffsetRecorder(Consumer consumer, Int32[] partitions) {
                _consumer = consumer;
                _partitions = partitions;
                _offsetSaved = Enumerable.Repeat(-1L, partitions.Length).ToArray();
                _offsetSubmited = new Int64[partitions.Length];
                _lastSubmit = DateTime.UtcNow;
            }

            public void Proceed(Int32 partition, Int64 offset) {
                Proceed(partition, offset, false);
            }

            public void Proceed(Int32 partition, Int64 offset, Boolean ignoreDiff) {
                var index = Array.IndexOf(_partitions, partition);
                if (_offsetSaved[index] > offset) {
                    throw new ArgumentOutOfRangeException("offset");
                }                
                if(_offsetSaved[index] > offset) {
                    return;
                }

                _offsetSaved[index] = offset;                
                if (ignoreDiff || _offsetSubmited[index] + 9 < offset) {
                    Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] OffsetCommit at group '{2}', topic '{3}'({4}), saved {5}, submitted {6}",
                        DateTime.Now, Thread.CurrentThread.ManagedThreadId, _consumer.GroupId, _consumer.Topic, partition, _offsetSaved[index], _offsetSubmited[index]);

                    OffsetCommit(partition, offset);
                    _offsetSubmited[index] = offset;
                }
            }

            private void OffsetCommit(int partition, long offset) {
                Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] OffsetCommit at group '{2}', topic '{3}'({4}), offset {5}",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _consumer.GroupId, _consumer.Topic, partition, offset);
                _consumer.Client.OffsetCommit(_consumer.Topic, partition, _consumer.GroupId, offset + 1);
            }

            public Int64 GetSavedOffset(Int32 partition) {
                var index = Array.IndexOf(_partitions, partition);
                var offset = _offsetSaved[index];
                if (offset == -1L) {
                    offset = _consumer.Client.OffsetFetch(_consumer.Topic, partition, _consumer.GroupId);
                }
                if (offset == -1L) {
                    offset = _consumer.Client.Offset(_consumer.Topic, partition, OffsetOption.Earliest);
                    _offsetSaved[index] = offset;
                }
                return offset;
            }
        }
    }
}
