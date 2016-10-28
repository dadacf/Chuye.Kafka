using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;

namespace Chuye.Kafka {
    public class Consumer {
        private readonly Client _client;
        private readonly String _groupId;
        private readonly String _topic;
        private readonly Coordinator _coordinator;
        private readonly KnownPartitionDispatcher _partitionDispatcher;
        private MessageChunk _messages;
        private ConsumerOffsetRecorder _offsets;
        private ConsumerConfig _config;

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
            _client                    = option.GetSharedClient();
            _config                    = option.ConsumerConfig;
            _groupId                   = groupId;
            _topic                     = topic;
            _coordinator               = new Coordinator(option, groupId);
            _coordinator.StateChanged += Coordinator_StateChanged;
            _partitionDispatcher       = new KnownPartitionDispatcher();
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
                _offsets = new ConsumerOffsetRecorder(this, partitionAssigned);
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
                if (_messages.Count == 0) {
                    needFetch = true;
                }
                else if (_messages.Position == _messages.Count - 1) {
                    needFetch = true;
                    executeCommit = true;
                }
                if (executeCommit) {
                    _offsets.Proceed(_messages.Partition, _messages.Offset, executeCommit);
                }
            }

            if (!needFetch) {
                return;
            }
            if (_coordinator.State != CoordinatorState.Stable) {
                Trace.TraceWarning("{0:HH:mm:ss.fff} [{1:d2}] #6 Rebalance {2}, fetch interrupted",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _coordinator.State);
                return;
            }

            var partition = _partitionDispatcher.SelectParition();
            var offset = _offsets.GetSavedOffset(partition);
            Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] Fetch group '{2}', topic '{3}'({4}), offset {5}",
                DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId, _topic, partition, offset);
            var messages = _client.Fetch(_topic, partition, offset).ToArray();
            _messages = new MessageChunk(messages, partition);
        }

        public IEnumerable<Message> Fetch() {
            return Fetch(CancellationToken.None);
        }

        public IEnumerable<Message> Fetch(CancellationToken token) {
            while (!token.IsCancellationRequested) {
                EnsureMessageFetched();
                foreach (var item in _messages.NextAll()) {
                    if (token.IsCancellationRequested) {
                        break;
                    }
                    yield return item;
                    _offsets.Proceed(_messages.Partition, item.Offset);
                }
            }
        }

        class MessageChunk : Enumerable<OffsetMessage> {
            public Int32 Partition { get; private set; }
            public Int64 Offset {
                get {
                    if (base.List == null || base.List.Count == 0) {
                        throw new ArgumentOutOfRangeException();
                    }
                    return base.List[0].Offset + base.Position;
                }
            }

            public MessageChunk(OffsetMessage[] array, Int32 partition)
                : base(array) {
                if (partition < 0) {
                    throw new ArgumentOutOfRangeException("partition");
                }
                Partition = partition;
            }
        }
    }
}