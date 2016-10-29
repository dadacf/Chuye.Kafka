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
        private readonly Coordinator _coordinator;
        private readonly KnownPartitionDispatcher _partitionDispatcher;
        private MessageChunk _messages;
        private ConsumerOffsetRecorder _offsets;
        private ConsumerConfig _config;

        internal Client Client {
            get { return _client; }
        }

        public String GroupId { get; private set; }
        public String Topic { get; private set; }
        public ConsumerConfig Config {
            get { return _config; }
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
            GroupId                    = groupId;
            Topic                      = topic;
            _partitionDispatcher       = new KnownPartitionDispatcher();
            _coordinator               = new Coordinator(option, groupId);
            _coordinator.StateChanged += Coordinator_StateChanged;
        }

        private void Coordinator_StateChanged(Object sender, CoordinatorStateChangedEventArgs e) {
            if (e.State == CoordinatorState.Stable) {
                if (_offsets != null) {
                    _offsets.MoveForward(_messages.Partition);
                }
                var partitionAssigned = _coordinator.GetPartitionAssigned(Topic);
                _partitionDispatcher.ChangeKnown(partitionAssigned);
                _offsets = new ConsumerOffsetRecorder(this, partitionAssigned);
            }
        }

        public void Initialize() {
            _coordinator.Topics = new[] { Topic };
            _coordinator.RebalanceAsync();
        }

        private void BlockForRebalace(Int32 retryTimtout) {
            var retryInterval = 100;
            var retryCount = retryTimtout / retryInterval;
            var retryUsed = 0;
            while (++retryUsed < retryCount && _coordinator.State != CoordinatorState.Stable) {
                Thread.Sleep(retryInterval);
            }
        }

        private void EnsureMessageFetched() {
            var maxBytes = _config.FetchBytes;
            var maxWaitTime = _config.FetchMilliseconds;
            if (_messages == null) {
                maxWaitTime = _config.FetchMilliseconds / 2;
            }
            else if (_messages.Count == 0 || _messages.Position == _messages.Count - 1) {
                maxWaitTime = _config.FetchMilliseconds * 2;
            }
            else {
                return;
            }

            if (_coordinator.State != CoordinatorState.Stable) {
                Trace.TraceWarning("{0:HH:mm:ss.fff} [{1:d2}] #6 Rebalance {2}, fetch interrupted",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _coordinator.State);
                BlockForRebalace(_config.RebalaceBlockMilliseconds);
            }
            if (_coordinator.State != CoordinatorState.Stable) {
                throw new InvalidOperationException("Load balancing is still not complete");
            }

            var partition = _partitionDispatcher.SelectParition();
            var offset = _offsets.GetCurrentOffset(partition);
            Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] Fetch group '{2}', topic '{3}'({4}), offset {5}",
                DateTime.Now, Thread.CurrentThread.ManagedThreadId, GroupId, Topic, partition, offset);
            var messages = _client.Fetch(Topic, partition, offset, maxBytes: maxBytes, maxWaitTime: maxWaitTime).ToArray();
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
                        _offsets.MoveForward(_messages.Partition);
                        break;
                    }
                    yield return item;
                    _offsets.MoveForward(_messages.Partition, item.Offset);
                }
                if (_messages.Count > 0) {
                    _offsets.MoveForward(_messages.Partition, _messages.EndingOffset, true);
                }
            }
            if (_messages.Count > 0) {
                _offsets.MoveForward(_messages.Partition);
            }
        }
    }
}