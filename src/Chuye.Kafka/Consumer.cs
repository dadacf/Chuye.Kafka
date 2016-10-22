using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;
using Chuye.Kafka.Protocol;
using System.Threading;
using System.Diagnostics;

namespace Chuye.Kafka {
    public class Consumer {
        private readonly Client _client;
        private readonly String _groupId;
        private readonly String _topic;
        private readonly Coordinator _coordinator;
        private readonly KnownPartitionDispatcher _partitionDispatcher;
        private Int32[] _partitionAssigned;

        public String GroupId {
            get { return _groupId; }
        }

        public Int32[] PartitionAssigned {
            get { return _partitionAssigned; }
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
            _groupId                   = groupId;
            _topic                     = topic;
            //_coordinator               = option.GetSharedCoordinator(groupId);
            _coordinator = new Coordinator(option, groupId);
            _coordinator.StateChanged += Coordinator_StateChanged;
            _partitionDispatcher       = new KnownPartitionDispatcher();
        }

        public void Initialize() {
            //var hash = new HashSet<String>(_coordinator.Topics ?? Enumerable.Empty<String>());
            //if (!hash.Contains(_topic)) {
            //    hash.Add(_topic);
            _coordinator.Topics = new[] { _topic };
                _coordinator.RebalanceAsync();
                WaitForRebalace(20, 100);
            //}
        }

        private void Coordinator_StateChanged(Object sender, CoordinatorStateChangedEventArgs e) {
            if (e.State == CoordinatorState.Stable) {
                _partitionAssigned = _coordinator.GetPartitionAssigned(_topic);
                _partitionDispatcher.ChangeKnown(_partitionAssigned);
            }
        }

        private void WaitForRebalace(Int32 retryCount, Int32 retryTimtout) {
            var retryUsed = 0;
            while (++retryUsed < retryCount && _coordinator.State != CoordinatorState.Stable) {
                Thread.Sleep(retryTimtout);
            }
        }

        public IEnumerable<Message> Fetch() {
            if (_coordinator.State != CoordinatorState.Stable) {
                Trace.TraceWarning("{0:HH:mm:ss.fff} [{1:d2}] #6 Rebalance at {2}, fetch interrupted",
                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, _coordinator.State);
                return Enumerable.Empty<Message>();
            }
            var partition = _partitionDispatcher.SelectParition();
            Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] Fetch topic '{2}'({3})",
                DateTime.Now, Thread.CurrentThread.ManagedThreadId, _topic, partition);
            var earliestOffset = _client.Offset(_topic, partition, OffsetOption.Earliest);
            //todo: Offset manage
            return _client.Fetch(_topic, partition, earliestOffset);
        }
    }
}
