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

        public String GroupId {
            get { return _groupId; }
        }

        public Consumer(Option option, String groupId, String topic) {
            _client = option.GetSharedClient();
            _groupId = groupId;
            _topic = topic;
            _coordinator = option.GetSharedCoordinatorAgent().SelectSpecified(groupId);
            _coordinator.StateChanged += Coordinator_StateChanged;
            _partitionDispatcher = new KnownPartitionDispatcher();
        }

        public void Initialize() {
            var hash = new HashSet<String>(_coordinator.Topics ?? Enumerable.Empty<String>());
            if (!hash.Contains(_topic)) {
                hash.Add(_topic);
                _coordinator.Topics = hash.ToArray();
                _coordinator.RebalanceAsync();
                WaitForRebalace();
            }
        }

        private void Coordinator_StateChanged(object sender, CoordinatorStateChangedEventArgs e) {
            if (e.State == CoordinatorState.Stable) {
                var partitions = _coordinator.GetPartitionAssigned(_topic);
                if (partitions == null || partitions.Length == 0) {
                    //todo:
                }
                _partitionDispatcher.ChangeKnown(partitions);
            }
        }

        private void WaitForRebalace() {
            var wait = 0;
            while (++wait < 100 && _coordinator.State != CoordinatorState.Stable) {
                Thread.Sleep(100);
            }

            if (_coordinator.State != CoordinatorState.Stable) {
                Console.WriteLine("Rebalance fail");
            }
        }

        public IEnumerable<Message> Fetch() {
            var partition = _partitionDispatcher.SelectParition();
            Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] Fetch topic '{2}'({3})",
                DateTime.Now, Thread.CurrentThread.ManagedThreadId, _topic, partition);
            var earliestOffset = _client.Offset(_topic, partition, OffsetOption.Earliest);
            //todo: Offset manage
            return _client.Fetch(_topic, partition, earliestOffset);
        }
    }
}
