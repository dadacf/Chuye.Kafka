using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    internal class CoordinatorAgent {
        private readonly Option _option;
        private readonly ReaderWriterLockSlim _sync;
        private readonly Dictionary<String, Coordinator> _coordinators;

        public CoordinatorAgent(Option option) {
            _option = option;
            _sync = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            _coordinators = new Dictionary<String, Coordinator>();
        }

        public Coordinator SelectSpecified(String groupId) {
            _sync.EnterUpgradeableReadLock();
            try {
                Coordinator coordinator;
                if (_coordinators.TryGetValue(groupId, out coordinator)) {
                    return coordinator;
                }
                _sync.EnterWriteLock();
                try {
                    coordinator = new Coordinator(_option, groupId);
                    _coordinators.Add(groupId, coordinator);
                    return coordinator;
                }
                finally {
                    _sync.ExitWriteLock();
                }
            }
            finally {
                _sync.ExitUpgradeableReadLock();
            }
        }
    }
}
