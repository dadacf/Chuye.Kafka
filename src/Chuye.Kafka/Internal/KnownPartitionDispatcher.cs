using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class KnownPartitionDispatcher {
        private Int32[] _paritions;
        private Int32 _sequence;

        public KnownPartitionDispatcher() {
        }

        public KnownPartitionDispatcher(Int32[] partitions) {
            ChangeKnown(partitions);
        }

        public void ChangeKnown(Int32[] partitions) {
            if (partitions == null || partitions.Length == 0) {
                throw new ArgumentOutOfRangeException("partitions");
            }
            _paritions = partitions;
            _sequence = 0;
        }

        public Int32 SelectParition() {
            if (_paritions.Length == 0) {
                return _paritions[0];
            }

            return _paritions[_sequence++ % _paritions.Length];
        }
    }
}
