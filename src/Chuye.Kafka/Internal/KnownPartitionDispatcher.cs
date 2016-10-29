using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class KnownPartitionDispatcher {
        private Int32[] _paritions;
        private Int32 _sequence;

        public Int32[] Paritions {
            get { return _paritions; }
        }

        public KnownPartitionDispatcher() {
        }        

        public KnownPartitionDispatcher(Int32[] partitions) {
            ChangeKnown(partitions);
        }

        public void ChangeKnown(Int32[] partitions) {
            _paritions = partitions;
            _sequence = 0;
        }

        public Int32 SelectParition() {
            if (_paritions == null || _paritions.Length == 0) {
                throw new InvalidOperationException("Has no partition to select");
            }
            if (_paritions.Length == 1) {
                return _paritions[0];
            }
            return _paritions[_sequence++ % _paritions.Length];
        }
    }
}
