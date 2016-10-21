using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    public class CoordinatorStateChangedEventArgs : EventArgs {
        public CoordinatorState State { get; private set; }
        public CoordinatorStateChangedEventArgs(CoordinatorState state) {
            State = state;
        }
    }

    public enum CoordinatorState {
        Unkown, Down, Initialize, Joining, AwaitingSync, Stable
    }
}
