using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class Enumerable<T> : IEnumerable<T> {
        private readonly IList<T> _array;
        private readonly Enumerator _enumerator;

        public Int32 Position {
            get {
                return _enumerator.Position;
            }
        }

        public Int32 Count {
            get {
                return _array.Count;
            }
        }

        public Enumerable(T[] array) {
            _array = array;
            _enumerator = new Enumerator(_array);
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return _enumerator;
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator() {
            return _enumerator;
        }

        public Boolean Next(out T item) {
            if (_enumerator.MoveNext()) {
                item = _enumerator.Current;
                return true;
            }
            item = default(T);
            return false;
        }

        public IEnumerable<T> NextAll() {
            T item;
            while (Next(out item)) {
                yield return item;
            }
        }

        class Enumerator : IEnumerator<T> {
            private readonly IList<T> _array;
            private Int32 _position;

            public T Current {
                get { return _array[_position]; }
            }

            object IEnumerator.Current {
                get { return Current; }
            }

            public Int32 Position {
                get { return _position; }
            }

            public Enumerator(IList<T> array) {
                _array = array;
                _position = -1;
            }

            public void Dispose() {
            }

            public bool MoveNext() {
                if (_position < _array.Count - 1) {
                    _position++;
                    return true;
                }
                return false;
            }

            public void Reset() {
                _position = 0;
            }
        }
    }
}