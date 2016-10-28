using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class Enumerable<T> : IEnumerable<T> {
        private readonly T[] _array;
        private readonly Enumerator _enumerator;

        protected IReadOnlyList<T> List {
            get { return _array; }
        }

        public Int32 Position {
            get { return _enumerator.Position; }
        }

        public Int32 Count {
            get { return _array.Length; }
        }

        public Enumerable(T[] array) {
            if (array == null) {
                throw new ArgumentNullException("array");
            }
            _array = array;
            _enumerator = new Enumerator(_array);
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return _enumerator;
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator() {
            return _enumerator;
        }

        public Boolean NextOne(out T item) {
            if (_enumerator.MoveNext()) {
                item = _enumerator.Current;
                return true;
            }
            item = default(T);
            return false;
        }

        public IEnumerable<T> NextAll() {
            T item;
            while (NextOne(out item)) {
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