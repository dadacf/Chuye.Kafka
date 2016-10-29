using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    static class EnumerableExtension {
        public static IEnumerable<IList<T>> Chunking<T>(this IEnumerable<T> collection, Int32 size) {
            var list = new List<T>(size);
            foreach (var msg in collection) {
                list.Add(msg);
                if (list.Count == size) {
                    yield return list;
                    list.Clear();
                }
            }
            if (list.Count > 0) {
                yield return list;
            }
        }
    }

    class Enumerable<T> : IEnumerable<T> {
        private readonly T[] _collection;
        private readonly Enumerator _enumerator;

#if NET40
        protected IList<T> List {
#else
        protected IReadOnlyList<T> List {
#endif
            get { return _collection; }

        }

        public Int32 Position {
            get { return _enumerator.Position; }
        }

        public Int32 Count {
            get { return _collection.Length; }
        }

        public Enumerable(T[] collection) {
            if (collection == null) {
                throw new ArgumentNullException("array");
            }
            _collection = collection;
            _enumerator = new Enumerator(_collection);
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
            private readonly IList<T> _collection;
            private Int32 _position;

            public T Current {
                get { return _collection[_position]; }
            }

            object IEnumerator.Current {
                get { return Current; }
            }

            public Int32 Position {
                get { return _position; }
            }

            public Enumerator(IList<T> collection) {
                _collection = collection;
                _position = -1;
            }

            public void Dispose() {
            }

            public bool MoveNext() {
                if (_position < _collection.Count - 1) {
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