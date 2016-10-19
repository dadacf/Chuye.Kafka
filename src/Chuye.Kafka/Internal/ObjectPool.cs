using System;
using System.Collections.Generic;
using System.Threading;

namespace Chuye.Kafka.Internal {
    public abstract class ObjectPool<T> where T : class {
        private readonly Stack<T> _avaliables;
        private readonly HashSet<T> _occupies;
        private readonly Object _sync;

        private Int32 _constructed;
        private Int32 _avaliable;
        private Int32 _occupied;
        private Int32 _detached;

        public Int32 Constructed {
            get { return _constructed; }
        }

        public Int32 Avaliable {
            get { return _avaliable; }
        }

        public Int32 Occupied {
            get { return _occupied; }
        }

        public Int32 Detached {
            get { return _detached; }
        }

        protected Stack<T> Avaliables {
            get { return _avaliables; }
        }

        public ObjectPool() {
            _sync = new Object();
            _avaliables = new Stack<T>();
            _occupies = new HashSet<T>();
        }

        public virtual T AcquireItem() {
            lock (_sync) {
                T item;
                if (_avaliables.Count > 0) {
                    item = _avaliables.Pop();
                    Interlocked.Decrement(ref _avaliable);
                    OnItemPoped(item);
                }
                else {
                    item = Constructing();
                    OnItemConstructed(item);
                    Interlocked.Increment(ref _constructed);
                }
                _occupies.Add(item);
                Interlocked.Increment(ref _occupied);
                OnItemOccupied(item);
                return item;
            }
        }

        public virtual void ReturnItem(T item) {
            lock (_sync) {
                var existed = _occupies.Remove(item);
                if (!existed) {
                    throw new InvalidOperationException("Return unknown item not supported");
                }
                Interlocked.Decrement(ref _occupied);
                _avaliables.Push(item);
                Interlocked.Increment(ref _avaliable);
                OnItemReturned(item);
            }
        }

        public virtual void DetachItem(T item) {
            lock (_sync) {
                var existed = _occupies.Remove(item);
                if (!existed) {
                    throw new InvalidOperationException("Return unknown item not supported");
                }
                Interlocked.Decrement(ref _occupied);
                OnItemDetached(item);
                Interlocked.Increment(ref _detached);
            }
        }

        public virtual void ReleaseAll() {
            lock (_sync) {
                while (_avaliables.Count > 0) {
                    var item = AcquireItem();
                    DetachItem(item);
                }
            }
        }

        protected abstract T Constructing();

        protected virtual void OnItemConstructed(T item) {
        }

        protected virtual void OnItemPoped(T item) {
        }

        protected virtual void OnItemOccupied(T item) {
        }

        protected virtual void OnItemReturned(T item) {
        }

        protected virtual void OnItemDetached(T item) {
        }
    }
}
