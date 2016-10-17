using System;
using System.Collections.Generic;
using System.Threading;

namespace Chuye.Kafka {
    public struct PoolState {
        public int Constructed;
        public int Avaliable;
        public int Occupied;
        public int Detached;
    }

    public class ItemCreatedEventArgs : EventArgs {
        private readonly Object _item;

        public Object Item {
            get { return _item; }
        }

        public ItemCreatedEventArgs(Object item) {
            _item = item;
        }
    }

    public class ObjectPool<T> : IDisposable {
        private readonly Stack<T> _avaliables;
        private readonly HashSet<T> _occupies;
        private readonly Func<T> _factory;
        private readonly Action<T> _detacher;
        private readonly Object _sync;

        private int _constructed;
        private int _avaliable;
        private int _occupied;
        private int _detached;

        public event EventHandler<ItemCreatedEventArgs> ItemCreated;

        public PoolState State {
            get {
                return new PoolState {
                    Constructed = _constructed,
                    Avaliable = _avaliable,
                    Occupied = _occupied,
                    Detached = _detached,
                };
            }
        }

        public ObjectPool(Func<T> factory)
            : this(factory, null) {
        }

        public ObjectPool(Func<T> factory, Action<T> detacher) {
            if (factory == null) {
                throw new ArgumentNullException("factory");
            }
            _detacher = detacher;
            _factory = factory;
            _sync = new Object();
            _avaliables = new Stack<T>();
            _occupies = new HashSet<T>();
        }

        public T AcquireItem() {
            lock (_sync) {
                T item;
                if (_avaliables.Count > 0) {
                    item = _avaliables.Pop();
                    Interlocked.Decrement(ref _avaliable);
                }
                else {
                    item = _factory();
                    OnItemCreated(item);
                    Interlocked.Increment(ref _constructed);
                }
                _occupies.Add(item);
                Interlocked.Increment(ref _occupied);
                return item;
            }
        }

        protected virtual void OnItemCreated(T item) {
            if (ItemCreated != null) {
                ItemCreated(this, new ItemCreatedEventArgs(item));
            }
        }

        public void ReturnItem(T item) {
            lock (_sync) {
                var existed = _occupies.Remove(item);
                if (!existed) {
                    throw new InvalidOperationException("Return unknown item not supported");
                }
                Interlocked.Decrement(ref _occupied);
                _avaliables.Push(item);
                Interlocked.Increment(ref _avaliable);
            }
        }

        public void DetachItem(T item) {
            lock (_sync) {
                var existed = _occupies.Remove(item);
                if (!existed) {
                    throw new InvalidOperationException("Return unknown item not supported");
                }
                Interlocked.Decrement(ref _occupied);

                if (_detacher == null) {
                    _detacher(item);
                }
                Interlocked.Increment(ref _detached);
            }
        }

        public void Dispose() {
            lock (_sync) {
                while (_avaliables.Count > 0) {
                    var item = _avaliables.Pop();
                    Interlocked.Decrement(ref _avaliable);
                    _occupies.Add(item);
                    Interlocked.Increment(ref _occupied);
                    DetachItem(item);
                }
            }
        }
    }
}
